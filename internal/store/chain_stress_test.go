package store_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"deepspace/internal/store"
)

// TestMixedChainWorkloadInvariants 混合高并发负载下的全局不变量：
// 并发创建指令（含前驱）、领取、送达/失败。结束后逐条核对：
//   - 任何处于 blocked 的指令都没有租约、没有结算，且 blocked_by 指向一个 failed 根；
//   - 不存在“祖先 failed/blocked、后代却仍 pending（可领取）”的破窗；
//   - 任何 pending 且带前驱的指令，其所有祖先都已 delivered。
func TestMixedChainWorkloadInvariants(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	// 预置一批独立根，供并发 worker 挂链与结算。
	var roots []int64
	for i := 0; i < 8; i++ {
		c, err := st.CreateCommand(ctx, []byte(`{}`), nil)
		if err != nil {
			t.Fatal(err)
		}
		roots = append(roots, c.ID)
	}

	const workers = 16
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		w := w
		go func() {
			defer wg.Done()
			root := roots[w%len(roots)]
			// 挂一个后继；若根此刻已失败，出生 blocked 也是合法结果（不视为错误）。
			c1, err := st.CreateCommand(ctx, []byte(`{}`), &root)
			if err != nil {
				t.Errorf("create c1 under root %d: %v", root, err)
				return
			}
			// 再挂一层，制造多级传播。
			if c1.Status == store.StatusPending {
				if _, err := st.CreateCommand(ctx, []byte(`{}`), &c1.ID); err != nil {
					t.Errorf("create c2 under %d: %v", c1.ID, err)
				}
			}
			for i := 0; i < 25; i++ {
				cl, err := st.Claim(ctx, 200*time.Millisecond)
				if err != nil {
					if !errorIsNoRows(err) {
						t.Errorf("claim: %v", err)
					}
					continue
				}
				result := store.StatusDelivered
				if (w+i)%2 == 0 {
					result = store.StatusFailed
				}
				if err := st.Ack(ctx, cl.CommandID, cl.LeaseToken, result); err != nil {
					// 租约过期等合法竞态可返回哨兵错误，但不应出现存储故障。
					if !errors.Is(err, store.ErrLeaseStale) &&
						!errors.Is(err, store.ErrAlreadySettled) &&
						!errors.Is(err, store.ErrInvalidLeaseToken) {
						t.Errorf("unexpected ack error on %d: %v", cl.CommandID, err)
					}
				}
			}
		}()
	}
	wg.Wait()

	// 排空残留：仍在有效租约内、但已被某 worker 领取的指令全部送达，
	// 使最终状态只剩终态或“确因前驱未送达而等待”的纯 pending，便于核对窗口不变量。
	for {
		cl, err := st.Claim(ctx, 5*time.Second)
		if err != nil {
			break
		}
		if err := st.Ack(ctx, cl.CommandID, cl.LeaseToken, store.StatusDelivered); err != nil {
			t.Fatalf("drain ack %d: %v", cl.CommandID, err)
		}
	}

	assertChainInvariants(t, pool)
}

// assertChainInvariants 直连数据库做带外全局核对。
func assertChainInvariants(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()

	// 1) blocked 指令：无租约、无结算、blocked_by 指向 failed 根。
	rows, err := pool.Query(ctx, `
		SELECT c.id, c.blocked_by,
		       (SELECT count(*) FROM leases l WHERE l.command_id = c.id) AS leases,
		       (SELECT count(*) FROM settlements s WHERE s.command_id = c.id) AS sets,
		       r.status
		FROM commands c
		LEFT JOIN commands r ON r.id = c.blocked_by
		WHERE c.status = 'blocked'`)
	if err != nil {
		t.Fatal(err)
	}
	type blocked struct {
		id, blockedBy, leases, sets int64
		rootStatus                  *string
	}
	var blockedRows []blocked
	for rows.Next() {
		var b blocked
		if err := rows.Scan(&b.id, &b.blockedBy, &b.leases, &b.sets, &b.rootStatus); err != nil {
			t.Fatal(err)
		}
		blockedRows = append(blockedRows, b)
	}
	rows.Close()
	for _, b := range blockedRows {
		if b.leases != 0 {
			t.Errorf("blocked %d has %d lease rows", b.id, b.leases)
		}
		if b.sets != 0 {
			t.Errorf("blocked %d has %d settlement rows", b.id, b.sets)
		}
		if b.blockedBy == 0 || b.rootStatus == nil || *b.rootStatus != store.StatusFailed {
			t.Errorf("blocked %d blocked_by=%d rootStatus=%v, want a failed root",
				b.id, b.blockedBy, b.rootStatus)
		}
	}

	// 2) 破窗检测：沿闭包，不存在“后代 pending、祖先非 delivered”的边。
	var bad int
	err = pool.QueryRow(ctx, `
		SELECT count(*)
		FROM commands child
		JOIN command_closure k ON k.command_id = child.id
		JOIN commands anc ON anc.id = k.ancestor_id
		WHERE child.status = 'pending' AND anc.status <> 'delivered'`).Scan(&bad)
	if err != nil {
		t.Fatal(err)
	}
	if bad != 0 {
		t.Errorf("found %d pending commands with a non-delivered ancestor (claimability window)", bad)
	}

	// 3) blocked/pending 互斥的来源一致性由迁移 CHECK 保证；再兜底统计一次。
	var inconsistent int
	err = pool.QueryRow(ctx, `
		SELECT count(*) FROM commands
		WHERE (status = 'blocked') IS DISTINCT FROM (blocked_by IS NOT NULL)`).Scan(&inconsistent)
	if err != nil {
		t.Fatal(err)
	}
	if inconsistent != 0 {
		t.Errorf("blocked/blocked_by consistency violated on %d rows", inconsistent)
	}
}
