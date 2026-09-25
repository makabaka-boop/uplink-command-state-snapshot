package store_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"deepspace/internal/store"
)

// assertConsistentSnapshot 校验单次 GetCommand 返回的视图内部自洽：
// 状态、租约历史、结算记录必须来自同一可解释的快照——
// 不允许“状态是旧代次、租约列表却含新代次”（跨代租约），
// 也不允许“pending 却已有结算”或“终态却无结算”（待处理且已结算及其反面）。
func assertConsistentSnapshot(t *testing.T, d *store.CommandDetail) {
	t.Helper()
	// 租约历史与当前代次必须同源：每代恰一条租约，代次从 1 连续递增，
	// 租约条数恰等于 commands.lease_generation。
	if int64(len(d.Leases)) != d.LeaseGeneration {
		t.Errorf("cross-generation view: lease_generation=%d but %d lease rows",
			d.LeaseGeneration, len(d.Leases))
	}
	for i, l := range d.Leases {
		if l.Generation != int64(i+1) {
			t.Errorf("lease history not contiguous: leases[%d].generation=%d", i, l.Generation)
		}
	}
	switch d.Status {
	case store.StatusPending:
		if d.Settlement != nil {
			t.Errorf("pending command carries a settlement: %+v", d.Settlement)
		}
	case store.StatusDelivered, store.StatusFailed:
		if d.Settlement == nil {
			t.Errorf("terminal %s without settlement", d.Status)
		} else {
			// 结算后指令不可再领取，结算代次必然等于当前代次。
			if d.Settlement.Generation != d.LeaseGeneration {
				t.Errorf("settlement generation %d != lease_generation %d",
					d.Settlement.Generation, d.LeaseGeneration)
			}
			if d.Settlement.Result != d.Status {
				t.Errorf("settlement result %s != status %s", d.Settlement.Result, d.Status)
			}
		}
	case store.StatusBlocked:
		if d.Settlement != nil || len(d.Leases) != 0 {
			t.Errorf("blocked command carries settlement=%+v leases=%d",
				d.Settlement, len(d.Leases))
		}
	default:
		t.Errorf("unknown status %q", d.Status)
	}
}

// TestGetCommandSnapshotConsistencyUnderChurn 可控的领取交错：
// 一个写协程反复“领取（1ms 租期）→到期→重领”，推过 60 代后以长租约结算，
// 多个读协程同时高频查询。任何一次查询都必须返回内部自洽的快照视图，
// 不允许跨代租约，也不允许“待处理且已结算”。
func TestGetCommandSnapshotConsistencyUnderChurn(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	c, err := st.CreateCommand(ctx, []byte(`{"churn":true}`), nil)
	if err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var readers sync.WaitGroup
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				d, err := st.GetCommand(ctx, c.ID)
				if err != nil {
					t.Errorf("get during churn: %v", err)
					return
				}
				assertConsistentSnapshot(t, d)
			}
		}()
	}

	stopReaders := func() {
		close(stop)
		readers.Wait()
	}

	// 写侧：60 代短租约更替（数据库时钟域：1ms 租期 + 2ms 等待必然到期）。
	const generations = 60
	for i := 0; i < generations; i++ {
		if _, err := st.Claim(ctx, time.Millisecond); err != nil {
			stopReaders()
			t.Fatalf("churn claim %d: %v", i, err)
		}
		time.Sleep(2 * time.Millisecond)
	}
	// 以长租约领取当前代并结算，作为写侧交错的终点。
	final, err := st.Claim(ctx, 5*time.Second)
	if err != nil {
		stopReaders()
		t.Fatalf("final churn claim: %v", err)
	}
	if err := st.Ack(ctx, c.ID, final.LeaseToken, store.StatusDelivered); err != nil {
		stopReaders()
		t.Fatalf("final churn ack: %v", err)
	}

	stopReaders()

	d, err := st.GetCommand(ctx, c.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertConsistentSnapshot(t, d)
	if d.Status != store.StatusDelivered || d.LeaseGeneration != generations+1 {
		t.Fatalf("final state: status=%s gen=%d want delivered/%d",
			d.Status, d.LeaseGeneration, generations+1)
	}
}

// TestLeaseVerdictIndependentOfInstanceClock 实例时钟偏移：
// 两台“实例”的本地时钟分别落后/超前 2 小时，租约裁决必须只依赖数据库时钟——
// 尚在有效期的凭证在两台实例上都被接受；已过期的凭证在两台实例上都被拒绝。
func TestLeaseVerdictIndependentOfInstanceClock(t *testing.T) {
	pool, _ := openTestDB(t)
	ctx := context.Background()

	lagging := store.NewWithClock(pool, func() time.Time { return time.Now().Add(-2 * time.Hour) })
	leading := store.NewWithClock(pool, func() time.Time { return time.Now().Add(2 * time.Hour) })

	// 尚在有效期的凭证：时钟超前的实例也必须接受（各用一条指令，避免重复结算）。
	for i, st := range []*store.Store{lagging, leading} {
		c, err := st.CreateCommand(ctx, []byte(fmt.Sprintf(`{"case":"valid","i":%d}`, i)), nil)
		if err != nil {
			t.Fatal(err)
		}
		cl, err := st.Claim(ctx, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if err := st.Ack(ctx, c.ID, cl.LeaseToken, store.StatusDelivered); err != nil {
			t.Fatalf("instance %d with skewed clock rejected a valid lease: %v", i, err)
		}
	}

	// 已过期的凭证：同一张令牌在两台实例上都必须被判 lease stale。
	c, err := lagging.CreateCommand(ctx, []byte(`{"case":"expired"}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	cl, err := lagging.Claim(ctx, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(160 * time.Millisecond)
	for i, st := range []*store.Store{lagging, leading} {
		if err := st.Ack(ctx, c.ID, cl.LeaseToken, store.StatusDelivered); !errors.Is(err, store.ErrLeaseStale) {
			t.Fatalf("instance %d with skewed clock accepted an expired lease: %v", i, err)
		}
	}
	// 两次拒绝都不得改变状态：仍 pending、无结算。
	d, err := lagging.GetCommand(ctx, c.ID)
	if err != nil {
		t.Fatal(err)
	}
	if d.Status != store.StatusPending || d.Settlement != nil {
		t.Fatalf("expired-token rejections changed state: status=%s settlement=%+v",
			d.Status, d.Settlement)
	}
}

// TestFailurePropagationSnapshotConsistency 失败回执与并发查询交错：
// 根被置 failed 期间，任意一次查询都不得让下游呈现“虚假的可执行状态”
// （pending 却带结算、blocked 却带租约等矛盾组合）；传播结束后下游状态与
// 结算记录必须相符——根 failed 且恰一条 failed 结算，后继 blocked、无租约、
// 无结算，且 blocked_by 指向失败根。
func TestFailurePropagationSnapshotConsistency(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	const rounds = 30
	for round := 0; round < rounds; round++ {
		root, err := st.CreateCommand(ctx, []byte(fmt.Sprintf(`{"round":%d}`, round)), nil)
		if err != nil {
			t.Fatal(err)
		}
		child, err := st.CreateCommand(ctx, []byte(`{}`), pid(root.ID))
		if err != nil {
			t.Fatal(err)
		}
		grand, err := st.CreateCommand(ctx, []byte(`{}`), pid(child.ID))
		if err != nil {
			t.Fatal(err)
		}
		cl, err := st.Claim(ctx, 5*time.Second)
		if err != nil || cl.CommandID != root.ID {
			t.Fatalf("round %d claim root: claim=%+v err=%v", round, cl, err)
		}

		// 读侧：失败回执提交前后高频查询根与整条后继链。
		stop := make(chan struct{})
		var readers sync.WaitGroup
		for _, id := range []int64{root.ID, child.ID, grand.ID} {
			id := id
			readers.Add(1)
			go func() {
				defer readers.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					d, err := st.GetCommand(ctx, id)
					if err != nil {
						t.Errorf("get %d during propagation: %v", id, err)
						return
					}
					assertConsistentSnapshot(t, d)
				}
			}()
		}

		if err := st.Ack(ctx, root.ID, cl.LeaseToken, store.StatusFailed); err != nil {
			close(stop)
			readers.Wait()
			t.Fatalf("round %d fail root: %v", round, err)
		}
		close(stop)
		readers.Wait()

		// 传播后的唯一允许状态：根 failed + 一条 failed 结算（代次相符）；
		// 后继 blocked、blocked_by=根、无租约、无结算。
		assertStatus(t, st, root.ID, store.StatusFailed, nil)
		assertStatus(t, st, child.ID, store.StatusBlocked, pid(root.ID))
		assertStatus(t, st, grand.ID, store.StatusBlocked, pid(root.ID))
		rd, _ := st.GetCommand(ctx, root.ID)
		if rd.Settlement == nil || rd.Settlement.Result != store.StatusFailed ||
			rd.Settlement.Generation != rd.LeaseGeneration {
			t.Fatalf("round %d: root settlement=%+v inconsistent with gen=%d",
				round, rd.Settlement, rd.LeaseGeneration)
		}
		for _, id := range []int64{child.ID, grand.ID} {
			d, _ := st.GetCommand(ctx, id)
			if len(d.Leases) != 0 || d.Settlement != nil {
				t.Fatalf("round %d: blocked node %d has leases=%d settlement=%+v",
					round, id, len(d.Leases), d.Settlement)
			}
		}
	}

	// 带外全库核对：blocked 行无租约无结算且指向 failed 根；
	// 不存在“祖先非 delivered、后代却仍 pending（可领取）”的破窗。
	assertChainInvariants(t, pool)
}
