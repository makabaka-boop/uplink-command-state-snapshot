package store_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"deepspace/internal/store"
)

// pid 是返回前驱编号指针的小助手。
func pid(id int64) *int64 { return &id }

// createChain 创建 payload 各异的前驱链 c0 <- c1 <- ...（ci 的前驱为 c_{i-1}）。
func createChain(t *testing.T, st *store.Store, n int) []int64 {
	t.Helper()
	ctx := context.Background()
	ids := make([]int64, n)
	for i := 0; i < n; i++ {
		var pred *int64
		if i > 0 {
			pred = pid(ids[i-1])
		}
		c, err := st.CreateCommand(ctx, []byte(fmt.Sprintf(`{"i":%d}`, i)), pred)
		if err != nil {
			t.Fatalf("create chain node %d: %v", i, err)
		}
		ids[i] = c.ID
	}
	return ids
}

// claimMust 返回当前可领取指令，断言领取成功且编号符合预期（或无可用时传 -1）。
func claimMust(t *testing.T, st *store.Store, want int64) *store.Claim {
	t.Helper()
	cl, err := st.Claim(context.Background(), 5*time.Second)
	if want < 0 {
		if !errors.Is(err, store.ErrNoAvailableCommand) {
			t.Fatalf("want no available command, got %+v err=%v", cl, err)
		}
		return nil
	}
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if cl.CommandID != want {
		t.Fatalf("claim got id=%d want %d", cl.CommandID, want)
	}
	return cl
}

// assertStatus 断言指令状态与阻断来源。
func assertStatus(t *testing.T, st *store.Store, id int64, wantStatus string, wantBlockedBy *int64) {
	t.Helper()
	d, err := st.GetCommand(context.Background(), id)
	if err != nil {
		t.Fatalf("get %d: %v", id, err)
	}
	if d.Status != wantStatus {
		t.Fatalf("command %d status=%s want %s", id, d.Status, wantStatus)
	}
	if (d.BlockedBy == nil) != (wantBlockedBy == nil) ||
		(d.BlockedBy != nil && *d.BlockedBy != *wantBlockedBy) {
		t.Fatalf("command %d blocked_by=%v want %v", id, d.BlockedBy, wantBlockedBy)
	}
}

// TestChainSequentialUnlock 链式解锁：前驱未全部送达时后继不可领取（不预发租约）；
// 根按序送达后，后继依次开放领取，且始终领取编号最小的可领取指令。
func TestChainSequentialUnlock(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	ids := createChain(t, st, 4)

	// 全部 pending 时只有根 c0 可领取；c1..c3 的任何并发领取都只能得到“无可用”。
	cl := claimMust(t, st, ids[0])
	claimMust(t, st, -1)

	// 根在持有租约期间未送达：后继依旧不开放。
	claimMust(t, st, -1)
	if err := st.Ack(ctx, ids[0], cl.LeaseToken, store.StatusDelivered); err != nil {
		t.Fatalf("deliver root: %v", err)
	}

	// c0 delivered 后 c1 才开放；领取 c1 但先不送达，c2 必须继续等待。
	cl1 := claimMust(t, st, ids[1])
	claimMust(t, st, -1)
	if err := st.Ack(ctx, ids[1], cl1.LeaseToken, store.StatusDelivered); err != nil {
		t.Fatalf("deliver c1: %v", err)
	}

	cl2 := claimMust(t, st, ids[2])
	if err := st.Ack(ctx, ids[2], cl2.LeaseToken, store.StatusDelivered); err != nil {
		t.Fatalf("deliver c2: %v", err)
	}
	cl3 := claimMust(t, st, ids[3])

	// 后继在等待期间从未被领取：c1..c3 各只有一代人。
	for i, id := range ids {
		d, err := st.GetCommand(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if len(d.Leases) != 1 {
			t.Fatalf("node %d leases=%d want 1 (no speculative leases)", i, len(d.Leases))
		}
	}
	// 不结算最后一个，保留 pending 也无妨。
	_ = cl3
}

// TestChainFIFOAroundBlocked 多条独立链交错：领取始终取全局编号最小的可领取指令，
// blocked/等待中的指令被跳过。
func TestChainFIFOAroundBlocked(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	// 独立根 a(id=1)，链 b0(id=2)<-b1(id=3)，独立根 c(id=4)。
	a, err := st.CreateCommand(ctx, []byte(`{"a":1}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	b0, err := st.CreateCommand(ctx, []byte(`{"b":0}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	b1, err := st.CreateCommand(ctx, []byte(`{"b":1}`), pid(b0.ID))
	if err != nil {
		t.Fatal(err)
	}
	c, err := st.CreateCommand(ctx, []byte(`{"c":1}`), nil)
	if err != nil {
		t.Fatal(err)
	}

	// 最小可领取是 a。领取 a 但不结算（持有有效租约）-> 下一个最小可领取是 b0。
	clA := claimMust(t, st, a.ID)
	claimMust(t, st, b0.ID) // b0 被领取后持有租约；b1 仍等待；c 尚不是最小。
	claimMust(t, st, c.ID)
	claimMust(t, st, -1) // a/b0 持有租约，b1 等待 -> 无可用。
	_ = clA
	_ = b1.ID
}

// TestFailurePropagatesDownChain 根确认失败：尚未领取的整条后继链原子转为 blocked，
// 记录同一阻断来源，且不产生租约或结算。
func TestFailurePropagatesDownChain(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	ids := createChain(t, st, 4)

	// 领取根并置 failed（c1..c3 从未被领取）。
	cl := claimMust(t, st, ids[0])
	if err := st.Ack(ctx, ids[0], cl.LeaseToken, store.StatusFailed); err != nil {
		t.Fatalf("fail root: %v", err)
	}

	for i := int64(1); i < 4; i++ {
		assertStatus(t, st, ids[i], store.StatusBlocked, pid(ids[0]))
		d, _ := st.GetCommand(ctx, ids[i])
		if len(d.Leases) != 0 {
			t.Fatalf("blocked node %d must have no leases, got %d", i, len(d.Leases))
		}
		if d.Settlement != nil {
			t.Fatalf("blocked node %d must have no settlement, got %+v", i, d.Settlement)
		}
	}
	assertStatus(t, st, ids[0], store.StatusFailed, nil)

	// blocked 链不可领取；队列视为无可用。
	claimMust(t, st, -1)

	// 对 blocked 后继的任何确认（即便伪造令牌）都被拒，状态不变。
	if err := st.Ack(ctx, ids[2], "00", store.StatusDelivered); !errors.Is(err, store.ErrAlreadySettled) {
		t.Fatalf("ack blocked: want ErrAlreadySettled, got %v", err)
	}
	assertStatus(t, st, ids[2], store.StatusBlocked, pid(ids[0]))
}

// TestFailurePropagationRespectsDelivered 已 delivered 的上游指令保持 delivered；
// 当链中段（c2）失败时，仅其未送达后代 c3 转 blocked 且 blocked_by=c2，
// 失败传播因此是“按当前状态”的且幂等。
func TestFailurePropagationRespectsDelivered(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	// c0 <- c1 <- c2 <- c3
	nodes := createChain(t, st, 4)

	deliver := func(id int64) {
		t.Helper()
		cl := claimMust(t, st, id)
		if err := st.Ack(ctx, id, cl.LeaseToken, store.StatusDelivered); err != nil {
			t.Fatalf("deliver %d: %v", id, err)
		}
	}
	deliver(nodes[0])
	deliver(nodes[1])
	// c2 现已开放领取、c3 仍等待。令 c2 failed -> c3 blocked，c0/c1 不受影响。
	cl2 := claimMust(t, st, nodes[2])
	if err := st.Ack(ctx, nodes[2], cl2.LeaseToken, store.StatusFailed); err != nil {
		t.Fatalf("fail c2: %v", err)
	}
	assertStatus(t, st, nodes[0], store.StatusDelivered, nil)
	assertStatus(t, st, nodes[1], store.StatusDelivered, nil)
	assertStatus(t, st, nodes[2], store.StatusFailed, nil)
	assertStatus(t, st, nodes[3], store.StatusBlocked, pid(nodes[2]))

	// 重复失败确认被 already_settled 拒绝，blocked 行不被翻转。
	if err := st.Ack(ctx, nodes[2], cl2.LeaseToken, store.StatusFailed); !errors.Is(err, store.ErrAlreadySettled) {
		t.Fatalf("dup fail: want ErrAlreadySettled, got %v", err)
	}
	assertStatus(t, st, nodes[3], store.StatusBlocked, pid(nodes[2]))
}

// TestCreateAfterFailureIsBornBlocked 前驱已 failed/blocked 后再创建后继：
// 出生即 blocked 并继承阻断来源，绝不进入可领取队列。
func TestCreateAfterFailureIsBornBlocked(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	root, err := st.CreateCommand(ctx, []byte(`{}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	cl := claimMust(t, st, root.ID)
	if err := st.Ack(ctx, root.ID, cl.LeaseToken, store.StatusFailed); err != nil {
		t.Fatal(err)
	}

	// 直接以失败根为前驱 -> blocked，blocked_by=root。
	child, err := st.CreateCommand(ctx, []byte(`{}`), pid(root.ID))
	if err != nil {
		t.Fatal(err)
	}
	assertStatus(t, st, child.ID, store.StatusBlocked, pid(root.ID))

	// 再以 blocked child 为前驱 -> 仍 blocked，且继承同一阻断来源 root。
	grand, err := st.CreateCommand(ctx, []byte(`{}`), pid(child.ID))
	if err != nil {
		t.Fatal(err)
	}
	assertStatus(t, st, grand.ID, store.StatusBlocked, pid(root.ID))

	claimMust(t, st, -1)
}

// TestCreateValidation 前驱引用规则：只能引用已存在的较早编号。
func TestCreateValidation(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	first, err := st.CreateCommand(ctx, []byte(`{}`), nil)
	if err != nil {
		t.Fatal(err)
	}

	// 引用不存在的编号。
	if _, err := st.CreateCommand(ctx, []byte(`{}`), pid(999999)); !errors.Is(err, store.ErrPredecessorNotFound) {
		t.Fatalf("missing predecessor: want ErrPredecessorNotFound, got %v", err)
	}
	// 非正编号。
	if _, err := st.CreateCommand(ctx, []byte(`{}`), pid(0)); !errors.Is(err, store.ErrPredecessorNotFound) {
		t.Fatalf("zero predecessor: want ErrPredecessorNotFound, got %v", err)
	}
	// 引用已存在的较早编号合法。
	if _, err := st.CreateCommand(ctx, []byte(`{}`), pid(first.ID)); err != nil {
		t.Fatalf("valid predecessor: %v", err)
	}
}

// TestLegacyCommandsUnaffected 旧请求（无前驱）在新契约下行为完全不变：
// 可领取、可送达、可失败，predecessor_id/blocked_by 均为空。
func TestLegacyCommandsUnaffected(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	c, err := st.CreateCommand(ctx, []byte(`{"legacy":true}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	d, _ := st.GetCommand(ctx, c.ID)
	if d.PredecessorID != nil || d.BlockedBy != nil {
		t.Fatalf("legacy command must have no predecessor/blocked_by: %+v", d.Command)
	}
	cl := claimMust(t, st, c.ID)
	if err := st.Ack(ctx, c.ID, cl.LeaseToken, store.StatusDelivered); err != nil {
		t.Fatal(err)
	}
	assertStatus(t, st, c.ID, store.StatusDelivered, nil)
}

// TestTwoInstancesInterleaveCreateAndFail 两个“实例”（两个独立连接池）交错：
// 实例 A 在一个事务里持有根行锁并随后落 failed（含后继传播），实例 B 同时以后继
// 身份创建指令，其创建事务会锁整条前驱链（首环即根）而被 A 挡住。
//
// 由数据库行锁裁决提交先后：A 提交失败后 B 才完成插入，B 的后继必须出生即 blocked，
// 绝不留下“根已失败、后继却仍可领取”的状态。这正是“创建与前驱结算交错”。
func TestTwoInstancesInterleaveCreateAndFail(t *testing.T) {
	pool, dbURL := openTestDB(t)
	stA := store.New(pool)
	ctx := context.Background()

	poolB, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer poolB.Close()
	stB := store.New(poolB)

	const rounds = 25
	for round := 0; round < rounds; round++ {
		root, err := stA.CreateCommand(ctx, []byte(fmt.Sprintf(`{"r":%d}`, round)), nil)
		if err != nil {
			t.Fatal(err)
		}
		cl := claimMust(t, stA, root.ID)

		// A：显式开事务并先取根行锁（等价失败确认事务的第一步），暂不提交。
		ax, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ax.Exec(ctx, `SELECT id FROM commands WHERE id=$1 FOR UPDATE`, root.ID); err != nil {
			t.Fatal(err)
		}

		// B：并发创建后继；必然在根行锁上等待。
		created := make(chan struct{})
		var child *store.Command
		var createErr error
		go func() {
			c, cerr := stB.CreateCommand(context.Background(), []byte(`{}`), pid(root.ID))
			child, createErr = c, cerr
			close(created)
		}()

		// 等到 B 确实在等待根行锁后，A 才在同一事务内完成失败结算与传播。
		waitForLockWait(t, pool, "command_closure", 0)

		if _, err := ax.Exec(ctx,
			`INSERT INTO settlements (command_id, generation, lease_token, result)
			 VALUES ($1,$2,$3,'failed')`, root.ID, cl.Generation, cl.LeaseToken); err != nil {
			t.Fatalf("round %d settle: %v", round, err)
		}
		if _, err := ax.Exec(ctx,
			`UPDATE commands SET status='failed', updated_at=now() WHERE id=$1`, root.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := ax.Exec(ctx, `
			WITH RECURSIVE d(x) AS (
				SELECT id FROM commands WHERE predecessor_id=$1
				UNION ALL SELECT c.id FROM commands c JOIN d ON c.predecessor_id=d.x)
			UPDATE commands SET status='blocked', blocked_by=$1, updated_at=now()
			WHERE id IN (SELECT x FROM d) AND status='pending'`, root.ID); err != nil {
			t.Fatal(err)
		}
		if err := ax.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		<-created
		if createErr != nil {
			t.Fatalf("round %d create: %v", round, createErr)
		}
		if child == nil {
			t.Fatalf("round %d child missing", round)
		}

		// 裁决后的唯一允许结果：根 failed，后继 blocked（阻断来源=根）。
		assertStatus(t, stA, root.ID, store.StatusFailed, nil)
		assertStatus(t, stA, child.ID, store.StatusBlocked, pid(root.ID))

		got, err := stA.Claim(ctx, time.Second)
		if err == nil && got.CommandID == child.ID {
			t.Fatalf("round %d: failed root's child %d was claimable", round, child.ID)
		}
		d, _ := stA.GetCommand(ctx, child.ID)
		if len(d.Leases) != 0 || d.Settlement != nil {
			t.Fatalf("round %d: blocked child has leases=%d settlement=%v",
				round, len(d.Leases), d.Settlement)
		}
	}
}

// waitForLockWait 轮询 pg_locks，直到出现等待 command_closure（插入闭包）或
// commands 行的未授权锁，表明交错中的另一方已被本事务挡在锁上。
func waitForLockWait(t *testing.T, admin *pgxpool.Pool, _ string, _ int64) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		err := admin.QueryRow(ctx, `
			SELECT count(*) FROM pg_locks
			WHERE NOT granted AND locktype IN ('tuple','transactionid')`).Scan(&waiting)
		if err == nil && waiting > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("interleaving transaction never reached the lock-wait state")
}

// TestTwoInstancesDeliveredUnlockInterleave 对称交错：实例 A 送达根，
// 实例 B 在送达前后分别创建后继——送达前已存在的后继在送达后自动解锁，
// 且解锁只开放编号最小者。
func TestTwoInstancesDeliveredUnlockInterleave(t *testing.T) {
	pool, dbURL := openTestDB(t)
	stA := store.New(pool)
	ctx := context.Background()

	poolB, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer poolB.Close()
	stB := store.New(poolB)

	root, err := stA.CreateCommand(ctx, []byte(`{}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	// 送达前就创建好两个后继：c1 <- c2。
	c1, err := stB.CreateCommand(ctx, []byte(`{}`), pid(root.ID))
	if err != nil {
		t.Fatal(err)
	}
	c2, err := stB.CreateCommand(ctx, []byte(`{}`), pid(c1.ID))
	if err != nil {
		t.Fatal(err)
	}

	// 根本身无前驱，是当前唯一可领取的最小者；后继链必须等待，不预发租约。
	cl := claimMust(t, stA, root.ID)
	claimMust(t, stA, -1) // 根被持有、c1..c2 等待。

	// 并发：A 送达根，B 同时再挂一个后继 c3(前驱 c1)。
	done := make(chan struct{})
	go func() {
		_ = stA.Ack(context.Background(), root.ID, cl.LeaseToken, store.StatusDelivered)
		close(done)
	}()
	c3, cerr := stB.CreateCommand(context.Background(), []byte(`{}`), pid(c1.ID))
	<-done
	if cerr != nil {
		t.Fatalf("concurrent create c3: %v", cerr)
	}

	// 根 delivered：c1 解锁成为最小可领取；c2、c3 仍等待。
	claimMust(t, stA, c1.ID)
	claimMust(t, stA, -1)
	for _, id := range []int64{c2.ID, c3.ID} {
		d, _ := stA.GetCommand(ctx, id)
		if d.Status != store.StatusPending || len(d.Leases) != 0 {
			t.Fatalf("waiting node %d status=%s leases=%d", id, d.Status, len(d.Leases))
		}
	}
}

// TestConcurrentClaimsAcrossChain 链式指令在高并发领取下也满足：
// 每条指令至多被一个领取者拿走，等待中的后继绝不被预发租约。
func TestConcurrentClaimsAcrossChain(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	// 一条链 + 若干独立根混合。
	chain := createChain(t, st, 5)
	var roots []int64
	for i := 0; i < 5; i++ {
		c, err := st.CreateCommand(ctx, []byte(`{}`), nil)
		if err != nil {
			t.Fatal(err)
		}
		roots = append(roots, c.ID)
	}

	claimAllExactlyOnce := func() {
		t.Helper()
		// 可领取集合 = 链根 + 5 个独立根，共 6 条；其余链节点等待。
		const eligible = 6
		const claimants = 64
		start := make(chan struct{})
		var mu sync.Mutex
		wins := map[int64]int{}
		empty := 0
		var wg sync.WaitGroup
		wg.Add(claimants)
		for i := 0; i < claimants; i++ {
			go func() {
				defer wg.Done()
				<-start
				cl, err := st.Claim(ctx, 5*time.Second)
				mu.Lock()
				defer mu.Unlock()
				switch {
				case err == nil:
					wins[cl.CommandID]++
				case errors.Is(err, store.ErrNoAvailableCommand):
					empty++
				default:
					t.Errorf("claim err: %v", err)
				}
			}()
		}
		close(start)
		wg.Wait()
		if len(wins) != eligible {
			t.Fatalf("distinct claims=%d want %d (%v)", len(wins), eligible, wins)
		}
		for id, n := range wins {
			if n != 1 {
				t.Fatalf("command %d claimed %d times", id, n)
			}
		}
		if empty != claimants-eligible {
			t.Fatalf("empty=%d want %d", empty, claimants-eligible)
		}
		// 等待中的链节点 c1..c4 零租约。
		for i := 1; i < len(chain); i++ {
			d, _ := st.GetCommand(ctx, chain[i])
			if len(d.Leases) != 0 {
				t.Fatalf("waiting chain node %d got %d speculative leases", chain[i], len(d.Leases))
			}
		}
	}
	claimAllExactlyOnce()
}
