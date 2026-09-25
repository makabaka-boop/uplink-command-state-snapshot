package store_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"deepspace/internal/store"
)

// detailInconsistency 校验单次详情读取是否对应同一可解释快照：
//   - 租约历史恰为第 1..lease_generation 代——不允许“旧领取代数配新代租约”
//     的跨代混合；
//   - pending 绝不携带结算记录——不允许“待处理且已结算”的矛盾组合；
//   - delivered/failed 必有结算，且结果与状态一致、结算代次不越界；
//   - blocked 绝不携带租约或结算——失败传播出的阻断不伴随虚假的可执行状态。
func detailInconsistency(d *store.CommandDetail) error {
	if int64(len(d.Leases)) != d.LeaseGeneration {
		return fmt.Errorf("lease_generation=%d but %d lease rows (cross-generation view)",
			d.LeaseGeneration, len(d.Leases))
	}
	for i, l := range d.Leases {
		if l.Generation != int64(i+1) {
			return fmt.Errorf("lease generations not contiguous from 1: leases[%d].generation=%d",
				i, l.Generation)
		}
	}
	switch d.Status {
	case store.StatusPending:
		if d.Settlement != nil {
			return fmt.Errorf("pending command carries settlement %+v", d.Settlement)
		}
		if d.BlockedBy != nil {
			return fmt.Errorf("pending command carries blocked_by=%d", *d.BlockedBy)
		}
	case store.StatusDelivered, store.StatusFailed:
		if d.Settlement == nil {
			return fmt.Errorf("terminal status %q without settlement", d.Status)
		}
		if d.Settlement.Result != d.Status {
			return fmt.Errorf("settlement result %q contradicts status %q",
				d.Settlement.Result, d.Status)
		}
		if d.Settlement.Generation < 1 || d.Settlement.Generation > d.LeaseGeneration {
			return fmt.Errorf("settlement generation %d outside 1..%d",
				d.Settlement.Generation, d.LeaseGeneration)
		}
	case store.StatusBlocked:
		if d.BlockedBy == nil {
			return errors.New("blocked command without blocked_by")
		}
		if len(d.Leases) != 0 || d.Settlement != nil || d.LeaseGeneration != 0 {
			return fmt.Errorf("blocked command carries phantom executable/settled state: "+
				"leases=%d settlement=%+v lease_generation=%d",
				len(d.Leases), d.Settlement, d.LeaseGeneration)
		}
	default:
		return fmt.Errorf("unknown status %q", d.Status)
	}
	return nil
}

func assertDetailConsistent(t *testing.T, d *store.CommandDetail) {
	t.Helper()
	if err := detailInconsistency(d); err != nil {
		t.Errorf("inconsistent command detail: %v", err)
	}
}

// commitBetweenTracer 精确制造“查询恰好跨过并发提交”的交错：
// 在包含 trigger 的 SQL 语句结束后立即执行 fire，从而把另一终端的领取/回执
// 提交夹进详情读取的两条语句之间。逐语句各自取快照的旧实现在该交错下必然
// 拼出矛盾页面；单快照读取则始终自洽。
type commitBetweenTracer struct {
	trigger string
	once    sync.Once
	fire    func()
}

// tracerHitKey 标记当前查询是否命中 trigger（TraceQueryEndData 不携带 SQL，
// 需在 Start 阶段匹配并经 context 传给 End）。
type tracerHitKey struct{}

func (t *commitBetweenTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, t.trigger) {
		return context.WithValue(ctx, tracerHitKey{}, true)
	}
	return ctx
}

func (t *commitBetweenTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	if hit, _ := ctx.Value(tracerHitKey{}).(bool); hit {
		t.once.Do(t.fire)
	}
}

// newConsoleStore 返回带交错探针的存储实例，模拟控制台所在的另一服务实例。
func newConsoleStore(t *testing.T, dbURL string, tracer pgx.QueryTracer) *store.Store {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return store.New(pool)
}

// TestReadAcrossConcurrentClaimCommit 控制台读取恰好跨过另一终端的领取提交：
// 结果必须是“领取前”或“领取后”的完整快照，绝不出现跨代混合
// （旧 lease_generation 配新一代的租约行）。
func TestReadAcrossConcurrentClaimCommit(t *testing.T) {
	pool, dbURL := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	c, err := st.CreateCommand(ctx, []byte(`{"seq":1}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := st.Claim(ctx, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if first.Generation != 1 {
		t.Fatalf("gen=%d want 1", first.Generation)
	}

	// 另一终端的领取事务已推进到第 2 代，但暂不提交。
	claimTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer claimTx.Rollback(ctx)
	newToken := strings.Repeat("cd", 32)
	if _, err := claimTx.Exec(ctx, `
		UPDATE commands
		SET lease_generation = lease_generation + 1,
		    lease_token = $2, lease_expires_at = now() + interval '5 seconds',
		    updated_at = now()
		WHERE id = $1`, c.ID, newToken); err != nil {
		t.Fatal(err)
	}
	if _, err := claimTx.Exec(ctx, `
		INSERT INTO leases (command_id, generation, lease_token, expires_at)
		VALUES ($1, 2, $2, now() + interval '5 seconds')`, c.ID, newToken); err != nil {
		t.Fatal(err)
	}

	// 控制台实例：把领取提交精确夹在详情读取的两条语句之间。
	var commitErr error
	console := newConsoleStore(t, dbURL, &commitBetweenTracer{
		trigger: "FROM commands WHERE id",
		fire:    func() { commitErr = claimTx.Commit(ctx) },
	})

	d, err := console.GetCommand(ctx, c.ID)
	if err != nil {
		t.Fatal(err)
	}
	if commitErr != nil {
		t.Fatalf("concurrent claim commit: %v", commitErr)
	}
	assertDetailConsistent(t, d)

	// 提交确已落库：随后读取必然观察到第 2 代的完整快照。
	d, err = console.GetCommand(ctx, c.ID)
	if err != nil {
		t.Fatal(err)
	}
	if d.LeaseGeneration != 2 || len(d.Leases) != 2 {
		t.Fatalf("after commit want gen=2 with 2 leases, got gen=%d leases=%d",
			d.LeaseGeneration, len(d.Leases))
	}
	assertDetailConsistent(t, d)
}

// TestReadAcrossConcurrentAckCommit 控制台读取恰好跨过成功回执的提交：
// 绝不出现“status=pending 且 settlement 已生成”的矛盾页面。
func TestReadAcrossConcurrentAckCommit(t *testing.T) {
	pool, dbURL := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	c, err := st.CreateCommand(ctx, []byte(`{"seq":2}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	cl, err := st.Claim(ctx, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// 另一终端的回执事务：写入结算并翻转状态，但暂不提交。
	ackTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer ackTx.Rollback(ctx)
	if _, err := ackTx.Exec(ctx, `
		INSERT INTO settlements (command_id, generation, lease_token, result)
		VALUES ($1, $2, $3, 'delivered')`, c.ID, cl.Generation, cl.LeaseToken); err != nil {
		t.Fatal(err)
	}
	if _, err := ackTx.Exec(ctx, `
		UPDATE commands SET status = 'delivered', updated_at = now() WHERE id = $1`, c.ID); err != nil {
		t.Fatal(err)
	}

	var commitErr error
	console := newConsoleStore(t, dbURL, &commitBetweenTracer{
		trigger: "FROM commands WHERE id",
		fire:    func() { commitErr = ackTx.Commit(ctx) },
	})

	d, err := console.GetCommand(ctx, c.ID)
	if err != nil {
		t.Fatal(err)
	}
	if commitErr != nil {
		t.Fatalf("concurrent ack commit: %v", commitErr)
	}
	assertDetailConsistent(t, d)

	// 提交确已落库：随后读取必然观察到 delivered + 结算记录。
	d, err = console.GetCommand(ctx, c.ID)
	if err != nil {
		t.Fatal(err)
	}
	if d.Status != store.StatusDelivered || d.Settlement == nil ||
		d.Settlement.Result != store.StatusDelivered {
		t.Fatalf("after commit want delivered+settlement, got status=%s settlement=%+v",
			d.Status, d.Settlement)
	}
	assertDetailConsistent(t, d)
}

// TestReadAcrossConcurrentFailurePropagation 控制台读取恰好跨过失败回执
// （含后继链阻断传播）的提交：下游要么整体呈现传播前的 pending（无租约、
// 无结算），要么整体呈现传播后的 blocked（记录阻断来源、无租约、无结算），
// 绝不伴随虚假的可执行状态；且失败后下游状态与结算记录相符。
func TestReadAcrossConcurrentFailurePropagation(t *testing.T) {
	pool, dbURL := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	ids := createChain(t, st, 3)
	cl := claimMust(t, st, ids[0])

	// 另一终端的失败回执事务：结算根 + 阻断传播，但暂不提交。
	failTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer failTx.Rollback(ctx)
	if _, err := failTx.Exec(ctx, `
		INSERT INTO settlements (command_id, generation, lease_token, result)
		VALUES ($1, $2, $3, 'failed')`, ids[0], cl.Generation, cl.LeaseToken); err != nil {
		t.Fatal(err)
	}
	if _, err := failTx.Exec(ctx, `
		UPDATE commands SET status = 'failed', updated_at = now() WHERE id = $1`, ids[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := failTx.Exec(ctx, `
		WITH RECURSIVE d(x) AS (
			SELECT id FROM commands WHERE predecessor_id = $1
			UNION ALL SELECT c.id FROM commands c JOIN d ON c.predecessor_id = d.x)
		UPDATE commands SET status = 'blocked', blocked_by = $1, updated_at = now()
		WHERE id IN (SELECT x FROM d) AND status = 'pending'`, ids[0]); err != nil {
		t.Fatal(err)
	}

	var commitErr error
	console := newConsoleStore(t, dbURL, &commitBetweenTracer{
		trigger: "FROM commands WHERE id",
		fire:    func() { commitErr = failTx.Commit(ctx) },
	})

	// 先读根（第一次读取恰好跨过传播提交），再读下游：每一页都必须自洽。
	for _, id := range []int64{ids[0], ids[1], ids[2]} {
		d, err := console.GetCommand(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		assertDetailConsistent(t, d)
	}
	if commitErr != nil {
		t.Fatalf("concurrent failure commit: %v", commitErr)
	}

	// 提交后的终态契约：根 failed 且结算记录为 failed；
	// 下游 blocked、阻断来源指向根，且无租约、无结算、不可领取。
	root, err := st.GetCommand(ctx, ids[0])
	if err != nil {
		t.Fatal(err)
	}
	if root.Status != store.StatusFailed || root.Settlement == nil ||
		root.Settlement.Result != store.StatusFailed {
		t.Fatalf("root: status=%s settlement=%+v, want failed with failed settlement",
			root.Status, root.Settlement)
	}
	for _, id := range ids[1:] {
		d, err := st.GetCommand(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if d.Status != store.StatusBlocked || d.BlockedBy == nil || *d.BlockedBy != ids[0] {
			t.Fatalf("descendant %d: status=%s blocked_by=%v, want blocked by %d",
				id, d.Status, d.BlockedBy, ids[0])
		}
		assertDetailConsistent(t, d)
	}
	claimMust(t, st, -1)
}

// TestLeaseExpiryBoundaryConsistentAcrossInstances 两个服务实例（独立连接池）
// 对同一张领取凭证的过期边界得出相同结论：有效期内任一实例都接受，过期后
// 任一实例都拒绝；且回执侧与领取侧共用同一边界——判定过期后任一实例都可
// 立即重领（代次推进），新凭证在另一实例同样可用。
//
// 过期裁决以数据库时钟为唯一权威，实例时钟偏差不参与裁决，
// 因此结论天然在各实例间一致。
func TestLeaseExpiryBoundaryConsistentAcrossInstances(t *testing.T) {
	poolA, dbURL := openTestDB(t)
	stA := store.New(poolA)
	ctx := context.Background()

	poolB, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer poolB.Close()
	stB := store.New(poolB)

	// 有效期内：实例 A 签发的凭证，实例 B 接受。
	c1, err := stA.CreateCommand(ctx, []byte(`{"n":1}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	cl1, err := stA.Claim(ctx, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if cl1.CommandID != c1.ID {
		t.Fatalf("claim got %d want %d", cl1.CommandID, c1.ID)
	}
	if err := stB.Ack(ctx, c1.ID, cl1.LeaseToken, store.StatusDelivered); err != nil {
		t.Fatalf("valid token rejected by peer instance: %v", err)
	}

	// 过期后：两个实例对同样的过期凭证一致拒绝。
	c2, err := stA.CreateCommand(ctx, []byte(`{"n":2}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	cl2, err := stA.Claim(ctx, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if cl2.CommandID != c2.ID {
		t.Fatalf("claim got %d want %d", cl2.CommandID, c2.ID)
	}
	c3, err := stB.CreateCommand(ctx, []byte(`{"n":3}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	cl3, err := stB.Claim(ctx, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if cl3.CommandID != c3.ID {
		t.Fatalf("claim got %d want %d", cl3.CommandID, c3.ID)
	}

	time.Sleep(250 * time.Millisecond)

	if err := stB.Ack(ctx, c2.ID, cl2.LeaseToken, store.StatusDelivered); !errors.Is(err, store.ErrLeaseStale) {
		t.Fatalf("expired token on instance B: want ErrLeaseStale, got %v", err)
	}
	if err := stA.Ack(ctx, c3.ID, cl3.LeaseToken, store.StatusDelivered); !errors.Is(err, store.ErrLeaseStale) {
		t.Fatalf("expired token on instance A: want ErrLeaseStale, got %v", err)
	}

	// 回执侧判过期 ⟺ 领取侧立即可重领：两侧共用同一边界。
	re2, err := stB.Claim(ctx, 2*time.Second)
	if err != nil {
		t.Fatalf("reclaim c2 via B: %v", err)
	}
	if re2.CommandID != c2.ID || re2.Generation != cl2.Generation+1 {
		t.Fatalf("reclaim c2: got id=%d gen=%d, want id=%d gen=%d",
			re2.CommandID, re2.Generation, c2.ID, cl2.Generation+1)
	}
	re3, err := stA.Claim(ctx, 2*time.Second)
	if err != nil {
		t.Fatalf("reclaim c3 via A: %v", err)
	}
	if re3.CommandID != c3.ID || re3.Generation != cl3.Generation+1 {
		t.Fatalf("reclaim c3: got id=%d gen=%d, want id=%d gen=%d",
			re3.CommandID, re3.Generation, c3.ID, cl3.Generation+1)
	}

	// 新一代凭证在另一实例同样被接受。
	if err := stA.Ack(ctx, c2.ID, re2.LeaseToken, store.StatusDelivered); err != nil {
		t.Fatalf("reclaimed token rejected by peer instance: %v", err)
	}
	if err := stB.Ack(ctx, c3.ID, re3.LeaseToken, store.StatusDelivered); err != nil {
		t.Fatalf("reclaimed token rejected by peer instance: %v", err)
	}
}

// TestConsistentReadsUnderClaimAckChurn 可控的领取/回执交错压力下，
// 控制台高频读取的每一页都必须自洽：不跨代、不“待处理且已结算”、
// 阻断不伴随虚假可执行状态。
func TestConsistentReadsUnderClaimAckChurn(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	// 一条链（失败会传播阻断）打底，后续由终端侧持续补充独立指令。
	chain := createChain(t, st, 3)

	var mu sync.Mutex
	ids := append([]int64{}, chain...)

	var stop atomic.Bool
	var wg sync.WaitGroup

	// 终端侧：持续领取并回执——送达、失败、失联（租约到期后被重领、代次推进）
	// 三种走向混合，制造代次推进、状态翻转与阻断传播。
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; !stop.Load(); i++ {
				cl, err := st.Claim(ctx, 150*time.Millisecond)
				if errors.Is(err, store.ErrNoAvailableCommand) {
					// 队列暂时排空：补充新指令维持压力（读取窗口有界）。
					c, cerr := st.CreateCommand(ctx, []byte(`{"churn":true}`), nil)
					if cerr == nil {
						mu.Lock()
						if len(ids) < 64 {
							ids = append(ids, c.ID)
						}
						mu.Unlock()
					}
					continue
				}
				if err != nil {
					continue
				}
				switch (w + i) % 3 {
				case 0:
					_ = st.Ack(ctx, cl.CommandID, cl.LeaseToken, store.StatusDelivered)
				case 1:
					_ = st.Ack(ctx, cl.CommandID, cl.LeaseToken, store.StatusFailed)
				default:
					// 失联：不回执，等待租约到期后被其他终端重领。
				}
			}
		}(w)
	}

	// 控制台侧：高频轮询已登记的指令，任何一次矛盾都立即报告。
	var inconsistent atomic.Int64
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for n := 0; !stop.Load(); n++ {
				mu.Lock()
				snapshot := append([]int64{}, ids...)
				mu.Unlock()
				if len(snapshot) == 0 {
					continue
				}
				id := snapshot[(n+r)%len(snapshot)]
				d, err := st.GetCommand(ctx, id)
				if err != nil {
					t.Errorf("get %d: %v", id, err)
					continue
				}
				if prob := detailInconsistency(d); prob != nil {
					if inconsistent.Add(1) == 1 {
						t.Errorf("command %d: %v", id, prob)
					}
				}
			}
		}(r)
	}

	time.Sleep(2500 * time.Millisecond)
	stop.Store(true)
	wg.Wait()

	if n := inconsistent.Load(); n != 0 {
		t.Fatalf("observed %d inconsistent console reads under churn", n)
	}

	// 等失联租约到期后排空：把仍可领取的指令全部送达，再做终态核对。
	time.Sleep(300 * time.Millisecond)
	for {
		cl, err := st.Claim(ctx, 5*time.Second)
		if err != nil {
			break
		}
		if err := st.Ack(ctx, cl.CommandID, cl.LeaseToken, store.StatusDelivered); err != nil {
			t.Fatalf("drain ack %d: %v", cl.CommandID, err)
		}
	}
	mu.Lock()
	all := append([]int64{}, ids...)
	mu.Unlock()
	for _, id := range all {
		d, err := st.GetCommand(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		assertDetailConsistent(t, d)
	}
	assertChainInvariants(t, pool)
}
