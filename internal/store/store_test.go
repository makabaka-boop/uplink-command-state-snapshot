package store_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"deepspace/internal/store"
	"deepspace/internal/testdb"
)

// openTestDB 创建隔离的临时数据库并执行迁移，返回连接池与连接串。
func openTestDB(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	pool := testdb.New(t)
	if err := store.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			`TRUNCATE settlements, leases, commands RESTART IDENTITY CASCADE`)
	})
	return pool, pool.Config().ConnString()
}

// TestConcurrentClaimIsExclusive 是核心不变量：
// 任意并发度下，同一条指令只能被一个领取者拿走，其余全部得到 ErrNoAvailableCommand。
func TestConcurrentClaimIsExclusive(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	if _, err := st.CreateCommand(ctx, []byte(`{"seq":1}`), nil); err != nil {
		t.Fatalf("create: %v", err)
	}

	const n = 32
	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins, noRows := 0, 0
	var otherErr error
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-start
			c, err := st.Claim(ctx, 2*time.Second)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				_ = c
				wins++
			case errors.Is(err, store.ErrNoAvailableCommand):
				noRows++
			default:
				otherErr = err
			}
		}()
	}
	close(start)
	wg.Wait()

	if otherErr != nil {
		t.Fatalf("unexpected claim error: %v", otherErr)
	}
	if wins != 1 {
		t.Fatalf("expected exactly 1 winner, got %d (204s=%d)", wins, noRows)
	}
	if noRows != n-1 {
		t.Fatalf("expected %d no-command results, got %d", n-1, noRows)
	}
}

// TestFIFOOrderAndMultipleCommands 验证最早可用优先（FIFO）且多条可依次领取。
func TestFIFOOrderAndMultipleCommands(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if _, err := st.CreateCommand(ctx, []byte(`{"i":1}`), nil); err != nil {
			t.Fatal(err)
		}
	}
	for want := int64(1); want <= 3; want++ {
		c, err := st.Claim(ctx, time.Second)
		if err != nil {
			t.Fatalf("claim %d: %v", want, err)
		}
		if c.CommandID != want {
			t.Fatalf("FIFO violated: want id=%d got %d", want, c.CommandID)
		}
		if c.Generation != 1 {
			t.Fatalf("first claim gen = %d, want 1", c.Generation)
		}
	}
	if _, err := st.Claim(ctx, time.Second); !errors.Is(err, store.ErrNoAvailableCommand) {
		t.Fatalf("exhausted queue: want ErrNoAvailableCommand, got %v", err)
	}
}

// TestExpiredLeaseReclaimAndStaleAck 完整交错：
// 领取→到期→重领（代次推进、令牌不同）→旧令牌迟到确认 409 且状态不变
// →新令牌结算→重复确认 409→终态不可再领。
func TestExpiredLeaseReclaimAndStaleAck(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	created, err := st.CreateCommand(ctx, []byte(`{"seq":42}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	id := created.ID

	first, err := st.Claim(ctx, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if first.Generation != 1 {
		t.Fatalf("first gen = %d", first.Generation)
	}

	time.Sleep(160 * time.Millisecond)

	second, err := st.Claim(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if second.Generation != 2 {
		t.Fatalf("second gen = %d, want 2", second.Generation)
	}
	if second.LeaseToken == first.LeaseToken {
		t.Fatal("new lease token must differ from expired one")
	}

	// 旧持有者迟到的确认必须被拒绝，且不得改变状态。
	if err := st.Ack(ctx, id, first.LeaseToken, store.StatusDelivered); !errors.Is(err, store.ErrLeaseStale) {
		t.Fatalf("stale ack: want ErrLeaseStale, got %v", err)
	}
	det, err := st.GetCommand(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if det.Status != store.StatusPending || det.Settlement != nil {
		t.Fatalf("stale ack changed state: status=%s settlement=%v", det.Status, det.Settlement)
	}

	// 陌生令牌 -> ErrInvalidLeaseToken。
	if err := st.Ack(ctx, id, "deadbeef", store.StatusDelivered); !errors.Is(err, store.ErrInvalidLeaseToken) {
		t.Fatalf("unknown token: want ErrInvalidLeaseToken, got %v", err)
	}

	// 当前持有者结算终态。
	if err := st.Ack(ctx, id, second.LeaseToken, store.StatusFailed); err != nil {
		t.Fatalf("valid ack: %v", err)
	}

	// 任意令牌重复确认均被拒绝。终态优先裁决：两者都得到 ErrAlreadySettled，
	// HTTP 层统一映射为 409。
	if err := st.Ack(ctx, id, first.LeaseToken, store.StatusFailed); !errors.Is(err, store.ErrAlreadySettled) {
		t.Fatalf("old token after settlement: want ErrAlreadySettled, got %v", err)
	}
	if err := st.Ack(ctx, id, second.LeaseToken, store.StatusDelivered); !errors.Is(err, store.ErrAlreadySettled) {
		t.Fatalf("duplicate settlement: want ErrAlreadySettled, got %v", err)
	}

	det, _ = st.GetCommand(ctx, id)
	if det.Status != store.StatusFailed {
		t.Fatalf("terminal status overwritten: %s", det.Status)
	}
	if det.Settlement == nil || det.Settlement.Generation != 2 || det.Settlement.Result != "failed" {
		t.Fatalf("unexpected settlement: %+v", det.Settlement)
	}
	if len(det.Leases) != 2 {
		t.Fatalf("observable generations = %d, want 2", len(det.Leases))
	}

	// 终态指令不可再领。
	if _, err := st.Claim(ctx, time.Second); !errors.Is(err, store.ErrNoAvailableCommand) {
		t.Fatalf("settled command claimed again: %v", err)
	}
}

// TestAckWithinValidLease 未过期令牌可正常置 delivered。
func TestAckWithinValidLease(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	c, _ := st.CreateCommand(ctx, []byte(`{}`), nil)
	cl, err := st.Claim(ctx, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Ack(ctx, c.ID, cl.LeaseToken, store.StatusDelivered); err != nil {
		t.Fatalf("ack: %v", err)
	}
	det, _ := st.GetCommand(ctx, c.ID)
	if det.Status != store.StatusDelivered {
		t.Fatalf("status = %s", det.Status)
	}
}

// TestExpiredTokenEvenWithinGenerationRejected 当前代次令牌到期后也必须拒绝。
func TestExpiredTokenEvenWithinGenerationRejected(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	c, _ := st.CreateCommand(ctx, []byte(`{}`), nil)
	cl, _ := st.Claim(ctx, 100*time.Millisecond)
	time.Sleep(160 * time.Millisecond)
	if err := st.Ack(ctx, c.ID, cl.LeaseToken, store.StatusDelivered); !errors.Is(err, store.ErrLeaseStale) {
		t.Fatalf("expired current-gen token: want ErrLeaseStale, got %v", err)
	}
	det, _ := st.GetCommand(ctx, c.ID)
	if det.Status != store.StatusPending {
		t.Fatalf("state changed: %s", det.Status)
	}
}

// TestGetUnknownCommand 未知编号返回 ErrCommandNotFound。
func TestGetUnknownCommand(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	if _, err := st.GetCommand(context.Background(), 987654321); !errors.Is(err, store.ErrCommandNotFound) {
		t.Fatalf("want ErrCommandNotFound, got %v", err)
	}
}

// TestRestartPersistsState 模拟重启：用新连接池重新迁移、读取，终态与代次保持。
func TestRestartPersistsState(t *testing.T) {
	pool, dbURL := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	c, _ := st.CreateCommand(ctx, []byte(`{"persist":true}`), nil)
	cl, _ := st.Claim(ctx, time.Second)
	if err := st.Ack(ctx, c.ID, cl.LeaseToken, store.StatusDelivered); err != nil {
		t.Fatal(err)
	}
	pool.Close()

	// 全新池等价于进程重启后重新连接。
	pool2, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool2.Close()
	if err := store.Migrate(ctx, pool2); err != nil {
		t.Fatalf("re-migrate after restart: %v", err)
	}
	st2 := store.New(pool2)
	det, err := st2.GetCommand(ctx, c.ID)
	if err != nil {
		t.Fatal(err)
	}
	if det.Status != store.StatusDelivered || det.LeaseGeneration != 1 || det.Settlement == nil {
		t.Fatalf("state lost after restart: %+v", det)
	}
}
