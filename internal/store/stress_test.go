package store_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"deepspace/internal/store"
	"deepspace/internal/testdb"
)

// TestHighConcurrencyMultipleCommands 多条指令、高并发领取：
// 每条指令恰被领取一次，无重复、无遗漏，无意外错误。
func TestHighConcurrencyMultipleCommands(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	const cmds = 10
	for i := 0; i < cmds; i++ {
		if _, err := st.CreateCommand(ctx, []byte(`{}`), nil); err != nil {
			t.Fatal(err)
		}
	}

	const claimants = 128
	start := make(chan struct{})
	var mu sync.Mutex
	wonIDs := map[int64]int{}
	empty := 0
	var wg sync.WaitGroup
	wg.Add(claimants)
	for i := 0; i < claimants; i++ {
		go func() {
			defer wg.Done()
			<-start
			c, err := st.Claim(ctx, time.Second)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				wonIDs[c.CommandID]++
			case errorIsNoRows(err):
				empty++
			default:
				t.Errorf("unexpected claim error: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if len(wonIDs) != cmds {
		t.Fatalf("distinct claimed commands = %d, want %d (wins=%v)", len(wonIDs), cmds, wonIDs)
	}
	for id, n := range wonIDs {
		if n != 1 {
			t.Fatalf("command %d claimed %d times, want exactly 1", id, n)
		}
	}
	if empty != claimants-cmds {
		t.Fatalf("empty results = %d, want %d", empty, claimants-cmds)
	}

	// 全部被持有时再领 -> 无可用。
	if _, err := st.Claim(ctx, time.Second); !errorIsNoRows(err) {
		t.Fatalf("want no available, got %v", err)
	}
}

// TestConcurrentMigrateIsSafe 多个“进程”同时在空库上迁移，不得出现唯一冲突。
func TestConcurrentMigrateIsSafe(t *testing.T) {
	_, dbURL := testdb.NewEmpty(t)

	const workers = 8
	var wg sync.WaitGroup
	errs := make([]error, workers)
	wg.Add(workers)
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			p, err := openPool(dbURL)
			if err != nil {
				errs[i] = err
				return
			}
			defer p.Close()
			errs[i] = store.Migrate(context.Background(), p)
		}(i)
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("migrator %d: %v", i, err)
		}
	}

	// 并发迁移后结构必须完整可用。
	p, err := openPool(dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	st := store.New(p)
	c, err := st.CreateCommand(context.Background(), []byte(`{}`), nil)
	if err != nil {
		t.Fatalf("create after concurrent migrate: %v", err)
	}
	cl, err := st.Claim(context.Background(), time.Second)
	if err != nil || cl.CommandID != c.ID {
		t.Fatalf("claim after concurrent migrate: %v %+v", err, cl)
	}
}
