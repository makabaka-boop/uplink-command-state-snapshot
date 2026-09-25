package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"deepspace/internal/store"
	"deepspace/internal/testdb"

	"github.com/jackc/pgx/v5/pgxpool"
)

// createCmd 通过 HTTP 创建指令，返回其 id。
func createCmd(t *testing.T, ts string, payload map[string]any, predecessor *int64) int64 {
	t.Helper()
	body := map[string]any{"payload": payload}
	if predecessor != nil {
		body["predecessor_id"] = *predecessor
	}
	status, m := doJSON(t, http.MethodPost, ts+"/commands", body)
	if status != http.StatusCreated {
		t.Fatalf("create status=%d body=%v", status, m)
	}
	return int64(m["id"].(float64))
}

func claimHTTP(t *testing.T, ts string) (int, map[string]any) {
	t.Helper()
	return doJSON(t, http.MethodPost, ts+"/claims", map[string]any{"lease_duration_ms": 2000})
}

func ackHTTP(t *testing.T, ts string, id int64, token, result string) (int, map[string]any) {
	t.Helper()
	return doJSON(t, http.MethodPost, ts+"/commands/"+strconv.FormatInt(id, 10)+"/ack",
		map[string]any{"lease_token": token, "result": result})
}

func getCmd(t *testing.T, ts string, id int64) map[string]any {
	t.Helper()
	status, m := doJSON(t, http.MethodGet, ts+"/commands/"+strconv.FormatInt(id, 10), nil)
	if status != http.StatusOK {
		t.Fatalf("get %d status=%d body=%v", id, status, m)
	}
	return m
}

// TestPredecessorFieldValidation 创建契约：predecessor_id 缺省合法、
// 非法类型 422 invalid_predecessor_id、引用未知编号 422 predecessor_not_found。
func TestPredecessorFieldValidation(t *testing.T) {
	ts := newTestServer(t)

	// 缺省 predecessor_id：旧请求原样工作，响应不含该字段。
	status, m := doJSON(t, http.MethodPost, ts.URL+"/commands", map[string]any{"payload": map[string]any{"x": 1}})
	if status != http.StatusCreated {
		t.Fatalf("legacy create: %d %v", status, m)
	}
	if _, ok := m["predecessor_id"]; ok {
		t.Fatalf("legacy command must omit predecessor_id, got %v", m["predecessor_id"])
	}
	if _, ok := m["blocked_by"]; ok {
		t.Fatal("legacy command must omit blocked_by")
	}
	if m["status"] != "pending" {
		t.Fatalf("legacy status=%v", m["status"])
	}
	root := int64(m["id"].(float64))

	// 非法类型 / 非正整数 -> 422 invalid_predecessor_id。
	for _, body := range []string{
		`{"payload":{},"predecessor_id":"1"}`,
		`{"payload":{},"predecessor_id":true}`,
		`{"payload":{},"predecessor_id":1.5}`,
		`{"payload":{},"predecessor_id":0}`,
		`{"payload":{},"predecessor_id":-3}`,
		`{"payload":{},"predecessor_id":[1]}`,
	} {
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/commands", bytes.NewReader([]byte(body)))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnprocessableEntity {
			t.Fatalf("body=%s status=%d want 422", body, resp.StatusCode)
		}
		var env map[string]any
		_ = json.Unmarshal(raw, &env)
		code := env["error"].(map[string]any)["code"]
		if code != "invalid_predecessor_id" {
			t.Fatalf("body=%s code=%v want invalid_predecessor_id", body, code)
		}
	}

	// 显式 null 视为未提供 -> 创建成功。
	if st, mm := doJSON(t, http.MethodPost, ts.URL+"/commands",
		map[string]any{"payload": map[string]any{}, "predecessor_id": nil}); st != http.StatusCreated {
		t.Fatalf("null predecessor: %d %v", st, mm)
	}

	// 引用不存在的编号 -> 422 predecessor_not_found。
	st, mm := doJSON(t, http.MethodPost, ts.URL+"/commands",
		map[string]any{"payload": map[string]any{}, "predecessor_id": 999999})
	if st != http.StatusUnprocessableEntity {
		t.Fatalf("unknown pred: %d %v", st, mm)
	}
	if mm["error"].(map[string]any)["code"] != "predecessor_not_found" {
		t.Fatalf("code=%v", mm["error"])
	}

	// 引用已存在的较早编号合法，响应回显 predecessor_id。
	st, mm = doJSON(t, http.MethodPost, ts.URL+"/commands",
		map[string]any{"payload": map[string]any{}, "predecessor_id": root})
	if st != http.StatusCreated {
		t.Fatalf("valid pred: %d %v", st, mm)
	}
	if int64(mm["predecessor_id"].(float64)) != root {
		t.Fatalf("predecessor_id echo=%v want %d", mm["predecessor_id"], root)
	}
}

// TestHTTPChainUnlockAndBlockOverHTTP 通过 HTTP 走完整链路：
// 根送达前后继不可领取；逐节解锁；根失败后全部后继 blocked，查询回显阻断来源，
// 且 blocked 不产生租约/结算。
func TestHTTPChainUnlockAndBlockOverHTTP(t *testing.T) {
	ts := newTestServer(t)

	root := createCmd(t, ts.URL, map[string]any{"n": 0}, nil)
	c1 := createCmd(t, ts.URL, map[string]any{"n": 1}, &root)
	c2 := createCmd(t, ts.URL, map[string]any{"n": 2}, &c1)

	// 初始只有根可领取。
	st, m := claimHTTP(t, ts.URL)
	if st != http.StatusOK || int64(m["command_id"].(float64)) != root {
		t.Fatalf("first claim should be root %d, got %d %v", root, st, m)
	}
	rootTok := m["lease_token"].(string)
	if st, _ := claimHTTP(t, ts.URL); st != http.StatusNoContent {
		t.Fatalf("while root leased and chain waiting, claim status=%d want 204", st)
	}

	// 送达根 -> c1 解锁。
	if st, m := ackHTTP(t, ts.URL, root, rootTok, "delivered"); st != http.StatusOK || m["status"] != "delivered" {
		t.Fatalf("ack root: %d %v", st, m)
	}
	st, m = claimHTTP(t, ts.URL)
	if st != http.StatusOK || int64(m["command_id"].(float64)) != c1 {
		t.Fatalf("claim should unlock c1 %d, got %d %v", c1, st, m)
	}
	c1Tok := m["lease_token"].(string)
	if st, _ := claimHTTP(t, ts.URL); st != http.StatusNoContent {
		t.Fatal("c2 must wait while c1 leased")
	}

	// c1 失败 -> c1 failed，c2（未领取）原子 blocked，blocked_by=c1。
	if st, m := ackHTTP(t, ts.URL, c1, c1Tok, "failed"); st != http.StatusOK || m["status"] != "failed" {
		t.Fatalf("ack c1 failed: %d %v", st, m)
	}
	d2 := getCmd(t, ts.URL, c2)
	if d2["status"] != "blocked" {
		t.Fatalf("c2 status=%v want blocked", d2["status"])
	}
	if int64(d2["blocked_by"].(float64)) != c1 {
		t.Fatalf("c2 blocked_by=%v want %d", d2["blocked_by"], c1)
	}
	leases := d2["leases"].([]any)
	if len(leases) != 0 {
		t.Fatalf("blocked c2 must have no leases, got %v", leases)
	}
	if d2["settlement"] != nil {
		t.Fatalf("blocked c2 must have no settlement, got %v", d2["settlement"])
	}

	// 列表视图也承载同一状态契约。
	st, m = doJSON(t, http.MethodGet, ts.URL+"/commands", nil)
	if st != http.StatusOK {
		t.Fatalf("list: %d", st)
	}
	list := m["commands"].([]any)
	gotStatus := map[int64]string{}
	for _, row := range list {
		rm := row.(map[string]any)
		gotStatus[int64(rm["id"].(float64))] = rm["status"].(string)
	}
	if gotStatus[root] != "delivered" || gotStatus[c1] != "failed" || gotStatus[c2] != "blocked" {
		t.Fatalf("list contract mismatch: %v", gotStatus)
	}

	// blocked 不可领取；对 blocked 的确认得到 409 already_settled。
	if st, _ := claimHTTP(t, ts.URL); st != http.StatusNoContent {
		t.Fatalf("blocked chain claim status=%d want 204", st)
	}
	if st, m := ackHTTP(t, ts.URL, c2, "deadbeef", "delivered"); st != http.StatusConflict {
		t.Fatalf("ack blocked status=%d want 409 body=%v", st, m)
	} else if m["error"].(map[string]any)["code"] != "already_settled" {
		t.Fatalf("ack blocked code=%v want already_settled", m["error"])
	}
}

// TestHTTPTwoInstancesInterleave 两个 store/池（模拟两个 API 实例）共享一库，
// 在 HTTP 之外直接驱动存储层，验证“失败确认 × 后继创建”交错后契约一致。
func TestHTTPTwoInstancesInterleave(t *testing.T) {
	pool := testdb.New(t)
	if err := store.Migrate(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			`TRUNCATE command_closure, settlements, leases, commands RESTART IDENTITY CASCADE`)
	})

	adminURL := pool.Config().ConnString()
	pool2, err := pgxpool.New(context.Background(), adminURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool2.Close()

	st1 := store.New(pool)
	st2 := store.New(pool2)
	ctx := context.Background()

	for round := 0; round < 12; round++ {
		root, err := st1.CreateCommand(ctx, []byte(fmt.Sprintf(`{"r":%d}`, round)), nil)
		if err != nil {
			t.Fatal(err)
		}
		cl, err := st1.Claim(ctx, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}

		// A 先持根行锁；B 创建后继被挡住；A 再失败提交。
		ax, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ax.Exec(ctx, `SELECT id FROM commands WHERE id=$1 FOR UPDATE`, root.ID); err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		var childID int64
		go func() {
			c, cerr := st2.CreateCommand(context.Background(), []byte(`{}`), &root.ID)
			if cerr == nil {
				childID = c.ID
			} else {
				t.Errorf("round %d create: %v", round, cerr)
			}
			close(done)
		}()

		waitForBlockedLock(t, pool)

		if _, err := ax.Exec(ctx,
			`INSERT INTO settlements (command_id,generation,lease_token,result)
			 VALUES ($1,$2,$3,'failed')`, root.ID, cl.Generation, cl.LeaseToken); err != nil {
			t.Fatal(err)
		}
		if _, err := ax.Exec(ctx, `UPDATE commands SET status='failed', updated_at=now() WHERE id=$1`, root.ID); err != nil {
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
		<-done

		d, err := st1.GetCommand(ctx, childID)
		if err != nil {
			t.Fatal(err)
		}
		if d.Status != "blocked" || d.BlockedBy == nil || *d.BlockedBy != root.ID {
			t.Fatalf("round %d child status=%s blocked_by=%v want blocked/%d",
				round, d.Status, d.BlockedBy, root.ID)
		}
		if _, err := st1.Claim(ctx, time.Second); err == nil {
			t.Fatalf("round %d: claim unexpectedly succeeded", round)
		}
	}
}

func waitForBlockedLock(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		if err := pool.QueryRow(context.Background(), `
			SELECT count(*) FROM pg_locks
			WHERE NOT granted AND locktype IN ('tuple','transactionid')`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("peer transaction never blocked on the row lock")
}
