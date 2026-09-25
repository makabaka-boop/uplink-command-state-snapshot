// Command verify 是一次性验收服务（compose: verify）。
//
// 它完整执行需求中的验收交错：
//  1. 并发抢领唯一指令，断言恰有一方成功，其余得到 204；
//  2. 用 100ms 租期等待到期后重新领取，断言代次推进且新令牌不同；
//  3. 逆序确认（旧令牌先、新令牌后），断言旧令牌 409 且不改变状态，
//     新令牌置终态；重复确认得到 409；
//  4. 重启 API 进程（同库重连），重启前后终态唯一且一致；
//  5. 直接查询 PostgreSQL 作为带外证据：settlements 仅一行、状态唯一。
//
// 退出码 0 表示全部通过；任何断言失败都会打印详细错误并以非零退出。
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type failure struct{ msg string }

func (f *failure) Error() string { return f.msg }
func fail(format string, a ...any) error {
	return &failure{msg: fmt.Sprintf(format, a...)}
}

type verifier struct {
	baseURL string
	dbURL   string
	client  *http.Client
}

func main() {
	v := &verifier{
		baseURL: envOr("BASE_URL", "http://api:8080"),
		dbURL:   envOr("DATABASE_URL", "postgres://postgres:postgres@db:5432/deepspace?sslmode=disable"),
		client:  &http.Client{Timeout: 10 * time.Second},
	}
	ctx := context.Background()
	if err := v.waitForAPI(ctx); err != nil {
		fatal(err)
	}
	if err := v.run(ctx); err != nil {
		fatal(err)
	}
	fmt.Println("VERIFY: all acceptance checks passed")
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "VERIFY FAILED: %v\n", err)
	os.Exit(1)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type apiError struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type claimOut struct {
	CommandID      int64     `json:"command_id"`
	Generation     int64     `json:"generation"`
	LeaseToken     string    `json:"lease_token"`
	LeaseExpiresAt time.Time `json:"lease_expires_at"`
}

type commandOut struct {
	ID              int64            `json:"id"`
	Status          string           `json:"status"`
	PredecessorID   *int64           `json:"predecessor_id"`
	BlockedBy       *int64           `json:"blocked_by"`
	LeaseGeneration int64            `json:"lease_generation"`
	Settlement      map[string]any   `json:"settlement"`
	Leases          []map[string]any `json:"leases"`
}

// createCommand 创建一条指令（predecessor 传 nil 表示无前驱），断言 201 并返回。
func (v *verifier) createCommand(ctx context.Context, payload map[string]any, predecessor *int64) commandOut {
	body := map[string]any{"payload": payload}
	if predecessor != nil {
		body["predecessor_id"] = *predecessor
	}
	status, raw := v.doJSON(ctx, http.MethodPost, "/commands", body)
	if status != http.StatusCreated {
		fatal(fail("create command: status=%d body=%s", status, raw))
	}
	return decode[commandOut](raw)
}

// doJSON 发送 JSON 请求并返回状态码与原始体。
func (v *verifier) doJSON(ctx context.Context, method, path string, body any) (int, []byte) {
	code, raw, err := v.doJSONErr(ctx, method, path, body)
	if err != nil {
		fatal(fmt.Errorf("%s %s: %w", method, path, err))
	}
	return code, raw
}

// doJSONErr 同 doJSON，但把传输层错误返回给调用方（用于健康检查重试）。
func (v *verifier) doJSONErr(ctx context.Context, method, path string, body any) (int, []byte, error) {
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, v.baseURL+path, rdr)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := v.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, raw, nil
}

func decode[T any](raw []byte) T {
	var out T
	if err := json.Unmarshal(raw, &out); err != nil {
		fatal(fmt.Errorf("decode response %s: %w", string(raw), err))
	}
	return out
}

func (v *verifier) waitForAPI(ctx context.Context) error {
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		status, _, err := v.doJSONErr(ctx, http.MethodGet, "/healthz", nil)
		if err == nil && status == http.StatusOK {
			return nil
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("healthz status=%d", status)
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fail("api did not become healthy within 60s: %v", lastErr)
}

func (v *verifier) run(ctx context.Context) error {
	// 场景零：输入校验契约。
	if err := v.checkValidation(ctx); err != nil {
		return err
	}

	// 前驱链：链式解锁、失败传播、阻断来源与旧指令兼容。
	if err := v.checkPredecessorChains(ctx); err != nil {
		return err
	}

	// 场景一：创建唯一指令，并发抢领。
	status, raw := v.doJSON(ctx, http.MethodPost, "/commands", map[string]any{
		"payload": map[string]any{
			"type":      "uplink",
			"target":    "mars-relay-7",
			"sequence":  1,
			"issued_at": "2026-09-18T00:00:00Z",
		},
	})
	if status != http.StatusCreated {
		return fail("create command: status=%d body=%s", status, raw)
	}
	created := decode[commandOut](raw)
	if created.ID <= 0 || created.Status != "pending" {
		return fail("unexpected created command: %s", raw)
	}
	cmdPath := fmt.Sprintf("/commands/%d", created.ID)
	fmt.Printf("created command id=%d\n", created.ID)

	const n = 16
	start := make(chan struct{})
	var wg sync.WaitGroup
	statuses := make([]int, n)
	bodies := make([][]byte, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			statuses[i], bodies[i] = v.doJSON(ctx, http.MethodPost, "/claims",
				map[string]any{"lease_duration_ms": 2000})
		}(i)
	}
	close(start)
	wg.Wait()

	winners := 0
	var first claimOut
	for i := 0; i < n; i++ {
		switch statuses[i] {
		case http.StatusOK:
			winners++
			first = decode[claimOut](bodies[i])
		case http.StatusNoContent:
		default:
			return fail("claim goroutine %d got unexpected status %d: %s", i, statuses[i], bodies[i])
		}
	}
	if winners != 1 {
		return fail("expected exactly 1 winning claim, got %d", winners)
	}
	if first.CommandID != created.ID || first.Generation != 1 || len(first.LeaseToken) < 32 {
		return fail("unexpected winning claim: %+v", first)
	}
	oldToken := first.LeaseToken
	fmt.Printf("concurrent claim: exactly 1 winner, gen=%d token=%s…\n", first.Generation, oldToken[:8])

	// 租约有效期内，再领取应无可用（204）。
	var st int
	if st, _ = v.doJSON(ctx, http.MethodPost, "/claims",
		map[string]any{"lease_duration_ms": 500}); st != http.StatusNoContent {
		return fail("claim during active lease should be 204, got %d", st)
	}

	// 场景二：等待第一代租期到期后重新领取（断言代次推进、令牌换新）。
	fmt.Println("waiting for first lease (2000ms) to expire…")
	time.Sleep(2150 * time.Millisecond)

	st, raw = v.doJSON(ctx, http.MethodPost, "/claims", map[string]any{"lease_duration_ms": 100})
	if st != http.StatusOK {
		return fail("reclaim after expiry: status=%d body=%s", st, raw)
	}
	second := decode[claimOut](raw)
	if second.CommandID != created.ID {
		return fail("reclaimed wrong command id: %+v", second)
	}
	if second.Generation != 2 {
		return fail("expected generation 2 after reclaim, got %d", second.Generation)
	}
	if second.LeaseToken == oldToken {
		return fail("new lease token must differ from the old one")
	}
	newToken := second.LeaseToken
	fmt.Printf("reclaim after expiry: gen=%d new token=%s…\n", second.Generation, newToken[:8])

	// 新代次生效期间：旧令牌迟到确认 -> 409，状态仍 pending。
	st, raw = v.doJSON(ctx, http.MethodPost, cmdPath+"/ack",
		map[string]any{"lease_token": oldToken, "result": "delivered"})
	if st != http.StatusConflict {
		return fail("stale old token ack: expected 409, got %d body=%s", st, raw)
	}
	ae := decode[apiError](raw)
	if ae.Error.Code != "lease_expired" {
		return fail("stale ack error code: expected lease_expired, got %q", ae.Error.Code)
	}
	got := v.getCommand(ctx, created.ID)
	if got.Status != "pending" || got.Settlement != nil {
		return fail("stale ack must not change state, got status=%s settlement=%v",
			got.Status, got.Settlement)
	}
	fmt.Println("old token late ack -> 409 lease_expired, state unchanged (pending)")

	// 逆序场景的另一半：新令牌若先到期，会产生第三代；构造“先等到第二代到期，
	// 再领第三代，然后用第二代旧令牌确认”，覆盖真正的逆序确认交错。
	fmt.Println("waiting for second lease (100ms) to expire…")
	time.Sleep(350 * time.Millisecond)

	st, raw = v.doJSON(ctx, http.MethodPost, "/claims", map[string]any{"lease_duration_ms": 3000})
	if st != http.StatusOK {
		return fail("third claim: expected 200, got %d body=%s", st, raw)
	}
	third := decode[claimOut](raw)
	if third.Generation != 3 || third.LeaseToken == newToken || third.LeaseToken == oldToken {
		return fail("third claim must advance generation and issue a fresh token: %+v", third)
	}
	holderToken := third.LeaseToken
	fmt.Printf("third claim: gen=%d token=%s…\n", third.Generation, holderToken[:8])

	// 第二代令牌（旧持有者）迟到确认 -> 409。
	st, raw = v.doJSON(ctx, http.MethodPost, cmdPath+"/ack",
		map[string]any{"lease_token": newToken, "result": "failed"})
	if st != http.StatusConflict {
		return fail("gen-2 stale ack: expected 409, got %d body=%s", st, raw)
	}
	got = v.getCommand(ctx, created.ID)
	if got.Status != "pending" {
		return fail("state must remain pending after gen-2 stale ack, got %s", got.Status)
	}
	fmt.Println("gen-2 token late ack -> 409, state still pending")

	// 当前持有者以 failed 结算终态。
	st, raw = v.doJSON(ctx, http.MethodPost, cmdPath+"/ack",
		map[string]any{"lease_token": holderToken, "result": "failed"})
	if st != http.StatusOK {
		return fail("current holder ack: expected 200, got %d body=%s", st, raw)
	}
	settled := decode[commandOut](raw)
	if settled.Status != "failed" || settled.Settlement["generation"].(float64) != 3 {
		return fail("unexpected settled body: %s", raw)
	}
	fmt.Println("current holder settled command -> failed (gen=3)")

	// 旧令牌、当前令牌重复确认 -> 均 409，终态不得被覆盖。
	for i, tok := range []string{oldToken, newToken, holderToken} {
		st, raw = v.doJSON(ctx, http.MethodPost, cmdPath+"/ack",
			map[string]any{"lease_token": tok, "result": "delivered"})
		if st != http.StatusConflict {
			return fail("duplicate ack #%d: expected 409, got %d body=%s", i, st, raw)
		}
	}
	got = v.getCommand(ctx, created.ID)
	if got.Status != "failed" {
		return fail("terminal state must remain failed, got %s", got.Status)
	}
	if got.Settlement["result"] != "failed" {
		return fail("settlement result overwritten: %v", got.Settlement)
	}
	fmt.Println("all duplicate/overriding acks -> 409, terminal state stays failed")

	// 终态指令不会再被领取：204。
	if st, _ := v.doJSON(ctx, http.MethodPost, "/claims",
		map[string]any{"lease_duration_ms": 100}); st != http.StatusNoContent {
		return fail("settled command must not be claimable, got %d", st)
	}

	// 观察性：代次历史应为 3 代。
	if len(got.Leases) != 3 {
		return fail("expected 3 observable lease generations, got %d", len(got.Leases))
	}

	// 带外：直接查库，settlements 恰一行。
	if err := v.assertDatabase(ctx, created.ID); err != nil {
		return err
	}

	// 场景三：重启 API 进程，重启后终态唯一且一致。
	if err := v.restartAPIAndRecheck(ctx, cmdPath); err != nil {
		return err
	}
	return nil
}

// checkPredecessorChains 验证前驱链完整契约：
//   - 创建可带可选 predecessor_id，只能引用已存在的较早编号；非法类型 422、
//     未知编号 422 predecessor_not_found；
//   - 前驱未全部送达的后继不可领取（不预发租约），根送达后逐节解锁；
//   - 链上某节点 failed，其尚未领取的后继原子转只读 blocked、记录同一阻断来源，
//     不写租约或结算，且 blocked 不可领取/不可确认；
//   - 无 predecessor_id 的旧指令行为完全不变。
func (v *verifier) checkPredecessorChains(ctx context.Context) error {
	// 非法 predecessor_id 类型/取值 -> 422 invalid_predecessor_id。
	for _, body := range []string{
		`{"payload":{},"predecessor_id":"1"}`,
		`{"payload":{},"predecessor_id":true}`,
		`{"payload":{},"predecessor_id":0}`,
		`{"payload":{},"predecessor_id":-7}`,
	} {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, v.baseURL+"/commands",
			bytes.NewReader([]byte(body)))
		req.Header.Set("Content-Type", "application/json")
		resp, err := v.client.Do(req)
		if err != nil {
			return fmt.Errorf("bad predecessor request: %w", err)
		}
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnprocessableEntity {
			return fail("invalid predecessor %s: status=%d body=%s", body, resp.StatusCode, raw)
		}
		if decode[apiError](raw).Error.Code != "invalid_predecessor_id" {
			return fail("invalid predecessor %s: wrong code %s", body, raw)
		}
	}
	// 未知（更晚/不存在）编号 -> 422 predecessor_not_found。
	st, raw := v.doJSON(ctx, http.MethodPost, "/commands",
		map[string]any{"payload": map[string]any{}, "predecessor_id": 987654321})
	if st != http.StatusUnprocessableEntity || decode[apiError](raw).Error.Code != "predecessor_not_found" {
		return fail("unknown predecessor: status=%d body=%s", st, raw)
	}

	// 链 r0 <- r1 <- r2 <- r3。无前驱旧指令的兼容性在链裁决结束后单独验证，
	// 以免它作为独立根提前进入可领取集合干扰链式解锁断言。
	r0 := v.createCommand(ctx, map[string]any{"node": 0}, nil)
	r1 := v.createCommand(ctx, map[string]any{"node": 1}, &r0.ID)
	r2 := v.createCommand(ctx, map[string]any{"node": 2}, &r1.ID)
	r3 := v.createCommand(ctx, map[string]any{"node": 3}, &r2.ID)
	for _, c := range []commandOut{r1, r2, r3} {
		if c.Status != "pending" {
			return fail("chain node %d created as %s, want pending", c.ID, c.Status)
		}
	}
	fmt.Printf("predecessor chain created: r0=%d r1=%d r2=%d r3=%d\n", r0.ID, r1.ID, r2.ID, r3.ID)

	// 最小可领取是 r0（编号最小且无前驱）；持有期间其余全部等待。
	claim := func() (int, claimOut) {
		s, b := v.doJSON(ctx, http.MethodPost, "/claims", map[string]any{"lease_duration_ms": 2000})
		var cl claimOut
		if s == http.StatusOK {
			cl = decode[claimOut](b)
		}
		return s, cl
	}
	s, cl := claim()
	if s != http.StatusOK || cl.CommandID != r0.ID {
		return fail("first chain claim: status=%d claim=%+v want r0=%d", s, cl, r0.ID)
	}
	if s, _ := claim(); s != http.StatusNoContent {
		return fail("while r0 leased, successors must not be pre-leased: status=%d", s)
	}
	if s, b := v.doJSON(ctx, http.MethodPost, fmt.Sprintf("/commands/%d/ack", r0.ID),
		map[string]any{"lease_token": cl.LeaseToken, "result": "delivered"}); s != http.StatusOK {
		return fail("deliver r0: status=%d body=%s", s, b)
	}
	// r0 delivered -> r1 解锁为最小可领取。
	if s, cl := claim(); s != http.StatusOK || cl.CommandID != r1.ID {
		return fail("after r0 delivered, want r1=%d claimable: status=%d %+v", r1.ID, s, cl)
	} else {
		// r1 失败 -> r1 failed；r2、r3（均未领取）原子 blocked，阻断来源=r1。
		if s, b := v.doJSON(ctx, http.MethodPost, fmt.Sprintf("/commands/%d/ack", r1.ID),
			map[string]any{"lease_token": cl.LeaseToken, "result": "failed"}); s != http.StatusOK {
			return fail("fail r1: status=%d body=%s", s, b)
		}
	}
	for _, id := range []int64{r2.ID, r3.ID} {
		got := v.getCommand(ctx, id)
		if got.Status != "blocked" || got.BlockedBy == nil || *got.BlockedBy != r1.ID {
			return fail("node %d: status=%s blocked_by=%v want blocked by r1=%d",
				id, got.Status, got.BlockedBy, r1.ID)
		}
		if len(got.Leases) != 0 || got.Settlement != nil {
			return fail("blocked node %d must have no leases/settlement: leases=%d settlement=%v",
				id, len(got.Leases), got.Settlement)
		}
	}
	// blocked 链不可领取；对 blocked 后继的确认被 409 already_settled 拒绝。
	if s, _ := claim(); s != http.StatusNoContent {
		return fail("blocked successors must not be claimable: status=%d", s)
	}
	if s, b := v.doJSON(ctx, http.MethodPost, fmt.Sprintf("/commands/%d/ack", r2.ID),
		map[string]any{"lease_token": "00", "result": "delivered"}); s != http.StatusConflict ||
		decode[apiError](b).Error.Code != "already_settled" {
		return fail("ack blocked r2: status=%d body=%s want 409 already_settled", s, b)
	}
	// 失败根 r1 之后再挂后继：出生即 blocked，继承阻断来源 r1。
	born := v.createCommand(ctx, map[string]any{"late": true}, &r1.ID)
	if born.Status != "blocked" || born.BlockedBy == nil || *born.BlockedBy != r1.ID {
		return fail("late successor of failed root must be born blocked: %+v", born)
	}

	// 旧指令不受影响：无 predecessor_id 的指令在 blocked 链旁仍正常创建与领取。
	// 它此刻是全库唯一可领取指令（r0 delivered、r1 failed、r2/r3 blocked）。
	legacy := v.createCommand(ctx, map[string]any{"legacy": true}, nil)
	if legacy.PredecessorID != nil || legacy.BlockedBy != nil {
		return fail("legacy command must carry no predecessor/blocked_by: %+v", legacy)
	}
	s, cl = claim()
	if s != http.StatusOK || cl.CommandID != legacy.ID {
		return fail("legacy command must be the claimable one: status=%d claim=%+v want %d",
			s, cl, legacy.ID)
	}
	if s, b := v.doJSON(ctx, http.MethodPost, fmt.Sprintf("/commands/%d/ack", legacy.ID),
		map[string]any{"lease_token": cl.LeaseToken, "result": "delivered"}); s != http.StatusOK {
		return fail("deliver legacy: status=%d body=%s", s, b)
	}
	if s, _ := claim(); s != http.StatusNoContent {
		return fail("after legacy delivered nothing should be claimable: status=%d", s)
	}
	fmt.Println("predecessor chain checks passed (sequential unlock + failure propagation + legacy)")
	return nil
}

func (v *verifier) checkValidation(ctx context.Context) error {
	// 非法租期：过小、过大、缺失。
	for _, body := range []map[string]any{
		{"lease_duration_ms": 99},
		{"lease_duration_ms": 5001},
		{"lease_duration_ms": -1},
		{},
	} {
		st, raw := v.doJSON(ctx, http.MethodPost, "/claims", body)
		if st != http.StatusUnprocessableEntity {
			return fail("invalid lease %v: expected 422, got %d body=%s", body, st, raw)
		}
		ae := decode[apiError](raw)
		if ae.Error.Code == "" {
			return fail("422 must carry stable error code, body=%s", raw)
		}
	}
	// 无可用指令 -> 204，且无响应体。
	st, raw := v.doJSON(ctx, http.MethodPost, "/claims", map[string]any{"lease_duration_ms": 100})
	if st != http.StatusNoContent || len(bytes.TrimSpace(raw)) != 0 {
		return fail("empty claim: expected 204 no body, got %d body=%q", st, raw)
	}
	// 未知编号 -> 404（查询与确认均如此）。
	if st, raw := v.doJSON(ctx, http.MethodGet, "/commands/99999999", nil); st != http.StatusNotFound {
		return fail("unknown GET: expected 404, got %d body=%s", st, raw)
	}
	if st, raw := v.doJSON(ctx, http.MethodPost, "/commands/99999999/ack",
		map[string]any{"lease_token": "x", "result": "delivered"}); st != http.StatusNotFound {
		return fail("unknown ack: expected 404, got %d body=%s", st, raw)
	}
	// 非法结果 / 缺失令牌 -> 422。
	if st, raw := v.doJSON(ctx, http.MethodPost, "/commands/1/ack",
		map[string]any{"lease_token": "x", "result": "lost_in_space"}); st != http.StatusUnprocessableEntity {
		return fail("invalid result: expected 422, got %d body=%s", st, raw)
	}
	if st, raw := v.doJSON(ctx, http.MethodPost, "/commands/1/ack",
		map[string]any{"result": "delivered"}); st != http.StatusUnprocessableEntity {
		return fail("missing token: expected 422, got %d body=%s", st, raw)
	}
	// 载荷缺失 -> 422；非法 JSON -> 400。
	if st, raw := v.doJSON(ctx, http.MethodPost, "/commands",
		map[string]any{"payload": nil}); st != http.StatusUnprocessableEntity {
		return fail("null payload: expected 422, got %d body=%s", st, raw)
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, v.baseURL+"/commands",
		bytes.NewReader([]byte("{not json")))
	req.Header.Set("Content-Type", "application/json")
	resp, err := v.client.Do(req)
	if err != nil {
		return fmt.Errorf("malformed json request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		return fail("malformed json: expected 400, got %d", resp.StatusCode)
	}
	fmt.Println("validation contract checks passed")
	return nil
}

func (v *verifier) getCommand(ctx context.Context, id int64) commandOut {
	st, raw := v.doJSON(ctx, http.MethodGet, fmt.Sprintf("/commands/%d", id), nil)
	if st != http.StatusOK {
		fatal(fmt.Errorf("get command: status=%d body=%s", st, raw))
	}
	return decode[commandOut](raw)
}

func (v *verifier) assertDatabase(ctx context.Context, id int64) error {
	pool, err := pgxpool.New(ctx, v.dbURL)
	if err != nil {
		return fmt.Errorf("db connect: %w", err)
	}
	defer pool.Close()

	var status string
	var gen int64
	if err := pool.QueryRow(ctx,
		`SELECT status, lease_generation FROM commands WHERE id=$1`, id).Scan(&status, &gen); err != nil {
		return fmt.Errorf("db query command: %w", err)
	}
	var settlementCount int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM settlements WHERE command_id=$1`, id).Scan(&settlementCount); err != nil {
		return fmt.Errorf("db query settlements: %w", err)
	}
	var leaseCount int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM leases WHERE command_id=$1`, id).Scan(&leaseCount); err != nil {
		return fmt.Errorf("db query leases: %w", err)
	}
	if status != "failed" || settlementCount != 1 || leaseCount != 3 || gen != 3 {
		return fail("db invariants violated: status=%s settlements=%d leases=%d gen=%d",
			status, settlementCount, leaseCount, gen)
	}
	fmt.Println("database invariants: 1 settlement row, 3 lease generations, status=failed")
	return nil
}

// restartAPIAndRecheck 在验证容器内重启 API 进程（重新执行迁移+连接同一数据库），
// 模拟服务重启后继续裁决。需要 API_BIN 指向同构二进制（Dockerfile 已内置）。
func (v *verifier) restartAPIAndRecheck(ctx context.Context, cmdPath string) error {
	bin := os.Getenv("API_BIN")
	if bin == "" {
		bin = "/usr/local/bin/api"
	}
	if _, err := os.Stat(bin); err != nil {
		return fail("API binary not found at %s (set API_BIN): %v", bin, err)
	}

	port := os.Getenv("RESTART_API_PORT")
	if port == "" {
		port = "18080"
	}
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(),
		"API_PORT="+port,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start restarted api: %w", err)
	}
	defer func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		_ = cmd.Wait()
	}()

	restarted := &verifier{
		baseURL: "http://127.0.0.1:" + port,
		dbURL:   v.dbURL,
		client:  &http.Client{Timeout: 10 * time.Second},
	}
	if err := restarted.waitForAPI(ctx); err != nil {
		return fmt.Errorf("restarted api unhealthy: %w", err)
	}

	st, raw := restarted.doJSON(ctx, http.MethodGet, cmdPath, nil)
	if st != http.StatusOK {
		return fail("after restart GET: status=%d body=%s", st, raw)
	}
	after := decode[commandOut](raw)
	if after.Status != "failed" || after.LeaseGeneration != 3 {
		return fail("after restart state changed: status=%s gen=%d", after.Status, after.LeaseGeneration)
	}
	if after.Settlement["result"] != "failed" ||
		int(after.Settlement["generation"].(float64)) != 3 {
		return fail("after restart settlement mismatch: %v", after.Settlement)
	}

	// 重启后仍要正确拒绝重复结算与覆盖。
	for _, tok := range []string{"garbage", "00"} {
		st, raw = restarted.doJSON(ctx, http.MethodPost, cmdPath+"/ack",
			map[string]any{"lease_token": tok, "result": "delivered"})
		if st != http.StatusConflict {
			return fail("after restart stale ack (%s): expected 409, got %d body=%s", tok, st, raw)
		}
	}
	st, _ = restarted.doJSON(ctx, http.MethodPost, "/claims",
		map[string]any{"lease_duration_ms": 100})
	if st != http.StatusNoContent {
		return fail("after restart settled command still claimable: %d", st)
	}
	fmt.Printf("restart recheck on %s: unique terminal state failed/gen3 preserved\n",
		restarted.baseURL)
	return nil
}
