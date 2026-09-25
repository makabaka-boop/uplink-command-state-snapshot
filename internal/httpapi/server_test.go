package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"deepspace/internal/httpapi"
	"deepspace/internal/store"
	"deepspace/internal/testdb"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	pool := testdb.New(t)
	if err := store.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			`TRUNCATE settlements, leases, commands RESTART IDENTITY CASCADE`)
	})

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := httpapi.New(store.New(pool), logger)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func doJSON(t *testing.T, method, url string, body any) (int, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var m map[string]any
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("non-json response %d: %s", resp.StatusCode, raw)
		}
	}
	return resp.StatusCode, m
}

// TestErrorEnvelopeIsStable 所有错误响应都是 {"error":{"code","message"}} 形态。
func TestErrorEnvelopeIsStable(t *testing.T) {
	ts := newTestServer(t)

	cases := []struct {
		name   string
		method string
		path   string
		body   any
		want   int
		code   string
	}{
		{"unknown command", "GET", "/commands/99999999", nil, 404, "unknown_command"},
		{"non-numeric id", "GET", "/commands/abc", nil, 404, "unknown_command"},
		{"lease too short", "POST", "/claims", map[string]any{"lease_duration_ms": 99}, 422, "invalid_lease_duration"},
		{"lease too long", "POST", "/claims", map[string]any{"lease_duration_ms": 5001}, 422, "invalid_lease_duration"},
		{"lease missing", "POST", "/claims", map[string]any{}, 422, "missing_lease_duration"},
		{"payload null", "POST", "/commands", map[string]any{"payload": nil}, 422, "missing_payload"},
		{"unknown route", "GET", "/nope", nil, 404, "route_not_found"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, m := doJSON(t, tc.method, ts.URL+tc.path, tc.body)
			if status != tc.want {
				t.Fatalf("status=%d want %d body=%v", status, tc.want, m)
			}
			errObj, ok := m["error"].(map[string]any)
			if !ok || errObj["code"] != tc.code {
				t.Fatalf("unexpected error envelope: %v", m)
			}
			if errObj["message"] == "" {
				t.Fatal("error message must not be empty")
			}
		})
	}

	// 405 也必须是稳定 JSON。
	req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/claims", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d want 405", resp.StatusCode)
	}
	if e, _ := m["error"].(map[string]any); e["code"] != "method_not_allowed" {
		t.Fatalf("405 envelope: %v", m)
	}
}

// TestMalformedJSONIs400 非法 JSON 返回 400 稳定错误。
func TestMalformedJSONIs400(t *testing.T) {
	ts := newTestServer(t)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/commands", bytes.NewReader([]byte("{oops")))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d want 400", resp.StatusCode)
	}
}

// TestInvalidFieldTypesAre422 错误的字段类型按语义归为 422（不是 400）。
func TestInvalidFieldTypesAre422(t *testing.T) {
	ts := newTestServer(t)

	// 先建一条指令供 ack 路径使用。
	status, m := doJSON(t, http.MethodPost, ts.URL+"/commands", map[string]any{"payload": map[string]any{"x": 1}})
	if status != http.StatusCreated {
		t.Fatalf("create: %d %v", status, m)
	}

	badLeases := []string{
		`{"lease_duration_ms": 100.5}`,
		`{"lease_duration_ms": "1000"}`,
		`{"lease_duration_ms": true}`,
		`{"lease_duration_ms": [100]}`,
	}
	for _, body := range badLeases {
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/claims", bytes.NewReader([]byte(body)))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnprocessableEntity {
			t.Fatalf("body %s: status=%d want 422", body, resp.StatusCode)
		}
	}

	badAcks := []string{
		`{"lease_token": 123, "result": "delivered"}`,
		`{"lease_token": "abc", "result": 42}`,
		`{"lease_token": "abc", "result": true}`,
		`{"lease_token": "", "result": "delivered"}`,
	}
	for _, body := range badAcks {
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/commands/1/ack", bytes.NewReader([]byte(body)))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnprocessableEntity {
			t.Fatalf("body %s: status=%d want 422 (%s)", body, resp.StatusCode, raw)
		}
	}
}

// TestClaimAckHappyPathAndStaleFlow 通过 HTTP 走完整裁决交错。
func TestClaimAckHappyPathAndStaleFlow(t *testing.T) {
	ts := newTestServer(t)

	// 空队列 -> 204 且无响应体。
	resp, err := http.Post(ts.URL+"/claims", "application/json",
		bytes.NewReader([]byte(`{"lease_duration_ms":100}`)))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("empty claim status=%d want 204", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if len(bytes.TrimSpace(raw)) != 0 {
		t.Fatalf("204 must have empty body, got %q", raw)
	}

	// 创建指令。
	status, m := doJSON(t, http.MethodPost, ts.URL+"/commands", map[string]any{"payload": map[string]any{"x": 1}})
	if status != http.StatusCreated {
		t.Fatalf("create status=%d body=%v", status, m)
	}
	id := int64(m["id"].(float64))

	// 领取第一代。
	status, m = doJSON(t, http.MethodPost, ts.URL+"/claims", map[string]any{"lease_duration_ms": 100})
	if status != http.StatusOK {
		t.Fatalf("claim status=%d body=%v", status, m)
	}
	tok1 := m["lease_token"].(string)
	if m["generation"].(float64) != 1 {
		t.Fatalf("gen=%v want 1", m["generation"])
	}

	// 有效令牌非法结果 -> 422（输入校验先于令牌裁决）。
	status, m = doJSON(t, http.MethodPost,
		ts.URL+"/commands/"+strconv.FormatInt(id, 10)+"/ack",
		map[string]any{"lease_token": tok1, "result": "bogus"})
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("invalid result status=%d want 422", status)
	}

	// 等待第一代到期后重领。
	time.Sleep(160 * time.Millisecond)
	status, m = doJSON(t, http.MethodPost, ts.URL+"/claims", map[string]any{"lease_duration_ms": 2000})
	if status != http.StatusOK {
		t.Fatalf("reclaim status=%d body=%v", status, m)
	}
	if m["generation"].(float64) != 2 {
		t.Fatalf("gen=%v want 2", m["generation"])
	}
	tok2 := m["lease_token"].(string)
	if tok2 == tok1 {
		t.Fatal("lease token must change across generations")
	}

	// 旧令牌迟到 -> 409 + lease_expired，状态不变。
	status, m = doJSON(t, http.MethodPost,
		ts.URL+"/commands/"+strconv.FormatInt(id, 10)+"/ack",
		map[string]any{"lease_token": tok1, "result": "delivered"})
	if status != http.StatusConflict {
		t.Fatalf("stale ack status=%d want 409 body=%v", status, m)
	}
	if m["error"].(map[string]any)["code"] != "lease_expired" {
		t.Fatalf("code=%v want lease_expired", m["error"])
	}
	status, m = doJSON(t, http.MethodGet, ts.URL+"/commands/"+strconv.FormatInt(id, 10), nil)
	if m["status"] != "pending" {
		t.Fatalf("status after stale ack = %v", m["status"])
	}
	if _, ok := m["lease_token"]; ok {
		t.Fatal("GET must never expose lease_token")
	}
	if len(m["leases"].([]any)) != 2 {
		t.Fatalf("observable generations=%v", m["leases"])
	}

	// 新持有者终态 delivered。
	status, m = doJSON(t, http.MethodPost,
		ts.URL+"/commands/"+strconv.FormatInt(id, 10)+"/ack",
		map[string]any{"lease_token": tok2, "result": "delivered"})
	if status != http.StatusOK {
		t.Fatalf("valid ack status=%d body=%v", status, m)
	}
	if m["status"] != "delivered" {
		t.Fatalf("status=%v want delivered", m["status"])
	}

	// 重复确认 -> 409 already_settled。
	status, m = doJSON(t, http.MethodPost,
		ts.URL+"/commands/"+strconv.FormatInt(id, 10)+"/ack",
		map[string]any{"lease_token": tok2, "result": "failed"})
	if status != http.StatusConflict {
		t.Fatalf("dup ack status=%d want 409", status)
	}
	if m["error"].(map[string]any)["code"] != "already_settled" {
		t.Fatalf("code=%v want already_settled", m["error"])
	}

	// 终态不可再领。
	status, _ = doJSON(t, http.MethodPost, ts.URL+"/claims", map[string]any{"lease_duration_ms": 100})
	if status != http.StatusNoContent {
		t.Fatalf("post-settlement claim status=%d want 204", status)
	}
}
