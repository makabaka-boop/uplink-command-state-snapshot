// Package httpapi 提供深空测控指令服务的 HTTP API。
package httpapi

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"deepspace/internal/store"
)

// 响应体缓冲上限，防止异常大响应在中间件中放大内存占用。
const maxBufferedBody = 1 << 20

// Server 持有路由与存储依赖。
type Server struct {
	store  *store.Store
	logger *slog.Logger
	mux    *http.ServeMux
}

func New(s *store.Store, logger *slog.Logger) *Server {
	srv := &Server{store: s, logger: logger}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", srv.handleHealth)
	mux.HandleFunc("POST /commands", srv.handleCreateCommand)
	mux.HandleFunc("GET /commands", srv.handleListCommands)
	mux.HandleFunc("GET /commands/{id}", srv.handleGetCommand)
	mux.HandleFunc("POST /commands/{id}/ack", srv.handleAck)
	mux.HandleFunc("POST /claims", srv.handleClaim)

	srv.mux = mux
	return srv
}

func (s *Server) Handler() http.Handler {
	// 最外层：panic 恢复 + 把 ServeMux 产生的纯文本 404/405 收敛为稳定 JSON，
	// 保证任何错误响应都符合契约，不泄漏 net/http 的默认文本。
	return s.recoverPanic(s.jsonizeErrors(s.mux))
}

func (s *Server) ListenAndServe(addr string) error {
	s.logger.Info("api listening", "addr", addr)
	return http.ListenAndServe(addr, s.Handler())
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Ping(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database_unavailable", "database not reachable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// jsonizeErrors 缓冲下游响应。业务处理器总是写 application/json；
// 因此任何非 JSON 错误响应只可能来自 ServeMux 的默认 404/405 纯文本，
// 此处统一替换为约定的 JSON 错误信封。204 等无体状态原样透传。
func (s *Server) jsonizeErrors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := &bufferedWriter{ResponseWriter: w}
		next.ServeHTTP(buf, r)
		buf.Flush(w)
	})
}

// bufferedWriter 拦截状态码与响应体，由 Flush 决定最终写法。
type bufferedWriter struct {
	http.ResponseWriter
	status    int
	body      bytes.Buffer
	committed bool
}

func (b *bufferedWriter) WriteHeader(code int) {
	if !b.committed {
		b.status = code
		b.committed = true
	}
}

func (b *bufferedWriter) Write(p []byte) (int, error) {
	if !b.committed {
		b.status = http.StatusOK
		b.committed = true
	}
	if b.body.Len() < maxBufferedBody {
		b.body.Write(p)
	}
	return len(p), nil
}

// Flush 把缓冲内容按契约写回真实 ResponseWriter，且只调用一次 WriteHeader。
func (b *bufferedWriter) Flush(w http.ResponseWriter) {
	if !b.committed {
		b.status = http.StatusOK
	}

	// 无体响应：仅落状态码（204 必须无体）。
	if b.status == http.StatusNoContent || b.body.Len() == 0 {
		if b.status != http.StatusOK {
			w.WriteHeader(b.status)
		}
		return
	}

	if strings.HasPrefix(b.Header().Get("Content-Type"), "application/json") {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(b.status)
		_, _ = w.Write(b.body.Bytes())
		return
	}

	// 非 JSON 响应只出现在 ServeMux 默认错误上。
	code, message := "route_not_found", "unknown endpoint or method"
	if b.status == http.StatusMethodNotAllowed {
		code, message = "method_not_allowed", "HTTP method not allowed for this endpoint"
		b.status = http.StatusMethodNotAllowed
	} else if b.status != http.StatusNotFound {
		// 其他非 JSON 状态（理论上不会发生）：保留原状态与体。
		w.WriteHeader(b.status)
		_, _ = w.Write(b.body.Bytes())
		return
	}
	w.Header().Del("Content-Length")
	writeJSON(w, b.status, errorEnvelope{Error: errorBody{Code: code, Message: message}})
}

func (s *Server) recoverPanic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.logger.Error("panic recovered", "error", rec, "path", r.URL.Path)
				writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// ---- 稳定 JSON 响应工具 ----

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type errorEnvelope struct {
	Error errorBody `json:"error"`
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorEnvelope{Error: errorBody{Code: code, Message: message}})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	_ = enc.Encode(v)
}
