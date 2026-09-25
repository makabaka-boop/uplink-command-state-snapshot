package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"deepspace/internal/store"
)

const maxBodyBytes = 1 << 20 // 1MiB

// ---- 请求体 ----

// createCommandRequest 用 RawMessage 承接前驱编号，以便把“类型错误（字符串/对象）”
// 与“引用不存在/非更早编号”区分开：前者 422 非法编号，后者 422 前驱不可引用。
type createCommandRequest struct {
	Payload       json.RawMessage `json:"payload"`
	PredecessorID json.RawMessage `json:"predecessor_id"`
}

// claimRequest 用 RawMessage 承接租期，以便把“类型错误（字符串/浮点/布尔）”
// 与“数值越界”统一归类为 422 非法租期，而非 400 JSON 语法错误。
type claimRequest struct {
	LeaseDurationMS json.RawMessage `json:"lease_duration_ms"`
}

type ackRequest struct {
	LeaseToken json.RawMessage `json:"lease_token"`
	Result     json.RawMessage `json:"result"`
}

// parseStrictInt 仅接受 JSON 整数字面量；浮点、字符串、布尔、对象均视为非法。
func parseStrictInt(raw json.RawMessage) (int, bool) {
	s := strings.TrimSpace(string(raw))
	if s == "" {
		return 0, false
	}
	for i, r := range s {
		if r == '-' {
			if i != 0 || len(s) == 1 {
				return 0, false
			}
			continue
		}
		if r < '0' || r > '9' {
			return 0, false
		}
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, false
	}
	return n, true
}

// parseStrictString 仅接受 JSON 字符串字面量并返回其解码值。
func parseStrictString(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 {
		return "", false
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false
	}
	return s, true
}

// ---- 响应 DTO（令牌绝不通过 GET 泄露）----

type leaseViewDTO struct {
	Generation int64     `json:"generation"`
	ClaimedAt  time.Time `json:"claimed_at"`
	ExpiresAt  time.Time `json:"expires_at"`
}

type settlementDTO struct {
	Generation int64     `json:"generation"`
	Result     string    `json:"result"`
	SettledAt  time.Time `json:"settled_at"`
}

type commandDTO struct {
	ID                    int64           `json:"id"`
	Payload               json.RawMessage `json:"payload"`
	Status                string          `json:"status"`
	PredecessorID         *int64          `json:"predecessor_id,omitempty"`
	BlockedBy             *int64          `json:"blocked_by,omitempty"`
	LeaseGeneration       int64           `json:"lease_generation"`
	CurrentLeaseExpiresAt *time.Time      `json:"current_lease_expires_at"`
	CreatedAt             time.Time       `json:"created_at"`
	UpdatedAt             time.Time       `json:"updated_at"`
}

type commandDetailDTO struct {
	commandDTO
	Leases     []leaseViewDTO `json:"leases"`
	Settlement *settlementDTO `json:"settlement"`
}

type claimResponse struct {
	CommandID       int64     `json:"command_id"`
	Generation      int64     `json:"generation"`
	LeaseToken      string    `json:"lease_token"`
	LeaseExpiresAt  time.Time `json:"lease_expires_at"`
	LeaseDurationMS int       `json:"lease_duration_ms"`
}

func toCommandDTO(c store.Command) commandDTO {
	return commandDTO{
		ID:                    c.ID,
		Payload:               json.RawMessage(c.Payload),
		Status:                c.Status,
		PredecessorID:         c.PredecessorID,
		BlockedBy:             c.BlockedBy,
		LeaseGeneration:       c.LeaseGeneration,
		CurrentLeaseExpiresAt: c.LeaseExpiresAt,
		CreatedAt:             c.CreatedAt,
		UpdatedAt:             c.UpdatedAt,
	}
}

func toDetailDTO(d *store.CommandDetail) commandDetailDTO {
	dto := commandDetailDTO{commandDTO: toCommandDTO(d.Command)}
	// 空租约历史序列化为 [] 而非 null：blocked/从未领取的指令同样给出稳定的空数组契约。
	dto.Leases = []leaseViewDTO{}
	for _, l := range d.Leases {
		dto.Leases = append(dto.Leases, leaseViewDTO{
			Generation: l.Generation, ClaimedAt: l.ClaimedAt, ExpiresAt: l.ExpiresAt,
		})
	}
	if d.Settlement != nil {
		dto.Settlement = &settlementDTO{
			Generation: d.Settlement.Generation,
			Result:     d.Settlement.Result,
			SettledAt:  d.Settlement.SettledAt,
		}
	}
	return dto
}

// decodeJSONBody 严格解析：限制体积、拒绝空体与尾随垃圾。
func decodeJSONBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(dst); err != nil {
		switch {
		case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
			writeError(w, http.StatusBadRequest, "invalid_json", "request body is empty or not valid JSON")
		case strings.Contains(err.Error(), "request body too large"):
			writeError(w, http.StatusRequestEntityTooLarge, "body_too_large", "request body exceeds 1MiB")
		default:
			writeError(w, http.StatusBadRequest, "invalid_json", "request body is not valid JSON")
		}
		return false
	}
	if dec.More() {
		writeError(w, http.StatusBadRequest, "invalid_json", "request body must contain a single JSON value")
		return false
	}
	return true
}

func (s *Server) handleCreateCommand(w http.ResponseWriter, r *http.Request) {
	var req createCommandRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	if len(req.Payload) == 0 || string(req.Payload) == "null" {
		writeError(w, http.StatusUnprocessableEntity, "missing_payload",
			`field "payload" is required and must not be null`)
		return
	}
	// 确保入库内容确为合法 JSON 值（RawMessage 已由解码器保证语法合法）。
	if !json.Valid(req.Payload) {
		writeError(w, http.StatusUnprocessableEntity, "invalid_payload", "payload must be valid JSON")
		return
	}

	// predecessor_id 可选：缺省（字段不存在）= 无前驱的旧请求。
	// 显式 null 视为未提供；其他非正整数类型一律 422。
	var predecessorID *int64
	if len(req.PredecessorID) > 0 && string(req.PredecessorID) != "null" {
		pid, ok := parseStrictInt(req.PredecessorID)
		if !ok || pid <= 0 {
			writeError(w, http.StatusUnprocessableEntity, "invalid_predecessor_id",
				`field "predecessor_id" must be a positive integer when present`)
			return
		}
		pid64 := int64(pid)
		predecessorID = &pid64
	}

	c, err := s.store.CreateCommand(r.Context(), req.Payload, predecessorID)
	if err != nil {
		if errors.Is(err, store.ErrPredecessorNotFound) {
			writeError(w, http.StatusUnprocessableEntity, "predecessor_not_found",
				"predecessor_id must reference an existing earlier command")
			return
		}
		s.logger.Error("create command failed", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to persist command")
		return
	}
	writeJSON(w, http.StatusCreated, toCommandDTO(*c))
}

func (s *Server) handleListCommands(w http.ResponseWriter, r *http.Request) {
	list, err := s.store.ListCommands(r.Context(), 200)
	if err != nil {
		s.logger.Error("list commands failed", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to list commands")
		return
	}
	out := make([]commandDTO, 0, len(list))
	for _, c := range list {
		out = append(out, toCommandDTO(c))
	}
	writeJSON(w, http.StatusOK, map[string]any{"commands": out})
}

func (s *Server) handleGetCommand(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	d, err := s.store.GetCommand(r.Context(), id)
	if err != nil {
		s.mapStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toDetailDTO(d))
}

const minLeaseMS, maxLeaseMS = 100, 5000

func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request) {
	var req claimRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	if len(req.LeaseDurationMS) == 0 {
		writeError(w, http.StatusUnprocessableEntity, "missing_lease_duration",
			`field "lease_duration_ms" is required`)
		return
	}
	ms, ok := parseStrictInt(req.LeaseDurationMS)
	if !ok || ms < minLeaseMS || ms > maxLeaseMS {
		writeError(w, http.StatusUnprocessableEntity, "invalid_lease_duration",
			"lease_duration_ms must be an integer between 100 and 5000")
		return
	}

	claim, err := s.store.Claim(r.Context(), time.Duration(ms)*time.Millisecond)
	if err != nil {
		if errors.Is(err, store.ErrNoAvailableCommand) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		s.logger.Error("claim failed", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "claim failed")
		return
	}

	writeJSON(w, http.StatusOK, claimResponse{
		CommandID:       claim.CommandID,
		Generation:      claim.Generation,
		LeaseToken:      claim.LeaseToken,
		LeaseExpiresAt:  claim.LeaseExpiresAt,
		LeaseDurationMS: ms,
	})
}

func (s *Server) handleAck(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	var req ackRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	token, ok := parseStrictString(req.LeaseToken)
	if !ok || strings.TrimSpace(token) == "" {
		writeError(w, http.StatusUnprocessableEntity, "missing_lease_token",
			`field "lease_token" is required and must be a non-empty string`)
		return
	}
	result, ok := parseStrictString(req.Result)
	if !ok || (result != store.StatusDelivered && result != store.StatusFailed) {
		writeError(w, http.StatusUnprocessableEntity, "invalid_result",
			`field "result" must be "delivered" or "failed"`)
		return
	}

	err := s.store.Ack(r.Context(), id, token, result)
	if err != nil {
		s.mapStoreError(w, r, err)
		return
	}

	d, err := s.store.GetCommand(r.Context(), id)
	if err != nil {
		s.mapStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toDetailDTO(d))
}

// parseID 解析路径编号；非数字按“未知编号”处理为 404。
func parseID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	raw := r.PathValue("id")
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusNotFound, "unknown_command", "command not found")
		return 0, false
	}
	return id, true
}

// mapStoreError 将存储层哨兵错误映射为稳定的 HTTP 契约。
func (s *Server) mapStoreError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, store.ErrCommandNotFound):
		writeError(w, http.StatusNotFound, "unknown_command", "command not found")
	case errors.Is(err, store.ErrInvalidLeaseToken):
		writeError(w, http.StatusConflict, "invalid_lease_token", "lease token was never issued for this command")
	case errors.Is(err, store.ErrLeaseStale):
		writeError(w, http.StatusConflict, "lease_expired",
			"lease token is expired or superseded by a newer generation")
	case errors.Is(err, store.ErrAlreadySettled):
		writeError(w, http.StatusConflict, "already_settled",
			"command already has a terminal status; duplicate settlement is rejected")
	default:
		s.logger.Error("request failed", "error", err, "path", r.URL.Path)
		writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}
