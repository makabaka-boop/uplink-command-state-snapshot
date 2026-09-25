// Package store 封装 PostgreSQL 持久化与租约裁决逻辑。
package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// 指令生命周期状态。迁移约束、存储裁决、HTTP 响应与查询视图共用这一份契约。
const (
	StatusPending   = "pending"
	StatusDelivered = "delivered"
	StatusFailed    = "failed"
	// StatusBlocked 是只读终态：前驱确认失败后，尚未送达的后继链原子转入，
	// 不产生租约、不产生结算。
	StatusBlocked = "blocked"
)

// 终态结果集合，供 HTTP 层做入参校验。
var ValidResults = map[string]bool{StatusDelivered: true, StatusFailed: true}

var (
	// ErrNoAvailableCommand 没有可领取（未结算且租约未生效）的指令。
	ErrNoAvailableCommand = errors.New("no available command")
	// ErrCommandNotFound 未知指令编号。
	ErrCommandNotFound = errors.New("command not found")
	// ErrInvalidLeaseToken 令牌无法识别（格式错误或从不属于该指令）。
	ErrInvalidLeaseToken = errors.New("invalid lease token")
	// ErrLeaseStale 令牌属于过期/被取代的旧代次，或当前代次已到期。
	ErrLeaseStale = errors.New("lease token is expired or superseded")
	// ErrAlreadySettled 指令已在终态（delivered/failed/blocked），禁止结算或覆盖。
	ErrAlreadySettled = errors.New("command already settled")
	// ErrPredecessorNotFound 创建指令时引用的前驱编号不存在（或引用了更晚的编号）。
	ErrPredecessorNotFound = errors.New("predecessor command not found")
)

// Store 基于 pgxpool 的存储实现。
type Store struct {
	pool *pgxpool.Pool
	// now 允许测试注入时钟；生产使用 time.Now。
	now func() time.Time
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, now: time.Now}
}

// Ping 用于健康检查。
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Command 是指令聚合根的内存表示。
type Command struct {
	ID              int64
	Payload         []byte
	Status          string
	LeaseGeneration int64
	LeaseExpiresAt  *time.Time
	// PredecessorID 为唯一前驱编号；nil 表示无前驱（旧请求保持原样）。
	PredecessorID *int64
	// BlockedBy 记录阻断来源（确认失败的那条根指令）；仅 status=blocked 时非空。
	BlockedBy *int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// LeaseView 描述某一代租约（观察用，不含令牌原文）。
type LeaseView struct {
	Generation int64
	ExpiresAt  time.Time
	ClaimedAt  time.Time
}

// Settlement 终态结算记录。
type Settlement struct {
	Generation int64
	Result     string
	SettledAt  time.Time
}

// CommandDetail 观察视图：指令 + 各代租约 + 终态。
type CommandDetail struct {
	Command
	Leases     []LeaseView
	Settlement *Settlement
}

// Claim 是领取成功后的返回值。
type Claim struct {
	CommandID      int64
	Generation     int64
	LeaseToken     string
	LeaseExpiresAt time.Time
}

// newToken 生成 256bit 不透明随机令牌（十六进制编码）。
// 每次领取都重新生成，配合 leases.lease_token 全局唯一约束，
// 新令牌在数学上必然不同于任何旧令牌。
func newToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

const commandColumns = `id, payload, status, lease_generation, lease_expires_at,
	predecessor_id, blocked_by, created_at, updated_at`

func scanCommand(row pgx.Row, c *Command) error {
	return row.Scan(&c.ID, &c.Payload, &c.Status, &c.LeaseGeneration, &c.LeaseExpiresAt,
		&c.PredecessorID, &c.BlockedBy, &c.CreatedAt, &c.UpdatedAt)
}

// CreateCommand 按调用顺序（由 IDENTITY 列保证）写入一条指令。
//
// predecessorID 为 nil 时是无前驱的旧请求，行为与过去完全一致。
// 否则只能引用已存在的较早编号；整个判定在单事务内完成：
//   - 沿前驱链自根向叶逐行加 FOR UPDATE 锁，与任何在途的确认事务串行化，
//     由数据库裁决“创建与前驱结算交错”，绝不留下前驱已失败却仍可领取的后继；
//   - 前驱（直接前驱状态即代表整条链——链上不变量由裁决维护）已 delivered
//     或仍 pending 时，新指令正常 pending 等待；
//   - 前驱已 failed/blocked 时，新指令出生即为 blocked 终态并记录阻断来源，
//     不写入任何租约或结算。
func (s *Store) CreateCommand(ctx context.Context, payload []byte, predecessorID *int64) (*Command, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("create begin: %w", err)
	}
	defer tx.Rollback(ctx)

	status := StatusPending
	var blockedBy *int64

	if predecessorID != nil {
		if *predecessorID <= 0 {
			return nil, ErrPredecessorNotFound
		}
		// 递归收集整条前驱链并按 id 升序加锁：
		// 与确认事务（首步即 FOR UPDATE 锁定被确认指令）形成统一锁序，
		// 既避免死锁，也保证锁释放后本事务看到的是结算后的最终状态。
		rows, err := tx.Query(ctx, `
			WITH RECURSIVE chain(id, predecessor_id) AS (
				SELECT id, predecessor_id FROM commands WHERE id = $1
				UNION ALL
				SELECT p.id, p.predecessor_id
				FROM chain ch JOIN commands p ON p.id = ch.predecessor_id
			)
			SELECT c.id, c.status, c.blocked_by
			FROM chain ch JOIN commands c ON c.id = ch.id
			ORDER BY c.id
			FOR UPDATE OF c`, *predecessorID)
		if err != nil {
			return nil, fmt.Errorf("lock predecessor chain: %w", err)
		}
		found := false
		for rows.Next() {
			var id int64
			var st string
			var bBy *int64
			if err := rows.Scan(&id, &st, &bBy); err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan predecessor chain: %w", err)
			}
			if id == *predecessorID {
				found = true
				switch st {
				case StatusFailed:
					// 直接前驱本身就是失败根。
					src := id
					blockedBy = &src
					status = StatusBlocked
				case StatusBlocked:
					// 继承阻断来源，链上所有 blocked 指向同一失败根。
					blockedBy = bBy
					status = StatusBlocked
				}
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate predecessor chain: %w", err)
		}
		if !found {
			return nil, ErrPredecessorNotFound
		}
	}

	c := &Command{}
	err = tx.QueryRow(ctx, `
		INSERT INTO commands (payload, predecessor_id, status, blocked_by)
		VALUES ($1::jsonb, $2, $3, $4)
		RETURNING `+commandColumns,
		string(payload), predecessorID, status, blockedBy,
	).Scan(scanArgs(c)...)
	if err != nil {
		return nil, fmt.Errorf("create command: %w", err)
	}

	// 双保险：自增 ID 单调，存在的前驱编号必然更早；若异常违反则整体回滚。
	if predecessorID != nil && *predecessorID >= c.ID {
		return nil, fmt.Errorf("predecessor %d is not an earlier command than %d", *predecessorID, c.ID)
	}

	if predecessorID != nil {
		// 维护传递闭包：新指令到直接前驱一条边，外加直接前驱的全部祖先。
		// 与插入同一事务，任何崩溃/回滚都不会留下“有前驱边却无闭包”的半成品。
		if _, err := tx.Exec(ctx, `
			INSERT INTO command_closure (command_id, ancestor_id)
			SELECT $1::bigint, $2::bigint
			UNION ALL
			SELECT $1::bigint, ancestor_id FROM command_closure WHERE command_id = $2`,
			c.ID, *predecessorID); err != nil {
			return nil, fmt.Errorf("create closure: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("create commit: %w", err)
	}
	return c, nil
}

// Claim 原子领取最早可用的一条指令：
//   - 资格（整条前驱链全部 delivered）由传递闭包反连接在 commands 的行锁扫描中
//     逐行求值：存在任一状态不为 delivered 的祖先即跳过。
//     该形态与“直接扫 commands 再 FOR UPDATE SKIP LOCKED”等价安全——锁在扫描行走时
//     施加，而非先在递归 CTE 中物化候选再加锁（后者在高并发下会放过同一行）；
//   - FOR UPDATE SKIP LOCKED 保证并发领取者各自拿到不同的行（唯一领取）；
//   - 前驱尚未送达的指令只能等待，绝不预发租约；
//   - 在同一事务内推进 lease_generation 并写入新一代租约。
func (s *Store) Claim(ctx context.Context, leaseFor time.Duration) (*Claim, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("claim begin: %w", err)
	}
	defer tx.Rollback(ctx)

	var id int64
	err = tx.QueryRow(ctx, `
		SELECT c.id
		FROM commands c
		WHERE c.status = 'pending'
		  AND (c.lease_expires_at IS NULL OR c.lease_expires_at <= now())
		  AND NOT EXISTS (
				SELECT 1
				FROM command_closure k
				JOIN commands a ON a.id = k.ancestor_id
				WHERE k.command_id = c.id AND a.status <> 'delivered'
		  )
		ORDER BY c.id ASC
		FOR UPDATE OF c SKIP LOCKED
		LIMIT 1`).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNoAvailableCommand
	}
	if err != nil {
		return nil, fmt.Errorf("claim select: %w", err)
	}

	token, err := newToken()
	if err != nil {
		return nil, fmt.Errorf("generate token: %w", err)
	}

	var gen int64
	var expiresAt time.Time
	err = tx.QueryRow(ctx, `
		UPDATE commands
		SET lease_generation = lease_generation + 1,
		    lease_token      = $2,
		    lease_expires_at = now() + ($3::bigint * interval '1 microsecond'),
		    updated_at       = now()
		WHERE id = $1
		RETURNING lease_generation, lease_expires_at`,
		id, token, leaseFor.Microseconds(),
	).Scan(&gen, &expiresAt)
	if err != nil {
		return nil, fmt.Errorf("claim update: %w", err)
	}

	if _, err = tx.Exec(ctx, `
		INSERT INTO leases (command_id, generation, lease_token, expires_at)
		VALUES ($1, $2, $3, $4)`,
		id, gen, token, expiresAt); err != nil {
		return nil, fmt.Errorf("claim lease insert: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("claim commit: %w", err)
	}
	return &Claim{CommandID: id, Generation: gen, LeaseToken: token, LeaseExpiresAt: expiresAt}, nil
}

// Ack 凭未过期且属于当前代次的令牌，将指令置为终态。
// 旧代次/已到期/已结算等冲突通过哨兵错误返回，状态绝不改变。
//
// result=failed 时在同一事务内把尚未送达（pending）的整条后继链原子转为
// blocked 终态并记录阻断来源；blocked 指令不写租约、不写结算。
// result=delivered 仅结算自身，随后继之而来的后继由领取查询自动解锁。
func (s *Store) Ack(ctx context.Context, id int64, token, result string) error {
	if !ValidResults[result] {
		// 双保险：HTTP 层已拦截，存储层不接受任何越界值。
		return fmt.Errorf("invalid result %q", result)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("ack begin: %w", err)
	}
	defer tx.Rollback(ctx)

	var status string
	var currentGen int64
	err = tx.QueryRow(ctx, `
		SELECT status, lease_generation
		FROM commands
		WHERE id = $1
		FOR UPDATE`, id).Scan(&status, &currentGen)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrCommandNotFound
	}
	if err != nil {
		return fmt.Errorf("ack load command: %w", err)
	}

	// 裁决顺序（行锁内，无并发可改写状态）：
	// 1) 已在终态（delivered/failed，以及失败传播产生的只读 blocked）：
	//    一律拒绝重复结算/覆盖——先于令牌裁决，保证 blocked 指令不可能因任何令牌被改写；
	// 2) 令牌从未对该指令签发；
	// 3) 令牌非当前代次：旧持有者迟到；
	// 4) 令牌已过期：租约到期。
	if status != StatusPending {
		return ErrAlreadySettled
	}

	// 令牌必须是系统签发过、且属于本指令的（其他指令的令牌视为从未签发）。
	var leaseGen int64
	var leaseExpires time.Time
	err = tx.QueryRow(ctx, `
		SELECT generation, expires_at
		FROM leases
		WHERE lease_token = $1 AND command_id = $2`, token, id).Scan(&leaseGen, &leaseExpires)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrInvalidLeaseToken
	}
	if err != nil {
		return fmt.Errorf("ack load lease: %w", err)
	}

	if leaseGen != currentGen {
		return ErrLeaseStale
	}
	if !leaseExpires.After(s.now()) {
		return ErrLeaseStale
	}

	// settlements.command_id 主键兜底：即使上层有漏洞也无法二次结算。
	if _, err = tx.Exec(ctx, `
		INSERT INTO settlements (command_id, generation, lease_token, result)
		VALUES ($1, $2, $3, $4)`,
		id, currentGen, token, result); err != nil {
		return fmt.Errorf("ack settlement insert: %w", err)
	}

	if _, err = tx.Exec(ctx, `
		UPDATE commands
		SET status = $2, updated_at = now()
		WHERE id = $1 AND status = 'pending'`, id, result); err != nil {
		return fmt.Errorf("ack status update: %w", err)
	}

	if result == StatusFailed {
		// 失败传播：沿 predecessor_id 边递归展开全部后继，仅把仍 pending
		// （即从未送达、也无有效租约——它们在根失败前根本不可领取）的后继
		// 原子转为 blocked，阻断来源统一记为失败根 $1。
		// 已在终态的行保持原样；传播因此天然幂等。
		if _, err = tx.Exec(ctx, `
			WITH RECURSIVE descendants(did) AS (
				SELECT id FROM commands WHERE predecessor_id = $1
				UNION ALL
				SELECT c.id FROM commands c JOIN descendants d ON c.predecessor_id = d.did
			)
			UPDATE commands
			SET status = 'blocked', blocked_by = $1, updated_at = now()
			WHERE id IN (SELECT did FROM descendants)
			  AND status = 'pending'`, id); err != nil {
			return fmt.Errorf("ack block propagation: %w", err)
		}
	}

	return tx.Commit(ctx)
}

// scanArgs 适配 Command 字段顺序的扫描助手。
func scanArgs(c *Command) []any {
	return []any{
		&c.ID, &c.Payload, &c.Status, &c.LeaseGeneration, &c.LeaseExpiresAt,
		&c.PredecessorID, &c.BlockedBy, &c.CreatedAt, &c.UpdatedAt,
	}
}

// GetCommand 返回指令详情（含领取代次历史与终态）。未知编号返回 ErrCommandNotFound。
func (s *Store) GetCommand(ctx context.Context, id int64) (*CommandDetail, error) {
	d := &CommandDetail{}
	err := scanCommand(s.pool.QueryRow(ctx, `
		SELECT `+commandColumns+`
		FROM commands WHERE id = $1`, id), &d.Command)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrCommandNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get command: %w", err)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT generation, expires_at, claimed_at
		FROM leases WHERE command_id = $1
		ORDER BY generation`, id)
	if err != nil {
		return nil, fmt.Errorf("get leases: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var lv LeaseView
		if err := rows.Scan(&lv.Generation, &lv.ExpiresAt, &lv.ClaimedAt); err != nil {
			return nil, fmt.Errorf("scan lease: %w", err)
		}
		d.Leases = append(d.Leases, lv)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var st Settlement
	err = s.pool.QueryRow(ctx, `
		SELECT generation, result, settled_at
		FROM settlements WHERE command_id = $1`, id).Scan(&st.Generation, &st.Result, &st.SettledAt)
	switch {
	case errors.Is(err, nil):
		d.Settlement = &st
	case errors.Is(err, pgx.ErrNoRows):
	default:
		return nil, fmt.Errorf("get settlement: %w", err)
	}
	return d, nil
}

// ListCommands 按创建顺序返回指令简要列表。
func (s *Store) ListCommands(ctx context.Context, limit int) ([]Command, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+commandColumns+`
		FROM commands
		ORDER BY id
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("list commands: %w", err)
	}
	defer rows.Close()
	var out []Command
	for rows.Next() {
		var c Command
		if err := scanCommand(rows, &c); err != nil {
			return nil, fmt.Errorf("scan command: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
