package store

import (
	"context"
	"embed"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations_sql/*.sql
var migrationFS embed.FS

// Migrate 以“全部包裹在单个事务中、IF NOT EXISTS 幂等”的方式执行内嵌迁移。
// 服务重启后重复调用安全；裁决所需结构始终存在。
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	entries, err := migrationFS.ReadDir("migrations_sql")
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("migrate begin: %w", err)
	}
	defer tx.Rollback(ctx)

	// 事务级咨询锁：多个进程/测试二进制同时启动时，迁移在此串行化，
	// 避免并发 CREATE INDEX IF NOT EXISTS 在 pg_class 上的唯一冲突。
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(918273645)"); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}

	for _, name := range names {
		sqlBytes, err := migrationFS.ReadFile("migrations_sql/" + name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, string(sqlBytes)); err != nil {
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
	}
	return tx.Commit(ctx)
}
