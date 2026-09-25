// Package testdb 为集成测试提供相互隔离的临时 PostgreSQL 数据库。
//
// 每个测试（或每个调用）获得独立数据库：既避免并行测试间 TRUNCATE 相互干扰，
// 也避免并发迁移在系统目录上冲突。数据库在测试结束时自动删除。
package testdb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewEmpty 创建随机命名的空测试数据库（不执行迁移），返回连接池与连接串。
func NewEmpty(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()

	adminURL := envOr("DEEPSPACE_TEST_ADMIN_URL", "postgres://postgres@localhost:5432/postgres?sslmode=disable")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 管理池不能在函数返回时关闭——删库清理在测试末尾才执行，因此交给 t.Cleanup。
	admin, err := pgxpool.New(ctx, adminURL)
	if err != nil {
		t.Skipf("test database unavailable: %v", err)
	}
	if err := admin.Ping(ctx); err != nil {
		admin.Close()
		t.Skipf("test database unreachable: %v", err)
	}
	t.Cleanup(admin.Close)

	name := "deepspace_test_" + randSuffix()
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %q`, name)); err != nil {
		t.Fatalf("create test database %s: %v", name, err)
	}

	dbURL, err := dbURLWithName(adminURL, name)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		t.Fatal(err)
	}

	// Cleanup 按 LIFO 执行：先断开并删除数据库，最后关管理池。
	// 先 terminate 残留连接再 DROP，避免 “database is in use”。
	t.Cleanup(func() {
		cleanupCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		_, _ = admin.Exec(cleanupCtx, `
			SELECT pg_terminate_backend(pid) FROM pg_stat_activity
			WHERE datname = $1 AND pid <> pg_backend_pid()`, name)
		// terminate 是异步通知，稍等连接退出后再删库。
		time.Sleep(50 * time.Millisecond)
		if _, err := admin.Exec(cleanupCtx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q`, name)); err != nil {
			t.Errorf("drop test database %s: %v", name, err)
		}
	})
	// 关闭测试池必须先于删库（LIFO 中后注册先执行，故最后注册）。
	t.Cleanup(pool.Close)

	return pool, dbURL
}

// New 创建随机命名的测试数据库并返回指向它的连接池（表结构由调用方迁移）。
func New(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, _ := NewEmpty(t)
	return pool
}

func randSuffix() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
