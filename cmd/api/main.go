// Command api 是深空测控指令裁决服务。
//
// 配置（环境变量）：
//
//	API_PORT                       监听端口，默认 8080（Docker Compose 通过它发布端口）
//	DATABASE_URL                   PostgreSQL 连接串，默认指向 compose 中的 db
//	DEEPSPACE_CLOCK_OFFSET_MS      （仅验收）实例本地时钟偏移毫秒数；缺省为正常行为
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"deepspace/internal/httpapi"
	"deepspace/internal/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	port := os.Getenv("API_PORT")
	if port == "" {
		port = "8080"
	}
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@db:5432/deepspace?sslmode=disable"
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 等待数据库就绪：重启/首启竞态下 Postgres 可能尚未接受连接。
	pool, err := connectWithRetry(ctx, dbURL, logger)
	if err != nil {
		logger.Error("database unreachable", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := store.Migrate(ctx, pool); err != nil {
		logger.Error("migrations failed", "error", err)
		os.Exit(1)
	}
	logger.Info("migrations applied")

	st := store.New(pool)
	// DEEPSPACE_CLOCK_OFFSET_MS（仅验收使用，缺省即正常生产行为）：为实例本地时钟
	// 注入固定偏移，供验收启动两台时钟分别偏移的实例，验证租约裁决不依赖实例时钟
	// （裁决以数据库时钟为准）。该偏移不影响任何裁决结论。
	if skewMS := os.Getenv("DEEPSPACE_CLOCK_OFFSET_MS"); skewMS != "" {
		ms, err := strconv.Atoi(skewMS)
		if err != nil {
			logger.Error("invalid DEEPSPACE_CLOCK_OFFSET_MS", "value", skewMS)
			os.Exit(1)
		}
		offset := time.Duration(ms) * time.Millisecond
		st = store.NewWithClock(pool, func() time.Time { return time.Now().Add(offset) })
		logger.Info("instance clock offset injected for acceptance", "offset_ms", ms)
	}
	srv := httpapi.New(st, logger)

	httpServer := &http.Server{
		Addr:              ":" + port,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- httpServer.ListenAndServe()
	}()
	logger.Info("server started", "port", port)

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server failed", "error", err)
			os.Exit(1)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
	}
	logger.Info("server stopped")
}

func connectWithRetry(ctx context.Context, url string, logger *slog.Logger) (*pgxpool.Pool, error) {
	var lastErr error
	for attempt := 1; attempt <= 30; attempt++ {
		pool, err := pgxpool.New(ctx, url)
		if err == nil {
			if pingErr := pool.Ping(ctx); pingErr == nil {
				return pool, nil
			} else {
				lastErr = pingErr
				pool.Close()
			}
		} else {
			lastErr = err
		}
		logger.Info("waiting for database", "attempt", attempt, "error", lastErr)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return nil, lastErr
}
