package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/orimono/flow/internal/config"
	"github.com/orimono/flow/internal/engine"
	"github.com/orimono/flow/internal/rpc"
	"github.com/orimono/flow/internal/store"
)

func main() {
	cfg := config.MustLoad()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("failed to connect to postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	nc, err := nats.Connect(cfg.NatsURL)
	if err != nil {
		slog.Error("failed to connect to nats", "err", err)
		os.Exit(1)
	}
	defer nc.Drain()

	timeout := time.Duration(cfg.RequestTimeout)
	if timeout == 0 {
		timeout = 15 * time.Second
	}

	flowStore := store.NewFlowStore(pool)
	runStore := store.NewFlowRunStore(pool)
	eng := engine.New(nc, flowStore, runStore, timeout)

	rpc.Register(nc, rpc.Deps{
		FlowStore: flowStore,
		RunStore:  runStore,
		Engine:    eng,
	})

	slog.Info("flow service ready")
	<-ctx.Done()
	slog.Info("flow service shutting down")
}
