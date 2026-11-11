package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/zaynereo/durable-webhook-delivery/internal/config"
	"github.com/zaynereo/durable-webhook-delivery/internal/delivery"
	"github.com/zaynereo/durable-webhook-delivery/internal/destination"
	"github.com/zaynereo/durable-webhook-delivery/internal/migrate"
	"github.com/zaynereo/durable-webhook-delivery/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := store.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("db connect", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := migrate.Up(cfg.DatabaseURL); err != nil {
		log.Error("migrate", "err", err)
		os.Exit(1)
	}

	st := store.New(pool)
	allowlist, err := destination.NewAllowlist(cfg.AllowedDestinations)
	if err != nil {
		log.Error("allowlist", "err", err)
		os.Exit(1)
	}

	w := delivery.NewWorker(cfg, st, allowlist, log)

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		cancel()
	}()

	if err := w.Run(ctx); err != nil && err != context.Canceled {
		log.Error("worker", "err", err)
		os.Exit(1)
	}
}
