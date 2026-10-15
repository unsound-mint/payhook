// Command worker consumes payment events and notifies merchants.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/unsound-mint/payhook/internal/broker"
	"github.com/unsound-mint/payhook/internal/config"
	"github.com/unsound-mint/payhook/internal/db"
	"github.com/unsound-mint/payhook/internal/metrics"
	"github.com/unsound-mint/payhook/internal/notifier"
	"github.com/unsound-mint/payhook/internal/payment"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("worker stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := db.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool); err != nil {
		return err
	}
	js, err := broker.Connect(ctx, cfg.NATSURL)
	if err != nil {
		return err
	}
	defer js.Close()

	go func() {
		addr := os.Getenv("METRICS_ADDR")
		if addr == "" {
			addr = ":9102"
		}
		if err := metrics.Serve(addr); err != nil {
			log.Error("metrics server", "err", err)
		}
	}()

	log.Info("worker started")
	return notifier.New(pool, payment.NewStore(pool), cfg.CallbackSecret, log).Run(ctx, js.JS)
}
