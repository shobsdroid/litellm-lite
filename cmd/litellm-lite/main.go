// litellm-lite: a single binary hosting the gateway, control plane and workers.
//
//	litellm-lite migrate   apply Postgres and ClickHouse schema
//	litellm-lite seed      load prices.yaml, create a vendor/project/keys for local use
//	litellm-lite serve     run everything
//	litellm-lite load      drive the gateway with synthetic traffic
package main

import (
	"context"
	"embed"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rzp/litellm-lite/internal/adapters"
	"github.com/rzp/litellm-lite/internal/analytics"
	"github.com/rzp/litellm-lite/internal/budget"
	"github.com/rzp/litellm-lite/internal/catalog"
	"github.com/rzp/litellm-lite/internal/config"
	"github.com/rzp/litellm-lite/internal/controlplane"
	"github.com/rzp/litellm-lite/internal/events"
	"github.com/rzp/litellm-lite/internal/gateway"
	"github.com/rzp/litellm-lite/internal/ledger"
	"github.com/rzp/litellm-lite/internal/pricing"
	"github.com/rzp/litellm-lite/internal/ratelimit"
	"github.com/rzp/litellm-lite/internal/router"
	"github.com/rzp/litellm-lite/internal/secrets"
	"github.com/rzp/litellm-lite/internal/store"
	"github.com/rzp/litellm-lite/internal/webhooks"
)

//go:embed all:migrations
var migrations embed.FS

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: litellm-lite migrate|seed|serve|load")
		os.Exit(2)
	}
	cfg, err := config.Load()
	if err != nil {
		fatal(err)
	}
	lvl := slog.LevelInfo
	if cfg.LogLevel == "debug" {
		lvl = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	switch os.Args[1] {
	case "migrate":
		fatal(migrate(ctx, cfg))
	case "seed":
		fatal(seed(ctx, cfg))
	case "serve":
		fatal(serve(ctx, cfg))
	case "load":
		fatal(load(ctx, cfg, os.Args[2:]))
	default:
		fmt.Println("unknown command", os.Args[1])
		os.Exit(2)
	}
}

func fatal(err error) {
	if err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func migrate(ctx context.Context, cfg *config.Config) error {
	db, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := store.Migrate(ctx, db, migrations, "migrations/postgres"); err != nil {
		return err
	}
	ch, err := analytics.Open(ctx, cfg.ClickHouseAddr, cfg.ClickHouseDB)
	if err != nil {
		return err
	}
	if err := ch.Migrate(ctx, migrations, "migrations/clickhouse"); err != nil {
		return err
	}
	slog.Info("migrations applied")
	return nil
}

func serve(ctx context.Context, cfg *config.Config) error {
	db, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, PoolSize: 256, MinIdleConns: 16, DialTimeout: 2 * time.Second, ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second})
	if err := rdb.Ping(ctx).Err(); err != nil {
		slog.Warn("redis unreachable at start; budgets will be enforced locally until it returns", "err", err)
	}
	ch, err := analytics.Open(ctx, cfg.ClickHouseAddr, cfg.ClickHouseDB)
	if err != nil {
		return fmt.Errorf("clickhouse: %w (run `make infra && make migrate`)", err)
	}
	keyring, err := secrets.NewEnvKeyring(cfg.EncryptionKey)
	if err != nil {
		return err
	}
	if err := pricing.Default().Warm("o200k_base"); err != nil {
		return err
	}

	loader := catalog.NewLoader(db, rdb, keyring, cfg.ConfigReloadInterval, cfg.KeyCacheTTL)
	if err := loader.Load(ctx); err != nil {
		return fmt.Errorf("initial config load: %w", err)
	}
	enforcer := budget.New(rdb, cfg.ReservationTTL, cfg.DegradedTolerancePct)
	limiter := ratelimit.New(rdb)
	breakers := router.NewBreakers()
	bus := events.NewChannelBus(65536, cfg.SpillDir)
	bus.Subscribe("clickhouse", ch.Handler())
	bus.Subscribe("ledger", ledger.New(db).Handler())
	dispatcher := webhooks.New(db, keyring)
	dispatcher.FastRetry = true

	gw := gateway.New(cfg, loader, enforcer, limiter, breakers, adapters.NewRegistry(), bus, db)
	cp := controlplane.New(db, ch, keyring, enforcer, breakers, loader)

	go loader.Run(ctx)
	go catalog.DrainOutbox(ctx, db, rdb)
	go enforcer.Run(ctx)
	go bus.Run(ctx)
	go dispatcher.Run(ctx)
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				breakers.Tick()
			}
		}
	}()

	gwSrv := &http.Server{Addr: cfg.GatewayAddr, Handler: gw.Router(), ReadHeaderTimeout: 10 * time.Second}
	cpSrv := &http.Server{Addr: cfg.ControlPlaneAddr, Handler: cp.Router(), ReadHeaderTimeout: 10 * time.Second}
	errc := make(chan error, 2)
	go func() { errc <- gwSrv.ListenAndServe() }()
	go func() { errc <- cpSrv.ListenAndServe() }()
	slog.Info("serving", "gateway", cfg.GatewayAddr, "controlplane", cfg.ControlPlaneAddr, "deployments", len(loader.Snapshot().Deployments))

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	slog.Info("shutting down: draining in-flight requests")
	sctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	_ = gwSrv.Shutdown(sctx)
	_ = cpSrv.Shutdown(sctx)
	time.Sleep(1500 * time.Millisecond) // let the bus flush its last batch
	return nil
}
