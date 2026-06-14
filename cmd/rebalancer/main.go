// Command rebalancer runs the auto-rebalance engine: a queue manager and
// workers that execute run_rebalancer, auto_schedule, and auto_enable using
// pgxpool and a shared LND connection.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/warioishere/lndg-blitz-go/internal/config"
	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	lnd "github.com/warioishere/lndg-blitz-go/internal/lnd"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/routerrpc"
	"github.com/warioishere/lndg-blitz-go/internal/rebalancer"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "rebalancer:", err)
		os.Exit(1)
	}
}

func run() error {
	// Shut down the loop cleanly on SIGINT/SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.Get()
	pool, err := pgxpool.New(ctx, cfg.DATABASE_URL)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer pool.Close()

	// Shared gRPC connection to LND (transient reconnects are handled by gRPC internally).
	conn, err := lnd.GetSharedChannel()
	if err != nil {
		return fmt.Errorf("connecting to lnd: %w", err)
	}

	// Context-aware sleep: returns immediately on cancellation so the
	// manager and workers exit promptly after SIGINT.
	sleep := func(d time.Duration) {
		select {
		case <-time.After(d):
		case <-ctx.Done():
		}
	}

	deps := rebalancer.NewQueueDeps(
		db.New(pool),
		lnrpc.NewLightningClient(conn),
		routerrpc.NewRouterClient(conn),
		time.Now,
		sleep,
	)
	deps.Run(ctx)
	return nil
}
