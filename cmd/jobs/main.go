// Command jobs connects a pgxpool and shared LND channel to jobs.DataLoop,
// running the background data-collection loop.
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
	"github.com/warioishere/lndg-blitz-go/internal/jobs"
	lnd "github.com/warioishere/lndg-blitz-go/internal/lnd"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "jobs:", err)
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

	// Establish the initial LND connection; errors surface here rather than later.
	if _, err := lnd.GetSharedChannel(); err != nil {
		return fmt.Errorf("connecting to lnd: %w", err)
	}

	deps := jobs.BuildDataDeps(
		db.New(pool),
		lnd.GetSharedChannel,
		func() {
			lnd.CloseSharedChannel()
			// Reopen; an error here re-enters the error path on the next iteration.
			_, _ = lnd.GetSharedChannel()
		},
		pool.Reset,
		20*time.Second,
	)

	if err := jobs.DataLoop(ctx, deps); err != nil && ctx.Err() == nil {
		return err
	}
	return nil
}
