// Command htlcstream subscribes to LND HTLC events and records failed
// forwards in gui_failedhtlcs.
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
	"github.com/warioishere/lndg-blitz-go/internal/htlcstream"
	lnd "github.com/warioishere/lndg-blitz-go/internal/lnd"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/routerrpc"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "htlcstream:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.Get()
	pool, err := pgxpool.New(ctx, cfg.DATABASE_URL)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer pool.Close()

	conn, err := lnd.GetSharedChannel()
	if err != nil {
		return fmt.Errorf("connecting to lnd: %w", err)
	}

	sleep := func(d time.Duration) {
		select {
		case <-time.After(d):
		case <-ctx.Done():
		}
	}

	deps := htlcstream.Deps{
		Q:      db.New(pool),
		LN:     lnrpc.NewLightningClient(conn),
		Router: routerrpc.NewRouterClient(conn),
		Now:    time.Now,
		Sleep:  sleep,
	}
	deps.Run(ctx)
	return nil
}
