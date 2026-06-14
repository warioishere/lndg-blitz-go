// Command controller is the process supervisor that starts all background
// daemons. Each daemon (jobs, rebalancer, htlc_stream, graph_watcher) runs
// in its own goroutine. p2p/trade support is intentionally omitted.
//
// Each daemon gets its OWN LND connection: jobs closes and reopens its
// connection on error, which would disrupt other daemons if they shared
// a connection.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	"github.com/warioishere/lndg-blitz-go/internal/config"
	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/graphwatcher"
	"github.com/warioishere/lndg-blitz-go/internal/htlcstream"
	"github.com/warioishere/lndg-blitz-go/internal/jobs"
	lnd "github.com/warioishere/lndg-blitz-go/internal/lnd"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/routerrpc"
	"github.com/warioishere/lndg-blitz-go/internal/rebalancer"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "controller:", err)
		os.Exit(1)
	}
}

// connHolder holds a dedicated jobs connection with lazy reconnect after reset.
type connHolder struct {
	mu   sync.Mutex
	conn *grpc.ClientConn
}

func (h *connHolder) get() (*grpc.ClientConn, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.conn == nil {
		c, err := lnd.Connect()
		if err != nil {
			return nil, err
		}
		h.conn = c
	}
	return h.conn, nil
}

func (h *connHolder) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.conn != nil {
		h.conn.Close()
		h.conn = nil
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

	// Dedicated connections per daemon.
	if _, err := lnd.Connect(); err != nil {
		return fmt.Errorf("connecting to lnd: %w", err)
	}
	rebConn, err := lnd.Connect()
	if err != nil {
		return fmt.Errorf("connecting to lnd: %w", err)
	}
	htlcConn, err := lnd.Connect()
	if err != nil {
		return fmt.Errorf("connecting to lnd: %w", err)
	}
	gwConn, err := lnd.Connect()
	if err != nil {
		return fmt.Errorf("connecting to lnd: %w", err)
	}

	q := db.New(pool)
	sleep := func(d time.Duration) {
		select {
		case <-time.After(d):
		case <-ctx.Done():
		}
	}

	fmt.Println("Controller is starting...")

	var wg sync.WaitGroup
	start := func(fn func()) {
		wg.Add(1)
		go func() { defer wg.Done(); fn() }()
	}

	// jobs (dedicated connection with reset support).
	jobsConn := &connHolder{}
	start(func() {
		deps := jobs.BuildDataDeps(q, jobsConn.get, jobsConn.reset, pool.Reset, 20*time.Second)
		_ = jobs.DataLoop(ctx, deps)
	})

	// rebalancer.
	start(func() {
		rebalancer.NewQueueDeps(q, lnrpc.NewLightningClient(rebConn), routerrpc.NewRouterClient(rebConn), time.Now, sleep).Run(ctx)
	})

	// htlc_stream.
	start(func() {
		htlcstream.Deps{Q: q, LN: lnrpc.NewLightningClient(htlcConn), Router: routerrpc.NewRouterClient(htlcConn), Now: time.Now, Sleep: sleep}.Run(ctx)
	})

	// graph_watcher.
	start(func() {
		graphwatcher.Deps{Q: q, LN: lnrpc.NewLightningClient(gwConn), Router: routerrpc.NewRouterClient(gwConn), Now: time.Now, Sleep: sleep}.Run(ctx)
	})

	wg.Wait()
	fmt.Println("Controller is stopping...")
	return nil
}
