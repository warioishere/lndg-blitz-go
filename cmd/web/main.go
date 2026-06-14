// Command web starts the HTTP server (chi router) with pgxpool as the DB
// backend. LND clients are wired up for the action endpoints.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/warioishere/lndg-blitz-go/internal/config"
	lnd "github.com/warioishere/lndg-blitz-go/internal/lnd"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/routerrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/walletrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/wtclientrpc"
	"github.com/warioishere/lndg-blitz-go/internal/web"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "web:", err)
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

	// Dedicated LND connection so a connection reset elsewhere does not affect
	// the web server. grpc.NewClient is lazy — if LND is unavailable, individual
	// RPC calls fail rather than startup failing.
	var lndDeps *web.LND
	conn, err := lnd.Connect()
	if err != nil {
		fmt.Fprintln(os.Stderr, "web: warning: connecting to lnd:", err)
	} else {
		defer conn.Close()
		lndDeps = &web.LND{
			Lightning:  lnrpc.NewLightningClient(conn),
			Wallet:     walletrpc.NewWalletKitClient(conn),
			Router:     routerrpc.NewRouterClient(conn),
			Watchtower: wtclientrpc.NewWatchtowerClientClient(conn),
		}
	}

	srv := web.NewServer(cfg, pool, web.WithLND(lndDeps))

	httpServer := &http.Server{
		Addr:    cfg.WEB_BIND_ADDR,
		Handler: srv.Handler(),
	}

	errc := make(chan error, 1)
	go func() {
		fmt.Printf("web: listening on %s\n", cfg.WEB_BIND_ADDR)
		errc <- httpServer.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return httpServer.Shutdown(shutdownCtx)
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
