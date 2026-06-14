package web

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/warioishere/lndg-blitz-go/internal/config"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

func TestHomePageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	fake := &fakeLightning{getInfoFn: func(_ context.Context, _ *lnrpc.GetInfoRequest, _ ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
		return &lnrpc.GetInfoResponse{
			Color:          "#aabbcc",
			Alias:          "MyNode",
			Version:        "0.18.0-beta",
			IdentityPubkey: "03nodepubkey",
			Uris:           []string{"03nodepubkey@1.2.3.4:9735"},
		}, nil
	}}
	srv := NewServer(&config.Settings{}, pool, WithLND(&LND{Lightning: fake}))

	html := getPage(t, srv, "/")
	require.Contains(t, html, "<title>LNDg - Dashboard</title>")
	require.Contains(t, html, "MyNode")
	require.Contains(t, html, "03nodepubkey")
	require.Contains(t, html, "#aabbcc")                   // node color
	require.Contains(t, html, "0.18.0-beta")               // LND version title
	require.Contains(t, html, "03nodepubkey@1.2.3.4:9735") // URI
	require.Contains(t, html, `checked="true"`)            // auto-refresh on (no cookie)
	require.Contains(t, html, "Unprofitable Channels")     // nav bar
	require.Contains(t, html, "Channel Performance")
	require.Contains(t, html, "Auto-Rebalancer")    // local_settings include
	require.Contains(t, html, "Rebalance Requests") // rebalances_table include
	require.Contains(t, html, "Connect to a Peer")
	require.Contains(t, html, "Sign a Message")
	require.Contains(t, html, "/static/home.js")
	require.Contains(t, html, "/static/rebalances_table.js")
}

func TestHomePageGetInfoErrorIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	fake := &fakeLightning{getInfoFn: func(_ context.Context, _ *lnrpc.GetInfoRequest, _ ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
		return nil, status.Error(codes.Unavailable, "node down")
	}}
	srv := NewServer(&config.Settings{}, pool, WithLND(&LND{Lightning: fake}))

	html := getPage(t, srv, "/")
	require.Contains(t, html, "StatusCode.UNAVAILABLE")
}
