package web

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/warioishere/lndg-blitz-go/internal/config"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

func TestBalancesIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id": "c1", "alias": "a1", "is_open": true,
		"local_balance": int64(100), "pending_outbound": int64(20),
	})

	fake := &fakeLightning{
		walletBalanceFn: func(context.Context, *lnrpc.WalletBalanceRequest, ...grpc.CallOption) (*lnrpc.WalletBalanceResponse, error) {
			return &lnrpc.WalletBalanceResponse{TotalBalance: 1000, ConfirmedBalance: 900, UnconfirmedBalance: 100}, nil
		},
		pendingChannelsFn: func(context.Context, *lnrpc.PendingChannelsRequest, ...grpc.CallOption) (*lnrpc.PendingChannelsResponse, error) {
			return &lnrpc.PendingChannelsResponse{
				TotalLimboBalance: 5,
				PendingOpenChannels: []*lnrpc.PendingChannelsResponse_PendingOpenChannel{
					{Channel: &lnrpc.PendingChannelsResponse_PendingChannel{LocalBalance: 50}},
				},
			}, nil
		},
	}
	srv := NewServer(&config.Settings{}, pool, WithLND(&LND{Lightning: fake}))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	var body struct {
		Message string         `json:"message"`
		Data    map[string]any `json:"data"`
	}
	getJSON(t, ts.URL+"/api/balances/", &body)
	require.Equal(t, "success", body.Message)
	// offchain = 100 + 20 + 50 + 5 = 175
	require.EqualValues(t, 175, body.Data["offchain_balance"])
	require.EqualValues(t, 1175, body.Data["total_balance"]) // 1000 + 175
	require.EqualValues(t, 1000, body.Data["onchain_balance"])
	require.EqualValues(t, 900, body.Data["confirmed_balance"])
}

func TestRebalanceStatsIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()
	now := time.Now()

	// A: 2 attempts (1 success), B: 1 attempt (1 success), C: old -> excluded.
	insertRow(t, pool, "gui_rebalancer", map[string]any{"id": int64(1), "last_hop_pubkey": "A", "status": 2, "stop": now})
	insertRow(t, pool, "gui_rebalancer", map[string]any{"id": int64(2), "last_hop_pubkey": "A", "status": 1, "stop": now})
	insertRow(t, pool, "gui_rebalancer", map[string]any{"id": int64(3), "last_hop_pubkey": "B", "status": 2, "stop": now})
	insertRow(t, pool, "gui_rebalancer", map[string]any{"id": int64(4), "last_hop_pubkey": "A", "status": 2, "stop": now.Add(-10 * 24 * time.Hour)})

	srv := NewServer(&config.Settings{}, pool)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	var stats []map[string]any
	getJSON(t, ts.URL+"/api/rebalance_stats/", &stats)

	byPubkey := map[string]map[string]any{}
	for _, s := range stats {
		byPubkey[s["last_hop_pubkey"].(string)] = s
	}
	require.Len(t, stats, 2) // C excluded
	require.EqualValues(t, 2, byPubkey["A"]["attempts"])
	require.EqualValues(t, 1, byPubkey["A"]["successes"])
	require.EqualValues(t, 1, byPubkey["B"]["attempts"])
	require.EqualValues(t, 1, byPubkey["B"]["successes"])
}

func TestRebalanceStatsNullSuccesses(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()
	now := time.Now()

	// Pubkey with no successes -> successes is NULL (SUM(CASE...) with no matches).
	insertRow(t, pool, "gui_rebalancer", map[string]any{"id": int64(1), "last_hop_pubkey": "X", "status": 1, "stop": now})

	srv := NewServer(&config.Settings{}, pool)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	var stats []map[string]any
	getJSON(t, ts.URL+"/api/rebalance_stats/", &stats)
	require.Len(t, stats, 1)
	require.EqualValues(t, 1, stats[0]["attempts"])
	require.Nil(t, stats[0]["successes"]) // NULL -> JSON null
}
