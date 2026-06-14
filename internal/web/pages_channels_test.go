package web

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warioishere/lndg-blitz-go/internal/config"
)

func TestChannelsPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	recent := time.Now().Add(-1 * time.Hour)

	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id": "111111111111111111", "short_chan_id": "111x222x1", "remote_pubkey": "03peer",
		"alias": "PerfPeer", "capacity": 10000000, "local_balance": 6000000, "remote_balance": 4000000,
		"pending_outbound": 0, "pending_inbound": 0, "num_updates": 100, "initiator": true,
		"is_open": true, "private": false,
	})

	// Outbound forward: amt_out=2,000,000,000 msat (-> 2.0M), fee 500.
	insertRow(t, pool, "gui_forwards", map[string]any{
		"id": 1, "forward_date": recent, "chan_id_in": "999", "chan_id_out": "111111111111111111",
		"amt_in_msat": 2000000000, "amt_out_msat": 2000000000, "fee": 500.0, "inbound_fee": 0.0,
	})
	// Inbound forward: amt_out=1,000,000,000 msat (-> 1.0M), fee 200 (assisted).
	insertRow(t, pool, "gui_forwards", map[string]any{
		"id": 2, "forward_date": recent, "chan_id_in": "111111111111111111", "chan_id_out": "999",
		"amt_in_msat": 1000000000, "amt_out_msat": 1000000000, "fee": 200.0, "inbound_fee": 0.0,
	})

	// Rebalance OUT payment (chan_out=A): value 500000 (-> 0.5M).
	insertRow(t, pool, "gui_payments", map[string]any{
		"creation_date": recent, "payment_hash": "rebaloutpay", "value": 500000.0, "fee": 15.0,
		"status": 2, "index": 1, "chan_out": "111111111111111111", "rebal_chan": "111111111111111111",
	})
	// Rebalance IN payment + invoice (chan_in=A): amt_paid 300000 (-> 0.3M), cost fee 8.
	insertRow(t, pool, "gui_payments", map[string]any{
		"creation_date": recent, "payment_hash": "rebalinpay", "value": 80000.0, "fee": 8.0,
		"status": 2, "index": 2, "chan_out": "999", "rebal_chan": "111111111111111111",
	})
	insertRow(t, pool, "gui_invoices", map[string]any{
		"creation_date": recent, "settle_date": recent, "r_hash": "rebalinpay", "value": 300000.0,
		"amt_paid": 300000, "state": 1, "index": 1, "chan_in": "111111111111111111", "is_revenue": false,
	})

	srv := NewServer(&config.Settings{}, pool)
	html := getPage(t, srv, "/channels/")

	require.Contains(t, html, "<title>LNDg - Channels</title>")
	require.Contains(t, html, "Channel Performance")
	require.Contains(t, html, "PerfPeer")
	require.Contains(t, html, "111x222x1")
	require.Contains(t, html, "10.0M") // mil_capacity

	// Node APY header.
	require.Contains(t, html, "APY: 0.43%")
	require.Contains(t, html, "APY: 0.1%")

	// 7-day activity.
	require.Contains(t, html, "2.0M") // amt_routed_out
	require.Contains(t, html, "1.0M") // amt_routed_in
	require.Contains(t, html, "0.5M") // amt_rebal_out
	require.Contains(t, html, "0.3M") // amt_rebal_in

	// Per-channel APY/CV.
	require.Contains(t, html, "0.43%") // apy_7day
	require.Contains(t, html, "0.7%")  // cv_7day
	require.Contains(t, html, "0.16%") // cv_30day

	// Revenue/profit/assisted.
	require.Contains(t, html, "500")
	require.Contains(t, html, "492")
	require.Contains(t, html, "200")

	require.Contains(t, html, "100%")  // updates
	require.Contains(t, html, "Local") // initiator true
}

func TestChannelsPageEmptyIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	srv := NewServer(&config.Settings{}, pool)
	html := getPage(t, srv, "/channels/")
	require.Contains(t, html, "You dont have any channels to analyze yet!")
	require.NotContains(t, html, "Channel Performance")
}
