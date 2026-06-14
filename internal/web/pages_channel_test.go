package web

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warioishere/lndg-blitz-go/internal/config"
)

func TestChannelDetailPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	recent := time.Now().Add(-1 * time.Hour)
	chanID := "850000000000000000"
	pubkey := "03peerchan"

	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id": chanID, "short_chan_id": "777x888x1", "remote_pubkey": pubkey,
		"alias": "ChanPeer", "capacity": 10000000, "local_balance": 6000000, "remote_balance": 4000000,
		"pending_outbound": 0, "pending_inbound": 0, "num_updates": 100,
		"local_fee_rate": 200, "remote_fee_rate": 100, "ar_in_target": 50, "ar_max_cost": 50,
		"ar_amt_target": 1000000, "auto_rebalance": false, "auto_fees": true,
		"local_disabled": false, "remote_disabled": false, "is_active": true, "is_open": true,
		"private": false, "initiator": true, "last_update": recent, "fees_updated": recent,
		"notes": "hello notes",
	})

	// Forwards (within 1 day -> all windows equal).
	insertRow(t, pool, "gui_forwards", map[string]any{
		"id": 1, "forward_date": recent, "chan_id_in": "999", "chan_id_out": chanID,
		"amt_in_msat": 2000000000, "amt_out_msat": 2000000000, "fee": 500.0, "inbound_fee": 0.0,
	})
	insertRow(t, pool, "gui_forwards", map[string]any{
		"id": 2, "forward_date": recent, "chan_id_in": chanID, "chan_id_out": "999",
		"amt_in_msat": 1000000000, "amt_out_msat": 1000000000, "fee": 300.0, "inbound_fee": 0.0,
	})

	// Rebalance OUT (chan_out=chan).
	insertRow(t, pool, "gui_payments", map[string]any{
		"creation_date": recent, "payment_hash": "rebaloutpay", "value": 500000.0, "fee": 15.0,
		"status": 2, "index": 1, "chan_out": chanID, "rebal_chan": "999",
	})
	// Rebalance IN (payment rebal_chan=chan + matching invoice -> cost 8).
	insertRow(t, pool, "gui_payments", map[string]any{
		"creation_date": recent, "payment_hash": "rebinhash", "value": 2000.0, "fee": 8.0,
		"status": 2, "index": 2, "chan_out": "999", "rebal_chan": chanID,
	})
	insertRow(t, pool, "gui_invoices", map[string]any{
		"creation_date": recent, "settle_date": recent, "r_hash": "rebinhash", "value": 2000.0,
		"amt_paid": 2000, "state": 1, "index": 1, "chan_in": chanID, "is_revenue": false,
	})

	// Rebalancer: 1 success (status=2) + 1 attempt (status=3) -> success_rate 50%.
	insertRow(t, pool, "gui_rebalancer", map[string]any{
		"id": 1, "requested": recent, "value": 1000, "fee_limit": 1.0, "duration": 1,
		"stop": recent, "status": 2, "last_hop_pubkey": pubkey,
	})
	insertRow(t, pool, "gui_rebalancer", map[string]any{
		"id": 2, "requested": recent, "value": 1000, "fee_limit": 1.0, "duration": 1,
		"stop": recent, "status": 3, "last_hop_pubkey": pubkey,
	})

	// Failed HTLC due to insufficient outbound liquidity.
	insertRow(t, pool, "gui_failedhtlcs", map[string]any{
		"id": 1, "timestamp": recent, "amount": 1000, "chan_id_in": "999", "chan_id_out": chanID,
		"chan_out_liq": 100, "chan_out_pending": 0, "wire_failure": 15, "failure_detail": 6,
		"missed_fee": 0.0,
	})

	// Autofee log.
	insertRow(t, pool, "gui_autofees", map[string]any{
		"id": 1, "timestamp": recent, "chan_id": chanID, "peer_alias": "ChanPeer",
		"setting": "Update", "old_value": 100, "new_value": 150,
	})

	// Peer.
	insertRow(t, pool, "gui_peers", map[string]any{
		"pubkey": pubkey, "alias": "ChanPeer", "address": "1.2.3.4:9735",
		"sat_sent": 0, "sat_recv": 0, "inbound": false, "connected": true,
	})

	// Pending HTLC (outgoing, forwarding_channel=0 -> Self).
	insertRow(t, pool, "gui_pendinghtlcs", map[string]any{
		"id": 1, "chan_id": chanID, "alias": "ChanPeer", "incoming": false, "amount": 5000,
		"hash_lock": "abcdef", "expiration_height": 800000, "forwarding_channel": "0",
		"forwarding_alias": "",
	})

	srv := NewServer(&config.Settings{}, pool)
	html := getPage(t, srv, "/channel?="+chanID)

	require.Contains(t, html, "<title>LNDg - Channel Card</title>")
	require.Contains(t, html, "ChanPeer")
	require.Contains(t, html, chanID)
	require.Contains(t, html, "777x888x1")
	require.Contains(t, html, "Connected")
	require.Contains(t, html, "1.2.3.4:9735")
	require.Contains(t, html, "60%") // out_percent
	require.Contains(t, html, "40%") // in_percent
	require.Contains(t, html, "Local")

	// Routing/rebalance amounts (7-day = lifetime here).
	require.Contains(t, html, "2,000,000") // amt_routed_out + average_out
	require.Contains(t, html, "1,000,000") // amt_routed_in
	require.Contains(t, html, "500,000")   // amt_rebal_out
	require.Contains(t, html, "2,000")     // amt_rebal_in

	// Profitability.
	require.Contains(t, html, "492 [246]") // profits + profits_vol
	require.Contains(t, html, "1/2")       // success/attempts
	require.Contains(t, html, "50%")       // success_rate

	// APY/iAPY/CV (7-day).
	require.Contains(t, html, "0.43%") // apy_7day
	require.Contains(t, html, "0.39%") // assisted_apy_7day
	require.Contains(t, html, "0.82%") // cv_7day

	// Channel settings.
	require.Contains(t, html, ">250<")  // out_rate
	require.Contains(t, html, ">4000<") // rebal_ppm
	require.Contains(t, html, "0.6")    // assisted_ratio
	require.Contains(t, html, "Fee Ratio: 50%")

	// Failed HTLCs + autofees + notes + HTLC.
	require.Contains(t, html, "Lifetime: 1")
	require.Contains(t, html, "50.0%") // autofee change
	require.Contains(t, html, "hello notes")
	require.Contains(t, html, "Self") // forwarding_channel 0
}

func TestChannelDetailNotFoundIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	srv := NewServer(&config.Settings{}, pool)
	html := getPage(t, srv, "/channel?=doesnotexist")
	require.Contains(t, html, "No data found for this channel!")
}
