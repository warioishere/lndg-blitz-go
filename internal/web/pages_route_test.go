package web

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/warioishere/lndg-blitz-go/internal/config"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

func TestRoutePageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	// gui_paymenthops.payment_hash_id -> gui_payments.payment_hash (FK).
	insertRow(t, pool, "gui_payments", map[string]any{
		"creation_date": time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC),
		"payment_hash":  "routehash", "value": 1000000.0, "fee": 15.0, "status": 2, "index": 1,
	})
	insertRow(t, pool, "gui_paymenthops", map[string]any{
		"id": 1, "payment_hash_id": "routehash", "attempt_id": 1, "step": 1,
		"chan_id": "111x1x0", "alias": "FirstHop", "chan_capacity": 5000000,
		"node_pubkey": "03nodeone", "amt": 1000000.0, "fee": 10.0, "cost_to": 10.0,
	})
	insertRow(t, pool, "gui_paymenthops", map[string]any{
		"id": 2, "payment_hash_id": "routehash", "attempt_id": 1, "step": 2,
		"chan_id": "222x1x0", "alias": "SecondHop", "chan_capacity": 4000000,
		"node_pubkey": "03nodetwo", "amt": 999990.0, "fee": 5.0, "cost_to": 15.0,
	})
	insertRow(t, pool, "gui_invoices", map[string]any{
		"creation_date": time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC),
		"settle_date":   time.Date(2024, 6, 1, 12, 1, 0, 0, time.UTC),
		"r_hash":        "routehash", "value": 1000.0, "amt_paid": 1000, "state": 1, "index": 1,
	})
	insertRow(t, pool, "gui_pendinghtlcs", map[string]any{
		"id": 1, "chan_id": "111x1x0", "alias": "HtlcPeer", "incoming": true,
		"amount": 50000, "hash_lock": "routehash", "expiration_height": 800144,
		"forwarding_channel": "0", "forwarding_alias": "",
	})

	fake := &fakeLightning{getInfoFn: func(_ context.Context, _ *lnrpc.GetInfoRequest, _ ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
		return &lnrpc.GetInfoResponse{BlockHeight: 800000}, nil
	}}
	srv := NewServer(&config.Settings{}, pool, WithLND(&LND{Lightning: fake}))

	html := getPage(t, srv, "/route?=routehash")
	require.Contains(t, html, "<title>LNDg - Routes</title>")
	require.Contains(t, html, "Route For : routehash")
	require.Contains(t, html, "Total Costs: 15.0 [15]") // round(10+5,3)=15.0; ppm=15
	require.Contains(t, html, "FirstHop")
	require.Contains(t, html, "SecondHop")
	require.Contains(t, html, "Linked Invoice")
	require.Contains(t, html, "Incoming HTLCs")
	require.Contains(t, html, "24 hours")
	// forwarding_channel "0" -> "---" (no link to /channel?=0).
	require.Contains(t, html, ">---<")
	require.NotContains(t, html, "/channel?=0\"")
	// rebalances_table partial included (Linked Rebalance).
	require.Contains(t, html, "Last Linked Rebalance")
	require.Contains(t, html, `<script src="/static/rebalances_table.js"></script>`)
}

func TestRoutesPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	insertRow(t, pool, "gui_payments", map[string]any{
		"creation_date": time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC),
		"payment_hash":  "routeshash", "value": 1000000.0, "fee": 7.0, "status": 2, "index": 1,
	})
	insertRow(t, pool, "gui_paymenthops", map[string]any{
		"id": 1, "payment_hash_id": "routeshash", "attempt_id": 1, "step": 1,
		"chan_id": "333x1x0", "alias": "ViaNode", "chan_capacity": 5000000,
		"node_pubkey": "03routenode", "amt": 1000000.0, "fee": 7.0, "cost_to": 7.0,
	})

	// routes() makes no LND calls -> no WithLND needed.
	srv := NewServer(&config.Settings{}, pool)
	html := getPage(t, srv, "/routes?=03routenode")
	require.Contains(t, html, "<title>LNDg - Routes</title>")
	require.Contains(t, html, "Route For : 03routenode")
	require.Contains(t, html, "ViaNode")
	require.NotContains(t, html, "Total Costs") // routes does not set total_cost
}

func TestRebalanceRouteDetailPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	insertRow(t, pool, "gui_rebalanceroute", map[string]any{
		"id": 1, "target_pubkey": "03target", "outgoing_chan_id": "555",
		"route": "03self-03peer-03target",
	})
	insertRow(t, pool, "gui_peers", map[string]any{
		"pubkey": "03peer", "alias": "PeerMid", "address": "1.2.3.4:9735",
		"sat_sent": 0, "sat_recv": 0, "inbound": false, "connected": true,
	})
	insertRow(t, pool, "gui_peers", map[string]any{
		"pubkey": "03target", "alias": "TargetNode", "address": "5.6.7.8:9735",
		"sat_sent": 0, "sat_recv": 0, "inbound": false, "connected": true,
	})
	// Successful rebalance via this source-chan/target.
	insertRow(t, pool, "gui_rebalancer", map[string]any{
		"id": 1, "value": 100000, "fee_limit": 50.0, "duration": 5,
		"outgoing_chan_ids": "[555]", "last_hop_pubkey": "03target",
		"status": 2, "payment_hash": "rebalhash",
	})
	// PaymentHops path == route -> match. FK: payment_hash_id -> payments.
	insertRow(t, pool, "gui_payments", map[string]any{
		"creation_date": time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC),
		"payment_hash":  "rebalhash", "value": 100000.0, "fee": 50.0, "status": 2, "index": 1,
	})
	for i, pub := range []string{"03self", "03peer", "03target"} {
		insertRow(t, pool, "gui_paymenthops", map[string]any{
			"id": i + 1, "payment_hash_id": "rebalhash", "attempt_id": 1, "step": i + 1,
			"chan_id": "555", "alias": "", "chan_capacity": 1000000,
			"node_pubkey": pub, "amt": 100000.0, "fee": 50.0, "cost_to": 50.0,
		})
	}
	insertRow(t, pool, "gui_invoices", map[string]any{
		"creation_date": time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC),
		"r_hash":        "rebalhash", "value": 100.0, "amt_paid": 100, "state": 1, "index": 1,
	})

	fake := &fakeLightning{getInfoFn: func(_ context.Context, _ *lnrpc.GetInfoRequest, _ ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
		return &lnrpc.GetInfoResponse{IdentityPubkey: "03self", Alias: "MyNode"}, nil
	}}
	srv := NewServer(&config.Settings{}, pool, WithLND(&LND{Lightning: fake}))

	html := getPage(t, srv, "/rebalanceroute/1")
	require.Contains(t, html, "<title>LNDg - Saved Route</title>")
	require.Contains(t, html, "Saved Route")
	require.Contains(t, html, "MyNode")     // self alias (03self)
	require.Contains(t, html, "PeerMid")    // peer lookup
	require.Contains(t, html, "TargetNode") // peer lookup
	require.Contains(t, html, "Linked Rebalance")
	require.Contains(t, html, "rebalhash")
	require.Contains(t, html, "Linked Invoice")

	// Unknown id -> error.html.
	htmlMissing := getPage(t, srv, "/rebalanceroute/999")
	require.Contains(t, htmlMissing, "An error has occured!")
	require.Contains(t, htmlMissing, "No RebalanceRoute matches the given query.")
}
