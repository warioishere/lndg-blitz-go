package web

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/warioishere/lndg-blitz-go/internal/config"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

func TestUnprofitableChannelsPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	recent := time.Now().Add(-1 * time.Hour)

	// chan_id with block 799712 in the upper bits -> at block height 800000 that
	// is 288 blocks = 2 days old (very-new bonus).
	openingBlock := int64(799712)
	chanID := strconv.FormatInt(openingBlock<<40, 10)

	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id": chanID, "remote_pubkey": "03peera", "alias": "PeerA",
		"capacity": 10000000, "local_balance": 2000000, "remote_balance": 8000000,
		"local_fee_rate": 500, "initiator": true, "is_active": true, "is_open": true,
	})

	// Outbound forward via A: 5,000,000,000 msat -> 5,000,000 sats, fee 100.
	insertRow(t, pool, "gui_forwards", map[string]any{
		"id": 1, "forward_date": recent, "chan_id_in": "999", "chan_id_out": chanID,
		"amt_in_msat": 5000000000, "amt_out_msat": 5000000000, "fee": 100.0, "inbound_fee": 0.0,
	})
	// Inbound forward via A: assisted_revenue 50.
	insertRow(t, pool, "gui_forwards", map[string]any{
		"id": 2, "forward_date": recent, "chan_id_in": chanID, "chan_id_out": "999",
		"amt_in_msat": 1000000, "amt_out_msat": 1000000, "fee": 50.0, "inbound_fee": 0.0,
	})

	// Rebalance OUT payment (display): value 300000.
	insertRow(t, pool, "gui_payments", map[string]any{
		"creation_date": recent, "payment_hash": "rebalout", "value": 300000.0, "fee": 10.0,
		"status": 2, "index": 1, "chan_out": chanID, "rebal_chan": chanID,
	})
	// Rebalance IN payment + matching invoice -> rebalance_fee_cost 7.
	insertRow(t, pool, "gui_payments", map[string]any{
		"creation_date": recent, "payment_hash": "rebalin", "value": 50000.0, "fee": 7.0,
		"status": 2, "index": 2, "chan_out": "999", "rebal_chan": chanID,
	})
	insertRow(t, pool, "gui_invoices", map[string]any{
		"creation_date": recent, "settle_date": recent, "r_hash": "rebalin", "value": 50000.0,
		"amt_paid": 50000, "state": 1, "index": 1, "chan_in": chanID, "is_revenue": false,
	})

	fake := &fakeLightning{getInfoFn: func(_ context.Context, _ *lnrpc.GetInfoRequest, _ ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
		return &lnrpc.GetInfoResponse{BlockHeight: 800000}, nil
	}}
	srv := NewServer(&config.Settings{}, pool, WithLND(&LND{Lightning: fake}))

	html := getPage(t, srv, "/unprofitable_channels/")
	require.Contains(t, html, "<title>LNDg - Unprofitable Channels</title>")
	require.Contains(t, html, "Unprofitable Channels (30 Days)")
	require.Contains(t, html, "PeerA")
	require.Contains(t, html, chanID)
	require.Contains(t, html, "5,000,000") // routed out + last routing amount
	require.Contains(t, html, "300,000")   // rebalanced out + last rebalance amount
	require.Contains(t, html, "20.0% (10.0M)")
	require.Contains(t, html, ">93</td>")     // profit = fee_revenue 100 - rebalance cost 7
	require.Contains(t, html, "Local")        // initiator true
	require.Contains(t, html, "0.14")         // stuck index (priority 1.0 / 7)
	require.Contains(t, html, "color: green") // stuck index <= 0.4

	// Timeframe parameter is applied.
	html7 := getPage(t, srv, "/unprofitable_channels/?timeframe=7")
	require.Contains(t, html7, "Unprofitable Channels (7 Days)")
	require.Contains(t, html7, `value="7" selected`)
}
