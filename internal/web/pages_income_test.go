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

func TestIncomePageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	recent := time.Now().Add(-1 * time.Hour)
	insertRow(t, pool, "gui_forwards", map[string]any{
		"id": 1, "forward_date": recent, "chan_id_in": "a", "chan_id_out": "b",
		"amt_in_msat": 1000000, "amt_out_msat": 1000000, "fee": 5.0, "inbound_fee": 0.0,
	})
	insertRow(t, pool, "gui_invoices", map[string]any{
		"creation_date": recent, "settle_date": recent, "r_hash": "inv1",
		"value": 100.0, "amt_paid": 100, "state": 1, "is_revenue": true, "index": 1,
	})
	insertRow(t, pool, "gui_payments", map[string]any{
		"creation_date": recent, "payment_hash": "pay1", "value": 2000.0, "fee": 10.0,
		"status": 2, "index": 1,
	})

	fake := &fakeLightning{getInfoFn: func(_ context.Context, _ *lnrpc.GetInfoRequest, _ ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
		return &lnrpc.GetInfoResponse{BlockHeight: 800000, IdentityPubkey: "03self", Alias: "MyNode"}, nil
	}}
	srv := NewServer(&config.Settings{}, pool, WithLND(&LND{Lightning: fake}))

	html := getPage(t, srv, "/income")
	require.Contains(t, html, "<title>LNDg - P&L</title>")
	require.Contains(t, html, "P&L Statement For")
	require.Contains(t, html, "MyNode")
	require.Contains(t, html, "1,000")      // forward_amount = 1000000/1000
	require.Contains(t, html, "105,000ₚₚₘ") // total_revenue_ppm
	require.Contains(t, html, "9%")         // percent_cost = int((10/105)*100)
	require.Contains(t, html, `<script src="/static/charts.js"></script>`)
}
