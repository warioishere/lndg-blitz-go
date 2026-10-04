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

func TestApiIncomeIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()
	now := time.Now()

	insertRow(t, pool, "gui_forwards", map[string]any{
		"id": int64(1), "forward_date": now, "chan_id_in": "i", "chan_id_out": "o",
		"amt_in_msat": int64(2100), "amt_out_msat": int64(2000), "fee": 3.0, "inbound_fee": 0.0,
	})
	insertRow(t, pool, "gui_invoices", map[string]any{
		"creation_date": now, "settle_date": now, "r_hash": "h1", "value": 0.0,
		"amt_paid": int64(500), "state": 1, "index": 1, "is_revenue": true,
	})
	insertRow(t, pool, "gui_payments", map[string]any{
		"creation_date": now, "payment_hash": "p1", "value": 1000.0, "fee": 2.0,
		"status": 2, "index": 1, "cleaned": false,
	})
	insertRow(t, pool, "gui_onchain", map[string]any{"tx_hash": "tx1", "fee": 10, "time_stamp": now})
	insertRow(t, pool, "gui_closures", map[string]any{"id": int64(1), "chan_id": "c", "closing_costs": 5, "close_height": 790000})

	fake := &fakeLightning{getInfoFn: func(context.Context, *lnrpc.GetInfoRequest, ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
		return &lnrpc.GetInfoResponse{BlockHeight: 800000}, nil
	}}
	srv := NewServer(&config.Settings{}, pool, WithLND(&LND{Lightning: fake}))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	var body struct {
		Data map[string]any `json:"data"`
	}
	getJSON(t, ts.URL+"/api/income/", &body)
	d := body.Data
	require.EqualValues(t, 1, d["forward_count"])
	require.EqualValues(t, 2, d["forward_amount"])  // int(2000/1000)
	require.EqualValues(t, 503, d["total_revenue"]) // int(3) + 500
	require.EqualValues(t, 251500000, d["total_revenue_ppm"])
	require.EqualValues(t, 2, d["total_fees"])
	require.EqualValues(t, 2000, d["total_fees_ppm"]) // int(2/(1000/1e6))
	require.EqualValues(t, 15, d["onchain_costs"])    // 10 + 5
	require.EqualValues(t, 486, d["profits"])         // 503 - 2 - 15
	require.EqualValues(t, 243000000, d["profits_ppm"])
	require.EqualValues(t, 3, d["percent_cost"]) // int((17/503)*100)
	// "?=7": only the last 7 days, closures by close_height >= 800000 - 7*144
	insertRow(t, pool, "gui_forwards", map[string]any{
		"id": int64(2), "forward_date": now.AddDate(0, 0, -10), "chan_id_in": "i", "chan_id_out": "o",
		"amt_in_msat": int64(9000), "amt_out_msat": int64(8000), "fee": 100.0, "inbound_fee": 0.0,
	})
	insertRow(t, pool, "gui_closures", map[string]any{"id": int64(2), "chan_id": "d", "funding_txid": "f2", "closing_costs": 7, "close_height": 799900})
	body.Data = nil
	getJSON(t, ts.URL+"/api/income/?=7", &body)
	d = body.Data
	require.EqualValues(t, 1, d["forward_count"])
	require.EqualValues(t, 503, d["total_revenue"])
	require.EqualValues(t, 17, d["onchain_costs"]) // 10 + 7, the old closure is outside
	require.EqualValues(t, 484, d["profits"])

	body.Data = nil
	getJSON(t, ts.URL+"/api/income/?days=7", &body) // not "?=N": all-time, as in Python
	require.EqualValues(t, 2, body.Data["forward_count"])
}
