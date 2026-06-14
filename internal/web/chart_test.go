package web

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warioishere/lndg-blitz-go/internal/config"
)

func TestChartIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	day := time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC)
	// 1 onchain record (first==last -> default 10-min interval).
	insertRow(t, pool, "gui_onchain", map[string]any{
		"tx_hash": "tx1", "time_stamp": day.Add(12 * time.Hour), "block_height": 100, "fee": 5,
	})
	insertRow(t, pool, "gui_payments", map[string]any{
		"creation_date": day.Add(8 * time.Hour), "payment_hash": "p1", "value": 100.0, "fee": 3.0,
		"status": 2, "index": 1, "cleaned": false,
	})
	insertRow(t, pool, "gui_forwards", map[string]any{
		"id": int64(1), "forward_date": day.Add(9 * time.Hour), "chan_id_in": "i", "chan_id_out": "o",
		"amt_in_msat": int64(10), "amt_out_msat": int64(10), "fee": 2.0, "inbound_fee": 0.0,
	})
	// Closure: close_height=100 -> estimated date = offset + 100*10min = onchain-ts -> day 2024-01-10.
	insertRow(t, pool, "gui_closures", map[string]any{
		"id": int64(1), "chan_id": "c", "close_height": 100, "closing_costs": 7,
	})

	srv := NewServer(&config.Settings{}, pool)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	var results []map[string]any
	getJSON(t, ts.URL+"/api/chart/", &results)

	byDt := map[string]map[string]any{}
	for _, r := range results {
		byDt[r["dt"].(string)] = r
	}
	row, ok := byDt["2024-01-10T00:00:00"]
	require.True(t, ok, "expected bucket for 2024-01-10, got %v", results)
	require.EqualValues(t, 3, row["cost"])     // payment fee
	require.EqualValues(t, 2, row["revenue"])  // forward fee
	require.EqualValues(t, 12, row["onchain"]) // onchain 5 + closure 7
}
