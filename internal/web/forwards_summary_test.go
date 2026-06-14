package web

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warioishere/lndg-blitz-go/internal/config"
)

func TestForwardsSummaryIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()
	now := time.Now()

	// Recent forward X->Y; old forward (>7d) on Z -> excluded.
	insertRow(t, pool, "gui_forwards", map[string]any{
		"id": int64(1), "forward_date": now, "chan_id_in": "Y", "chan_id_out": "X",
		"amt_in_msat": int64(2100), "amt_out_msat": int64(2000), "fee": 1.5, "inbound_fee": 0.0,
	})
	insertRow(t, pool, "gui_forwards", map[string]any{
		"id": int64(2), "forward_date": now.Add(-10 * 24 * time.Hour), "chan_id_in": "Z", "chan_id_out": "Z",
		"amt_in_msat": int64(1), "amt_out_msat": int64(1), "fee": 0.0, "inbound_fee": 0.0,
	})

	srv := NewServer(&config.Settings{}, pool)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	var body struct {
		Results []map[string]any `json:"results"`
	}
	getJSON(t, ts.URL+"/api/forwards_summary/", &body)

	byChan := map[string]map[string]any{}
	for _, r := range body.Results {
		byChan[r["chan_id"].(string)] = r
	}
	require.Len(t, body.Results, 2) // OUT row X + IN row Y; Z excluded
	require.Contains(t, byChan, "X")
	require.Contains(t, byChan, "Y")
	require.NotContains(t, byChan, "Z")

	out := byChan["X"]
	require.EqualValues(t, 1, out["count_outgoing_1day"])
	require.EqualValues(t, 2000, out["sum_outgoing_1day"])
	require.EqualValues(t, 1.5, out["sum_fees_7day"])
	require.EqualValues(t, 0, out["count_incoming_1day"])

	in := byChan["Y"]
	require.EqualValues(t, 1, in["count_incoming_1day"])
	require.EqualValues(t, 2100, in["sum_incoming_7day"])
	require.EqualValues(t, 0, in["count_outgoing_1day"])
}
