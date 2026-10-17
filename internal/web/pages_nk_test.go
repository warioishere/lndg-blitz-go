package web

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warioishere/lndg-blitz-go/internal/config"
)

func TestNkPagesIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	for _, id := range []string{"700", "701"} {
		insertRow(t, pool, "gui_channels", map[string]any{
			"chan_id": id, "short_chan_id": "1x1x" + id, "alias": "NKPeer" + id, "remote_pubkey": "02nk" + id,
			"funding_txid": "f" + id, "output_index": 0, "local_fee_rate": 800, "is_open": true, "is_active": true,
			"auto_fees": true, "ar_in_target": 60, "ar_max_cost": 90, "capacity": 1000000, "local_balance": 300000,
		})
	}
	// 700: the prober measured 640 ppm at 80k; 701: no probe got through
	_, err0 := pool.Exec(ctx, `UPDATE gui_channels SET refill_ppm = CASE chan_id WHEN '700' THEN 640 END,
		refill_amount_sat = CASE chan_id WHEN '700' THEN 80000 END, refill_checked = '2026-10-17 21:00:00+00'`)
	require.NoError(t, err0)
	insertRow(t, pool, "nk_fee_log", map[string]any{
		"timestamp": time.Now(), "chan_id": "700", "peer_alias": "NKPeer700", "applied": false, "reason": "below target",
		"local_pct": 30.0, "target_pct": 40, "fee_old": 800, "fee_new": 824, "cap_ppm": 1000,
		"max_htlc_old": 990000000, "max_htlc_new": 250000000, "inbound_old": 0, "inbound_new": 0,
	})

	// 100k sold at 800 ppm and a 100k balance-out valued at 1000 ppm: 900 ppm
	insertRow(t, pool, "gui_forwards", map[string]any{
		"forward_date": time.Now().Add(-time.Hour), "chan_id_in": "701", "chan_id_out": "700",
		"amt_in_msat": 100080000, "amt_out_msat": 100000000, "fee": 80.0, "inbound_fee": 0.0,
	})
	insertRow(t, pool, "gui_payments", map[string]any{
		"creation_date": time.Now().Add(-2 * time.Hour), "payment_hash": "nkpay", "value": 100000.0, "fee": 10.0,
		"status": 2, "index": 1, "cleaned": false, "chan_out": "700", "rebal_chan": "701", "source_fee_rate": 1000,
	})

	srv := NewServer(&config.Settings{}, pool)
	html := getPage(t, srv, "/nk/")
	require.Contains(t, html, "<td>900</td>", "sold price 24 h")
	require.Contains(t, html, "<title>LNDg - NK Fee Mode</title>")
	require.Contains(t, html, "NKPeer700")
	require.Contains(t, html, `100 - iTarget; set iTarget on the main page or switch the channel to NK">40</span>`, "an LNDg channel's target is 100 - ar_in_target, read-only")
	require.Contains(t, html, "below target: 800 → 824 (shadow)")
	require.Contains(t, html, "NK-StepUp", "settings block")
	require.Contains(t, html, `<td title="route fee ppm before the source fee; 80,000 sats got through (10-17 21:00 UTC)">640</td>`)
	require.Contains(t, html, `<td title="no probe reached us over the peer down to AR-MinAmount (10-17 21:00 UTC)">none</td>`)
	require.Contains(t, getPage(t, srv, "/channel?=700"), `80,000 sats got through (10-17 21:00 UTC)">640</td>`, "channel page")

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// rebalancer budget fee x max cost, NK budget below the fee
	var budgets []map[string]any
	getJSON(t, ts.URL+"/api/nk_budgets/", &budgets)
	require.Len(t, budgets, 2)
	require.Equal(t, "700", budgets[0]["chan_id"])
	require.Equal(t, "lndg", budgets[0]["af_mode"])
	require.EqualValues(t, 720, budgets[0]["budget_ppm"], "800 x 90 %")
	nkBudget, ok := budgets[0]["nk_budget_ppm"].(float64)
	require.True(t, ok)
	require.True(t, nkBudget > 0 && nkBudget < 800, "min(fee, sold price) less the fill margin: %v", nkBudget)
	require.EqualValues(t, 640, budgets[0]["refill_ppm"])
	require.Equal(t, "640", budgets[0]["refill"])
	require.Nil(t, budgets[1]["refill_ppm"])
	require.Equal(t, "none", budgets[1]["refill"])

	// a stored shadow: the mode change of 700 clears it, 701 keeps its own
	_, err := pool.Exec(ctx, `UPDATE gui_channels SET nk_shadow_fee = 850, nk_shadow_inbound = 0, nk_shadow_change = now()`)
	require.NoError(t, err)

	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.PostForm(ts.URL+"/nk/", url.Values{
		"channels": {"700", "701"}, "nk": {"700"}, "target_700": {"5"}, "target_701": {"20"},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusFound, resp.StatusCode)
	var shadow700, shadow701 *int32
	require.NoError(t, pool.QueryRow(ctx, `SELECT nk_shadow_fee FROM gui_channels WHERE chan_id='700'`).Scan(&shadow700))
	require.Nil(t, shadow700)
	require.NoError(t, pool.QueryRow(ctx, `SELECT nk_shadow_fee FROM gui_channels WHERE chan_id='701'`).Scan(&shadow701))
	require.NotNil(t, shadow701)
	require.EqualValues(t, 850, *shadow701)
	var mode string
	var arIn int32
	require.NoError(t, pool.QueryRow(ctx, `SELECT af_mode, ar_in_target FROM gui_channels WHERE chan_id='700'`).Scan(&mode, &arIn))
	require.Equal(t, "nk", mode)
	require.EqualValues(t, 95, arIn, "NK target 5 % local: iTarget 95")
	require.NoError(t, pool.QueryRow(ctx, `SELECT af_mode, ar_in_target FROM gui_channels WHERE chan_id='701'`).Scan(&mode, &arIn))
	require.Equal(t, "lndg", mode)
	require.EqualValues(t, 60, arIn, "an LNDg channel keeps its iTarget")

	logHTML := getPage(t, srv, "/nk/log/?chan=700")
	require.Contains(t, logHTML, "LNDg - NK Decision Log")
	require.Contains(t, logHTML, "250,000")
}
