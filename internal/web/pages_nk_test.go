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
	require.Contains(t, html, `placeholder="40"`, "default target 100 - ar_in_target")
	require.Contains(t, html, "below target: 800 → 824 (shadow)")
	require.Contains(t, html, "NK-StepUp", "settings block")

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.PostForm(ts.URL+"/nk/", url.Values{
		"channels": {"700", "701"}, "nk": {"700"}, "budget": {"700"}, "target_700": {"5"}, "target_701": {""},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusFound, resp.StatusCode)
	var mode string
	var budget bool
	var target *int32
	require.NoError(t, pool.QueryRow(ctx, `SELECT af_mode, nk_budget, nk_target FROM gui_channels WHERE chan_id='700'`).Scan(&mode, &budget, &target))
	require.Equal(t, "nk", mode)
	require.True(t, budget)
	require.NotNil(t, target)
	require.EqualValues(t, 5, *target)
	require.NoError(t, pool.QueryRow(ctx, `SELECT af_mode, nk_budget, nk_target FROM gui_channels WHERE chan_id='701'`).Scan(&mode, &budget, &target))
	require.Equal(t, "lndg", mode)
	require.Nil(t, target)

	logHTML := getPage(t, srv, "/nk/log/?chan=700")
	require.Contains(t, logHTML, "LNDg - NK Decision Log")
	require.Contains(t, logHTML, "250,000")
}
