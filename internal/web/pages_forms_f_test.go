package web

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/warioishere/lndg-blitz-go/internal/config"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

func TestAutoMaxhtlcAndGraphWatcherPostIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id": "900", "alias": "MX", "funding_txid": "fm", "output_index": 0,
		"local_balance": 8000000, "pending_outbound": 0, "local_fee_rate": 100,
		"local_base_fee": 1000, "local_cltv": 40, "is_open": true,
	})
	insertRow(t, pool, "gui_graphevent", map[string]any{"id": 1})
	insertRow(t, pool, "gui_graphevent", map[string]any{"id": 2})

	var maxHtlc uint64
	fake := &fakeLightning{updateChanPolicyFn: func(_ context.Context, in *lnrpc.PolicyUpdateRequest, _ ...grpc.CallOption) (*lnrpc.PolicyUpdateResponse, error) {
		maxHtlc = in.GetMaxHtlcMsat()
		return &lnrpc.PolicyUpdateResponse{}, nil
	}}
	srv := NewServer(&config.Settings{}, pool, WithLND(&LND{Lightning: fake}))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	flashMsg := func(resp *http.Response) string {
		var c *http.Cookie
		for _, ck := range resp.Cookies() {
			if ck.Name == flashCookie {
				c = ck
			}
		}
		req, _ := http.NewRequest(http.MethodGet, ts.URL+"/peers", nil)
		if c != nil {
			req.AddCookie(c)
		}
		gr, err := client.Do(req)
		require.NoError(t, err)
		defer gr.Body.Close()
		b, _ := io.ReadAll(gr.Body)
		return string(b)
	}

	// auto_maxhtlc: percent 10 -> target = 8_000_000 * 90 / 100 = 7_200_000.
	resp, err := client.PostForm(ts.URL+"/auto-maxhtlc/", url.Values{
		"chan_id": {"900"}, "percent": {"10"}, "mx_liq_threshold": {"0"}, "mx_liq_value": {"0"}, "mx_liq_upper": {"0"},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusFound, resp.StatusCode)
	require.Contains(t, flashMsg(resp), "Max HTLC for channel MX (900) updated to a value of: 7200000")
	require.EqualValues(t, 7200000000, maxHtlc)
	var pct int
	var maxMsat int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT maxhtlc_percent, local_max_htlc_msat FROM gui_channels WHERE chan_id='900'`).Scan(&pct, &maxMsat))
	require.Equal(t, 10, pct)
	require.Equal(t, int64(7200000000), maxMsat)

	// graph_watcher purge.
	resp, err = client.PostForm(ts.URL+"/graphwatcher", url.Values{"action": {"purge_events"}})
	require.NoError(t, err)
	require.Equal(t, http.StatusFound, resp.StatusCode)
	require.Contains(t, flashMsg(resp), "Purged 2 graph watcher events.")
	var cnt int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM gui_graphevent`).Scan(&cnt))
	require.Equal(t, 0, cnt)
}
