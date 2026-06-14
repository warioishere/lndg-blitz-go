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

func TestUpdateSettingPostIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	for _, cid := range []string{"1000", "1001"} {
		insertRow(t, pool, "gui_channels", map[string]any{
			"chan_id": cid, "alias": "US", "remote_pubkey": "03us", "funding_txid": "fu" + cid,
			"output_index": 0, "local_fee_rate": 100, "local_base_fee": 1000, "local_cltv": 40,
			"inbound_offset": 0, "is_open": true, "private": false, "auto_fees": false,
		})
	}

	fake := &fakeLightning{updateChanPolicyFn: func(_ context.Context, _ *lnrpc.PolicyUpdateRequest, _ ...grpc.CallOption) (*lnrpc.PolicyUpdateResponse, error) {
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
	post := func(form url.Values) *http.Response {
		resp, err := client.PostForm(ts.URL+"/update_setting/", form)
		require.NoError(t, err)
		require.Equal(t, http.StatusFound, resp.StatusCode)
		return resp
	}

	// ALL-oRate: all open channels set to 300 (including sibling sync).
	require.Contains(t, flashMsg(post(url.Values{"key": {"ALL-oRate"}, "value": {"300"}})),
		"Fee rate for all open channels updated to a value of: 300")
	for _, cid := range []string{"1000", "1001"} {
		var rate int
		require.NoError(t, pool.QueryRow(ctx, `SELECT local_fee_rate FROM gui_channels WHERE chan_id=$1`, cid).Scan(&rate))
		require.Equal(t, 300, rate, "chan %s", cid)
	}

	// ALL-AF: auto_fees on + AF-Enabled.
	require.Contains(t, flashMsg(post(url.Values{"key": {"ALL-AF"}, "value": {"1"}})), "Auto Fees globally enabled")
	var af bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT auto_fees FROM gui_channels WHERE chan_id='1000'`).Scan(&af))
	require.True(t, af)
	var afEnabled string
	require.NoError(t, pool.QueryRow(ctx, `SELECT value FROM gui_localsettings WHERE key='AF-Enabled'`).Scan(&afEnabled))
	require.Equal(t, "1", afEnabled)

	// GW-Cooldown: LocalSetting upsert.
	require.Contains(t, flashMsg(post(url.Values{"key": {"GW-Cooldown"}, "value": {"600"}})), "Graph Watcher cooldown updated to: 600s")
	var gw string
	require.NoError(t, pool.QueryRow(ctx, `SELECT value FROM gui_localsettings WHERE key='GW-Cooldown'`).Scan(&gw))
	require.Equal(t, "600", gw)

	// RR-CollectRoutes: '1'/'0' normalisation.
	require.Contains(t, flashMsg(post(url.Values{"key": {"RR-CollectRoutes"}, "value": {"1"}})), "Route collecting enabled")

	// Unknown key.
	require.Contains(t, flashMsg(post(url.Values{"key": {"ZZZ"}, "value": {"1"}})), "Invalid Request. Please try again. [ZZZ]")
}
