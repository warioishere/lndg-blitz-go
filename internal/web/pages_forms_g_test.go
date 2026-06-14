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

func flashCookieMsg(t *testing.T, ts *httptest.Server, client *http.Client, resp *http.Response) string {
	t.Helper()
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

func TestBatchOpenPostIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	insertRow(t, pool, "gui_peers", map[string]any{"pubkey": pubkey66, "connected": true})

	var gotChannels int
	var gotFee int64
	fake := &fakeLightning{batchOpenFn: func(_ context.Context, in *lnrpc.BatchOpenChannelRequest, _ ...grpc.CallOption) (*lnrpc.BatchOpenChannelResponse, error) {
		gotChannels = len(in.GetChannels())
		gotFee = in.GetSatPerVbyte()
		return &lnrpc.BatchOpenChannelResponse{}, nil
	}}
	srv := NewServer(&config.Settings{}, pool, WithLND(&LND{Lightning: fake}))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	resp, err := client.PostForm(ts.URL+"/batchopen/", url.Values{
		"pubkey1": {pubkey66}, "amt1": {"1000000"}, "fee_rate": {"5"},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusFound, resp.StatusCode)
	require.Contains(t, flashCookieMsg(t, ts, client, resp), "Batch opened channels!")
	require.Equal(t, 1, gotChannels)
	require.EqualValues(t, 5, gotFee)

	// No channels specified.
	resp, err = client.PostForm(ts.URL+"/batchopen/", url.Values{"fee_rate": {"5"}})
	require.NoError(t, err)
	require.Contains(t, flashCookieMsg(t, ts, client, resp), "No channels specified!")
}

func TestAdvancedRebalancingPostIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id": "2000", "alias": "SRC", "remote_pubkey": "03src",
		"local_fee_rate": 100, "ar_source_ppm_diff": 0, "auto_rebalance": true, "is_open": true,
	})

	srv := NewServer(&config.Settings{}, pool)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	post := func(form url.Values) *http.Response {
		resp, err := client.PostForm(ts.URL+"/advanced_rebalancing", form)
		require.NoError(t, err)
		require.Equal(t, http.StatusFound, resp.StatusCode)
		return resp
	}

	// Apply All.
	require.Contains(t, flashCookieMsg(t, ts, client, post(url.Values{"action": {"Apply All"}, "ppm_diff_value": {"50"}})),
		"Applied PPM Diff (50) to 1 channel.")
	var ppm int
	require.NoError(t, pool.QueryRow(ctx, `SELECT ar_source_ppm_diff FROM gui_channels WHERE chan_id='2000'`).Scan(&ppm))
	require.Equal(t, 50, ppm)

	// Save Targets.
	require.Contains(t, flashCookieMsg(t, ts, client, post(url.Values{
		"action": {"Save Targets"}, "source_chan_id": {"2000"}, "targets": {"03t1", "03t2"},
	})), "Allowed targets updated.")
	var atCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM gui_allowedtarget WHERE source_chan_id='2000'`).Scan(&atCount))
	require.Equal(t, 2, atCount)

	// Rebalance with explicit target.
	require.Contains(t, flashCookieMsg(t, ts, client, post(url.Values{
		"source_chan_id": {"2000"}, "targets": {"03tgt"}, "value": {"100000"}, "fee_limit": {"10"}, "duration": {"5"},
	})), "1 rebalance request created!")
	var ocids, lastHop string
	require.NoError(t, pool.QueryRow(ctx, `SELECT outgoing_chan_ids, last_hop_pubkey FROM gui_rebalancer ORDER BY id DESC LIMIT 1`).Scan(&ocids, &lastHop))
	require.Equal(t, "[2000]", ocids) // stored without quotes (JSON-parseable)
	require.Equal(t, "03tgt", lastHop)
}
