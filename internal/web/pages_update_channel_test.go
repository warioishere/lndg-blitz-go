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
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/routerrpc"
)

func TestUpdateChannelFormIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id": "100", "short_chan_id": "1x1x1", "remote_pubkey": "03p", "alias": "P",
		"funding_txid": "ftx", "output_index": 0, "capacity": 1000000,
		"local_fee_rate": 100, "local_base_fee": 1000, "local_cltv": 40,
		"auto_rebalance": false, "auto_fees": false, "local_disabled": false, "is_open": true,
	})

	var policyCalls int
	fakeL := &fakeLightning{
		updateChanPolicyFn: func(_ context.Context, _ *lnrpc.PolicyUpdateRequest, _ ...grpc.CallOption) (*lnrpc.PolicyUpdateResponse, error) {
			policyCalls++
			return &lnrpc.PolicyUpdateResponse{}, nil
		},
		getInfoFn: func(_ context.Context, _ *lnrpc.GetInfoRequest, _ ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
			return &lnrpc.GetInfoResponse{Version: "0.18.0-beta"}, nil
		},
	}
	fakeR := &fakeRouter{updateChanStatusFn: func(_ context.Context, _ *routerrpc.UpdateChanStatusRequest, _ ...grpc.CallOption) (*routerrpc.UpdateChanStatusResponse, error) {
		return &routerrpc.UpdateChanStatusResponse{}, nil
	}}

	srv := NewServer(&config.Settings{}, pool, WithLND(&LND{Lightning: fakeL, Router: fakeR}))
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
		resp, err := client.PostForm(ts.URL+"/update_channel/", form)
		require.NoError(t, err)
		require.Equal(t, http.StatusFound, resp.StatusCode)
		return resp
	}

	// target 2: ar_amt_target (DB-only).
	msg := flashMsg(post(url.Values{"chan_id": {"100"}, "target": {"5000000"}, "update_target": {"2"}}))
	require.Contains(t, msg, "Auto rebalancer target amount for channel P (100) updated to a value of: 5000000.0")
	var arAmt int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT ar_amt_target FROM gui_channels WHERE chan_id='100'`).Scan(&arAmt))
	require.Equal(t, int64(5000000), arAmt)

	// target 5: toggle auto_rebalance.
	msg = flashMsg(post(url.Values{"chan_id": {"100"}, "target": {"0"}, "update_target": {"5"}}))
	require.Contains(t, msg, "Auto rebalancer status for channel P (100) updated to a value of: True")
	var ar bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT auto_rebalance FROM gui_channels WHERE chan_id='100'`).Scan(&ar))
	require.True(t, ar)

	// target 1: fee rate (LND + autofees).
	msg = flashMsg(post(url.Values{"chan_id": {"100"}, "target": {"250"}, "update_target": {"1"}}))
	require.Contains(t, msg, "Fee rate for channel P (100) updated to a value of: 250.0")
	require.GreaterOrEqual(t, policyCalls, 1)
	var feeRate int
	require.NoError(t, pool.QueryRow(ctx, `SELECT local_fee_rate FROM gui_channels WHERE chan_id='100'`).Scan(&feeRate))
	require.Equal(t, 250, feeRate)
	var afCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM gui_autofees WHERE chan_id='100' AND old_value=100 AND new_value=250`).Scan(&afCount))
	require.Equal(t, 1, afCount)

	// target 7: disable channel (LND router).
	msg = flashMsg(post(url.Values{"chan_id": {"100"}, "target": {"0"}, "update_target": {"7"}}))
	require.Contains(t, msg, "Toggled channel state for channel P (100) to a value of: Disabled")
	require.Contains(t, msg, "Use with caution")
	var disabled bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT local_disabled FROM gui_channels WHERE chan_id='100'`).Scan(&disabled))
	require.True(t, disabled)

	// invalid channel.
	msg = flashMsg(post(url.Values{"chan_id": {"999"}, "target": {"0"}, "update_target": {"2"}}))
	require.Contains(t, msg, invalidRequest)
}

func TestUpdatePendingFormIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	srv := NewServer(&config.Settings{}, pool)
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

	// get-or-create + set fee rate.
	resp, err := client.PostForm(ts.URL+"/update_pending/", url.Values{
		"funding_txid": {"pftx"}, "output_index": {"1"}, "target": {"50"}, "update_target": {"1"},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusFound, resp.StatusCode)
	require.Contains(t, flashMsg(resp), "Fee rate for pending channel (pftx) updated to a value of: 50")

	var feeRate int
	require.NoError(t, pool.QueryRow(ctx, `SELECT local_fee_rate FROM gui_pendingchannels WHERE funding_txid='pftx' AND output_index=1`).Scan(&feeRate))
	require.Equal(t, 50, feeRate)

	// toggle auto_rebalance (None -> True).
	resp, err = client.PostForm(ts.URL+"/update_pending/", url.Values{
		"funding_txid": {"pftx"}, "output_index": {"1"}, "target": {"0"}, "update_target": {"5"},
	})
	require.NoError(t, err)
	require.Contains(t, flashMsg(resp), "Auto rebalancer status for pending channel (pftx) updated to a value of: True")
	var ar bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT auto_rebalance FROM gui_pendingchannels WHERE funding_txid='pftx' AND output_index=1`).Scan(&ar))
	require.True(t, ar)
}
