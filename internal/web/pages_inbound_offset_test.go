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

func TestInboundOffsetPostIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id": "600", "alias": "IOPeer", "funding_txid": "f6", "output_index": 0,
		"local_fee_rate": 200, "local_base_fee": 1000, "local_cltv": 40, "inbound_offset": 0,
		"is_open": true, "ar_max_cost": 100, "ar_in_target": 50, "capacity": 1000000, "local_balance": 0,
	})
	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id": "601", "alias": "IOPeer2", "funding_txid": "f7", "output_index": 0,
		"local_fee_rate": 100, "local_base_fee": 1000, "local_cltv": 40, "inbound_offset": 0,
		"is_open": true, "ar_max_cost": 100, "ar_in_target": 50, "capacity": 1000000, "local_balance": 0,
	})

	var lastInbound int32
	fake := &fakeLightning{
		getInfoFn: func(_ context.Context, _ *lnrpc.GetInfoRequest, _ ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
			return &lnrpc.GetInfoResponse{Version: "0.18.0-beta"}, nil
		},
		updateChanPolicyFn: func(_ context.Context, in *lnrpc.PolicyUpdateRequest, _ ...grpc.CallOption) (*lnrpc.PolicyUpdateResponse, error) {
			if in.GetInboundFee() != nil {
				lastInbound = in.GetInboundFee().GetFeeRatePpm()
			}
			return &lnrpc.PolicyUpdateResponse{}, nil
		},
	}
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

	// Channel 600 is switched on, channel 601 (dynamic before) off.
	_, err := pool.Exec(ctx, `UPDATE gui_channels SET inbound_offset=-30, local_inbound_fee_rate=-70 WHERE chan_id='601'`)
	require.NoError(t, err)
	resp, err := client.PostForm(ts.URL+"/inbound-offset/", url.Values{"channels": {"600", "601"}, "dynamic": {"600"}})
	require.NoError(t, err)
	require.Equal(t, http.StatusFound, resp.StatusCode)
	require.Contains(t, flashMsg(resp), "Dynamic inbound fees switched on for 1 and off for 1 channel(s).")
	var off, irate int
	// empty channel, max cost 100% of 200 ppm -> -200
	require.NoError(t, pool.QueryRow(ctx, `SELECT inbound_offset, local_inbound_fee_rate FROM gui_channels WHERE chan_id='600'`).Scan(&off, &irate))
	require.NotEqual(t, 0, off, "marked dynamic")
	require.Equal(t, -200, irate)
	require.NoError(t, pool.QueryRow(ctx, `SELECT inbound_offset, local_inbound_fee_rate FROM gui_channels WHERE chan_id='601'`).Scan(&off, &irate))
	require.Equal(t, 0, off)
	require.Equal(t, 0, irate)
	require.EqualValues(t, 0, lastInbound, "LND gets 0 for the channel switched off")
	var logs int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM gui_inboundfeelog WHERE (chan_id='600' AND setting='Dynamic On') OR (chan_id='601' AND setting='Dynamic Off')`).Scan(&logs))
	require.Equal(t, 2, logs)

	// Saving the same state changes nothing.
	resp, err = client.PostForm(ts.URL+"/inbound-offset/", url.Values{"channels": {"600", "601"}, "dynamic": {"600"}})
	require.NoError(t, err)
	require.Contains(t, flashMsg(resp), "switched on for 0 and off for 0 channel(s).")

	// A balanced channel switched on gets no discount.
	_, err = pool.Exec(ctx, `UPDATE gui_channels SET local_balance=500000 WHERE chan_id='601'`)
	require.NoError(t, err)
	resp, err = client.PostForm(ts.URL+"/inbound-offset/", url.Values{"channels": {"601"}, "dynamic": {"601"}})
	require.NoError(t, err)
	require.Equal(t, http.StatusFound, resp.StatusCode)
	require.NoError(t, pool.QueryRow(ctx, `SELECT inbound_offset, local_inbound_fee_rate FROM gui_channels WHERE chan_id='601'`).Scan(&off, &irate))
	require.NotEqual(t, 0, off)
	require.Equal(t, 0, irate)
}
