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
		"is_open": true,
	})
	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id": "601", "alias": "IOPeer2", "funding_txid": "f7", "output_index": 0,
		"local_fee_rate": 100, "local_base_fee": 1000, "local_cltv": 40, "inbound_offset": 0,
		"is_open": true,
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

	// Single: offset -50 -> balance 150 -> inbound fee rate -150.
	resp, err := client.PostForm(ts.URL+"/inbound-offset/", url.Values{"chan_id": {"600"}, "offset": {"-50"}})
	require.NoError(t, err)
	require.Equal(t, http.StatusFound, resp.StatusCode)
	require.Contains(t, flashMsg(resp), "Inbound fee rate for channel IOPeer (600) updated to a value of: -150")
	require.EqualValues(t, -150, lastInbound)
	var off, irate int
	require.NoError(t, pool.QueryRow(ctx, `SELECT inbound_offset, local_inbound_fee_rate FROM gui_channels WHERE chan_id='600'`).Scan(&off, &irate))
	require.Equal(t, -50, off)
	require.Equal(t, -150, irate)
	var logCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM gui_inboundfeelog WHERE chan_id='600' AND setting='Manual Offset'`).Scan(&logCount))
	require.Equal(t, 1, logCount)

	// Bulk: delta -30 on selected channel 601 -> balance 70 -> rate -70.
	resp, err = client.PostForm(ts.URL+"/inbound-offset/", url.Values{"bulk": {"1"}, "delta_offset": {"-30"}, "channels": {"601"}})
	require.NoError(t, err)
	require.Equal(t, http.StatusFound, resp.StatusCode)
	require.Contains(t, flashMsg(resp), "Inbound offset adjusted by -30 for 1 channel(s).")
	require.NoError(t, pool.QueryRow(ctx, `SELECT inbound_offset, local_inbound_fee_rate FROM gui_channels WHERE chan_id='601'`).Scan(&off, &irate))
	require.Equal(t, -30, off)
	require.Equal(t, -70, irate)
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM gui_inboundfeelog WHERE chan_id='601' AND setting='Bulk Offset'`).Scan(&logCount))
	require.Equal(t, 1, logCount)
}
