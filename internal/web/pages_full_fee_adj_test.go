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

func TestFullFeeAdjPostIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	// Two channels with the same peer -> sibling sync + dedup.
	for _, cid := range []string{"800", "801"} {
		insertRow(t, pool, "gui_channels", map[string]any{
			"chan_id": cid, "alias": "FFA", "remote_pubkey": "03ffa", "funding_txid": "ff" + cid,
			"output_index": 0, "local_fee_rate": 100, "local_base_fee": 1000, "local_cltv": 40,
			"inbound_offset": 0, "is_open": true,
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

	resp, err := client.PostForm(ts.URL+"/full-fee-adj/", url.Values{"delta_ppm": {"50"}})
	require.NoError(t, err)
	require.Equal(t, http.StatusFound, resp.StatusCode)
	// html/template escapes '+' to &#43; -> assert without the '+'.
	require.Contains(t, flashMsg(resp), "50 ppm for 2 channel(s).")

	for _, cid := range []string{"800", "801"} {
		var rate int
		require.NoError(t, pool.QueryRow(ctx, `SELECT local_fee_rate FROM gui_channels WHERE chan_id=$1`, cid).Scan(&rate))
		require.Equal(t, 150, rate, "chan %s", cid)
	}
	var afCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM gui_autofees WHERE new_value=150 AND setting='Manual'`).Scan(&afCount))
	require.Equal(t, 2, afCount)

	// Invalid delta.
	resp, err = client.PostForm(ts.URL+"/full-fee-adj/", url.Values{"delta_ppm": {"abc"}})
	require.NoError(t, err)
	require.Contains(t, flashMsg(resp), "Invalid PPM delta.")
}
