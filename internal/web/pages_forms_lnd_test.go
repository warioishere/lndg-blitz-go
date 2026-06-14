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

func TestFormsBatchCIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	insertRow(t, pool, "gui_peers", map[string]any{"pubkey": pubkey66, "connected": true})
	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id": "123", "short_chan_id": "800x1x1", "alias": "a",
		"funding_txid": "ftxid", "output_index": 2, "is_open": true,
	})

	fake := &fakeLightning{
		connectPeerFn: func(_ context.Context, _ *lnrpc.ConnectPeerRequest, _ ...grpc.CallOption) (*lnrpc.ConnectPeerResponse, error) {
			return &lnrpc.ConnectPeerResponse{}, nil
		},
		addInvoiceFn: func(_ context.Context, in *lnrpc.Invoice, _ ...grpc.CallOption) (*lnrpc.AddInvoiceResponse, error) {
			require.EqualValues(t, 1000, in.GetValue())
			return &lnrpc.AddInvoiceResponse{PaymentRequest: "lnbcrt1test"}, nil
		},
		openChannelFn: func(_ context.Context, _ *lnrpc.OpenChannelRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[lnrpc.OpenStatusUpdate], error) {
			return &fakeStream[lnrpc.OpenStatusUpdate]{msgs: []*lnrpc.OpenStatusUpdate{
				{Update: &lnrpc.OpenStatusUpdate_ChanPending{ChanPending: &lnrpc.PendingUpdate{Txid: []byte{1, 2, 3, 4}, OutputIndex: 1}}},
			}}, nil
		},
		closeChannelFn: func(_ context.Context, in *lnrpc.CloseChannelRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[lnrpc.CloseStatusUpdate], error) {
			require.True(t, in.GetForce())
			return &fakeStream[lnrpc.CloseStatusUpdate]{msgs: []*lnrpc.CloseStatusUpdate{
				{Update: &lnrpc.CloseStatusUpdate_ClosePending{ClosePending: &lnrpc.PendingUpdate{Txid: []byte{0xaa, 0xbb}, OutputIndex: 0}}},
			}}, nil
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
	post := func(path string, form url.Values) *http.Response {
		resp, err := client.PostForm(ts.URL+path, form)
		require.NoError(t, err)
		require.Equal(t, http.StatusFound, resp.StatusCode)
		return resp
	}

	// connect_peer form with pubkey@host.
	require.Contains(t, flashMsg(post("/connectpeer/", url.Values{"peer_id": {pubkey66 + "@1.2.3.4:9735"}})), "Connection successful!")
	// invalid connection string.
	require.Contains(t, flashMsg(post("/connectpeer/", url.Values{"peer_id": {"garbage"}})), "Invalid peer pubkey or connection string.")

	// add_invoice form.
	require.Contains(t, flashMsg(post("/createinvoice/", url.Values{"value": {"1000"}})), "Invoice created! lnbcrt1test")

	// open_channel form (peer already connected).
	require.Contains(t, flashMsg(post("/openchannel/", url.Values{"peer_pubkey": {pubkey66}, "local_amt": {"1000000"}, "sat_per_byte": {"5"}})), "Channel created! Funding TXID: 04030201:1")

	// close_channel form (force).
	require.Contains(t, flashMsg(post("/closechannel/", url.Values{"chan_id": {"123"}, "target_fee": {"1"}, "force": {"on"}})), "Channel force closed! Closing TXID: bbaa:0")
}
