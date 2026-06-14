package web

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/warioishere/lndg-blitz-go/internal/config"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

const pubkey66 = "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestConnectPeerHostString(t *testing.T) {
	var gotPubkey, gotHost string
	fake := &fakeLightning{connectPeerFn: func(_ context.Context, in *lnrpc.ConnectPeerRequest, _ ...grpc.CallOption) (*lnrpc.ConnectPeerResponse, error) {
		gotPubkey = in.GetAddr().GetPubkey()
		gotHost = in.GetAddr().GetHost()
		return &lnrpc.ConnectPeerResponse{}, nil
	}}
	srv := NewServer(&config.Settings{}, nil, WithLND(&LND{Lightning: fake}))

	out := postJSON(t, srv, "/api/connectpeer/", `{"peer_id":"`+pubkey66+`@1.2.3.4:9735"}`)
	require.Equal(t, "Connection successful!", out["message"])
	require.Equal(t, pubkey66, gotPubkey)
	require.Equal(t, "1.2.3.4:9735", gotHost)
}

func TestConnectPeerInvalid(t *testing.T) {
	srv := NewServer(&config.Settings{}, nil, WithLND(&LND{Lightning: &fakeLightning{}}))
	out := postJSON(t, srv, "/api/connectpeer/", `{"peer_id":"shortstring"}`)
	require.Equal(t, "Invalid peer pubkey or connection string.", out["error"])
}

func TestAddInvoice(t *testing.T) {
	fake := &fakeLightning{addInvoiceFn: func(_ context.Context, in *lnrpc.Invoice, _ ...grpc.CallOption) (*lnrpc.AddInvoiceResponse, error) {
		require.EqualValues(t, 1000, in.GetValue())
		return &lnrpc.AddInvoiceResponse{PaymentRequest: "lnbc1..."}, nil
	}}
	srv := NewServer(&config.Settings{}, nil, WithLND(&LND{Lightning: fake}))

	out := postJSON(t, srv, "/api/createinvoice/", `{"value":1000}`)
	require.Equal(t, "Invoice created!", out["message"])
	require.Equal(t, "lnbc1...", out["data"])

	out = postJSON(t, srv, "/api/createinvoice/", `{"value":-5}`)
	require.Equal(t, "Invalid request!", out["error"])
}

func TestNewAddress(t *testing.T) {
	var gotType lnrpc.AddressType
	fake := &fakeLightning{
		getInfoFn: func(context.Context, *lnrpc.GetInfoRequest, ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
			return &lnrpc.GetInfoResponse{Version: "0.18.0-beta"}, nil
		},
		newAddressFn: func(_ context.Context, in *lnrpc.NewAddressRequest, _ ...grpc.CallOption) (*lnrpc.NewAddressResponse, error) {
			gotType = in.GetType()
			return &lnrpc.NewAddressResponse{Address: "bc1qxyz"}, nil
		},
	}
	srv := NewServer(&config.Settings{}, nil, WithLND(&LND{Lightning: fake}))

	out := postJSON(t, srv, "/api/newaddress/", `{}`)
	require.Equal(t, "Retrieved new deposit address!", out["message"])
	require.Equal(t, "bc1qxyz", out["data"])
	require.Equal(t, lnrpc.AddressType(4), gotType) // p2tr from 0.15+

	postJSON(t, srv, "/api/newaddress/", `{"legacy":true}`)
	require.Equal(t, lnrpc.AddressType(0), gotType)
}

func TestConsolidateUtxos(t *testing.T) {
	fake := &fakeLightning{
		getInfoFn: func(context.Context, *lnrpc.GetInfoRequest, ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
			return &lnrpc.GetInfoResponse{Version: "0.18.0"}, nil
		},
		newAddressFn: func(context.Context, *lnrpc.NewAddressRequest, ...grpc.CallOption) (*lnrpc.NewAddressResponse, error) {
			return &lnrpc.NewAddressResponse{Address: "bc1self"}, nil
		},
		sendCoinsFn: func(_ context.Context, in *lnrpc.SendCoinsRequest, _ ...grpc.CallOption) (*lnrpc.SendCoinsResponse, error) {
			require.True(t, in.GetSendAll())
			require.EqualValues(t, 5, in.GetSatPerVbyte())
			require.Equal(t, "bc1self", in.GetAddr())
			return &lnrpc.SendCoinsResponse{Txid: "txABC"}, nil
		},
	}
	srv := NewServer(&config.Settings{}, nil, WithLND(&LND{Lightning: fake}))

	out := postJSON(t, srv, "/api/consolidate/", `{"sat_per_vbyte":5}`)
	require.Equal(t, "Successfully consolidated UXTOs: txABC", out["message"])
	require.Equal(t, "txABC", out["txid"])
}

func TestDisconnectPeerIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()
	insertRow(t, pool, "gui_peers", map[string]any{"pubkey": pubkey66, "connected": true})

	fake := &fakeLightning{disconnectPeerFn: func(context.Context, *lnrpc.DisconnectPeerRequest, ...grpc.CallOption) (*lnrpc.DisconnectPeerResponse, error) {
		return &lnrpc.DisconnectPeerResponse{}, nil
	}}
	srv := NewServer(&config.Settings{}, pool, WithLND(&LND{Lightning: fake}))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	req := strings.NewReader(`{"peer_id":"` + pubkey66 + `"}`)
	resp, err := ts.Client().Post(ts.URL+"/api/disconnectpeer/", "application/json", req)
	require.NoError(t, err)
	resp.Body.Close()

	var connected bool
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT connected FROM gui_peers WHERE pubkey=$1`, pubkey66).Scan(&connected))
	require.False(t, connected)
}

func TestResetApiIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	insertRow(t, pool, "gui_payments", map[string]any{
		"creation_date": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "payment_hash": "ph1",
		"value": 1.0, "fee": 0.0, "status": 2, "index": 1, "cleaned": false,
	})
	insertRow(t, pool, "gui_paymenthops", map[string]any{
		"id": int64(1), "attempt_id": 1, "step": 1, "chan_id": "c", "alias": "a",
		"chan_capacity": int64(1), "node_pubkey": "n", "amt": 1.0, "fee": 0.0,
		"payment_hash_id": "ph1", "cost_to": 0.0,
	})

	srv := NewServer(&config.Settings{}, pool)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := ts.Client().Post(ts.URL+"/api/reset/", "application/json", strings.NewReader(`{"table":"Payments"}`))
	require.NoError(t, err)
	resp.Body.Close()

	var payCount, hopCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM gui_payments`).Scan(&payCount))
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM gui_paymenthops`).Scan(&hopCount))
	require.Equal(t, 0, payCount)
	require.Equal(t, 0, hopCount) // cascade delete

	// Unknown table -> KeyError-shape.
	out := postJSON(t, srv, "/api/reset/", `{"table":"Foo"}`)
	require.Equal(t, "Error deleting table: 'Foo'", out["error"])
}
