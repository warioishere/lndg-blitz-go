package web

import (
	"context"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/warioishere/lndg-blitz-go/internal/config"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/routerrpc"
)

type fakeRouter struct {
	updateChanStatusFn func(context.Context, *routerrpc.UpdateChanStatusRequest, ...grpc.CallOption) (*routerrpc.UpdateChanStatusResponse, error)
}

func (f *fakeRouter) UpdateChanStatus(ctx context.Context, in *routerrpc.UpdateChanStatusRequest, _ ...grpc.CallOption) (*routerrpc.UpdateChanStatusResponse, error) {
	return f.updateChanStatusFn(ctx, in)
}

func TestChanPolicyFeeRateSyncsSiblings(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id": "100", "remote_pubkey": "pk", "funding_txid": "ft1", "output_index": 0,
		"alias": "a", "is_open": true, "local_fee_rate": 500, "local_base_fee": 1000, "local_cltv": 40,
	})
	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id": "200", "remote_pubkey": "pk", "funding_txid": "ft2", "output_index": 0,
		"alias": "b", "is_open": true, "local_fee_rate": 300, "local_base_fee": 1000, "local_cltv": 40,
	})

	var policyCalls int64
	fake := &fakeLightning{
		getInfoFn: func(context.Context, *lnrpc.GetInfoRequest, ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
			return &lnrpc.GetInfoResponse{Version: "0.18.0"}, nil
		},
		updateChanPolicyFn: func(context.Context, *lnrpc.PolicyUpdateRequest, ...grpc.CallOption) (*lnrpc.PolicyUpdateResponse, error) {
			atomic.AddInt64(&policyCalls, 1)
			return &lnrpc.PolicyUpdateResponse{}, nil
		},
	}
	srv := NewServer(&config.Settings{}, pool, WithLND(&LND{Lightning: fake}))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	out := postViaServer(t, ts, "/api/chanpolicy/", `{"chan_id":"100","fee_rate":750}`)
	require.EqualValues(t, 750, out["fee_rate"])
	require.EqualValues(t, 1, out["synced_siblings"])
	require.EqualValues(t, 2, atomic.LoadInt64(&policyCalls)) // channel + sibling
	var rate100, rate200 int32
	require.NoError(t, pool.QueryRow(ctx, `SELECT local_fee_rate FROM gui_channels WHERE chan_id='100'`).Scan(&rate100))
	require.NoError(t, pool.QueryRow(ctx, `SELECT local_fee_rate FROM gui_channels WHERE chan_id='200'`).Scan(&rate200))
	require.EqualValues(t, 750, rate100)
	require.EqualValues(t, 750, rate200)

	var autofeeCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM gui_autofees`).Scan(&autofeeCount))
	require.Equal(t, 2, autofeeCount) // channel + sibling
}

func TestChanPolicyDisabled(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id": "100", "remote_pubkey": "pk", "funding_txid": "ft1", "output_index": 0,
		"alias": "a", "is_open": true, "local_disabled": false,
	})

	var gotAction routerrpc.ChanStatusAction
	fake := &fakeLightning{}
	router := &fakeRouter{updateChanStatusFn: func(_ context.Context, in *routerrpc.UpdateChanStatusRequest, _ ...grpc.CallOption) (*routerrpc.UpdateChanStatusResponse, error) {
		gotAction = in.GetAction()
		return &routerrpc.UpdateChanStatusResponse{}, nil
	}}
	srv := NewServer(&config.Settings{}, pool, WithLND(&LND{Lightning: fake, Router: router}))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	out := postViaServer(t, ts, "/api/chanpolicy/", `{"chan_id":"100","disabled":1}`)
	require.Contains(t, out, "disabled")
	require.EqualValues(t, 1, out["disabled"]) // returns the disabled value
	require.Equal(t, routerrpc.ChanStatusAction(1), gotAction)

	var disabled bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT local_disabled FROM gui_channels WHERE chan_id='100'`).Scan(&disabled))
	require.True(t, disabled)
}

func TestChanPolicyInvalid(t *testing.T) {
	srv := NewServer(&config.Settings{}, nil, WithLND(&LND{Lightning: &fakeLightning{}}))
	// missing chan_id -> Invalid request!
	out := postJSON(t, srv, "/api/chanpolicy/", `{"fee_rate":750}`)
	require.Equal(t, "Invalid request!", out["error"])
}
