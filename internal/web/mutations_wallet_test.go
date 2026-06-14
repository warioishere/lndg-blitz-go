package web

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/warioishere/lndg-blitz-go/internal/config"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/walletrpc"
)

type fakeWallet struct {
	bumpFeeFn       func(context.Context, *walletrpc.BumpFeeRequest, ...grpc.CallOption) (*walletrpc.BumpFeeResponse, error)
	publishFn       func(context.Context, *walletrpc.Transaction, ...grpc.CallOption) (*walletrpc.PublishResponse, error)
	listAddressesFn func(context.Context, *walletrpc.ListAddressesRequest, ...grpc.CallOption) (*walletrpc.ListAddressesResponse, error)
	pendingSweepsFn func(context.Context, *walletrpc.PendingSweepsRequest, ...grpc.CallOption) (*walletrpc.PendingSweepsResponse, error)
	listUnspentFn   func(context.Context, *walletrpc.ListUnspentRequest, ...grpc.CallOption) (*walletrpc.ListUnspentResponse, error)
}

func (f *fakeWallet) PendingSweeps(ctx context.Context, in *walletrpc.PendingSweepsRequest, _ ...grpc.CallOption) (*walletrpc.PendingSweepsResponse, error) {
	return f.pendingSweepsFn(ctx, in)
}
func (f *fakeWallet) ListUnspent(ctx context.Context, in *walletrpc.ListUnspentRequest, _ ...grpc.CallOption) (*walletrpc.ListUnspentResponse, error) {
	return f.listUnspentFn(ctx, in)
}

func (f *fakeWallet) BumpFee(ctx context.Context, in *walletrpc.BumpFeeRequest, _ ...grpc.CallOption) (*walletrpc.BumpFeeResponse, error) {
	return f.bumpFeeFn(ctx, in)
}
func (f *fakeWallet) PublishTransaction(ctx context.Context, in *walletrpc.Transaction, _ ...grpc.CallOption) (*walletrpc.PublishResponse, error) {
	return f.publishFn(ctx, in)
}
func (f *fakeWallet) ListAddresses(ctx context.Context, in *walletrpc.ListAddressesRequest, _ ...grpc.CallOption) (*walletrpc.ListAddressesResponse, error) {
	return f.listAddressesFn(ctx, in)
}

func TestBumpFee(t *testing.T) {
	fake := &fakeWallet{bumpFeeFn: func(_ context.Context, in *walletrpc.BumpFeeRequest, _ ...grpc.CallOption) (*walletrpc.BumpFeeResponse, error) {
		require.Equal(t, "abc123", in.GetOutpoint().GetTxidStr())
		require.EqualValues(t, 2, in.GetOutpoint().GetOutputIndex())
		require.EqualValues(t, 15, in.GetSatPerVbyte())
		require.True(t, in.GetForce())
		return &walletrpc.BumpFeeResponse{}, nil
	}}
	srv := NewServer(&config.Settings{}, nil, WithLND(&LND{Wallet: fake}))

	out := postJSON(t, srv, "/api/bumpfee/", `{"txid":"abc123","index":2,"target_fee":15,"force":true}`)
	require.Equal(t, "Fee bumped to 15 sats/vbyte for outpoint: abc123:2", out["message"])

	out = postJSON(t, srv, "/api/bumpfee/", `{"txid":"abc123","index":2}`) // missing target_fee
	require.Equal(t, "Invalid request!", out["error"])
}

func TestBroadcastTx(t *testing.T) {
	fake := &fakeWallet{publishFn: func(_ context.Context, in *walletrpc.Transaction, _ ...grpc.CallOption) (*walletrpc.PublishResponse, error) {
		require.Equal(t, []byte{0xde, 0xad, 0xbe, 0xef}, in.GetTxHex())
		return &walletrpc.PublishResponse{PublishError: ""}, nil
	}}
	srv := NewServer(&config.Settings{}, nil, WithLND(&LND{Wallet: fake}))
	out := postJSON(t, srv, "/api/broadcast_tx/", `{"raw_tx":"deadbeef"}`)
	require.Equal(t, "Successfully broadcast tx!", out["message"])

	// publish_error set -> error message returned
	fake.publishFn = func(context.Context, *walletrpc.Transaction, ...grpc.CallOption) (*walletrpc.PublishResponse, error) {
		return &walletrpc.PublishResponse{PublishError: "already in mempool"}, nil
	}
	srv = NewServer(&config.Settings{}, nil, WithLND(&LND{Wallet: fake}))
	out = postJSON(t, srv, "/api/broadcast_tx/", `{"raw_tx":"deadbeef"}`)
	require.Equal(t, "Error while broadcasting TX: already in mempool", out["error"])

	// invalid hex -> broadcast failed
	out = postJSON(t, srv, "/api/broadcast_tx/", `{"raw_tx":"xyz"}`)
	require.Contains(t, out["error"], "TX broadcast failed! Error:")
}
