package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/warioishere/lndg-blitz-go/internal/config"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// fakeLightning implements the LightningClient subset via function fields.
type fakeLightning struct {
	getInfoFn          func(context.Context, *lnrpc.GetInfoRequest, ...grpc.CallOption) (*lnrpc.GetInfoResponse, error)
	walletBalanceFn    func(context.Context, *lnrpc.WalletBalanceRequest, ...grpc.CallOption) (*lnrpc.WalletBalanceResponse, error)
	pendingChannelsFn  func(context.Context, *lnrpc.PendingChannelsRequest, ...grpc.CallOption) (*lnrpc.PendingChannelsResponse, error)
	signMessageFn      func(context.Context, *lnrpc.SignMessageRequest, ...grpc.CallOption) (*lnrpc.SignMessageResponse, error)
	getNodeInfoFn      func(context.Context, *lnrpc.NodeInfoRequest, ...grpc.CallOption) (*lnrpc.NodeInfo, error)
	connectPeerFn      func(context.Context, *lnrpc.ConnectPeerRequest, ...grpc.CallOption) (*lnrpc.ConnectPeerResponse, error)
	disconnectPeerFn   func(context.Context, *lnrpc.DisconnectPeerRequest, ...grpc.CallOption) (*lnrpc.DisconnectPeerResponse, error)
	addInvoiceFn       func(context.Context, *lnrpc.Invoice, ...grpc.CallOption) (*lnrpc.AddInvoiceResponse, error)
	newAddressFn       func(context.Context, *lnrpc.NewAddressRequest, ...grpc.CallOption) (*lnrpc.NewAddressResponse, error)
	sendCoinsFn        func(context.Context, *lnrpc.SendCoinsRequest, ...grpc.CallOption) (*lnrpc.SendCoinsResponse, error)
	openChannelFn      func(context.Context, *lnrpc.OpenChannelRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[lnrpc.OpenStatusUpdate], error)
	closeChannelFn     func(context.Context, *lnrpc.CloseChannelRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[lnrpc.CloseStatusUpdate], error)
	updateChanPolicyFn func(context.Context, *lnrpc.PolicyUpdateRequest, ...grpc.CallOption) (*lnrpc.PolicyUpdateResponse, error)
	batchOpenFn        func(context.Context, *lnrpc.BatchOpenChannelRequest, ...grpc.CallOption) (*lnrpc.BatchOpenChannelResponse, error)
}

func (f *fakeLightning) UpdateChannelPolicy(ctx context.Context, in *lnrpc.PolicyUpdateRequest, _ ...grpc.CallOption) (*lnrpc.PolicyUpdateResponse, error) {
	return f.updateChanPolicyFn(ctx, in)
}

func (f *fakeLightning) BatchOpenChannel(ctx context.Context, in *lnrpc.BatchOpenChannelRequest, _ ...grpc.CallOption) (*lnrpc.BatchOpenChannelResponse, error) {
	return f.batchOpenFn(ctx, in)
}

func (f *fakeLightning) OpenChannel(ctx context.Context, in *lnrpc.OpenChannelRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[lnrpc.OpenStatusUpdate], error) {
	return f.openChannelFn(ctx, in)
}
func (f *fakeLightning) CloseChannel(ctx context.Context, in *lnrpc.CloseChannelRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[lnrpc.CloseStatusUpdate], error) {
	return f.closeChannelFn(ctx, in)
}

func (f *fakeLightning) SignMessage(ctx context.Context, in *lnrpc.SignMessageRequest, _ ...grpc.CallOption) (*lnrpc.SignMessageResponse, error) {
	return f.signMessageFn(ctx, in)
}
func (f *fakeLightning) GetNodeInfo(ctx context.Context, in *lnrpc.NodeInfoRequest, _ ...grpc.CallOption) (*lnrpc.NodeInfo, error) {
	return f.getNodeInfoFn(ctx, in)
}
func (f *fakeLightning) ConnectPeer(ctx context.Context, in *lnrpc.ConnectPeerRequest, _ ...grpc.CallOption) (*lnrpc.ConnectPeerResponse, error) {
	return f.connectPeerFn(ctx, in)
}
func (f *fakeLightning) DisconnectPeer(ctx context.Context, in *lnrpc.DisconnectPeerRequest, _ ...grpc.CallOption) (*lnrpc.DisconnectPeerResponse, error) {
	return f.disconnectPeerFn(ctx, in)
}
func (f *fakeLightning) AddInvoice(ctx context.Context, in *lnrpc.Invoice, _ ...grpc.CallOption) (*lnrpc.AddInvoiceResponse, error) {
	return f.addInvoiceFn(ctx, in)
}
func (f *fakeLightning) NewAddress(ctx context.Context, in *lnrpc.NewAddressRequest, _ ...grpc.CallOption) (*lnrpc.NewAddressResponse, error) {
	return f.newAddressFn(ctx, in)
}
func (f *fakeLightning) SendCoins(ctx context.Context, in *lnrpc.SendCoinsRequest, _ ...grpc.CallOption) (*lnrpc.SendCoinsResponse, error) {
	return f.sendCoinsFn(ctx, in)
}

func (f *fakeLightning) GetInfo(ctx context.Context, in *lnrpc.GetInfoRequest, _ ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
	return f.getInfoFn(ctx, in)
}
func (f *fakeLightning) WalletBalance(ctx context.Context, in *lnrpc.WalletBalanceRequest, _ ...grpc.CallOption) (*lnrpc.WalletBalanceResponse, error) {
	return f.walletBalanceFn(ctx, in)
}
func (f *fakeLightning) PendingChannels(ctx context.Context, in *lnrpc.PendingChannelsRequest, _ ...grpc.CallOption) (*lnrpc.PendingChannelsResponse, error) {
	return f.pendingChannelsFn(ctx, in)
}

func TestGetInfoSuccess(t *testing.T) {
	fake := &fakeLightning{getInfoFn: func(context.Context, *lnrpc.GetInfoRequest, ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
		return &lnrpc.GetInfoResponse{
			IdentityPubkey:    "pk123",
			Alias:             "mynode",
			NumActiveChannels: 3,
			NumPeers:          5,
			BlockHeight:       800000,
			SyncedToChain:     true,
			Uris:              []string{"pk123@1.2.3.4:9735"},
			Chains:            []*lnrpc.Chain{{Chain: "bitcoin", Network: "mainnet"}},
			Color:             "#000000",
		}, nil
	}}
	srv := NewServer(&config.Settings{}, nil, WithLND(&LND{Lightning: fake}))

	req := httptest.NewRequest(http.MethodGet, "/api/getinfo/", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, "success", body["message"])
	data := body["data"].(map[string]any)
	require.Equal(t, "pk123", data["identity_pubkey"])
	require.EqualValues(t, 3, data["num_active_channels"])
	require.Equal(t, true, data["synced_to_chain"])
	require.Equal(t, []any{"pk123@1.2.3.4:9735"}, data["uris"])
	chains := data["chains"].([]any)
	require.Equal(t, "bitcoin", chains[0].(map[string]any)["chain"])
}

func TestGetInfoError(t *testing.T) {
	fake := &fakeLightning{getInfoFn: func(context.Context, *lnrpc.GetInfoRequest, ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
		return nil, status.Error(codes.Unavailable, "node down")
	}}
	srv := NewServer(&config.Settings{}, nil, WithLND(&LND{Lightning: fake}))

	req := httptest.NewRequest(http.MethodGet, "/api/getinfo/", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code) // errors also return 200

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, "Failed to call getinfo! Error: node down", body["error"])
	require.Nil(t, body["message"])
}
