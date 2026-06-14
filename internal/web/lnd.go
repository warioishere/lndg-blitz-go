package web

import (
	"context"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/routerrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/walletrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/wtclientrpc"
)

// WalletClient is the subset of walletrpc.WalletKitClient used by action endpoints
// (bump_fee, broadcast_tx, address listing, UTXO management).
type WalletClient interface {
	BumpFee(context.Context, *walletrpc.BumpFeeRequest, ...grpc.CallOption) (*walletrpc.BumpFeeResponse, error)
	PublishTransaction(context.Context, *walletrpc.Transaction, ...grpc.CallOption) (*walletrpc.PublishResponse, error)
	ListAddresses(context.Context, *walletrpc.ListAddressesRequest, ...grpc.CallOption) (*walletrpc.ListAddressesResponse, error)
	PendingSweeps(context.Context, *walletrpc.PendingSweepsRequest, ...grpc.CallOption) (*walletrpc.PendingSweepsResponse, error)
	ListUnspent(context.Context, *walletrpc.ListUnspentRequest, ...grpc.CallOption) (*walletrpc.ListUnspentResponse, error)
}

// RouterClient is the subset of routerrpc.RouterClient used by the channel policy
// endpoint (enable/disable via UpdateChanStatus).
type RouterClient interface {
	UpdateChanStatus(context.Context, *routerrpc.UpdateChanStatusRequest, ...grpc.CallOption) (*routerrpc.UpdateChanStatusResponse, error)
}

// WatchtowerClient is the subset of wtclientrpc.WatchtowerClientClient used by
// the towers page (list, stats, add, remove).
type WatchtowerClient interface {
	ListTowers(context.Context, *wtclientrpc.ListTowersRequest, ...grpc.CallOption) (*wtclientrpc.ListTowersResponse, error)
	Stats(context.Context, *wtclientrpc.StatsRequest, ...grpc.CallOption) (*wtclientrpc.StatsResponse, error)
	AddTower(context.Context, *wtclientrpc.AddTowerRequest, ...grpc.CallOption) (*wtclientrpc.AddTowerResponse, error)
	RemoveTower(context.Context, *wtclientrpc.RemoveTowerRequest, ...grpc.CallOption) (*wtclientrpc.RemoveTowerResponse, error)
}

// LightningClient is the subset of lnrpc.LightningClient used by the action
// endpoints. lnrpc.NewLightningClient(conn) satisfies this interface; tests mock
// only the methods they need.
type LightningClient interface {
	GetInfo(context.Context, *lnrpc.GetInfoRequest, ...grpc.CallOption) (*lnrpc.GetInfoResponse, error)
	WalletBalance(context.Context, *lnrpc.WalletBalanceRequest, ...grpc.CallOption) (*lnrpc.WalletBalanceResponse, error)
	PendingChannels(context.Context, *lnrpc.PendingChannelsRequest, ...grpc.CallOption) (*lnrpc.PendingChannelsResponse, error)
	SignMessage(context.Context, *lnrpc.SignMessageRequest, ...grpc.CallOption) (*lnrpc.SignMessageResponse, error)
	GetNodeInfo(context.Context, *lnrpc.NodeInfoRequest, ...grpc.CallOption) (*lnrpc.NodeInfo, error)
	ConnectPeer(context.Context, *lnrpc.ConnectPeerRequest, ...grpc.CallOption) (*lnrpc.ConnectPeerResponse, error)
	DisconnectPeer(context.Context, *lnrpc.DisconnectPeerRequest, ...grpc.CallOption) (*lnrpc.DisconnectPeerResponse, error)
	AddInvoice(context.Context, *lnrpc.Invoice, ...grpc.CallOption) (*lnrpc.AddInvoiceResponse, error)
	NewAddress(context.Context, *lnrpc.NewAddressRequest, ...grpc.CallOption) (*lnrpc.NewAddressResponse, error)
	SendCoins(context.Context, *lnrpc.SendCoinsRequest, ...grpc.CallOption) (*lnrpc.SendCoinsResponse, error)
	OpenChannel(context.Context, *lnrpc.OpenChannelRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[lnrpc.OpenStatusUpdate], error)
	CloseChannel(context.Context, *lnrpc.CloseChannelRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[lnrpc.CloseStatusUpdate], error)
	UpdateChannelPolicy(context.Context, *lnrpc.PolicyUpdateRequest, ...grpc.CallOption) (*lnrpc.PolicyUpdateResponse, error)
	BatchOpenChannel(context.Context, *lnrpc.BatchOpenChannelRequest, ...grpc.CallOption) (*lnrpc.BatchOpenChannelResponse, error)
}

// LND bundles the LND gRPC clients for the web layer. In production it is built
// over its own dedicated gRPC connection so that a channel reset elsewhere does not
// affect the web server.
type LND struct {
	Lightning  LightningClient
	Wallet     WalletClient
	Router     RouterClient
	Watchtower WatchtowerClient
}

// grpcErrorMsg extracts the gRPC status message from an error, falling back to
// err.Error() for non-status errors.
func grpcErrorMsg(err error) string {
	if st, ok := status.FromError(err); ok {
		return st.Message()
	}
	return err.Error()
}

// writeSuccess writes {"message":"success","data":data} with HTTP 200.
func writeSuccess(w http.ResponseWriter, data any) {
	writeJSON(w, http.StatusOK, newOrderedMap().Set("message", "success").Set("data", data))
}

// writeAPIError writes {"error": msg} with HTTP 200. Action endpoints return
// errors as a 200 body with an error key so that the frontend can check data.error.
func writeAPIError(w http.ResponseWriter, msg string) {
	writeJSON(w, http.StatusOK, newOrderedMap().Set("error", msg))
}
