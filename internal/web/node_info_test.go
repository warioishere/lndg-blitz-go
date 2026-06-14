package web

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/warioishere/lndg-blitz-go/internal/config"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

func TestNodeInfoIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	insertRow(t, pool, "gui_peers", map[string]any{"pubkey": "pk", "alias": "peeralias"})

	fake := &fakeLightning{
		getInfoFn: func(context.Context, *lnrpc.GetInfoRequest, ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
			return &lnrpc.GetInfoResponse{
				Version: "0.18", NumPeers: 7, BlockHeight: 800123, BlockHash: "abc",
				NumActiveChannels: 4, SyncedToChain: true,
				Chains: []*lnrpc.Chain{{Chain: "bitcoin", Network: "mainnet"}},
			}, nil
		},
		walletBalanceFn: func(context.Context, *lnrpc.WalletBalanceRequest, ...grpc.CallOption) (*lnrpc.WalletBalanceResponse, error) {
			return &lnrpc.WalletBalanceResponse{TotalBalance: 1000, ConfirmedBalance: 1000}, nil
		},
		pendingChannelsFn: func(context.Context, *lnrpc.PendingChannelsRequest, ...grpc.CallOption) (*lnrpc.PendingChannelsResponse, error) {
			return &lnrpc.PendingChannelsResponse{
				PendingOpenChannels: []*lnrpc.PendingChannelsResponse_PendingOpenChannel{
					{
						Channel: &lnrpc.PendingChannelsResponse_PendingChannel{
							RemoteNodePub: "pk", ChannelPoint: "txid:0",
							Capacity: 1000, LocalBalance: 500, RemoteBalance: 500,
						},
						CommitFee: 10, CommitWeight: 600, FeePerKw: 250,
					},
				},
			}, nil
		},
	}
	srv := NewServer(&config.Settings{LND_DATABASE_PATH: "/nonexistent/channel.db"}, pool, WithLND(&LND{Lightning: fake}))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	var body map[string]any
	getJSON(t, ts.URL+"/api/node_info/", &body)

	require.Equal(t, "0.18", body["version"])
	require.EqualValues(t, 7, body["num_peers"])
	require.EqualValues(t, 800123, body["block"].(map[string]any)["height"])
	require.Equal(t, "abc", body["block"].(map[string]any)["hash"])
	require.Equal(t, []any{"bitcoin-mainnet"}, body["chains"])
	require.EqualValues(t, 0, body["db_size"]) // file does not exist -> 0

	bal := body["balance"].(map[string]any)
	require.EqualValues(t, 1500, bal["total"]) // 1000 + 500 (+ limbo 0)
	require.EqualValues(t, 1000, bal["onchain"])

	open := body["pending_open"].([]any)
	require.Len(t, open, 1)
	o := open[0].(map[string]any)
	require.Equal(t, "peeralias", o["alias"])
	require.EqualValues(t, 90, o["ar_in_target"])  // default AR inbound %
	require.EqualValues(t, 30, o["ar_amt_target"]) // int(3/100 * 1000)
	require.Equal(t, false, o["auto_rebalance"])   // no override
	require.Equal(t, "", o["local_base_fee"])      // no override -> ''
	require.EqualValues(t, 250, o["fee_per_kw"])

	require.Nil(t, body["pending_closed"])
	require.Nil(t, body["pending_force_closed"])
	require.Nil(t, body["waiting_for_close"])
}
