package web

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/warioishere/lndg-blitz-go/internal/config"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/walletrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/wtclientrpc"
)

type fakeWatchtower struct {
	listTowersFn  func(context.Context, *wtclientrpc.ListTowersRequest, ...grpc.CallOption) (*wtclientrpc.ListTowersResponse, error)
	statsFn       func(context.Context, *wtclientrpc.StatsRequest, ...grpc.CallOption) (*wtclientrpc.StatsResponse, error)
	addTowerFn    func(context.Context, *wtclientrpc.AddTowerRequest, ...grpc.CallOption) (*wtclientrpc.AddTowerResponse, error)
	removeTowerFn func(context.Context, *wtclientrpc.RemoveTowerRequest, ...grpc.CallOption) (*wtclientrpc.RemoveTowerResponse, error)
}

func (f *fakeWatchtower) ListTowers(ctx context.Context, in *wtclientrpc.ListTowersRequest, _ ...grpc.CallOption) (*wtclientrpc.ListTowersResponse, error) {
	return f.listTowersFn(ctx, in)
}
func (f *fakeWatchtower) Stats(ctx context.Context, in *wtclientrpc.StatsRequest, _ ...grpc.CallOption) (*wtclientrpc.StatsResponse, error) {
	return f.statsFn(ctx, in)
}
func (f *fakeWatchtower) AddTower(ctx context.Context, in *wtclientrpc.AddTowerRequest, _ ...grpc.CallOption) (*wtclientrpc.AddTowerResponse, error) {
	return f.addTowerFn(ctx, in)
}
func (f *fakeWatchtower) RemoveTower(ctx context.Context, in *wtclientrpc.RemoveTowerRequest, _ ...grpc.CallOption) (*wtclientrpc.RemoveTowerResponse, error) {
	return f.removeTowerFn(ctx, in)
}

func TestTowersPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	wt := &fakeWatchtower{
		listTowersFn: func(_ context.Context, in *wtclientrpc.ListTowersRequest, _ ...grpc.CallOption) (*wtclientrpc.ListTowersResponse, error) {
			require.False(t, in.GetIncludeSessions())
			return &wtclientrpc.ListTowersResponse{Towers: []*wtclientrpc.Tower{
				{Pubkey: []byte{0x02, 0xab, 0xcd}, Addresses: []string{"1.2.3.4:9911"}, ActiveSessionCandidate: true, NumSessions: 3},
				{Pubkey: []byte{0x03, 0xde, 0xad}, Addresses: []string{"5.6.7.8:9911"}, ActiveSessionCandidate: false, NumSessions: 0},
			}}, nil
		},
		statsFn: func(_ context.Context, _ *wtclientrpc.StatsRequest, _ ...grpc.CallOption) (*wtclientrpc.StatsResponse, error) {
			return &wtclientrpc.StatsResponse{NumBackups: 1234, NumPendingBackups: 5, NumFailedBackups: 2}, nil
		},
	}
	srv := NewServer(&config.Settings{}, pool, WithLND(&LND{Watchtower: wt}))

	html := getPage(t, srv, "/towers")
	require.Contains(t, html, "<title>LNDg - Watch Towers</title>")
	require.Contains(t, html, "Watch Tower Stats")
	require.Contains(t, html, "1,234") // num_backups via intcomma
	require.Contains(t, html, "Active Towers")
	require.Contains(t, html, "02abcd") // pubkey hex (active)
	require.Contains(t, html, "1.2.3.4:9911")
	require.Contains(t, html, "Inactive Towers")
	require.Contains(t, html, "03dead") // pubkey hex (inactive)
}

func TestBalancesPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	insertRow(t, pool, "gui_onchain", map[string]any{
		"id": 1, "tx_hash": "unconftx", "amount": 5000, "block_height": 0, "fee": 100, "label": "pending",
	})
	insertRow(t, pool, "gui_onchain", map[string]any{
		"id": 2, "tx_hash": "conftx", "amount": 9000, "block_height": 800000, "fee": 200, "label": "done",
	})

	wallet := &fakeWallet{
		pendingSweepsFn: func(_ context.Context, _ *walletrpc.PendingSweepsRequest, _ ...grpc.CallOption) (*walletrpc.PendingSweepsResponse, error) {
			return &walletrpc.PendingSweepsResponse{PendingSweeps: []*walletrpc.PendingSweep{{
				Outpoint:  &lnrpc.OutPoint{TxidBytes: []byte{0xaa, 0xbb}, OutputIndex: 1},
				AmountSat: 50000, WitnessType: walletrpc.WitnessType(1), SatPerVbyte: 10,
				RequestedConfTarget: 6, BroadcastAttempts: 2, NextBroadcastHeight: 800000, Force: true,
			}}}, nil
		},
		listUnspentFn: func(_ context.Context, in *walletrpc.ListUnspentRequest, _ ...grpc.CallOption) (*walletrpc.ListUnspentResponse, error) {
			require.EqualValues(t, 9999999, in.GetMaxConfs())
			return &walletrpc.ListUnspentResponse{Utxos: []*lnrpc.Utxo{{
				Address: "bc1qtest", AmountSat: 100000,
				Outpoint: &lnrpc.OutPoint{TxidStr: "utxotxid", OutputIndex: 0}, Confirmations: 0,
			}}}, nil
		},
	}
	srv := NewServer(&config.Settings{}, pool, WithLND(&LND{Wallet: wallet}))

	html := getPage(t, srv, "/balances")
	require.Contains(t, html, "<title>LNDg - Balances</title>")
	require.Contains(t, html, "Pending Sweeps")
	require.Contains(t, html, "bbaa")                 // reversed txid hex
	require.Contains(t, html, "Commitment Time Lock") // witness_type=1
	require.Contains(t, html, ">Yes<")                // force
	require.Contains(t, html, "bc1qtest")
	require.Contains(t, html, "Transactions")
	require.Contains(t, html, "unconftx")
	require.Contains(t, html, "conftx")
}

func TestBatchPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	fake := &fakeLightning{walletBalanceFn: func(_ context.Context, _ *lnrpc.WalletBalanceRequest, _ ...grpc.CallOption) (*lnrpc.WalletBalanceResponse, error) {
		return &lnrpc.WalletBalanceResponse{TotalBalance: 1000000}, nil
	}}
	srv := NewServer(&config.Settings{}, pool, WithLND(&LND{Lightning: fake}))

	html := getPage(t, srv, "/batch")
	require.Contains(t, html, "<title>LNDg - Batch Open</title>")
	require.Contains(t, html, "Batch Open Up To 10 Channels")
	require.Contains(t, html, "Remaining Onchain Balance: 1,000,000")
	require.Contains(t, html, `id="pubkey10"`) // 10 rows
}

func TestPendingHtlcsPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	insertRow(t, pool, "gui_pendinghtlcs", map[string]any{
		"id": 1, "chan_id": "900x1x0", "alias": "HtlcPeer", "incoming": true,
		"amount": 50000, "hash_lock": "lockhash1", "expiration_height": 800144,
		"forwarding_channel": "0", "forwarding_alias": "",
	})
	insertRow(t, pool, "gui_pendinghtlcs", map[string]any{
		"id": 2, "chan_id": "901x1x0", "alias": "OutPeer", "incoming": false,
		"amount": 70000, "hash_lock": "lockhash2", "expiration_height": 800200,
		"forwarding_channel": "555x1x0", "forwarding_alias": "FwdPeer",
	})

	fake := &fakeLightning{getInfoFn: func(_ context.Context, _ *lnrpc.GetInfoRequest, _ ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
		return &lnrpc.GetInfoResponse{BlockHeight: 800000}, nil
	}}
	srv := NewServer(&config.Settings{}, pool, WithLND(&LND{Lightning: fake}))

	html := getPage(t, srv, "/pending_htlcs")
	require.Contains(t, html, "<title>LNDg - HTLCs</title>")
	require.Contains(t, html, "Outgoing HTLCs")
	require.Contains(t, html, "Incoming HTLCs")
	require.Contains(t, html, "HtlcPeer")
	require.Contains(t, html, "24 hours") // (800144-800000)*10/60 = 1440/60 = 24
	require.Contains(t, html, "Self")     // forwarding_channel == "0"
	require.Contains(t, html, "FwdPeer")
}

func TestAddressesPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	fake := &fakeWallet{listAddressesFn: func(_ context.Context, _ *walletrpc.ListAddressesRequest, _ ...grpc.CallOption) (*walletrpc.ListAddressesResponse, error) {
		return &walletrpc.ListAddressesResponse{
			AccountWithAddresses: []*walletrpc.AccountWithAddresses{
				{
					AddressType: walletrpc.AddressType(4), // Taproot
					Addresses: []*walletrpc.AddressProperty{
						{Address: "bc1ptaproot", IsInternal: false, Balance: 50000},
						{Address: "bc1pchange", IsInternal: true, Balance: 0},
					},
				},
			},
		}, nil
	}}
	srv := NewServer(&config.Settings{}, pool, WithLND(&LND{Wallet: fake}))

	html := getPage(t, srv, "/addresses/")
	require.Contains(t, html, "<title>LNDg - Addresses</title>")
	require.Contains(t, html, "Taproot Addresses")
	require.Contains(t, html, "bc1ptaproot")
	require.Contains(t, html, "50,000")
	require.Contains(t, html, "False") // is_internal=false via pybool
	require.Contains(t, html, "True")  // is_internal=true (change)
	require.Contains(t, html, "---")   // balance==0
}
