package web

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/warioishere/lndg-blitz-go/internal/config"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

func TestClosuresPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	// Closure with matching channel -> alias/short_chan_id from the join.
	insertRow(t, pool, "gui_closures", map[string]any{
		"id": 1, "chan_id": "123456789", "funding_txid": "txaaa", "funding_index": 0,
		"closing_tx": "txbbb", "remote_pubkey": "03peer", "capacity": 1000000,
		"close_height": 800000, "settled_balance": 500000, "time_locked_balance": 0,
		"close_type": 0, "open_initiator": 1, "close_initiator": 2,
		"resolution_count": 2, "closing_costs": 100,
	})
	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id": "123456789", "short_chan_id": "700x1x0", "alias": "ClosedPeer",
		"is_open": false, "last_update": "2024-01-01T00:00:00Z",
	})
	// Closure without a channel -> short_chan_id computed from chan_id (1<<40 -> 1x0x0).
	insertRow(t, pool, "gui_closures", map[string]any{
		"id": 2, "chan_id": "1099511627776", "funding_txid": "txccc", "funding_index": 0,
		"closing_tx": "txddd", "remote_pubkey": "03peer2", "capacity": 2000000,
		"close_height": 790000, "settled_balance": 0, "time_locked_balance": 0,
		"close_type": 1, "open_initiator": 1, "close_initiator": 1,
		"resolution_count": 0, "closing_costs": 0,
	})

	fake := &fakeLightning{pendingChannelsFn: func(_ context.Context, _ *lnrpc.PendingChannelsRequest, _ ...grpc.CallOption) (*lnrpc.PendingChannelsResponse, error) {
		return &lnrpc.PendingChannelsResponse{}, nil
	}}
	srv := NewServer(&config.Settings{}, pool, WithLND(&LND{Lightning: fake}))

	html := getPage(t, srv, "/closures")
	require.Contains(t, html, "<title>LNDg - Closures</title>")
	require.Contains(t, html, "ClosedPeer")
	require.Contains(t, html, "700x1x0")     // short_chan_id from channel join
	require.Contains(t, html, "Cooperative") // close_type=0
	require.Contains(t, html, "Details")     // resolution_count>0
	require.Contains(t, html, "1x0x0")       // computed short_chan_id (2^40)
	require.Contains(t, html, "Local Force") // close_type=1
}
