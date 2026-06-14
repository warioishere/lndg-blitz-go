package web

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/warioishere/lndg-blitz-go/internal/config"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

func TestOpensPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	recent := time.Now().Add(-1 * time.Hour)
	insertRow(t, pool, "gui_payments", map[string]any{
		"creation_date": recent, "payment_hash": "openpay", "value": 1000000.0, "fee": 50.0,
		"status": 2, "index": 1,
	})
	insertRow(t, pool, "gui_paymenthops", map[string]any{
		"id": 1, "payment_hash_id": "openpay", "attempt_id": 1, "step": 1,
		"chan_id": "1", "alias": "SuggestedNode", "chan_capacity": 5000000,
		"node_pubkey": "03suggested", "amt": 1000000.0, "fee": 50.0, "cost_to": 50.0,
	})
	insertRow(t, pool, "gui_avoidnodes", map[string]any{
		"pubkey": "03avoid", "notes": "bad peer", "updated": recent,
	})

	fake := &fakeLightning{getInfoFn: func(_ context.Context, _ *lnrpc.GetInfoRequest, _ ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
		return &lnrpc.GetInfoResponse{IdentityPubkey: "03self"}, nil
	}}
	srv := NewServer(&config.Settings{}, pool, WithLND(&LND{Lightning: fake}))

	html := getPage(t, srv, "/opens/")
	require.Contains(t, html, "<title>LNDg - Opens</title>")
	require.Contains(t, html, "Suggested Open List")
	require.Contains(t, html, "03suggested")
	require.Contains(t, html, "SuggestedNode")
	require.Contains(t, html, "1,000,000") // amount routed
	require.Contains(t, html, "Avoid/Exclude List")
	require.Contains(t, html, "03avoid")
	require.Contains(t, html, "bad peer")
}
