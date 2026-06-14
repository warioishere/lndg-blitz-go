package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/warioishere/lndg-blitz-go/internal/config"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

func TestPendingChannelsEmpty(t *testing.T) {
	fake := &fakeLightning{pendingChannelsFn: func(context.Context, *lnrpc.PendingChannelsRequest, ...grpc.CallOption) (*lnrpc.PendingChannelsResponse, error) {
		return &lnrpc.PendingChannelsResponse{}, nil
	}}
	srv := NewServer(&config.Settings{}, nil, WithLND(&LND{Lightning: fake}))

	req := httptest.NewRequest(http.MethodGet, "/api/pendingchannels/", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, "success", body["message"])
	require.Contains(t, body, "data")
	require.Nil(t, body["data"]) // no category, no limbo -> data:null
}

func TestPendingChannelsForceClosingIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	fake := &fakeLightning{pendingChannelsFn: func(context.Context, *lnrpc.PendingChannelsRequest, ...grpc.CallOption) (*lnrpc.PendingChannelsResponse, error) {
		return &lnrpc.PendingChannelsResponse{
			TotalLimboBalance: 200,
			PendingForceClosingChannels: []*lnrpc.PendingChannelsResponse_ForceClosedChannel{
				{
					Channel:           &lnrpc.PendingChannelsResponse_PendingChannel{RemoteNodePub: "pk", ChannelPoint: "tx:0", Capacity: 1000, LocalBalance: 400},
					ClosingTxid:       "ctx",
					LimboBalance:      200,
					MaturityHeight:    900,
					BlocksTilMaturity: 144,
				},
			},
		}, nil
	}}
	srv := NewServer(&config.Settings{}, pool, WithLND(&LND{Lightning: fake}))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	var body struct {
		Message string         `json:"message"`
		Data    map[string]any `json:"data"`
	}
	getJSON(t, ts.URL+"/api/pendingchannels/", &body)
	require.Equal(t, "success", body.Message)
	require.NotNil(t, body.Data)

	fc := body.Data["pending_force_closing"].([]any)
	require.Len(t, fc, 1)
	item := fc[0].(map[string]any)
	require.Equal(t, "ctx", item["closing_txid"])
	require.EqualValues(t, 200, item["limbo_balance"])
	require.EqualValues(t, 144, item["blocks_til_maturity"])
	require.Nil(t, item["short_chan_id"]) // no channel match -> null
	require.EqualValues(t, 200, body.Data["total_limbo_balance"])
	require.NotContains(t, body.Data, "pending_open")
}
