package web

import (
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/warioishere/lndg-blitz-go/internal/config"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// fakeStream implements grpc.ServerStreamingClient[T] (Recv + ClientStream).
type fakeStream[T any] struct {
	grpc.ClientStream
	msgs []*T
	idx  int
}

func (s *fakeStream[T]) Recv() (*T, error) {
	if s.idx < len(s.msgs) {
		m := s.msgs[s.idx]
		s.idx++
		return m, nil
	}
	return nil, io.EOF
}

func TestOpenChannelIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()
	insertRow(t, pool, "gui_peers", map[string]any{"pubkey": pubkey66, "connected": true})

	fake := &fakeLightning{openChannelFn: func(_ context.Context, in *lnrpc.OpenChannelRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[lnrpc.OpenStatusUpdate], error) {
		require.EqualValues(t, 1000000, in.GetLocalFundingAmount())
		require.EqualValues(t, 5, in.GetSatPerByte())
		require.Len(t, in.GetNodePubkey(), 33) // 66 hex -> 33 bytes
		return &fakeStream[lnrpc.OpenStatusUpdate]{msgs: []*lnrpc.OpenStatusUpdate{
			{Update: &lnrpc.OpenStatusUpdate_ChanPending{ChanPending: &lnrpc.PendingUpdate{Txid: []byte{1, 2, 3, 4}, OutputIndex: 1}}},
		}}, nil
	}}
	srv := NewServer(&config.Settings{}, pool, WithLND(&LND{Lightning: fake}))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	out := postViaServer(t, ts, "/api/openchannel/", `{"peer_pubkey":"`+pubkey66+`","local_amt":1000000,"sat_per_byte":5}`)
	require.Equal(t, "Channel created! Funding TXID: 04030201:1", out["message"])
}

func TestCloseChannelIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()
	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id": "123", "short_chan_id": "800x1x1", "alias": "a",
		"funding_txid": "ftxid", "output_index": 2,
	})

	var gotForce bool
	fake := &fakeLightning{closeChannelFn: func(_ context.Context, in *lnrpc.CloseChannelRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[lnrpc.CloseStatusUpdate], error) {
		gotForce = in.GetForce()
		require.Equal(t, "ftxid", in.GetChannelPoint().GetFundingTxidStr())
		require.EqualValues(t, 2, in.GetChannelPoint().GetOutputIndex())
		return &fakeStream[lnrpc.CloseStatusUpdate]{msgs: []*lnrpc.CloseStatusUpdate{
			{Update: &lnrpc.CloseStatusUpdate_ClosePending{ClosePending: &lnrpc.PendingUpdate{Txid: []byte{0xaa, 0xbb}, OutputIndex: 0}}},
		}}, nil
	}}
	srv := NewServer(&config.Settings{}, pool, WithLND(&LND{Lightning: fake}))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// graceful close by chan_id
	out := postViaServer(t, ts, "/api/closechannel/", `{"chan_id":"123","target_fee":5}`)
	require.Equal(t, "Channel gracefully closed! Closing TXID: bbaa:0", out["message"])
	require.False(t, gotForce)

	// force close by short_chan_id
	out = postViaServer(t, ts, "/api/closechannel/", `{"chan_id":"800x1x1","target_fee":5,"force":true}`)
	require.Equal(t, "Channel force closed! Closing TXID: bbaa:0", out["message"])
	require.True(t, gotForce)

	// unknown channel
	out = postViaServer(t, ts, "/api/closechannel/", `{"chan_id":"nope","target_fee":5}`)
	require.Equal(t, "Channel ID is not valid.", out["error"])
}

func postViaServer(t *testing.T, ts *httptest.Server, path, body string) map[string]any {
	t.Helper()
	resp, err := ts.Client().Post(ts.URL+path, "application/json", strings.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return out
}
