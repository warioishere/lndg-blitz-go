package web

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/warioishere/lndg-blitz-go/internal/config"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// Every manual outbound fee change (update_channel, ALL-oRate, chan_policy,
// sibling sync) re-applies the inbound offset of the channels it touches.
func TestOutboundFeeAppliesInboundOffset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	var mu sync.Mutex
	var reqs []*lnrpc.PolicyUpdateRequest
	fake := &fakeLightning{
		getInfoFn: func(context.Context, *lnrpc.GetInfoRequest, ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
			return &lnrpc.GetInfoResponse{Version: "0.21.0-beta"}, nil
		},
		updateChanPolicyFn: func(_ context.Context, in *lnrpc.PolicyUpdateRequest, _ ...grpc.CallOption) (*lnrpc.PolicyUpdateResponse, error) {
			mu.Lock()
			reqs = append(reqs, in)
			mu.Unlock()
			return &lnrpc.PolicyUpdateResponse{}, nil
		},
	}
	ts := httptest.NewServer(NewServer(&config.Settings{}, pool, WithLND(&LND{Lightning: fake})).Handler())
	defer ts.Close()
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	reset := func() {
		_, err := pool.Exec(ctx, `DELETE FROM gui_channels; DELETE FROM gui_inboundfeelog; DELETE FROM gui_autofees`)
		require.NoError(t, err)
		reqs = nil
		for _, c := range []struct {
			id, peer string
			offset   int
		}{{"1", "peerA", -100}, {"2", "peerA", -50}, {"3", "peerB", 0}} {
			insertRow(t, pool, "gui_channels", map[string]any{
				"chan_id": c.id, "remote_pubkey": c.peer, "funding_txid": "ft" + c.id, "output_index": 0,
				"alias": "a" + c.id, "is_open": true, "local_fee_rate": 500, "local_base_fee": 1000,
				"local_cltv": 40, "inbound_offset": c.offset,
			})
		}
	}
	fees := func(id string) (rate, inbound int32) {
		require.NoError(t, pool.QueryRow(ctx, `SELECT local_fee_rate, local_inbound_fee_rate FROM gui_channels WHERE chan_id=$1`, id).Scan(&rate, &inbound))
		return
	}
	reqFor := func(id string) *lnrpc.PolicyUpdateRequest {
		var last *lnrpc.PolicyUpdateRequest
		for _, r := range reqs {
			if r.GetChanPoint().GetFundingTxidStr() == "ft"+id {
				last = r
			}
		}
		require.NotNil(t, last, "no policy update for %s", id)
		return last
	}
	postForm := func(path string, form url.Values) {
		resp, err := noRedirect.PostForm(ts.URL+path, form)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, http.StatusFound, resp.StatusCode)
	}

	t.Run("ALL-oRate", func(t *testing.T) {
		reset()
		postForm("/update_setting/", url.Values{"key": {"ALL-oRate"}, "value": {"700"}})
		rate, inbound := fees("1")
		assert.EqualValues(t, 700, rate)
		assert.EqualValues(t, -600, inbound, "primary channel gets its offset")
		_, inbound = fees("2")
		assert.EqualValues(t, -650, inbound, "sibling gets its offset")
		assert.Nil(t, reqFor("3").GetInboundFee(), "no offset, no inbound fee sent")
		var logs int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM gui_inboundfeelog WHERE setting='Fee Adj Offset'`).Scan(&logs))
		assert.Equal(t, 2, logs)
	})

	t.Run("update_channel", func(t *testing.T) {
		reset()
		postForm("/update_channel/", url.Values{"chan_id": {"1"}, "target": {"600"}, "update_target": {"1"}})
		rate, inbound := fees("1")
		assert.EqualValues(t, 600, rate)
		assert.EqualValues(t, -500, inbound)
		rate, inbound = fees("2")
		assert.EqualValues(t, 600, rate)
		assert.EqualValues(t, -550, inbound)
	})

	t.Run("chan_policy", func(t *testing.T) {
		reset()
		out := postViaServer(t, ts, "/api/chanpolicy/", `{"chan_id":"1","fee_rate":800}`)
		assert.EqualValues(t, 800, out["fee_rate"])
		assert.EqualValues(t, -700, reqFor("1").GetInboundFee().GetFeeRatePpm(), "offset sent with the new rate")
		_, inbound := fees("1")
		assert.EqualValues(t, -700, inbound)

		reqs = nil
		postViaServer(t, ts, "/api/chanpolicy/", `{"chan_id":"3","inbound_fee_rate":0}`)
		require.NotNil(t, reqFor("3").GetInboundFee(), "an explicit 0 must reach LND")
		assert.EqualValues(t, 0, reqFor("3").GetInboundFee().GetFeeRatePpm())
	})
}
