package web

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/warioishere/lndg-blitz-go/internal/config"
	lnd "github.com/warioishere/lndg-blitz-go/internal/lnd"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/wtclientrpc"
)

func TestFormsBatchDIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()
	// update_alias populates the process-global node cache -> clear it afterwards
	// so other tests (e.g. advanced "Node cache: 0 entries") are not affected.
	defer lnd.ResetMemoryCache()
	ctx := context.Background()

	// Channel for rebalance (open+active) and for update_alias (unique pubkey
	// due to the process-global node cache).
	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id": "500", "remote_pubkey": "03rebalpeer", "alias": "x",
		"is_open": true, "is_active": true,
	})
	aliasPubkey := "03aliastestbatchd"
	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id": "501", "remote_pubkey": aliasPubkey, "alias": "old",
		"is_open": true, "is_active": true,
	})

	fakeL := &fakeLightning{
		getNodeInfoFn: func(_ context.Context, _ *lnrpc.NodeInfoRequest, _ ...grpc.CallOption) (*lnrpc.NodeInfo, error) {
			return &lnrpc.NodeInfo{Node: &lnrpc.LightningNode{Alias: "FreshAlias"}}, nil
		},
	}
	fakeW := &fakeWatchtower{
		addTowerFn: func(_ context.Context, _ *wtclientrpc.AddTowerRequest, _ ...grpc.CallOption) (*wtclientrpc.AddTowerResponse, error) {
			return &wtclientrpc.AddTowerResponse{}, nil
		},
		removeTowerFn: func(_ context.Context, _ *wtclientrpc.RemoveTowerRequest, _ ...grpc.CallOption) (*wtclientrpc.RemoveTowerResponse, error) {
			return &wtclientrpc.RemoveTowerResponse{}, nil
		},
	}

	srv := NewServer(&config.Settings{}, pool, WithLND(&LND{Lightning: fakeL, Watchtower: fakeW}))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	flashMsg := func(resp *http.Response) string {
		var c *http.Cookie
		for _, ck := range resp.Cookies() {
			if ck.Name == flashCookie {
				c = ck
			}
		}
		req, _ := http.NewRequest(http.MethodGet, ts.URL+"/peers", nil)
		if c != nil {
			req.AddCookie(c)
		}
		gr, err := client.Do(req)
		require.NoError(t, err)
		defer gr.Body.Close()
		b, _ := io.ReadAll(gr.Body)
		return string(b)
	}
	post := func(path string, form url.Values) *http.Response {
		resp, err := client.PostForm(ts.URL+path, form)
		require.NoError(t, err)
		require.Equal(t, http.StatusFound, resp.StatusCode)
		return resp
	}

	// Tower forms.
	require.Contains(t, flashMsg(post("/addtower/", url.Values{"tower": {pubkey66 + "@1.2.3.4:9911"}})), "Tower addition successful!")
	require.Contains(t, flashMsg(post("/addtower/", url.Values{"tower": {"garbage"}})), "Invalid tower connection string.")
	require.Contains(t, flashMsg(post("/deletetower/", url.Values{"pubkey": {pubkey66}, "address": {"1.2.3.4:9911"}})), "Tower deletion successful!")
	require.Contains(t, flashMsg(post("/removetower/", url.Values{"pubkey": {pubkey66}})), "Tower removal successful!")

	// Rebalancer request.
	require.Contains(t, flashMsg(post("/rebalancer/", url.Values{
		"value": {"100000"}, "fee_limit": {"1000"}, "duration": {"5"},
		"outgoing_chan_ids": {"500"}, "last_hop_pubkey": {""},
	})), "Rebalancer request created!")
	var ocids string
	var feeLimit float64
	require.NoError(t, pool.QueryRow(ctx, `SELECT outgoing_chan_ids, fee_limit FROM gui_rebalancer ORDER BY id DESC LIMIT 1`).Scan(&ocids, &feeLimit))
	require.Equal(t, "[500]", ocids)
	require.Equal(t, 100.0, feeLimit)

	// Rebalancer with invalid outgoing channel.
	require.Contains(t, flashMsg(post("/rebalancer/", url.Values{
		"value": {"100000"}, "fee_limit": {"1000"}, "duration": {"5"}, "outgoing_chan_ids": {"999"},
	})), invalidRequest)

	// update_alias (api/updatealias).
	require.Contains(t, flashMsg(post("/api/updatealias/", url.Values{"peer_pubkey": {aliasPubkey}})), "Alias updated to: FreshAlias")
	var newAlias string
	require.NoError(t, pool.QueryRow(ctx, `SELECT alias FROM gui_channels WHERE chan_id='501'`).Scan(&newAlias))
	require.Equal(t, "FreshAlias", newAlias)

	// update_alias unknown peer.
	require.Contains(t, flashMsg(post("/api/updatealias/", url.Values{"peer_pubkey": {"03unknownpeer"}})), "Pubkey not in channels list.")
}
