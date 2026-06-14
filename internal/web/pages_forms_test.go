package web

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warioishere/lndg-blitz-go/internal/config"
)

func TestFormsBatchAIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	srv := NewServer(&config.Settings{}, pool)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	flashOf := func(resp *http.Response) *http.Cookie {
		for _, c := range resp.Cookies() {
			if c.Name == flashCookie {
				return c
			}
		}
		return nil
	}
	// pageWith fetches /peers (no LND needed) with the given flash cookie.
	pageWith := func(c *http.Cookie) string {
		req, _ := http.NewRequest(http.MethodGet, ts.URL+"/peers", nil)
		if c != nil {
			req.AddCookie(c)
		}
		resp, err := client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return string(body)
	}
	post := func(path string, form url.Values) *http.Response {
		resp, err := client.PostForm(ts.URL+path, form)
		require.NoError(t, err)
		require.Equal(t, http.StatusFound, resp.StatusCode)
		return resp
	}

	// --- add_avoid ---
	resp := post("/add_avoid/", url.Values{"pubkey": {"03avoidme"}, "notes": {"bad peer"}})
	c := flashOf(resp)
	require.NotNil(t, c)
	var notes string
	require.NoError(t, pool.QueryRow(ctx, `SELECT notes FROM gui_avoidnodes WHERE pubkey='03avoidme'`).Scan(&notes))
	require.Equal(t, "bad peer", notes)
	require.Contains(t, pageWith(c), "Successfully added node 03avoidme to the avoid list.")

	// Flash cookie is cleared after display -> subsequent GET without cookie shows nothing.
	require.NotContains(t, pageWith(nil), "Successfully added node")

	// --- remove_avoid ---
	resp = post("/remove_avoid/", url.Values{"pubkey": {"03avoidme"}})
	require.Contains(t, pageWith(flashOf(resp)), "Successfully removed node 03avoidme from the avoid list.")
	var cnt int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM gui_avoidnodes WHERE pubkey='03avoidme'`).Scan(&cnt))
	require.Equal(t, 0, cnt)

	// remove_avoid on non-existent node -> Invalid Request.
	resp = post("/remove_avoid/", url.Values{"pubkey": {"03nope"}})
	require.Contains(t, pageWith(flashOf(resp)), invalidRequest)

	// --- update_keysend ---
	insertRow(t, pool, "gui_invoices", map[string]any{
		"creation_date": time.Now(), "r_hash": "kstest", "value": 1000.0,
		"amt_paid": 1000, "state": 1, "index": 1, "is_revenue": false,
	})
	resp = post("/update_keysend/", url.Values{"r_hash": {"kstest"}})
	require.Contains(t, pageWith(flashOf(resp)), "Marked invoice kstest as revenue.")
	var isRev bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT is_revenue FROM gui_invoices WHERE r_hash='kstest'`).Scan(&isRev))
	require.True(t, isRev)

	// --- update_closing ---
	insertRow(t, pool, "gui_closures", map[string]any{
		"chan_id": "1", "funding_txid": "ftx", "funding_index": 0, "closing_costs": 0,
	})
	resp = post("/update_closing/", url.Values{"funding_txid": {"ftx"}, "funding_index": {"0"}, "target": {"1234"}})
	require.Contains(t, pageWith(flashOf(resp)), "Updated closing costs for ftx:0 updated to a value of: 1234")
	var costs int
	require.NoError(t, pool.QueryRow(ctx, `SELECT closing_costs FROM gui_closures WHERE funding_txid='ftx' AND funding_index=0`).Scan(&costs))
	require.Equal(t, 1234, costs)

	// --- reset_node_reputation ---
	insertRow(t, pool, "gui_nodereputation", map[string]any{"pubkey": "03a"})
	insertRow(t, pool, "gui_nodereputation", map[string]any{"pubkey": "03b"})
	resp = post("/reset_node_reputation/", url.Values{})
	require.Contains(t, pageWith(flashOf(resp)), "Cleared 2 node reputation records.")
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM gui_nodereputation`).Scan(&cnt))
	require.Equal(t, 0, cnt)
}
