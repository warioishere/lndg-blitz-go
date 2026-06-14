package web

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warioishere/lndg-blitz-go/internal/config"
)

// fakeTxFees returns a fee (or error) based on a URL suffix match.
type fakeTxFees struct {
	fees map[string]int64
	err  map[string]bool
}

func (f *fakeTxFees) GetTxFee(_ context.Context, url string) (int64, error) {
	for k := range f.err {
		if strings.HasSuffix(url, k) {
			return 0, context.DeadlineExceeded
		}
	}
	for k, v := range f.fees {
		if strings.HasSuffix(url, k) {
			return v, nil
		}
	}
	return 0, nil
}

func TestGetFeesIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	insertRow(t, pool, "gui_closures", map[string]any{
		"id": 1, "chan_id": "C1", "funding_txid": "fund1", "funding_index": 0,
		"closing_tx": "ctx1", "open_initiator": 1,
		"close_type": 0, "resolution_count": 1, "closing_costs": 0,
	})
	insertRow(t, pool, "gui_resolutions", map[string]any{
		"id": 1, "chan_id": "C1", "sweep_txid": "sw1", "resolution_type": 0,
	})

	fetch := &fakeTxFees{fees: map[string]int64{"ctx1": 100, "sw1": 50}}
	srv := NewServer(&config.Settings{}, pool, WithTxFeeFetcher(fetch))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	resp, err := client.Get(ts.URL + "/get_fees/")
	require.NoError(t, err)
	require.Equal(t, http.StatusFound, resp.StatusCode)
	var costs int
	require.NoError(t, pool.QueryRow(ctx, `SELECT closing_costs FROM gui_closures WHERE id=1`).Scan(&costs))
	require.Equal(t, 150, costs) // closing-tx 100 + sweep 50

	// Error case: fee fetch fails -> closure stays 0, error message shown.
	insertRow(t, pool, "gui_closures", map[string]any{
		"id": 2, "chan_id": "C2", "funding_txid": "fund2", "funding_index": 0,
		"closing_tx": "ctxerr", "open_initiator": 1,
		"close_type": 0, "resolution_count": 1, "closing_costs": 0,
	})
	fetch.err = map[string]bool{"ctxerr": true}
	resp, err = client.Get(ts.URL + "/get_fees/")
	require.NoError(t, err)
	require.Equal(t, http.StatusFound, resp.StatusCode)
	require.NoError(t, pool.QueryRow(ctx, `SELECT closing_costs FROM gui_closures WHERE id=2`).Scan(&costs))
	require.Equal(t, 0, costs)
}
