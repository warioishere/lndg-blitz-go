package web

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warioishere/lndg-blitz-go/internal/config"
)

func TestForwardsPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	srv := NewServer(&config.Settings{}, pool)
	html := getPage(t, srv, "/forwards")
	require.Contains(t, html, "<title>LNDg - Forwards</title>")
	require.Contains(t, html, "Last Forwards")
	require.Contains(t, html, "loadForwards()")
}

func TestFailedHtlcsPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	recent := time.Now().Add(-1 * time.Hour)
	for i, amt := range []int{1000, 2000} {
		insertRow(t, pool, "gui_failedhtlcs", map[string]any{
			"id": i + 1, "timestamp": recent, "amount": amt,
			"chan_id_in": "aaa", "chan_id_out": "bbb",
			"chan_in_alias": "InP", "chan_out_alias": "OutP", "wire_failure": 99,
		})
	}
	// wire_failure != 99 -> excluded.
	insertRow(t, pool, "gui_failedhtlcs", map[string]any{
		"id": 3, "timestamp": recent, "amount": 5000,
		"chan_id_in": "ccc", "chan_id_out": "ddd", "wire_failure": 1,
	})

	srv := NewServer(&config.Settings{}, pool)
	html := getPage(t, srv, "/failed_htlcs")
	require.Contains(t, html, "<title>LNDg - Failed HTLCs</title>")
	require.Contains(t, html, "Top 21 Failed Downstream Routes")
	require.Contains(t, html, "InP")
	require.Contains(t, html, "OutP")
	require.Contains(t, html, "3,000") // volume = 1000+2000
	require.Contains(t, html, "Last Failed HTLCs")
	require.NotContains(t, html, "ccc") // wire_failure != 99
}

func TestGraphWatcherPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	insertRow(t, pool, "gui_localsettings", map[string]any{"key": "GW-Enabled", "value": "1"})
	insertRow(t, pool, "gui_localsettings", map[string]any{"key": "GW-Exclude", "value": "pk1, pk2"})

	srv := NewServer(&config.Settings{}, pool)
	html := getPage(t, srv, "/graphwatcher")
	require.Contains(t, html, "<title>LNDg - Graph Watcher</title>")
	require.Contains(t, html, "Graph Watcher Settings")
	require.Contains(t, html, `<option value="1" selected>On</option>`) // gw_enabled
	// gw_exclude_json injected as a raw JS array.
	require.Contains(t, html, `new Set(["pk1","pk2"].map(String))`)
}

func TestRebalancingPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	srv := NewServer(&config.Settings{}, pool)
	html := getPage(t, srv, "/rebalancing?auto_rebalance=true&is_active=true")
	require.Contains(t, html, "<title>LNDg - Rebalancing</title>")
	require.Contains(t, html, `<script src="/static/rebalancing.js"></script>`)
	require.Contains(t, html, `<script src="/static/rebalances_table.js"></script>`)
	require.Regexp(t, `window\.REB_AUTO_REBALANCE\s*=\s*"true"`, html)
	require.Regexp(t, `window\.REB_IS_ACTIVE\s*=\s*"true"`, html)
	require.Contains(t, html, "Auto-Rebalancer Settings") // local_settings partial
}

func TestRebalanceRoutesPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	insertRow(t, pool, "gui_localsettings", map[string]any{"key": "RR-RouteLimit", "value": "25"})
	insertRow(t, pool, "gui_localsettings", map[string]any{"key": "QR-Enabled", "value": "1"})

	srv := NewServer(&config.Settings{}, pool)
	html := getPage(t, srv, "/rebalanceroutes")
	require.Contains(t, html, "<title>LNDg - Saved Routes</title>")
	require.Contains(t, html, "Route Settings")
	require.Contains(t, html, `<script src="/static/rebalance_routes.js"></script>`)
	require.Contains(t, html, `value="25"`)                             // route_limit override
	require.Contains(t, html, `<option value="1" selected>On</option>`) // qr_enabled
}
