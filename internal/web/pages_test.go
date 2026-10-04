package web

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warioishere/lndg-blitz-go/internal/config"
)

// getPage starts a test server, fetches path, and returns the HTML body.
func getPage(t *testing.T, srv *Server, path string) string {
	t.Helper()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	resp, err := http.Get(ts.URL + path)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, resp.Header.Get("Content-Type"), "text/html")
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(body)
}

func TestPaymentsPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	insertRow(t, pool, "gui_payments", map[string]any{
		"creation_date":  time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC),
		"payment_hash":   "abc123",
		"value":          1000000.0,
		"fee":            10.0,
		"status":         2,
		"index":          1,
		"chan_out":       "111x222x0",
		"chan_out_alias": "PeerAlias",
	})
	// status=3 (failed) must be excluded.
	insertRow(t, pool, "gui_payments", map[string]any{
		"creation_date": time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC),
		"payment_hash":  "failedhash",
		"value":         500.0,
		"fee":           1.0,
		"status":        3,
		"index":         2,
	})

	srv := NewServer(&config.Settings{}, pool)
	html := getPage(t, srv, "/payments")
	require.Contains(t, html, "<title>LNDg - Payments</title>")
	require.Contains(t, html, "Last 150 Payments")
	require.Contains(t, html, "abc123")
	require.Contains(t, html, "Succeeded")
	require.Contains(t, html, "PeerAlias")
	require.Contains(t, html, "1,000,000") // value via add 0 | intcomma
	require.NotContains(t, html, "failedhash")
}

func TestInvoicesPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	insertRow(t, pool, "gui_invoices", map[string]any{
		"creation_date": time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC),
		"settle_date":   time.Date(2024, 6, 1, 12, 5, 0, 0, time.UTC),
		"r_hash":        "rhash001",
		"value":         500.0,
		"amt_paid":      500,
		"state":         1,
		"chan_in":       "111x222x0",
		"chan_in_alias": "InAlias",
		"index":         1,
	})
	// state != 1 must not appear.
	insertRow(t, pool, "gui_invoices", map[string]any{
		"creation_date": time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC),
		"r_hash":        "openinvoice",
		"value":         100.0,
		"amt_paid":      0,
		"state":         0,
		"index":         2,
	})

	srv := NewServer(&config.Settings{}, pool)
	html := getPage(t, srv, "/invoices")
	require.Contains(t, html, "<title>LNDg - Invoices</title>")
	require.Contains(t, html, "Last 150 Invoices")
	require.Contains(t, html, "rhash001")
	require.Contains(t, html, "Settled")
	require.Contains(t, html, "InAlias")
	require.NotContains(t, html, "openinvoice")
}

func TestPeersPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	insertRow(t, pool, "gui_peers", map[string]any{
		"pubkey":    "02deadbeef",
		"alias":     "NodeBob",
		"address":   "1.2.3.4:9735",
		"sat_sent":  1234,
		"sat_recv":  5678,
		"inbound":   true,
		"connected": true,
		"ping_time": 42,
	})
	insertRow(t, pool, "gui_peers", map[string]any{
		"pubkey":    "02disconnected",
		"address":   "5.6.7.8:9735",
		"sat_sent":  0,
		"sat_recv":  0,
		"inbound":   false,
		"connected": false,
	})

	srv := NewServer(&config.Settings{}, pool)
	html := getPage(t, srv, "/peers")
	require.Contains(t, html, "<title>LNDg - Peers</title>")
	require.Contains(t, html, "Peers List")
	require.Contains(t, html, "02deadbeef")
	require.Contains(t, html, "NodeBob")
	require.Contains(t, html, "True") // pybool inbound
	require.NotContains(t, html, "02disconnected")
}

func TestResolutionsPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	insertRow(t, pool, "gui_resolutions", map[string]any{
		"chan_id":         "789012",
		"resolution_type": 2,
		"outcome":         1,
		"outpoint_tx":     "txdeadbeef",
		"outpoint_index":  0,
		"amount_sat":      150000,
		"sweep_txid":      "sweeptxid",
	})

	srv := NewServer(&config.Settings{}, pool)
	html := getPage(t, srv, "/resolutions?=789012")
	require.Contains(t, html, "<title>LNDg - Resolutions</title>")
	require.Contains(t, html, "Resolutions For Channel: 789012")
	require.Contains(t, html, "Incoming HTLC")
	require.Contains(t, html, "Claimed")
	require.Contains(t, html, "150,000")
	require.Contains(t, html, "txdeadbeef")
	// resolution_type==2 -> sweep TX shows "---", not the sweep link
	require.NotContains(t, html, "sweeptxid")

	// Unknown channel -> empty state
	htmlEmpty := getPage(t, srv, "/resolutions?=000000")
	require.Contains(t, htmlEmpty, "No resolutions were found for this channel!")
}

func TestKeysendsPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	insertRow(t, pool, "gui_invoices", map[string]any{
		"creation_date":    time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC),
		"settle_date":      time.Date(2024, 6, 1, 12, 1, 0, 0, time.UTC),
		"r_hash":           "keysendhash",
		"value":            42.0,
		"amt_paid":         42,
		"state":            1,
		"chan_in":          "111x222x0",
		"chan_in_alias":    "FromPeer",
		"keysend_preimage": "deadbeefpreimage",
		"message":          "thanks!",
		"is_revenue":       true,
		"index":            1,
	})
	// Without keysend_preimage -> not listed.
	insertRow(t, pool, "gui_invoices", map[string]any{
		"creation_date": time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC),
		"r_hash":        "plaininvoice",
		"value":         10.0,
		"amt_paid":      10,
		"state":         1,
		"index":         2,
	})

	srv := NewServer(&config.Settings{}, pool)
	html := getPage(t, srv, "/keysends/")
	require.Contains(t, html, "<title>LNDg - Keysends</title>")
	require.Contains(t, html, "Received Keysends")
	require.Contains(t, html, "keysendhash")
	require.Contains(t, html, "FromPeer")
	require.Contains(t, html, "thanks!")
	require.Contains(t, html, "Unmark") // is_revenue=true
	require.NotContains(t, html, "plaininvoice")
}

func TestFeeLogPagesIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	recent := time.Now().Add(-1 * time.Hour)
	old := time.Now().Add(-30 * 24 * time.Hour)

	for _, tbl := range []string{"gui_autofees", "gui_inboundfeelog", "gui_autopilot"} {
		insertRow(t, pool, tbl, map[string]any{
			"id":         1,
			"timestamp":  recent,
			"chan_id":    "555x1x0",
			"peer_alias": "LogPeer",
			"setting":    "increase",
			"old_value":  100,
			"new_value":  150,
		})
		insertRow(t, pool, tbl, map[string]any{
			"id":         2,
			"timestamp":  old,
			"chan_id":    "555x1x0",
			"peer_alias": "OldPeer",
			"setting":    "stale",
			"old_value":  1,
			"new_value":  2,
		})
	}
	// 14 days old: outside the 7-day fee-log window, but within the
	// 21-day autopilot window.
	insertRow(t, pool, "gui_autopilot", map[string]any{
		"id":         3,
		"timestamp":  time.Now().Add(-14 * 24 * time.Hour),
		"chan_id":    "555x1x0",
		"peer_alias": "MidPeer",
		"setting":    "increase",
		"old_value":  10,
		"new_value":  20,
	})

	srv := NewServer(&config.Settings{}, pool)

	out := getPage(t, srv, "/autofees/")
	require.Contains(t, out, "<title>LNDg - Outbound Fee Log</title>")
	require.Contains(t, out, "LogPeer")
	require.NotContains(t, out, "OldPeer") // older than 7 days

	in := getPage(t, srv, "/inbound-fee-log/")
	require.Contains(t, in, "<title>LNDg - Inbound Fee Log</title>")
	require.Contains(t, in, "LogPeer")
	require.NotContains(t, in, "OldPeer")

	ap := getPage(t, srv, "/autopilot/")
	require.Contains(t, ap, "<title>LNDg - Autopilot</title>")
	require.Contains(t, ap, "LogPeer")
	require.Contains(t, ap, "MidPeer")    // 14d < autopilot window (21 days)
	require.NotContains(t, ap, "OldPeer") // 30d > 21 days
}

func TestPeerEventsPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	srv := NewServer(&config.Settings{}, pool)
	html := getPage(t, srv, "/peerevents")
	require.Contains(t, html, "<title>LNDg - Peer Events</title>")
	require.Contains(t, html, "Last Peer Events")
	require.Contains(t, html, "loadPeerEvents()")
}

func TestLogsPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	logfile := filepath.Join(t.TempDir(), "controller.log")
	require.NoError(t, os.WriteFile(logfile,
		[]byte("line one\nrebalance succeeded\nline three\n"), 0o644))
	prev := controllerLogPath
	controllerLogPath = logfile
	defer func() { controllerLogPath = prev }()

	srv := NewServer(&config.Settings{}, pool)

	// HTML page
	html := getPage(t, srv, "/logs/")
	require.Contains(t, html, "<title>LNDg - Logs</title>")
	require.Contains(t, html, "line one")
	require.Contains(t, html, "rebalance succeeded")

	// JSON format
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/logs/?format=json")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), `"size":40`)
	require.Contains(t, string(body), `"lines":[`)
	require.Contains(t, string(body), "line one\\n")

	// grep filters
	resp2, err := http.Get(ts.URL + "/logs/?format=json&grep=succeeded")
	require.NoError(t, err)
	defer resp2.Body.Close()
	body2, err := io.ReadAll(resp2.Body)
	require.NoError(t, err)
	require.Contains(t, string(body2), "rebalance succeeded")
	require.NotContains(t, string(body2), "line one")

	// no match: an empty list, which the live view's lines.map() needs
	resp3, err := http.Get(ts.URL + "/logs/?format=json&grep=nothing-matches")
	require.NoError(t, err)
	defer resp3.Body.Close()
	body3, err := io.ReadAll(resp3.Body)
	require.NoError(t, err)
	require.JSONEq(t, `{"size":40,"lines":[]}`, string(body3))
}

func TestEmergencyFeesPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id":           "700x1x0",
		"capacity":          1000000,
		"local_balance":     500000,
		"pending_outbound":  0,
		"alias":             "EmergPeer",
		"is_open":           true,
		"ep_enabled":        false,
		"ep_target":         50,
		"ep_inc_pct":        10.0,
		"ep_cooldown":       10,
		"ep_live_threshold": 40,
		"ep_live_inc_pct":   5.0,
		"last_update":       time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	// DB override for an EP setting (validates getLocalSettings + select logic).
	insertRow(t, pool, "gui_localsettings", map[string]any{"key": "EP-DefaultTarget", "value": "80"})
	insertRow(t, pool, "gui_localsettings", map[string]any{"key": "EP-Enabled", "value": "1"})

	srv := NewServer(&config.Settings{}, pool)
	html := getPage(t, srv, "/emergency-fees/")
	require.Contains(t, html, "<title>LNDg - Emergency Fees</title>")
	require.Contains(t, html, "Emergency Fee Settings")
	require.Contains(t, html, "EmergPeer")
	require.Contains(t, html, "50.0") // outbound_percent via floatformat:1
	// Settings form (partial) rendered
	require.Contains(t, html, "Emergency Fees Settings")
	require.Contains(t, html, "Default Target")
	require.Contains(t, html, `value="80"`) // DB override applied
	// EP-Enabled=1 -> option "On" selected
	require.Contains(t, html, `<option value="1" selected>On</option>`)
}

func TestInboundOffsetPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id":        "701x1x0",
		"short_chan_id":  "701x1x0",
		"remote_pubkey":  "03abcabcabc",
		"alias":          "OffsetPeer",
		"private":        false,
		"is_open":        true,
		"local_fee_rate": 100,
		"inbound_offset": -50,
		"last_update":    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	})

	srv := NewServer(&config.Settings{}, pool)
	html := getPage(t, srv, "/inbound-offset/")
	require.Contains(t, html, "<title>LNDg - Inbound Offset</title>")
	require.Contains(t, html, "Inbound Offset")
	require.Contains(t, html, "OffsetPeer")
	require.Contains(t, html, `value="-50"`)
	require.Contains(t, html, "Inbound Offset Settings") // IO partial
	require.Contains(t, html, "IO Update")
}

func TestAutoMaxhtlcPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id":          "710x1x0",
		"short_chan_id":    "710x1x0",
		"remote_pubkey":    "03aaa",
		"alias":            "MxPeer",
		"private":          false,
		"is_open":          true,
		"capacity":         2000000,
		"local_balance":    800000,
		"pending_outbound": 200000,
		"maxhtlc_percent":  0, // -> falls back to mx_percent
		"last_update":      time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	insertRow(t, pool, "gui_localsettings", map[string]any{"key": "MX-Percent", "value": "7"})

	srv := NewServer(&config.Settings{}, pool)
	html := getPage(t, srv, "/auto-maxhtlc/")
	require.Contains(t, html, "<title>LNDg - Auto MaxHTLC</title>")
	require.Contains(t, html, "MxPeer")
	require.Contains(t, html, "1,000,000") // outbound = 800000+200000
	require.Contains(t, html, `value="7"`) // mx_percent fallback
	require.Contains(t, html, "Auto MaxHTLC Settings")
}

func TestFullFeeAdjPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id":        "711x1x0",
		"short_chan_id":  "711x1x0",
		"alias":          "FeeAdjPeer",
		"is_open":        true,
		"local_fee_rate": 250,
		"last_update":    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	})

	srv := NewServer(&config.Settings{}, pool)
	html := getPage(t, srv, "/full-fee-adj/")
	require.Contains(t, html, "<title>LNDg - Full-Fee Adj</title>")
	require.Contains(t, html, "Full-Fee Adjustment")
	require.Contains(t, html, "FeeAdjPeer")
	require.Contains(t, html, ">250<")
}

func TestFeeLimitProtectionPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	// AR channel with two successful rebalance payments -> avg_ppm.
	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id":        "712x1x0",
		"alias":          "FlpPeer",
		"is_open":        true,
		"auto_rebalance": true,
		"flp_enabled":    true,
		"flp_safety":     25,
		"last_update":    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	// AR channel with no payments -> N/A.
	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id":        "713x1x0",
		"alias":          "FlpPeerNoPay",
		"is_open":        true,
		"auto_rebalance": true,
		"flp_enabled":    false,
		"flp_safety":     0,
		"last_update":    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	insertRow(t, pool, "gui_payments", map[string]any{
		"creation_date": time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC),
		"payment_hash":  "flp_p1", "value": 1000000.0, "fee": 10.0, "status": 2,
		"index": 1, "rebal_chan": "712x1x0",
	})
	insertRow(t, pool, "gui_payments", map[string]any{
		"creation_date": time.Date(2024, 6, 2, 12, 0, 0, 0, time.UTC),
		"payment_hash":  "flp_p2", "value": 1000000.0, "fee": 30.0, "status": 2,
		"index": 2, "rebal_chan": "712x1x0",
	})

	srv := NewServer(&config.Settings{}, pool)
	html := getPage(t, srv, "/fee-limit-protection/")
	require.Contains(t, html, "<title>LNDg - AF Fee Limit Protection</title>")
	require.Contains(t, html, "FlpPeer")
	require.Contains(t, html, "20")  // avg_ppm = int((10+30)/2) = 20
	require.Contains(t, html, "N/A") // FlpPeerNoPay
	require.Contains(t, html, "Fee Limit Protection Settings")
}

func TestFeesPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id":        "720x1x0",
		"short_chan_id":  "720x1x0",
		"remote_pubkey":  "03feefee",
		"alias":          "FeesPeer",
		"private":        false,
		"is_open":        true,
		"capacity":       1000000,
		"local_balance":  500000,
		"local_fee_rate": 100,
		"last_update":    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	// Private channel -> excluded from analysis.
	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id":       "721x1x0",
		"short_chan_id": "721x1x0",
		"remote_pubkey": "03privpriv",
		"alias":         "PrivatePeer",
		"private":       true,
		"is_open":       true,
		"capacity":      1000000,
		"local_balance": 500000,
		"last_update":   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	})

	srv := NewServer(&config.Settings{}, pool)
	html := getPage(t, srv, "/fees/")
	require.Contains(t, html, "<title>LNDg - Fee Rates</title>")
	require.Contains(t, html, "Suggested Fee Rates")
	require.Contains(t, html, "FeesPeer")
	require.Contains(t, html, "50%") // out_percent = 500000/1000000
	require.Contains(t, html, "Auto-Fees Settings")
	require.NotContains(t, html, "PrivatePeer") // private=True excluded
}

func TestAdvancedPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id":             "730x1x0",
		"short_chan_id":       "730x1x0",
		"remote_pubkey":       "03advadv",
		"alias":               "AdvPeer",
		"private":             false,
		"is_open":             true,
		"is_active":           true,
		"capacity":            1000000,
		"local_balance":       600000,
		"pending_outbound":    0,
		"remote_balance":      400000,
		"pending_inbound":     0,
		"local_fee_rate":      200,
		"remote_fee_rate":     100,
		"local_min_htlc_msat": 1000,
		"local_max_htlc_msat": 1650000000,
		"last_update":         time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	})

	srv := NewServer(&config.Settings{}, pool)
	html := getPage(t, srv, "/advanced/")
	require.Contains(t, html, "<title>LNDg - Advanced</title>")
	require.Contains(t, html, "Advanced Channel Settings")
	require.Contains(t, html, "AdvPeer")
	require.Contains(t, html, "60%")                   // out_percent = round(600/10)
	require.Contains(t, html, "Fee Ratio: 50%")        // (100/200)*100
	require.Contains(t, html, `value="1.0"`)           // local_min_htlc = 1000/1000
	require.Contains(t, html, "Update Local Settings") // local_settings partial
	require.Contains(t, html, "Node cache: 0 entries")
}

func TestAdvancedRebalancingPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	// Source: low outgoing rate, AR source active.
	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id":            "800x1x0",
		"remote_pubkey":      "03aaa",
		"alias":              "SourcePeer",
		"is_open":            true,
		"auto_rebalance":     true,
		"ar_source":          true,
		"local_fee_rate":     100,
		"ar_max_cost":        80,
		"ar_source_ppm_diff": 0,
		"ar_out_target":      75,
		"ar_amt_target":      100000,
		"capacity":           1000000,
		"local_balance":      500000,
		"pending_outbound":   0,
		"last_update":        time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	// Candidate: higher outgoing rate -> allowed as a target for the source.
	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id":        "801x1x0",
		"remote_pubkey":  "03bbb",
		"alias":          "TargetPeer",
		"is_open":        true,
		"auto_rebalance": true,
		"ar_source":      false,
		"local_fee_rate": 300,
		"ar_max_cost":    50,
		"capacity":       1000000,
		"local_balance":  500000,
		"last_update":    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	insertRow(t, pool, "gui_allowedtarget", map[string]any{
		"id": 1, "source_chan_id": "800x1x0", "target_pubkey": "03bbb",
	})

	srv := NewServer(&config.Settings{}, pool)
	html := getPage(t, srv, "/advanced_rebalancing")
	require.Contains(t, html, "<title>LNDg - Advanced Rebalancing</title>")
	require.Contains(t, html, "Advanced Rebalancing")
	require.Contains(t, html, "SourcePeer (100)")
	require.Contains(t, html, "Disable") // ar_source=true on source
	// TargetPeer is selected as an allowed target for the source.
	require.Contains(t, html, `<option value="03bbb" selected>TargetPeer (300)</option>`)
}

func TestAmbossFeesPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	insertRow(t, pool, "gui_localsettings", map[string]any{"key": "AMB-Enabled", "value": "1"})
	insertRow(t, pool, "gui_localsettings", map[string]any{"key": "AMB-UpdateHours", "value": "0.5"})

	srv := NewServer(&config.Settings{}, pool)
	html := getPage(t, srv, "/amboss-fees/")
	require.Contains(t, html, "<title>LNDg - Amboss Fees</title>")
	require.Contains(t, html, "Channel Fee History")
	require.Contains(t, html, "Amboss Settings") // local_settings partial
	// Server values as window config + external JS (html/template JS escaper
	// pads the value with spaces -> check with whitespace tolerance).
	require.Regexp(t, `window\.AMB_ENABLED\s*=\s*1\s*;`, html)
	require.Regexp(t, `window\.AMB_UPDATE_HOURS\s*=\s*0\.5\s*;`, html)
	require.Contains(t, html, `<script src="/static/amboss_fees.js"></script>`)
	require.Contains(t, html, `<script src="/static/charts.js"></script>`)

	// The external JS is also served.
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/static/amboss_fees.js")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestRebalancesPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	srv := NewServer(&config.Settings{}, pool)
	html := getPage(t, srv, "/rebalances")
	require.Contains(t, html, "<title>LNDg - Rebalances</title>")
	// External JS loaded once.
	require.Contains(t, html, `<script src="/static/rebalances_table.js"></script>`)
	// Four tables with status-specific headings + IDs.
	require.Contains(t, html, "In-Flight Rebalance Requests")  // status=1
	require.Contains(t, html, "Pending Rebalance Requests")    // status=0
	require.Contains(t, html, "Successful Rebalance Requests") // status=2
	require.Contains(t, html, "Last Rebalance Requests")       // no status filter
	require.Contains(t, html, `id="rebalances_div_1"`)
	require.Contains(t, html, `id="rebalances_div_"`) // empty status
	require.Contains(t, html, `<a data-status="2" onclick="window['loadRebals_'+this.dataset.status]()">Load More</a>`)
	// Config script calls initRebalancesTable with server params.
	require.Regexp(t, `initRebalancesTable\(\{\s*status:\s*"1"`, html)
	require.Regexp(t, `count:\s*"50"`, html) // 4th block

	// The external JS is served.
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/static/rebalances_table.js")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestAppendSlashRedirect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	srv := NewServer(&config.Settings{}, pool)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// No redirect follow: we want to see the 301 directly.
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	resp, err := client.Get(ts.URL + "/keysends?=123")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusMovedPermanently, resp.StatusCode)
	require.Equal(t, "/keysends/?=123", resp.Header.Get("Location"))
}
