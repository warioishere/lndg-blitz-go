package web

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warioishere/lndg-blitz-go/internal/config"
)

func TestActionsPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	recent := time.Now().Add(-1 * time.Hour)

	chanDefaults := func(over map[string]any) map[string]any {
		base := map[string]any{
			"capacity": 10000000, "pending_outbound": 0, "pending_inbound": 0,
			"unsettled_balance": 0, "local_base_fee": 0, "remote_base_fee": 0,
			"is_active": true, "is_open": true, "private": false,
		}
		for k, v := range over {
			base[k] = v
		}
		return base
	}

	// Channel E: enable AR (o7D > i7D, inbound > 75%, AR off, local_fee > remote_fee).
	insertRow(t, pool, "gui_channels", chanDefaults(map[string]any{
		"chan_id": "111", "short_chan_id": "111x111", "remote_pubkey": "03enable",
		"alias": "EnablePeer", "local_balance": 2000000, "remote_balance": 8000000,
		"local_fee_rate": 500, "remote_fee_rate": 100, "auto_rebalance": false,
	}))
	// Channel D: disable AR (o7D < i7D, outbound > 75%, AR on).
	insertRow(t, pool, "gui_channels", chanDefaults(map[string]any{
		"chan_id": "222", "short_chan_id": "222x222", "remote_pubkey": "03disable",
		"alias": "DisablePeer", "local_balance": 8000000, "remote_balance": 2000000,
		"local_fee_rate": 100, "remote_fee_rate": 100, "auto_rebalance": true,
	}))
	// Channel N: balanced, no forwards -> filtered out (continue).
	insertRow(t, pool, "gui_channels", chanDefaults(map[string]any{
		"chan_id": "333", "short_chan_id": "333x333", "remote_pubkey": "03neutral",
		"alias": "NeutralPeer", "local_balance": 5000000, "remote_balance": 5000000,
		"local_fee_rate": 100, "remote_fee_rate": 100, "auto_rebalance": false,
	}))

	// E: one outbound forward (o7D=1.0, i7D=0).
	insertRow(t, pool, "gui_forwards", map[string]any{
		"id": 1, "forward_date": recent, "chan_id_in": "999", "chan_id_out": "111",
		"amt_in_msat": 1000000000, "amt_out_msat": 1000000000, "fee": 1.0, "inbound_fee": 0.0,
	})
	// D: one inbound forward (i7D=1.0, o7D=0).
	insertRow(t, pool, "gui_forwards", map[string]any{
		"id": 2, "forward_date": recent, "chan_id_in": "222", "chan_id_out": "999",
		"amt_in_msat": 1000000000, "amt_out_msat": 1000000000, "fee": 1.0, "inbound_fee": 0.0,
	})

	srv := NewServer(&config.Settings{}, pool)

	html := getPage(t, srv, "/actions/")
	require.Contains(t, html, "<title>LNDg - Actions</title>")
	require.Contains(t, html, "Suggested Action List")

	// Enable AR row.
	require.Contains(t, html, "111x111")
	require.Contains(t, html, "EnablePeer")
	require.Contains(t, html, "Enable AR")
	require.Contains(t, html, `value="Enable"`)
	require.Contains(t, html, "1.0M") // o7D
	require.Contains(t, html, "80%")  // inbound percent

	// Disable AR row.
	require.Contains(t, html, "222x222")
	require.Contains(t, html, "DisablePeer")
	require.Contains(t, html, "Disable AR")
	require.Contains(t, html, `value="Disable"`)

	// Neutral channel is filtered out.
	require.NotContains(t, html, "NeutralPeer")
	require.NotContains(t, html, "333x333")
}

func TestActionsPageEmptyIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	srv := NewServer(&config.Settings{}, pool)
	html := getPage(t, srv, "/actions/")
	require.Contains(t, html, "Nothing to see here! Great job!")
	require.NotContains(t, html, "Suggested Action List")
}
