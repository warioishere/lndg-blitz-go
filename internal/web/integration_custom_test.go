package web

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/warioishere/lndg-blitz-go/internal/config"
)

// insertRow inserts a row into table: all NOT-NULL columns without a default
// are filled with typed zero values; overrides set the relevant fields (and
// optionally nullable columns). Builds the INSERT from information_schema so
// the test does not need to hard-code ~65 channel columns.
func insertRow(t *testing.T, pool *pgxpool.Pool, table string, overrides map[string]any) {
	t.Helper()
	ctx := context.Background()
	rows, err := pool.Query(ctx,
		`SELECT column_name, data_type, is_nullable, column_default
		 FROM information_schema.columns WHERE table_name=$1 ORDER BY ordinal_position`, table)
	require.NoError(t, err)
	type col struct {
		name, dtype, nullable string
		hasDefault            bool
	}
	var cols []col
	for rows.Next() {
		var c col
		var def *string
		require.NoError(t, rows.Scan(&c.name, &c.dtype, &c.nullable, &def))
		c.hasDefault = def != nil
		cols = append(cols, c)
	}
	require.NoError(t, rows.Err())

	var names []string
	var placeholders []string
	var args []any
	for _, c := range cols {
		val, overridden := overrides[c.name]
		if !overridden {
			if c.nullable == "YES" || c.hasDefault {
				continue // null/default is sufficient
			}
			val = zeroForType(c.dtype)
		}
		names = append(names, `"`+c.name+`"`)
		args = append(args, val)
		placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)))
	}
	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table,
		strings.Join(names, ", "), strings.Join(placeholders, ", "))
	_, err = pool.Exec(ctx, sql, args...)
	require.NoError(t, err, sql)
}

func zeroForType(dtype string) any {
	switch dtype {
	case "integer", "bigint", "smallint":
		return 0
	case "double precision", "numeric", "real":
		return 0.0
	case "boolean":
		return false
	case "timestamp with time zone", "timestamp without time zone":
		return time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	case "date":
		return time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	case "jsonb", "json":
		return []byte("{}")
	case "bytea":
		return []byte{}
	default: // character varying, text, ...
		return ""
	}
}

func TestPeerEventsViewSetIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	insertRow(t, pool, "gui_channels", map[string]any{"chan_id": "chanX", "alias": "chanXalias", "capacity": int64(1000)})
	insertRow(t, pool, "gui_peerevents", map[string]any{
		"id": int64(1), "chan_id": "chanX", "peer_alias": "p", "event": "MaxHTLC",
		"new_value": int64(5), "out_liq": int64(250),
	})

	srv := NewServer(&config.Settings{}, pool)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	var page struct {
		Count   int              `json:"count"`
		Results []map[string]any `json:"results"`
	}
	getJSON(t, ts.URL+"/api/peerevents/", &page)
	require.Equal(t, 1, page.Count)
	// round(250/1000*100, 1) = 25.0 -> int 25
	require.EqualValues(t, 25, page.Results[0]["out_liq_percent"])
	require.EqualValues(t, 250, page.Results[0]["out_liq"])
}

func TestRebalanceRouteViewSetIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	insertRow(t, pool, "gui_peers", map[string]any{"pubkey": "pk1", "alias": "peer1"})
	insertRow(t, pool, "gui_channels", map[string]any{"chan_id": "cid1", "alias": "chanA", "capacity": int64(1)})
	insertRow(t, pool, "gui_rebalanceroute", map[string]any{
		"id": int64(1), "target_pubkey": "pk1", "outgoing_chan_id": "cid1",
		"route": "pk1", "success_count": 3, "failure_count": 1,
	})

	srv := NewServer(&config.Settings{}, pool)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	var page struct {
		Count   int              `json:"count"`
		Results []map[string]any `json:"results"`
	}
	getJSON(t, ts.URL+"/api/rebalanceroutes/", &page)
	require.Equal(t, 1, page.Count)
	r := page.Results[0]
	require.InDelta(t, 4.0/6.0, r["success_ratio"], 1e-9)
	require.InDelta(t, (4.0/6.0)*(4.0/14.0), r["weighted_ratio"], 1e-9)
	require.Equal(t, "peer1", r["target_alias"])
	require.Equal(t, "chanA", r["outgoing_alias"])
}

func TestNodeReputationViewSetIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	insertRow(t, pool, "gui_peers", map[string]any{"pubkey": "pk2", "alias": "peer2"})
	insertRow(t, pool, "gui_nodereputation", map[string]any{
		"pubkey": "pk2", "success_count": 2, "failure_count": 0,
	})

	srv := NewServer(&config.Settings{}, pool)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	var page struct {
		Count   int              `json:"count"`
		Results []map[string]any `json:"results"`
	}
	getJSON(t, ts.URL+"/api/nodereputation/", &page)
	require.Equal(t, 1, page.Count)
	r := page.Results[0]
	// exactly the 7 fields from the explicit serializer fields list
	keys := make([]string, 0, len(r))
	for k := range r {
		keys = append(keys, k)
	}
	require.ElementsMatch(t, []string{"pubkey", "success_count", "failure_count", "last_success", "last_failure", "weighted_ratio", "alias"}, keys)
	require.InDelta(t, (3.0/4.0)*(2.0/12.0), r["weighted_ratio"], 1e-9)
	require.Equal(t, "peer2", r["alias"])
}

func TestProbeLogViewSetIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	insertRow(t, pool, "gui_probelog", map[string]any{
		"id": 1, "targets_scanned": 5, "routes_found": 2,
		"details": []byte(`{"a":1,"b":[2,3]}`),
	})

	srv := NewServer(&config.Settings{}, pool)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	var page struct {
		Count   int              `json:"count"`
		Results []map[string]any `json:"results"`
	}
	getJSON(t, ts.URL+"/api/probelogs/", &page)
	require.Equal(t, 1, page.Count)
	// details (jsonb) must appear as a JSON object, not a base64 string.
	details, ok := page.Results[0]["details"].(map[string]any)
	require.True(t, ok, "details should decode to a JSON object, got %T", page.Results[0]["details"])
	require.EqualValues(t, 1, details["a"])
}
