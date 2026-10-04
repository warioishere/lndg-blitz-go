package web

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warioishere/lndg-blitz-go/internal/config"
)

// sendJSON issues method url with a JSON body and returns status and raw body.
func sendJSON(t *testing.T, method, url string, body any) (int, []byte) {
	t.Helper()
	b, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequest(method, url, bytes.NewReader(b))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, out
}

// TestViewSetWritesIntegration checks the DRF write routes against the answers
// the Python API gives for the same requests.
func TestViewSetWritesIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	now := time.Now()
	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id": "879609302220865536", "short_chan_id": "800000x1x0", "local_fee_rate": 500,
		"ar_in_target": 90, "last_update": now, "fees_updated": now, "ep_updated": now,
	})
	insertRow(t, pool, "gui_invoices", map[string]any{"r_hash": "ab", "index": 7, "creation_date": now})
	insertRow(t, pool, "gui_localsettings", map[string]any{"key": "T-X", "value": "1"})

	ts := httptest.NewServer(NewServer(&config.Settings{}, pool).Handler())
	defer ts.Close()
	api := ts.URL + "/api/"
	obj := func(b []byte) map[string]any {
		var m map[string]any
		require.NoError(t, json.Unmarshal(b, &m), string(b))
		return m
	}

	t.Run("channel update", func(t *testing.T) {
		st, b := sendJSON(t, "PUT", api+"channels/879609302220865536/",
			map[string]any{"ar_in_target": "55", "notes": " x ", "local_fee_rate": 999, "bogus": 1, "auto_rebalance": true})
		require.Equal(t, 200, st, string(b))
		m := obj(b)
		assert.EqualValues(t, 55, m["ar_in_target"])
		assert.Equal(t, "x", m["notes"], "trim_whitespace")
		assert.EqualValues(t, 500, m["local_fee_rate"], "read-only field ignored")
		assert.Equal(t, true, m["auto_rebalance"])
		assert.EqualValues(t, 800000, m["opened_in"])

		st, b = sendJSON(t, "PUT", api+"channels/879609302220865536/", map[string]any{"ar_in_target": "abc", "auto_fees": "maybe"})
		require.Equal(t, 200, st)
		assert.JSONEq(t, `{"ar_in_target":["A valid integer is required."],"auto_fees":["Must be a valid boolean."]}`, string(b))

		var notes string
		require.NoError(t, pool.QueryRow(ctx, `SELECT notes FROM gui_channels WHERE chan_id='879609302220865536'`).Scan(&notes))
		assert.Equal(t, "x", notes, "invalid request changes nothing")

		st, b = sendJSON(t, "PUT", api+"channels/999/", map[string]any{"ar_in_target": 5})
		require.Equal(t, 404, st)
		assert.JSONEq(t, `{"detail":"No Channels matches the given query."}`, string(b))
	})

	t.Run("rebalancer create", func(t *testing.T) {
		st, b := sendJSON(t, "POST", api+"rebalancer/", map[string]any{
			"value": "50000", "fee_limit": 10, "outgoing_chan_ids": "[1, 2]", "last_hop_pubkey": "02aa",
			"duration": 5, "target_alias": "x", "manual": false, "status": 2, "stop": "2020-01-01"})
		require.Equal(t, 200, st, string(b))
		m := obj(b)
		assert.EqualValues(t, 50000, m["value"])
		assert.EqualValues(t, 2, m["status"])
		assert.Nil(t, m["stop"], "read-only field ignored")
		assert.NotNil(t, m["requested"])
		assert.NotNil(t, m["id"])

		st, b = sendJSON(t, "POST", api+"rebalancer/", map[string]any{"value": 1})
		require.Equal(t, 200, st)
		assert.Equal(t, `{"fee_limit":["This field is required."],"duration":["This field is required."]}`, string(b), "DRF field order")

		st, b = sendJSON(t, "POST", api+"rebalancer/", map[string]any{"value": "x", "fee_limit": "y", "duration": 1})
		require.Equal(t, 200, st)
		assert.Equal(t, `{"value":["A valid integer is required."],"fee_limit":["A valid number is required."]}`, string(b))

		// defaults for omitted fields
		st, b = sendJSON(t, "POST", api+"rebalancer/", map[string]any{"value": 1000, "fee_limit": 1.5, "duration": 1})
		require.Equal(t, 200, st)
		m = obj(b)
		assert.Equal(t, "[]", m["outgoing_chan_ids"])
		assert.EqualValues(t, 0, m["status"])
		assert.Equal(t, false, m["manual"])
	})

	t.Run("rebalancer cancel", func(t *testing.T) {
		var id int64
		require.NoError(t, pool.QueryRow(ctx, `SELECT id FROM gui_rebalancer ORDER BY id LIMIT 1`).Scan(&id))
		st, b := sendJSON(t, "PUT", api+"rebalancer/"+jsonInt(id)+"/", map[string]any{"status": 499})
		require.Equal(t, 200, st, string(b))
		m := obj(b)
		assert.EqualValues(t, 499, m["status"])
		assert.NotNil(t, m["stop"], "stop stamped on update")
	})

	t.Run("invoice and setting", func(t *testing.T) {
		st, b := sendJSON(t, "PUT", api+"invoices/ab/", map[string]any{"is_revenue": true})
		require.Equal(t, 200, st)
		assert.JSONEq(t, `{"id":["This field is required."]}`, string(b))
		st, b = sendJSON(t, "PUT", api+"invoices/ab/", map[string]any{"is_revenue": true, "id": 7})
		require.Equal(t, 200, st)
		m := obj(b)
		assert.Equal(t, true, m["is_revenue"])
		assert.EqualValues(t, 7, m["id"])

		st, b = sendJSON(t, "PUT", api+"settings/T-X/", map[string]any{"value": "2"})
		require.Equal(t, 200, st)
		assert.Equal(t, `{"url":"`+api+`settings/T-X/","key":"T-X","value":"2"}`, string(b), "DRF shape: url first")
		st, b = sendJSON(t, "PUT", api+"settings/T-X/", map[string]any{})
		require.Equal(t, 200, st)
		assert.Equal(t, `{"url":"`+api+`settings/T-X/","key":"T-X","value":"2"}`, string(b), "value not required, unchanged")
	})

	t.Run("rebalance routes", func(t *testing.T) {
		insertRow(t, pool, "gui_peers", map[string]any{"pubkey": "02known", "alias": "Known", "address": "x", "inbound": false, "connected": true})
		ids := map[string]int64{}
		for _, rt := range []struct {
			name, route string
			success     int
		}{{"tested", "02known-03unknown", 1}, {"untested", "02known", 0}, {"gone", "02x", 0}} {
			var id int64
			require.NoError(t, pool.QueryRow(ctx, `INSERT INTO gui_rebalanceroute (target_pubkey, outgoing_chan_id, route, final_cltv_delta, success_count, failure_count)
				VALUES ('t', '1', $1, 40, $2, 0) RETURNING id`, rt.route, rt.success).Scan(&id))
			ids[rt.name] = id
		}

		// retrieve() adds the hops with peer aliases ("" for an unknown peer)
		resp, err := http.Get(api + "rebalanceroutes/" + jsonInt(ids["tested"]) + "/")
		require.NoError(t, err)
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		hops := obj(b)["hops"]
		assert.Equal(t, []any{
			map[string]any{"attempt_id": float64(1), "step": float64(1), "alias": "Known", "pubkey": "02known"},
			map[string]any{"attempt_id": float64(1), "step": float64(2), "alias": "", "pubkey": "03unknown"},
		}, hops)

		st, b := sendJSON(t, "DELETE", api+"rebalanceroutes/"+jsonInt(ids["gone"])+"/", nil)
		require.Equal(t, 200, st)
		assert.JSONEq(t, `{"message":"Deleted"}`, string(b))
		st, b = sendJSON(t, "DELETE", api+"rebalanceroutes/"+jsonInt(ids["gone"])+"/", nil)
		require.Equal(t, 404, st)
		assert.JSONEq(t, `{"detail":"No RebalanceRoute matches the given query."}`, string(b))

		st, b = sendJSON(t, "POST", api+"rebalanceroutes/cleanup_untested/", nil)
		require.Equal(t, 200, st)
		assert.JSONEq(t, `{"deleted":1}`, string(b), "only the never tried route")
		var left int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM gui_rebalanceroute`).Scan(&left))
		assert.Equal(t, 1, left)
	})
}

func jsonInt(n int64) string { b, _ := json.Marshal(n); return string(b) }
