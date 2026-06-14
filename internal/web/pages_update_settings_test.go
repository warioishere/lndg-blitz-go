package web

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warioishere/lndg-blitz-go/internal/config"
)

func TestUpdateSettingsFormIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	insertRow(t, pool, "gui_channels", map[string]any{
		"chan_id": "700", "capacity": 10000000, "is_open": true,
	})

	srv := NewServer(&config.Settings{}, pool)
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

	resp, err := client.PostForm(ts.URL+"/update_settings/", url.Values{
		"enabled":          {"1"},
		"target_percent":   {"3"},
		"outbound_percent": {"80"},
		"gui_graphLinks":   {"https://example.com/ln"},
		"update_channels":  {"on"},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusFound, resp.StatusCode)
	msg := flashMsg(resp)
	require.Contains(t, msg, "AR-Enabled updated to: 1")
	require.Contains(t, msg, "AR-Target% updated to: 3.0")
	require.Contains(t, msg, "GUI-GraphLinks updated to: https://example.com/ln")
	require.Contains(t, msg, "All channels AR-Outbound% updated to: 80")

	settings := map[string]string{}
	rows, err := pool.Query(ctx, `SELECT key, value FROM gui_localsettings`)
	require.NoError(t, err)
	for rows.Next() {
		var k, v string
		require.NoError(t, rows.Scan(&k, &v))
		settings[k] = v
	}
	rows.Close()
	require.Equal(t, "1", settings["AR-Enabled"])
	require.Equal(t, "3.0", settings["AR-Target%"])
	require.Equal(t, "80", settings["AR-Outbound%"])
	require.Equal(t, "https://example.com/ln", settings["GUI-GraphLinks"])

	var arOut int
	var arAmt int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT ar_out_target, ar_amt_target FROM gui_channels WHERE chan_id='700'`).Scan(&arOut, &arAmt))
	require.Equal(t, 80, arOut)
	require.Equal(t, int64(300000), arAmt) // round(10_000_000 * 0.03)

	// Invalid int -> entire form rejected.
	resp, err = client.PostForm(ts.URL+"/update_settings/", url.Values{"enabled": {"abc"}})
	require.NoError(t, err)
	require.Contains(t, flashMsg(resp), invalidRequest)
}
