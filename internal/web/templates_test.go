package web

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warioishere/lndg-blitz-go/internal/config"
)

func TestParseTemplates(t *testing.T) {
	tmpls, err := parseTemplates()
	require.NoError(t, err)
	require.Contains(t, tmpls, "reset.html")
}

func TestIntcomma(t *testing.T) {
	require.Equal(t, "1,234,567", intcomma(1234567))
	require.Equal(t, "1,234", intcomma(int64(1234)))
	require.Equal(t, "999", intcomma(999))
	require.Equal(t, "-12,345", intcomma(-12345))
	require.Equal(t, "1,234.5", intcomma(1234.5))
	require.Equal(t, "0", intcomma(0))
}

func TestDjangoSlice(t *testing.T) {
	require.Equal(t, "abcdefg", djangoSlice(":7", "abcdefghij"))
	require.Equal(t, "cdefghij", djangoSlice("2:", "abcdefghij"))
	require.Equal(t, "cde", djangoSlice("2:5", "abcdefghij"))
}

func TestFloatformat(t *testing.T) {
	require.Equal(t, "1.50", floatformat(2, 1.5))
	require.Equal(t, "2", floatformat(0, 2.4))
	require.Equal(t, "3.14", floatformat(2, 3.14159))
}

func TestDjangoDefaultAndAdd(t *testing.T) {
	require.Equal(t, "---", djangoDefault("---", ""))
	require.Equal(t, "x", djangoDefault("---", "x"))
	require.EqualValues(t, int64(5), djangoAdd(2, 3))
	require.Equal(t, "ab", djangoAdd("b", "a"))
}

func TestResetPageIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()

	srv := NewServer(&config.Settings{}, pool)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/reset/")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, resp.Header.Get("Content-Type"), "text/html")

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	html := string(body)
	require.Contains(t, html, "Reset Data")
	require.Contains(t, html, "<title>LNDg - Reset</title>")
	require.Contains(t, html, "Forwards")
	require.Contains(t, html, "LocalSettings")
	// base.html layout present (config script + external JS file)
	require.Contains(t, html, "window.GRAPH_LINKS")
	require.Contains(t, html, "/static/gui_base.js")
}
