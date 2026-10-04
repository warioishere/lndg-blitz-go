package web

import (
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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
	// reference values from Django's floatformat (half up on str(value))
	for _, c := range []struct {
		v    any
		arg  int
		want string
	}{
		{1234.5, 0, "1235"}, {2.675, 2, "2.68"}, {-0.4, 0, "0"}, {-2.5, 0, "-3"}, {0.05, 1, "0.1"},
		{7, 1, "7.0"}, {int64(5), 0, "5"}, {-0.04, 1, "0.0"}, {1e21, 0, "1000000000000000000000"}, {99.95, 1, "100.0"},
	} {
		require.Equal(t, c.want, floatformat(c.arg, c.v), "%v|floatformat:%d", c.v, c.arg)
	}
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

// Reference strings from Django's naturaltime (count and unit joined by U+00A0).
func TestNaturaltime(t *testing.T) {
	now := time.Now()
	for _, c := range []struct {
		ago  time.Duration
		want string
	}{
		{1 * time.Second, "a second ago"},
		{30 * time.Second, "30 seconds ago"},
		{90 * time.Second, "a minute ago"},
		{5 * time.Minute, "5 minutes ago"},
		{time.Hour + time.Minute, "an hour ago"},
		{3*time.Hour + time.Minute, "3 hours ago"},
		{29*time.Hour + time.Minute, "1 day, 5 hours ago"},
		{9*24*time.Hour + time.Minute, "1 week, 2 days ago"},
		{-(3*time.Hour - 30*time.Minute), "2 hours from now"},
	} {
		require.Equal(t, c.want, naturaltime(now.Add(-c.ago)), "%v ago", c.ago)
	}
	require.Equal(t, "1 year, 1 month",
		timesince(time.Date(2013, 2, 10, 0, 0, 0, 0, time.UTC), time.Date(2014, 3, 10, 0, 0, 0, 0, time.UTC)))
}

// Reference strings from Python's str(float).
func TestPyFloatString(t *testing.T) {
	for f, want := range map[float64]string{
		12: "12.0", 0.1: "0.1", 1e-05: "1e-05", 0.0001: "0.0001", 1.5e16: "1.5e+16",
		1234567.0: "1234567.0", 1e15: "1000000000000000.0", math.Inf(1): "inf", math.Inf(-1): "-inf",
	} {
		require.Equal(t, want, pyFloatString(f), "%v", f)
	}
	require.Equal(t, "nan", pyFloatString(math.NaN()))
	require.Equal(t, "-0.0", pyFloatString(math.Copysign(0, -1)))
}
