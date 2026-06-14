package web

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/warioishere/lndg-blitz-go/internal/config"
)

func TestServerServesStaticAssets(t *testing.T) {
	srv := NewServer(&config.Settings{}, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/static/w3style.css")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestServerFaviconRedirect(t *testing.T) {
	srv := NewServer(&config.Settings{}, nil)

	req := httptest.NewRequest(http.MethodGet, "/favicon.ico", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusFound, rec.Code)
	require.Equal(t, "/static/favicon.ico", rec.Header().Get("Location"))
}

func TestBasicAuth(t *testing.T) {
	cfg := &config.Settings{WEB_BASIC_AUTH_USER: "alice", WEB_BASIC_AUTH_PASS: "secret"}
	srv := NewServer(cfg, nil)

	// Without credentials -> 401 with WWW-Authenticate.
	req := httptest.NewRequest(http.MethodGet, "/static/w3style.css", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.NotEmpty(t, rec.Header().Get("WWW-Authenticate"))

	// With correct credentials -> 200.
	req = httptest.NewRequest(http.MethodGet, "/static/w3style.css", nil)
	req.SetBasicAuth("alice", "secret")
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Wrong password -> 401.
	req = httptest.NewRequest(http.MethodGet, "/static/w3style.css", nil)
	req.SetBasicAuth("alice", "wrong")
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestBasicAuthDisabledWhenNoCredentials(t *testing.T) {
	srv := NewServer(&config.Settings{}, nil)
	require.False(t, srv.basicAuthEnabled())

	req := httptest.NewRequest(http.MethodGet, "/static/w3style.css", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}
