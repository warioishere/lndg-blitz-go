package web

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

// Django's form views redirect a GET; DRF answers a wrong method with a JSON
// 405; the plain Django amboss view has its own 405 message.
func TestMethodFallbacks(t *testing.T) {
	ts := httptest.NewServer(NewServer(&config.Settings{}, nil).Handler())
	defer ts.Close()
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	for path, want := range map[string]string{
		"/update_channel/": "/back", "/openchannel/": "/", "/batchopen/": "/batch",
		"/reset_node_reputation/": "/back",
	} {
		req, _ := http.NewRequest("GET", ts.URL+path, nil)
		req.Header.Set("Referer", "/back")
		resp, err := noRedirect.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, http.StatusFound, resp.StatusCode, path)
		require.Equal(t, want, resp.Header.Get("Location"), path)
	}
	resp, err := noRedirect.Get(ts.URL + "/reset_node_reputation/")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, "/rebalanceroutes", resp.Header.Get("Location"), "no referer")

	for path, want := range map[string]string{
		"/api/chanpolicy/":          `{"detail":"Method \"GET\" not allowed."}`,
		"/api/amboss_channel_fees/": `{"error":"Only GET method allowed"}`,
	} {
		method := "GET"
		if path == "/api/amboss_channel_fees/" {
			method = "POST"
		}
		req, _ := http.NewRequest(method, ts.URL+path, nil)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode, path)
		require.JSONEq(t, want, string(b), path)
	}
}

func TestGrpcCodeString(t *testing.T) {
	require.Equal(t, "StatusCode.DEADLINE_EXCEEDED", grpcCodeString(status.Error(codes.DeadlineExceeded, "x")))
	require.Equal(t, "StatusCode.UNAVAILABLE", grpcCodeString(status.Error(codes.Unavailable, "x")))
	require.Equal(t, "StatusCode.OK", grpcCodeString(status.Error(codes.OK, "")))
	require.Equal(t, "plain", grpcCodeString(errors.New("plain")))
}
