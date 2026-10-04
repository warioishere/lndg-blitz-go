package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/warioishere/lndg-blitz-go/internal/config"
)

type fakeAmboss struct {
	result                             ambossResult
	err                                error
	gotChannelID, gotAPIKey, gotPeriod string
}

func (f *fakeAmboss) FetchChannelFeeHistory(_ context.Context, channelID, apiKey, timePeriod string) (ambossResult, error) {
	f.gotChannelID, f.gotAPIKey, f.gotPeriod = channelID, apiKey, timePeriod
	return f.result, f.err
}

func ambossGet(t *testing.T, srv *Server, url string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	return rec.Code, body
}

func TestAmbossValidation(t *testing.T) {
	srv := NewServer(&config.Settings{}, nil, WithAmboss(&fakeAmboss{}))

	code, body := ambossGet(t, srv, "/api/amboss_channel_fees/")
	require.Equal(t, http.StatusBadRequest, code)
	require.Equal(t, "channel_id parameter required", body["error"])

	code, body = ambossGet(t, srv, "/api/amboss_channel_fees/?channel_id=abc")
	require.Equal(t, http.StatusBadRequest, code)
	require.Equal(t, "channel_id must be numeric", body["error"])
}

func TestAmbossNoApiKey(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()
	srv := NewServer(&config.Settings{}, pool, WithAmboss(&fakeAmboss{}))

	code, body := ambossGet(t, srv, "/api/amboss_channel_fees/?channel_id=123")
	require.Equal(t, http.StatusBadRequest, code)
	require.Equal(t, "Amboss API key not configured", body["error"])
}

func TestAmbossSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()
	insertRow(t, pool, "gui_localsettings", map[string]any{"key": "AMB-ApiKey", "value": "secret"})

	fake := &fakeAmboss{result: ambossResult{
		ChannelID:      "123",
		ShortChannelID: "800x1x1",
		FeeHistory: []ambossFeePoint{
			{Timestamp: "2024-01-01T00:00:00Z", FeeRateMilliMsat: float64(100)},
			{Timestamp: "2024-01-02T00:00:00Z", FeeRateMilliMsat: float64(150)},
		},
	}}
	srv := NewServer(&config.Settings{}, pool, WithAmboss(fake))

	code, body := ambossGet(t, srv, "/api/amboss_channel_fees/?channel_id=123&time_period=1d")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "123", body["channel_id"])
	require.Equal(t, "800x1x1", body["short_channel_id"])
	require.Equal(t, []any{"2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"}, body["labels"])
	require.Equal(t, []any{float64(100), float64(150)}, body["data"])
	require.Equal(t, "secret", fake.gotAPIKey)
	require.Equal(t, "1d", fake.gotPeriod)
}

func TestAmbossAPIError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()
	insertRow(t, pool, "gui_localsettings", map[string]any{"key": "AMB-ApiKey", "value": "secret"})

	// AmbossAPIError -> 500 with message; generic error -> "Internal server error".
	srv := NewServer(&config.Settings{}, pool, WithAmboss(&fakeAmboss{err: &ambossAPIError{msg: "Amboss API error: boom"}}))
	code, body := ambossGet(t, srv, "/api/amboss_channel_fees/?channel_id=123")
	require.Equal(t, http.StatusInternalServerError, code)
	require.Equal(t, "Amboss API error: boom", body["error"])
}

// An HTTP error carries requests' raise_for_status() text.
func TestHTTPAmbossFetcherStatusError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	f := newHTTPAmbossFetcher()
	f.url = srv.URL
	_, err := f.FetchChannelFeeHistory(context.Background(), "1", "key", "1w")
	require.EqualError(t, err, "Error fetching Amboss channel fee history: 400 Client Error: Bad Request for url: "+srv.URL)
	require.Equal(t, 10*time.Second, f.client.Timeout)
}
