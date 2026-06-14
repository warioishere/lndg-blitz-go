package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/warioishere/lndg-blitz-go/internal/config"
	idb "github.com/warioishere/lndg-blitz-go/internal/db"
)

// setupDB starts a Postgres container, migrates the schema, and returns a
// pgxpool and a cleanup function.
func setupDB(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	ctx := context.Background()
	ctr, err := postgres.Run(ctx,
		"postgres:18-alpine",
		postgres.WithDatabase("lndg"),
		postgres.WithUsername("lndg"),
		postgres.WithPassword("lndg"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(60*time.Second),
		),
	)
	require.NoError(t, err)

	url, err := ctr.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	migrateURL := "pgx5" + url[len("postgres"):]
	require.NoError(t, idb.Migrate(migrateURL))

	pool, err := pgxpool.New(ctx, url)
	require.NoError(t, err)

	cleanup := func() {
		pool.Close()
		_ = ctr.Terminate(ctx)
	}
	return pool, cleanup
}

func TestPaymentsViewSetIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	// Insert three payments with different creation_date/status.
	rows := []struct {
		hash   string
		value  float64
		fee    float64
		status int
		index  int
		date   time.Time
	}{
		{"hashA", 1000, 1.5, 2, 1, time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)},
		{"hashB", 2000, 2.0, 2, 2, time.Date(2024, 1, 2, 10, 0, 0, 0, time.UTC)},
		{"hashC", 3000, 0.0, 1, 3, time.Date(2024, 1, 3, 10, 0, 0, 0, time.UTC)},
	}
	for _, r := range rows {
		_, err := pool.Exec(ctx,
			`INSERT INTO gui_payments (creation_date, payment_hash, value, fee, status, index, cleaned)
			 VALUES ($1,$2,$3,$4,$5,$6,false)`,
			r.date, r.hash, r.value, r.fee, r.status, r.index)
		require.NoError(t, err)
	}

	srv := NewServer(&config.Settings{}, pool)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// 1) Full list: count=3, order -creation_date (hashC first), id==index, ISO date.
	var page struct {
		Count    int              `json:"count"`
		Next     *string          `json:"next"`
		Previous *string          `json:"previous"`
		Results  []map[string]any `json:"results"`
	}
	getJSON(t, ts.URL+"/api/payments/?format=json", &page)
	require.Equal(t, 3, page.Count)
	require.Nil(t, page.Next)
	require.Nil(t, page.Previous)
	require.Len(t, page.Results, 3)
	require.Equal(t, "hashC", page.Results[0]["payment_hash"])
	require.Equal(t, "2024-01-03T10:00:00", page.Results[0]["creation_date"])
	require.EqualValues(t, 3, page.Results[0]["id"])    // id == index
	require.EqualValues(t, 3, page.Results[0]["index"]) // index still present
	require.Nil(t, page.Results[0]["chan_out"])         // NULL -> null

	// 2) Pagination: limit=2 -> count=3, next set, previous nil.
	getJSON(t, ts.URL+"/api/payments/?limit=2", &page)
	require.Equal(t, 3, page.Count)
	require.Len(t, page.Results, 2)
	require.NotNil(t, page.Next)
	require.Nil(t, page.Previous)

	// 3) Filter status=2 -> 2 matches.
	getJSON(t, ts.URL+"/api/payments/?status=2", &page)
	require.Equal(t, 2, page.Count)

	// 4) Filter status__lt=2 -> only hashC (status 1).
	getJSON(t, ts.URL+"/api/payments/?status__lt=2", &page)
	require.Equal(t, 1, page.Count)
	require.Equal(t, "hashC", page.Results[0]["payment_hash"])

	// 5) Filter creation_date__gte -> 2 matches (>= 2024-01-02).
	getJSON(t, ts.URL+"/api/payments/?creation_date__gte=2024-01-02T00:00:00", &page)
	require.Equal(t, 2, page.Count)

	// 6) Invalid filter value -> 400.
	resp, err := http.Get(ts.URL + "/api/payments/?status=abc")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func getJSON(t *testing.T, url string, dst any) {
	t.Helper()
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, json.NewDecoder(resp.Body).Decode(dst))
}

func TestOpenedIn(t *testing.T) {
	require.Equal(t, 800000, openedIn("800000x1x1"))
	require.Equal(t, 123, openedIn("123x4x5"))
	require.Equal(t, 0, openedIn(""))
}
