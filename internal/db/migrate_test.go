package db

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// startPostgres starts a throwaway Postgres container and returns the pgx URL.
// Shared by all DB tests.
func startPostgres(t *testing.T) (string, func()) {
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
	cleanup := func() { _ = ctr.Terminate(ctx) }

	url, err := ctr.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	// The golang-migrate pgx/v5 driver requires the "pgx5://" scheme.
	migrateURL := "pgx5" + url[len("postgres"):]
	return migrateURL, cleanup
}

// TestMigrationsApply verifies that the consolidated initial migration applies
// without error and creates exactly 29 gui_ tables.
func TestMigrationsApply(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	migrateURL, cleanup := startPostgres(t)
	defer cleanup()

	require.NoError(t, Migrate(migrateURL))

	// Connect and count gui_ tables.
	ctx := context.Background()
	connURL := "postgres" + migrateURL[len("pgx5"):]
	conn, err := pgx.Connect(ctx, connURL)
	require.NoError(t, err)
	defer conn.Close(ctx)

	var count int
	err = conn.QueryRow(ctx,
		`SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'gui_%'`,
	).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 29, count, "expected 29 gui_ tables from the initial schema")

	// Spot-check: the probelog family must reflect the final production state:
	// gui_probelog, gui_graphprobelog, gui_graphevent.
	for _, tbl := range []string{"gui_probelog", "gui_graphprobelog", "gui_graphevent", "gui_channels", "gui_localsettings"} {
		var exists bool
		err = conn.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename=$1)`, tbl,
		).Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "table %s should exist", tbl)
	}

	// 000002: rebalance source fee (opportunity cost) on payments.
	var hasSourceFee bool
	err = conn.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='gui_payments' AND column_name='source_fee_rate')`,
	).Scan(&hasSourceFee)
	require.NoError(t, err)
	assert.True(t, hasSourceFee, "gui_payments.source_fee_rate should exist")

	// Idempotency: a second call to Migrate returns ErrNoChange and no error.
	require.NoError(t, Migrate(migrateURL))
}

// A database set up by the Django app (gui migrations through 0061) is taken
// over: 000001 is marked as applied, later migrations run.
func TestMigrateTakesOverDjangoDatabase(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	url, cleanup := startPostgres(t)
	defer cleanup()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres"+url[len("pgx5"):])
	require.NoError(t, err)
	defer conn.Close(ctx)

	// stand-in for `manage.py migrate` up to gui 0061: the 000001 schema plus
	// Django's migration table, with one row of data that must survive
	schema, err := migrationsFS.ReadFile("migrations/000001_initial_schema.up.sql")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, string(schema))
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `CREATE TABLE django_migrations (id serial, app varchar(255), name varchar(255), applied timestamptz);
		INSERT INTO django_migrations (app, name, applied) VALUES ('gui', '0060_probelog', now());
		INSERT INTO gui_localsettings (key, value) VALUES ('AR-Enabled', '1')`)
	require.NoError(t, err)

	err = Migrate(url)
	require.ErrorContains(t, err, "older than gui 0061", "an older Django schema is refused")

	_, err = conn.Exec(ctx, `INSERT INTO django_migrations (app, name, applied) VALUES ('gui', '0061_graphprobelog_restore_probelog', now())`)
	require.NoError(t, err)
	require.NoError(t, Migrate(url))
	require.NoError(t, Migrate(url), "second run: nothing to do")

	var version int
	var dirty bool
	require.NoError(t, conn.QueryRow(ctx, `SELECT version, dirty FROM schema_migrations`).Scan(&version, &dirty))
	assert.Equal(t, 2, version)
	assert.False(t, dirty)
	var value string
	require.NoError(t, conn.QueryRow(ctx, `SELECT value FROM gui_localsettings WHERE key='AR-Enabled'`).Scan(&value))
	assert.Equal(t, "1", value, "data kept")
	var hasCol bool
	require.NoError(t, conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='gui_payments' AND column_name='source_fee_rate')`).Scan(&hasCol))
	assert.True(t, hasCol, "000002 applied")
}
