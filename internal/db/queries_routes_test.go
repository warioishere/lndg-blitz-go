package db

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	gen "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

// A tested route stays until its latest evidence (payment, failure or prober
// verification) is older than the cutoff, also when it was never paid.
func TestDeleteStaleTestedRoutesKeepsRecentEvidence(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	migrateURL, cleanup := startPostgres(t)
	defer cleanup()
	require.NoError(t, Migrate(migrateURL))
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres"+migrateURL[len("pgx5"):])
	require.NoError(t, err)
	defer conn.Close(ctx)
	q := gen.New(conn)

	now := time.Unix(1_700_000_000, 0)
	day := 24 * time.Hour
	at := func(d time.Duration) any { return now.Add(-d) }
	insert := func(route string, success, failure int, lastSuccess, lastFailure, lastProbed any) {
		_, err := conn.Exec(ctx, `INSERT INTO gui_rebalanceroute
			(target_pubkey, outgoing_chan_id, route, final_cltv_delta, success_count, failure_count, last_success, last_failure, last_probed)
			VALUES ('t', '1', $1, 144, $2, $3, $4, $5, $6)`, route, success, failure, lastSuccess, lastFailure, lastProbed)
		require.NoError(t, err)
	}
	insert("failed-yesterday", 0, 1, nil, at(day), nil)
	insert("failed-8-days-ago", 0, 1, nil, at(8*day), nil)
	insert("paid-8-days-probed-yesterday", 1, 0, at(8*day), nil, at(day))
	insert("paid-8-days-ago", 1, 0, at(8*day), nil, nil)
	insert("never-tested", 0, 0, nil, nil, nil)

	require.NoError(t, q.DeleteStaleTestedRoutes(ctx, pgtype.Timestamptz{Time: now.Add(-7 * day), Valid: true}))
	rows, err := conn.Query(ctx, `SELECT route FROM gui_rebalanceroute ORDER BY route`)
	require.NoError(t, err)
	left, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	assert.Equal(t, []string{"failed-yesterday", "never-tested", "paid-8-days-probed-yesterday"}, left)
}
