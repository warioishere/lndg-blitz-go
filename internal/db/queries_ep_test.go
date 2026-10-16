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

// Emergency fees leave NK channels alone: both EP channel lists skip them.
func TestEpListsSkipNkChannels(t *testing.T) {
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

	at := pgtype.Timestamptz{Time: time.Unix(1_700_000_000, 0), Valid: true}
	for _, id := range []string{"1", "2"} {
		require.NoError(t, q.InsertChannel(ctx, gen.InsertChannelParams{ChanID: id, RemotePubkey: "peer" + id, IsOpen: true, Capacity: 1_000_000,
			LastUpdate: at, FeesUpdated: at, OffsetUpdated: at, MaxhtlcUpdated: at, EpUpdated: at, HtlcBoostChecked: at}))
	}
	_, err = conn.Exec(ctx, `UPDATE gui_channels SET ep_enabled = true`)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `UPDATE gui_channels SET af_mode = 'nk' WHERE chan_id = '2'`)
	require.NoError(t, err)

	all, err := q.ListEpEnabledChannels(ctx)
	require.NoError(t, err)
	require.Len(t, all, 1)
	assert.Equal(t, "1", all[0].ChanID)
	byID, err := q.ListEpEnabledChannelsByIDs(ctx, []string{"1", "2"})
	require.NoError(t, err)
	require.Len(t, byID, 1)
	assert.Equal(t, "1", byID[0].ChanID)
}
