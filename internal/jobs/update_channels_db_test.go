package jobs

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"

	idb "github.com/warioishere/lndg-blitz-go/internal/db"
	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// setupJobsDB starts a Postgres container with the migrated schema.
func setupJobsDB(t *testing.T) (*pgxpool.Pool, func()) {
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
	require.NoError(t, idb.Migrate("pgx5"+url[len("postgres"):]))
	pool, err := pgxpool.New(ctx, url)
	require.NoError(t, err)
	return pool, func() {
		pool.Close()
		_ = ctr.Terminate(ctx)
	}
}

// hookClient runs hook right before answering GetChanInfo, i.e. after the sync
// loaded the channel row: the place where a concurrent UI write would land.
type hookClient struct {
	*fakeUCClient
	hook func()
}

func (c *hookClient) GetChanInfo(ctx context.Context, in *lnrpc.ChanInfoRequest, opts ...grpc.CallOption) (*lnrpc.ChannelEdge, error) {
	if c.hook != nil {
		c.hook()
	}
	return c.fakeUCClient.GetChanInfo(ctx, in, opts...)
}

// TestUpdateChannelsDB checks the guarded writes of UpdateChannels on a real
// database: external policy changes are persisted once, while UI writes made
// during a sync run are never reverted.
func TestUpdateChannelsDB(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupJobsDB(t)
	defer cleanup()
	ctx := context.Background()
	q := db.New(pool)
	now := time.Now()

	seed := func(mod func(*db.GuiChannel)) {
		_, err := pool.Exec(ctx, `DELETE FROM gui_channels; DELETE FROM gui_autofees`)
		require.NoError(t, err)
		ch := db.GuiChannel{
			RemotePubkey: remotePub, ChanID: "555", FundingTxid: "abcdef", Capacity: 1_000_000,
			Alias: "bob", IsActive: true, IsOpen: true,
			LocalFeeRate: 500, LocalCltv: 40, LocalMinHtlcMsat: 1000, LocalMaxHtlcMsat: 990_000_000,
			RemoteBaseFee: 1000, RemoteFeeRate: 250, RemoteCltv: 80,
			RemoteMinHtlcMsat: 1000, RemoteMaxHtlcMsat: 990_000_000,
			ArOutTarget: 75, ArInTarget: 90, ArAmtTarget: 30000, ArMaxCost: 65, AutoFees: true,
			LastUpdate: ts(now), FeesUpdated: ts(now.Add(-72 * time.Hour)), EpUpdated: ts(now),
		}
		if mod != nil {
			mod(&ch)
		}
		require.NoError(t, q.InsertChannel(ctx, insertChannelParamsFrom(ch)))
	}
	client := func(localRate int64, hook func()) *hookClient {
		e := edge(555, localRate, 250, false)
		e.Node2Policy.InboundFeeRateMilliMsat = 25
		return &hookClient{fakeUCClient: &fakeUCClient{
			channels: []*lnrpc.Channel{basicChannel(555)},
			blockHt:  800000,
			version:  "0.18.0-beta",
			chanInfo: map[uint64]*lnrpc.ChannelEdge{555: e},
		}, hook: hook}
	}
	get := func(id string) db.GuiChannel {
		c, err := q.GetChannel(ctx, id)
		require.NoError(t, err)
		return c
	}
	extRows := func() int {
		var n int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM gui_autofees WHERE setting = 'Ext'`).Scan(&n))
		return n
	}

	t.Run("external change persisted and logged once", func(t *testing.T) {
		seed(nil)
		for i := 0; i < 3; i++ {
			require.NoError(t, UpdateChannels(ctx, q, client(600, nil)))
		}
		c := get("555")
		assert.Equal(t, int32(600), c.LocalFeeRate)
		assert.Equal(t, int32(25), c.LocalInboundFeeRate)
		assert.Equal(t, 1, extRows())
		assert.WithinDuration(t, time.Now(), c.FeesUpdated.Time, time.Minute)
	})

	t.Run("UI fee change during sync wins", func(t *testing.T) {
		seed(nil)
		uiWrite := func() {
			_, err := pool.Exec(ctx, `UPDATE gui_channels SET local_fee_rate = 691 WHERE chan_id = '555'`)
			require.NoError(t, err)
		}
		require.NoError(t, UpdateChannels(ctx, q, client(700, uiWrite)))
		assert.Equal(t, int32(691), get("555").LocalFeeRate)
		assert.Equal(t, 0, extRows())
	})

	t.Run("UI settings change during sync survives", func(t *testing.T) {
		seed(nil)
		uiWrite := func() {
			_, err := pool.Exec(ctx, `UPDATE gui_channels SET ar_in_target = 55, notes = 'mine', inbound_offset = -100 WHERE chan_id = '555'`)
			require.NoError(t, err)
		}
		require.NoError(t, UpdateChannels(ctx, q, client(500, uiWrite)))
		c := get("555")
		assert.Equal(t, int32(55), c.ArInTarget)
		assert.Equal(t, "mine", c.Notes)
		assert.Equal(t, int32(-100), c.InboundOffset)
	})

	t.Run("unset default filled", func(t *testing.T) {
		seed(func(c *db.GuiChannel) { c.ArOutTarget = 0 })
		require.NoError(t, UpdateChannels(ctx, q, client(500, nil)))
		assert.Equal(t, int32(75), get("555").ArOutTarget)
	})

	t.Run("channel missing from LND is closed", func(t *testing.T) {
		seed(nil)
		require.NoError(t, q.InsertChannel(ctx, insertChannelParamsFrom(db.GuiChannel{
			RemotePubkey: "03cccc", ChanID: "999", FundingTxid: "deadbeef", Capacity: 500_000,
			IsActive: true, IsOpen: true, ArOutTarget: 75, ArInTarget: 90, ArAmtTarget: 15000, ArMaxCost: 65,
			LastUpdate: ts(now), FeesUpdated: ts(now), EpUpdated: ts(now),
		})))
		require.NoError(t, UpdateChannels(ctx, q, client(500, nil)))
		closed := get("999")
		assert.False(t, closed.IsOpen)
		assert.False(t, closed.IsActive)
		assert.True(t, get("555").IsOpen)
		assert.Equal(t, int64(600_000), get("555").LocalBalance, "LND state written")
	})
}
