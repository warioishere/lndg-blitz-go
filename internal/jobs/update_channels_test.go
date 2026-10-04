package jobs

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// ---- Fake querier ----

type fakeUCQ struct {
	settings    map[string]string
	channels    map[string]db.GuiChannel // by chan_id
	peers       map[string]db.GuiPeer    // by pubkey
	peerAliases map[string]pgtype.Text   // by pubkey
	pending     map[string]db.GuiPendingchannel
	// concurrentWrite makes SyncChannelLocalPolicy report 0 rows, as if a UI /
	// auto-fees write changed the row after the sync loaded it.
	concurrentWrite bool

	pendingHTLCDeleted bool
	insertedHTLCs      []db.InsertPendingHTLCParams
	insertedChannels   []db.InsertChannelParams
	updatedChannels    []db.UpdateChannelSyncParams
	policySyncs        []db.SyncChannelLocalPolicyParams
	filledDefaults     []db.FillChannelDefaultsParams
	closeCalls         []db.CloseMissingChannelsParams
	peerEvents         []db.InsertPeerEventParams
	autofees           []db.InsertAutofeeParams
	deletedPending     []db.DeletePendingChannelParams
	connSet            []db.SetPeerConnectedParams
	reconnSet          []db.SetPeerLastReconnectedParams
}

func newFakeUCQ() *fakeUCQ {
	return &fakeUCQ{
		settings:    map[string]string{},
		channels:    map[string]db.GuiChannel{},
		peers:       map[string]db.GuiPeer{},
		peerAliases: map[string]pgtype.Text{},
		pending:     map[string]db.GuiPendingchannel{},
	}
}

func pkey(funding string, idx int32) string { return fmt.Sprintf("%s:%d", funding, idx) }

func (f *fakeUCQ) GetLocalSetting(ctx context.Context, key string) (db.GuiLocalsetting, error) {
	if v, ok := f.settings[key]; ok {
		return db.GuiLocalsetting{Key: key, Value: v}, nil
	}
	return db.GuiLocalsetting{}, pgx.ErrNoRows
}
func (f *fakeUCQ) GetOrCreateLocalSetting(ctx context.Context, arg db.GetOrCreateLocalSettingParams) (db.GuiLocalsetting, error) {
	if v, ok := f.settings[arg.Key]; ok {
		return db.GuiLocalsetting{Key: arg.Key, Value: v}, nil
	}
	f.settings[arg.Key] = arg.Value
	return db.GuiLocalsetting{Key: arg.Key, Value: arg.Value}, nil
}
func (f *fakeUCQ) GetNodeCache(ctx context.Context, pubkey string) (db.GuiNodecache, error) {
	return db.GuiNodecache{}, pgx.ErrNoRows
}
func (f *fakeUCQ) UpsertNodeCache(ctx context.Context, arg db.UpsertNodeCacheParams) error {
	return nil
}
func (f *fakeUCQ) GetChannel(ctx context.Context, chanID string) (db.GuiChannel, error) {
	if c, ok := f.channels[chanID]; ok {
		return c, nil
	}
	return db.GuiChannel{}, pgx.ErrNoRows
}
func (f *fakeUCQ) GetChannelAlias(ctx context.Context, chanID string) (string, error) {
	if c, ok := f.channels[chanID]; ok {
		return c.Alias, nil
	}
	return "", pgx.ErrNoRows
}
func (f *fakeUCQ) InsertChannel(ctx context.Context, arg db.InsertChannelParams) error {
	f.insertedChannels = append(f.insertedChannels, arg)
	return nil
}
func (f *fakeUCQ) UpdateChannelSync(ctx context.Context, arg db.UpdateChannelSyncParams) error {
	f.updatedChannels = append(f.updatedChannels, arg)
	return nil
}
func (f *fakeUCQ) SyncChannelLocalPolicy(ctx context.Context, arg db.SyncChannelLocalPolicyParams) (int64, error) {
	f.policySyncs = append(f.policySyncs, arg)
	if f.concurrentWrite {
		return 0, nil
	}
	return 1, nil
}
func (f *fakeUCQ) FillChannelDefaults(ctx context.Context, arg db.FillChannelDefaultsParams) error {
	f.filledDefaults = append(f.filledDefaults, arg)
	return nil
}
func (f *fakeUCQ) CloseMissingChannels(ctx context.Context, arg db.CloseMissingChannelsParams) error {
	f.closeCalls = append(f.closeCalls, arg)
	return nil
}
func (f *fakeUCQ) GetPeer(ctx context.Context, pubkey string) (db.GuiPeer, error) {
	if p, ok := f.peers[pubkey]; ok {
		return p, nil
	}
	return db.GuiPeer{}, pgx.ErrNoRows
}
func (f *fakeUCQ) GetPeerAlias(ctx context.Context, pubkey string) (pgtype.Text, error) {
	if a, ok := f.peerAliases[pubkey]; ok {
		return a, nil
	}
	return pgtype.Text{}, pgx.ErrNoRows
}
func (f *fakeUCQ) SetPeerConnected(ctx context.Context, arg db.SetPeerConnectedParams) error {
	f.connSet = append(f.connSet, arg)
	return nil
}
func (f *fakeUCQ) SetPeerLastReconnected(ctx context.Context, arg db.SetPeerLastReconnectedParams) error {
	f.reconnSet = append(f.reconnSet, arg)
	return nil
}
func (f *fakeUCQ) DeleteAllPendingHTLCs(ctx context.Context) error {
	f.pendingHTLCDeleted = true
	return nil
}
func (f *fakeUCQ) InsertPendingHTLC(ctx context.Context, arg db.InsertPendingHTLCParams) error {
	f.insertedHTLCs = append(f.insertedHTLCs, arg)
	return nil
}
func (f *fakeUCQ) GetPendingChannelByFunding(ctx context.Context, arg db.GetPendingChannelByFundingParams) (db.GuiPendingchannel, error) {
	if p, ok := f.pending[pkey(arg.FundingTxid, arg.OutputIndex)]; ok {
		return p, nil
	}
	return db.GuiPendingchannel{}, pgx.ErrNoRows
}
func (f *fakeUCQ) DeletePendingChannel(ctx context.Context, arg db.DeletePendingChannelParams) error {
	f.deletedPending = append(f.deletedPending, arg)
	return nil
}
func (f *fakeUCQ) InsertPeerEvent(ctx context.Context, arg db.InsertPeerEventParams) error {
	f.peerEvents = append(f.peerEvents, arg)
	return nil
}
func (f *fakeUCQ) InsertAutofee(ctx context.Context, arg db.InsertAutofeeParams) error {
	f.autofees = append(f.autofees, arg)
	return nil
}

// ---- Fake client ----

type fakeUCClient struct {
	channels   []*lnrpc.Channel
	blockHt    uint32
	version    string
	chanInfo   map[uint64]*lnrpc.ChannelEdge // by chan_id; missing -> error
	policyReqs []*lnrpc.PolicyUpdateRequest
	disconnect []string
	nodeAlias  string
}

func (c *fakeUCClient) ListChannels(ctx context.Context, in *lnrpc.ListChannelsRequest, opts ...grpc.CallOption) (*lnrpc.ListChannelsResponse, error) {
	return &lnrpc.ListChannelsResponse{Channels: c.channels}, nil
}
func (c *fakeUCClient) GetInfo(ctx context.Context, in *lnrpc.GetInfoRequest, opts ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
	return &lnrpc.GetInfoResponse{BlockHeight: c.blockHt, Version: c.version}, nil
}
func (c *fakeUCClient) GetChanInfo(ctx context.Context, in *lnrpc.ChanInfoRequest, opts ...grpc.CallOption) (*lnrpc.ChannelEdge, error) {
	if e, ok := c.chanInfo[in.ChanId]; ok {
		return e, nil
	}
	return nil, fmt.Errorf("edge not found")
}
func (c *fakeUCClient) UpdateChannelPolicy(ctx context.Context, in *lnrpc.PolicyUpdateRequest, opts ...grpc.CallOption) (*lnrpc.PolicyUpdateResponse, error) {
	c.policyReqs = append(c.policyReqs, in)
	return &lnrpc.PolicyUpdateResponse{}, nil
}
func (c *fakeUCClient) DisconnectPeer(ctx context.Context, in *lnrpc.DisconnectPeerRequest, opts ...grpc.CallOption) (*lnrpc.DisconnectPeerResponse, error) {
	c.disconnect = append(c.disconnect, in.PubKey)
	return &lnrpc.DisconnectPeerResponse{}, nil
}
func (c *fakeUCClient) GetNodeInfo(ctx context.Context, in *lnrpc.NodeInfoRequest, opts ...grpc.CallOption) (*lnrpc.NodeInfo, error) {
	return &lnrpc.NodeInfo{Node: &lnrpc.LightningNode{Alias: c.nodeAlias}}, nil
}

// ---- Helpers ----

func eventsByName(events []db.InsertPeerEventParams) map[string]db.InsertPeerEventParams {
	m := map[string]db.InsertPeerEventParams{}
	for _, e := range events {
		m[e.Event] = e
	}
	return m
}

const remotePub = "03aaaa"

func basicChannel(chanID uint64) *lnrpc.Channel {
	return &lnrpc.Channel{
		Active:                true,
		RemotePubkey:          remotePub,
		ChannelPoint:          "abcdef:0",
		ChanId:                chanID,
		Capacity:              1_000_000,
		LocalBalance:          600_000,
		RemoteBalance:         400_000,
		UnsettledBalance:      0,
		CommitFee:             183,
		LocalChanReserveSat:   10_000,
		NumUpdates:            42,
		Initiator:             true,
		TotalSatoshisSent:     1234,
		TotalSatoshisReceived: 5678,
		Private:               false,
		PushAmountSat:         0,
		CloseAddress:          "",
	}
}

// edge: node1 = remote peer (so local = node2_policy, remote = node1_policy).
func edge(chanID uint64, localRate, remoteRate int64, remoteDisabled bool) *lnrpc.ChannelEdge {
	return &lnrpc.ChannelEdge{
		ChannelId: chanID,
		Node1Pub:  remotePub,
		Node1Policy: &lnrpc.RoutingPolicy{
			FeeBaseMsat:             1000,
			FeeRateMilliMsat:        remoteRate,
			TimeLockDelta:           80,
			Disabled:                remoteDisabled,
			MinHtlc:                 1000,
			MaxHtlcMsat:             990_000_000,
			InboundFeeBaseMsat:      0,
			InboundFeeRateMilliMsat: 0,
		},
		Node2Pub: "03self",
		Node2Policy: &lnrpc.RoutingPolicy{
			FeeBaseMsat:             0,
			FeeRateMilliMsat:        localRate,
			TimeLockDelta:           40,
			Disabled:                false,
			MinHtlc:                 1000,
			MaxHtlcMsat:             990_000_000,
			InboundFeeBaseMsat:      0,
			InboundFeeRateMilliMsat: 0,
		},
	}
}

// TestUpdateChannels_NewChannelSuccess covers the main path for a new channel:
// InsertChannel (full row), Connection event (old=None), all remote events
// (old=None, since change-detection runs against None), and default fields.
func TestUpdateChannels_NewChannelSuccess(t *testing.T) {
	q := newFakeUCQ()
	q.peerAliases[remotePub] = pgtype.Text{String: "bob", Valid: true}
	chanID := uint64(123456789)
	c := &fakeUCClient{
		channels: []*lnrpc.Channel{basicChannel(chanID)},
		blockHt:  800000,
		version:  "0.18.0-beta",
		chanInfo: map[uint64]*lnrpc.ChannelEdge{chanID: edge(chanID, 500, 250, false)},
	}

	require.NoError(t, UpdateChannels(context.Background(), q, c))

	require.True(t, q.pendingHTLCDeleted)
	require.Len(t, q.insertedChannels, 1)
	require.Empty(t, q.updatedChannels)
	ins := q.insertedChannels[0]
	assert.Equal(t, "123456789", ins.ChanID)
	assert.Equal(t, fmt.Sprintf("%dx%dx%d", chanID>>40, (chanID>>16)&0xFFFFFF, chanID&0xFFFF), ins.ShortChanID)
	assert.Equal(t, "bob", ins.Alias) // peer alias overrides node-info alias via is_active branch
	assert.Equal(t, int32(500), ins.LocalFeeRate)
	assert.Equal(t, int32(250), ins.RemoteFeeRate)
	assert.True(t, ins.IsActive)
	assert.True(t, ins.IsOpen)
	assert.Equal(t, int64(600_000), ins.LocalBalance)
	// Default field values present on a fresh row.
	assert.Equal(t, int32(50), ins.EpTarget)
	assert.Equal(t, float64(10), ins.EpIncPct)
	assert.True(t, ins.FeesUpdated.Valid)
	assert.False(t, ins.OffsetUpdated.Valid)
	assert.False(t, ins.HtlcBoostChecked.Valid)
	// apply_channel_defaults seeded ar/auto_fees.
	assert.Equal(t, int32(75), ins.ArOutTarget)
	assert.Equal(t, int32(90), ins.ArInTarget)

	// PeerEvents: Connection + 6 remote + 2 inbound, all old=None.
	ev := eventsByName(q.peerEvents)
	require.Contains(t, ev, "Connection")
	assert.False(t, ev["Connection"].OldValue.Valid)
	assert.Equal(t, int64(1), ev["Connection"].NewValue)
	assert.Equal(t, int64(600_000), ev["Connection"].OutLiq)
	for _, name := range []string{"BaseFee", "FeeRate", "Disabled", "CLTV", "MinHTLC", "MaxHTLC", "IncomingBaseFee", "IncomingFeeRate"} {
		require.Contains(t, ev, name)
		assert.False(t, ev[name].OldValue.Valid, "event %s old_value should be None", name)
		assert.Equal(t, "bob", ev[name].PeerAlias)
	}
	assert.Equal(t, int64(250), ev["FeeRate"].NewValue)
	// New channel with non-zero local fee logs an Ext autofee (old_fee_rate=0 -> 500).
	require.Len(t, q.autofees, 1)
	assert.Equal(t, int32(0), q.autofees[0].OldValue)
	assert.Equal(t, int32(500), q.autofees[0].NewValue)
}

// TestUpdateChannels_NewChannelExtFeeFromZero: a new channel with a non-zero local fee
// logs an Ext autofee (old=0 -> new=rate), because old_fee_rate is 0 for new channels.
func TestUpdateChannels_NewChannelExtFeeFromZero(t *testing.T) {
	q := newFakeUCQ()
	chanID := uint64(987654321)
	c := &fakeUCClient{
		channels: []*lnrpc.Channel{basicChannel(chanID)},
		blockHt:  800000,
		version:  "0.18.0-beta",
		chanInfo: map[uint64]*lnrpc.ChannelEdge{chanID: edge(chanID, 500, 250, false)},
	}
	require.NoError(t, UpdateChannels(context.Background(), q, c))
	require.Len(t, q.autofees, 1)
	assert.Equal(t, "Ext", q.autofees[0].Setting)
	assert.Equal(t, int32(0), q.autofees[0].OldValue)
	assert.Equal(t, int32(500), q.autofees[0].NewValue)
}

// TestUpdateChannels_ExistingFeeChange: an existing channel whose local fee changed
// in LND logs an Ext autofee and calls UpdateChannelSync.
func TestUpdateChannels_ExistingFeeChange(t *testing.T) {
	q := newFakeUCQ()
	chanID := uint64(555)
	existing := db.GuiChannel{
		RemotePubkey: remotePub, ChanID: "555", FundingTxid: "abcdef", OutputIndex: 0,
		Capacity: 1_000_000, Alias: "bob", IsActive: true, IsOpen: true,
		LocalFeeRate: 100, // old rate
		LocalBaseFee: 0, LocalCltv: 40, LocalMinHtlcMsat: 1000, LocalMaxHtlcMsat: 990_000_000,
		RemoteBaseFee: 1000, RemoteFeeRate: 250, RemoteCltv: 80, RemoteDisabled: false,
		RemoteMinHtlcMsat: 1000, RemoteMaxHtlcMsat: 990_000_000,
		ArOutTarget: 75, ArInTarget: 90, ArAmtTarget: 30000, ArMaxCost: 65, AutoFees: true,
	}
	q.channels["555"] = existing
	q.peerAliases[remotePub] = pgtype.Text{String: "bob", Valid: true}
	c := &fakeUCClient{
		channels: []*lnrpc.Channel{basicChannel(chanID)},
		blockHt:  800000,
		version:  "0.18.0-beta",
		chanInfo: map[uint64]*lnrpc.ChannelEdge{chanID: edge(chanID, 500, 250, false)}, // local now 500
	}
	require.NoError(t, UpdateChannels(context.Background(), q, c))

	require.Empty(t, q.insertedChannels)
	require.Len(t, q.updatedChannels, 1)
	upd := q.updatedChannels[0]
	assert.Equal(t, "555", upd.ChanID)
	// The external fee is persisted, guarded by the value the sync loaded.
	require.Len(t, q.policySyncs, 1)
	ps := q.policySyncs[0]
	assert.Equal(t, int32(100), ps.OldLocalFeeRate)
	assert.Equal(t, int32(500), ps.LocalFeeRate)
	assert.True(t, ps.FeeChanged)
	// Ext autofee old=100 -> new=500, logged because the write went through.
	require.Len(t, q.autofees, 1)
	assert.Equal(t, int32(100), q.autofees[0].OldValue)
	assert.Equal(t, int32(500), q.autofees[0].NewValue)
	// remote unchanged (250 -> 250) so no FeeRate peer event; is_active unchanged -> no Connection.
	assert.Empty(t, q.peerEvents)
}

// TestUpdateChannels_ExistingFeeChange_ConcurrentWriteWins: when the row changed after
// the sync loaded it (UI / auto-fees write), the policy is not written and no Ext is logged.
func TestUpdateChannels_ExistingFeeChange_ConcurrentWriteWins(t *testing.T) {
	q := newFakeUCQ()
	chanID := uint64(555)
	q.channels["555"] = db.GuiChannel{
		RemotePubkey: remotePub, ChanID: "555", FundingTxid: "abcdef", Capacity: 1_000_000,
		Alias: "bob", IsActive: true, IsOpen: true, LocalFeeRate: 100, LocalCltv: 40,
		RemoteBaseFee: 1000, RemoteFeeRate: 250, RemoteCltv: 80,
		ArOutTarget: 75, ArInTarget: 90, ArAmtTarget: 30000, ArMaxCost: 65, AutoFees: true,
	}
	q.peerAliases[remotePub] = pgtype.Text{String: "bob", Valid: true}
	q.concurrentWrite = true
	c := &fakeUCClient{
		channels: []*lnrpc.Channel{basicChannel(chanID)},
		blockHt:  800000,
		version:  "0.18.0-beta",
		chanInfo: map[uint64]*lnrpc.ChannelEdge{chanID: edge(chanID, 500, 250, false)},
	}
	require.NoError(t, UpdateChannels(context.Background(), q, c))
	require.Len(t, q.policySyncs, 1)
	assert.Empty(t, q.autofees)
}

// TestUpdateChannels_ExistingNoPolicyChange: LND reports what the DB holds -> no policy write.
func TestUpdateChannels_ExistingNoPolicyChange(t *testing.T) {
	q := newFakeUCQ()
	chanID := uint64(555)
	c := &fakeUCClient{
		channels: []*lnrpc.Channel{basicChannel(chanID)},
		blockHt:  800000,
		version:  "0.18.0-beta",
		chanInfo: map[uint64]*lnrpc.ChannelEdge{chanID: edge(chanID, 500, 250, false)},
	}
	// first run creates the channel with exactly what LND reports
	require.NoError(t, UpdateChannels(context.Background(), q, c))
	require.Len(t, q.insertedChannels, 1)
	ins := q.insertedChannels[0]
	q.channels["555"] = db.GuiChannel{
		RemotePubkey: ins.RemotePubkey, ChanID: "555", FundingTxid: ins.FundingTxid, Capacity: ins.Capacity,
		Alias: ins.Alias, IsActive: ins.IsActive, IsOpen: true,
		LocalBaseFee: ins.LocalBaseFee, LocalFeeRate: ins.LocalFeeRate, LocalInboundBaseFee: ins.LocalInboundBaseFee,
		LocalInboundFeeRate: ins.LocalInboundFeeRate, LocalCltv: ins.LocalCltv, LocalDisabled: ins.LocalDisabled,
		LocalMinHtlcMsat: ins.LocalMinHtlcMsat, LocalMaxHtlcMsat: ins.LocalMaxHtlcMsat,
		RemoteBaseFee: ins.RemoteBaseFee, RemoteFeeRate: ins.RemoteFeeRate, RemoteCltv: ins.RemoteCltv,
		RemoteMinHtlcMsat: ins.RemoteMinHtlcMsat, RemoteMaxHtlcMsat: ins.RemoteMaxHtlcMsat,
		ArOutTarget: 75, ArInTarget: 90, ArAmtTarget: 30000, ArMaxCost: 65, AutoFees: true,
	}
	q.autofees = nil
	require.NoError(t, UpdateChannels(context.Background(), q, c))
	assert.Empty(t, q.policySyncs)
	assert.Empty(t, q.autofees)
	assert.Empty(t, q.filledDefaults)
}

// TestUpdateChannels_FillsUnsetDefault: an existing channel with ar_out_target 0 gets
// the AR-Outbound% default through the guarded FillChannelDefaults write.
func TestUpdateChannels_FillsUnsetDefault(t *testing.T) {
	q := newFakeUCQ()
	chanID := uint64(555)
	q.channels["555"] = db.GuiChannel{
		RemotePubkey: remotePub, ChanID: "555", FundingTxid: "abcdef", Capacity: 1_000_000,
		Alias: "bob", IsActive: true, IsOpen: true, LocalFeeRate: 500, RemoteCltv: 80,
		ArOutTarget: 0, ArInTarget: 90, ArAmtTarget: 30000, ArMaxCost: 65, AutoFees: true,
	}
	q.peerAliases[remotePub] = pgtype.Text{String: "bob", Valid: true}
	c := &fakeUCClient{
		channels: []*lnrpc.Channel{basicChannel(chanID)},
		blockHt:  800000,
		version:  "0.18.0-beta",
		chanInfo: map[uint64]*lnrpc.ChannelEdge{chanID: edge(chanID, 500, 250, false)},
	}
	require.NoError(t, UpdateChannels(context.Background(), q, c))
	require.Len(t, q.filledDefaults, 1)
	assert.Equal(t, int32(75), q.filledDefaults[0].ArOutTarget)
	assert.Equal(t, int32(90), q.filledDefaults[0].ArInTarget)
}

// TestUpdateChannels_ExistingRemoteCltvMinusOne: remote_cltv == -1 triggers the
// first-time branch, firing all remote events with old=None even for existing channels.
func TestUpdateChannels_ExistingRemoteCltvMinusOne(t *testing.T) {
	q := newFakeUCQ()
	chanID := uint64(777)
	q.channels["777"] = db.GuiChannel{
		RemotePubkey: remotePub, ChanID: "777", FundingTxid: "abcdef", Capacity: 1_000_000,
		Alias: "bob", IsActive: true, IsOpen: true, LocalFeeRate: 500,
		RemoteCltv:  -1, // triggers first-time branch
		ArOutTarget: 75, ArInTarget: 90, ArAmtTarget: 30000, ArMaxCost: 65, AutoFees: true,
	}
	q.peerAliases[remotePub] = pgtype.Text{String: "bob", Valid: true}
	c := &fakeUCClient{
		channels: []*lnrpc.Channel{basicChannel(chanID)},
		blockHt:  800000,
		version:  "0.17.0-beta", // < 0.18 -> no inbound events
		chanInfo: map[uint64]*lnrpc.ChannelEdge{chanID: edge(chanID, 500, 250, true)},
	}
	require.NoError(t, UpdateChannels(context.Background(), q, c))
	ev := eventsByName(q.peerEvents)
	for _, name := range []string{"BaseFee", "FeeRate", "Disabled", "CLTV", "MinHTLC", "MaxHTLC"} {
		require.Contains(t, ev, name)
		assert.False(t, ev[name].OldValue.Valid)
	}
	assert.Equal(t, int64(1), ev["Disabled"].NewValue) // remote disabled
	assert.NotContains(t, ev, "IncomingBaseFee")       // version < 0.18
}

// TestUpdateChannels_NewChannelGraphError_NoPending: GetChanInfo fails with no
// pending channel present -> fee fields set to -1, channel is still inserted.
func TestUpdateChannels_NewChannelGraphError_NoPending(t *testing.T) {
	q := newFakeUCQ()
	chanID := uint64(111)
	c := &fakeUCClient{
		channels: []*lnrpc.Channel{basicChannel(chanID)},
		blockHt:  800000,
		version:  "0.18.0-beta",
		chanInfo: map[uint64]*lnrpc.ChannelEdge{}, // GetChanInfo errors
	}
	require.NoError(t, UpdateChannels(context.Background(), q, c))
	require.Len(t, q.insertedChannels, 1)
	ins := q.insertedChannels[0]
	assert.Equal(t, int32(-1), ins.LocalFeeRate)
	assert.Equal(t, int32(-1), ins.RemoteCltv)
	assert.Equal(t, int32(-1), ins.LocalBaseFee)
	assert.Equal(t, int32(-1), ins.RemoteInboundFeeRate)
	// only the Connection event (is_active branch) fired; no remote events.
	ev := eventsByName(q.peerEvents)
	assert.Contains(t, ev, "Connection")
	assert.NotContains(t, ev, "FeeRate")
	assert.Empty(t, q.autofees) // old_fee_rate=None -> Ext skipped
}

// TestUpdateChannels_PendingGraphError_Skips: GetChanInfo fails with a pending channel
// present -> channel is skipped (no insert), but pending HTLCs are still inserted.
func TestUpdateChannels_PendingGraphError_Skips(t *testing.T) {
	q := newFakeUCQ()
	chanID := uint64(222)
	ch := basicChannel(chanID)
	ch.PendingHtlcs = []*lnrpc.HTLC{{Incoming: false, Amount: 5000, HashLock: []byte{0xab, 0xcd}, ExpirationHeight: 900000, ForwardingChannel: 0}}
	q.pending[pkey("abcdef", 0)] = db.GuiPendingchannel{FundingTxid: "abcdef", OutputIndex: 0}
	c := &fakeUCClient{
		channels: []*lnrpc.Channel{ch},
		blockHt:  800000,
		version:  "0.18.0-beta",
		chanInfo: map[uint64]*lnrpc.ChannelEdge{}, // errors
	}
	require.NoError(t, UpdateChannels(context.Background(), q, c))
	assert.Empty(t, q.insertedChannels)
	assert.Empty(t, q.updatedChannels)
	require.Len(t, q.insertedHTLCs, 1)
	assert.Equal(t, "abcd", q.insertedHTLCs[0].HashLock)
}

// TestUpdateChannels_PendingChannelSettings: a pending channel with local fee and
// AR settings applies them via UpdateChannelPolicy, populates the inserted row,
// and deletes the pending record.
func TestUpdateChannels_PendingChannelSettings(t *testing.T) {
	q := newFakeUCQ()
	chanID := uint64(333)
	q.pending[pkey("abcdef", 0)] = db.GuiPendingchannel{
		FundingTxid: "abcdef", OutputIndex: 0,
		LocalFeeRate:  pgtype.Int4{Int32: 750, Valid: true},
		AutoRebalance: pgtype.Bool{Bool: true, Valid: true},
		ArMaxCost:     pgtype.Int4{Int32: 50, Valid: true},
		AutoFees:      pgtype.Bool{Bool: false, Valid: true},
	}
	c := &fakeUCClient{
		channels: []*lnrpc.Channel{basicChannel(chanID)},
		blockHt:  800000,
		version:  "0.18.0-beta",
		chanInfo: map[uint64]*lnrpc.ChannelEdge{chanID: edge(chanID, 500, 250, false)},
	}
	require.NoError(t, UpdateChannels(context.Background(), q, c))
	require.Len(t, c.policyReqs, 1)
	assert.Equal(t, float64(750)/1000000, c.policyReqs[0].FeeRate)
	require.Len(t, q.insertedChannels, 1)
	ins := q.insertedChannels[0]
	assert.Equal(t, int32(750), ins.LocalFeeRate)
	assert.True(t, ins.AutoRebalance)
	assert.Equal(t, int32(50), ins.ArMaxCost)
	assert.False(t, ins.AutoFees) // pending set auto_fees=false; defaults must NOT override
	require.Len(t, q.deletedPending, 1)
}

// TestUpdateChannels_ExpiringHTLCDisconnect: an HTLC expiring within 13 blocks causes
// the peer to be disconnected and last_reconnected to be set.
func TestUpdateChannels_ExpiringHTLCDisconnect(t *testing.T) {
	q := newFakeUCQ()
	chanID := uint64(444)
	ch := basicChannel(chanID)
	ch.PendingHtlcs = []*lnrpc.HTLC{{Incoming: true, Amount: 7000, HashLock: []byte{0x01}, ExpirationHeight: 800010, ForwardingChannel: 0}}
	q.channels["444"] = db.GuiChannel{
		RemotePubkey: remotePub, ChanID: "444", FundingTxid: "abcdef", Capacity: 1_000_000,
		Alias: "bob", IsActive: true, IsOpen: true, LocalFeeRate: 500, RemoteCltv: 80,
		RemoteBaseFee: 1000, RemoteFeeRate: 250, RemoteMinHtlcMsat: 1000, RemoteMaxHtlcMsat: 990_000_000,
		ArOutTarget: 75, ArInTarget: 90, ArAmtTarget: 30000, ArMaxCost: 65, AutoFees: true,
	}
	q.peers[remotePub] = db.GuiPeer{Pubkey: remotePub, Alias: pgtype.Text{String: "bob", Valid: true}}
	q.peerAliases[remotePub] = pgtype.Text{String: "bob", Valid: true}
	c := &fakeUCClient{
		channels: []*lnrpc.Channel{ch},
		blockHt:  800000, // expiration 800010 - 800000 = 10 <= 13
		version:  "0.18.0-beta",
		chanInfo: map[uint64]*lnrpc.ChannelEdge{chanID: edge(chanID, 500, 250, false)},
	}
	require.NoError(t, UpdateChannels(context.Background(), q, c))
	require.Len(t, c.disconnect, 1)
	assert.Equal(t, remotePub, c.disconnect[0])
	require.Len(t, q.reconnSet, 1)
	require.Len(t, q.connSet, 1) // disconnectPeer set connected=false
}

// TestUpdateChannels_ClosedChannelDetection: a channel that is open in the DB but
// absent from the LND list is marked is_open=false and is_active=false.
func TestUpdateChannels_ClosedChannelDetection(t *testing.T) {
	q := newFakeUCQ()
	chanID := uint64(666)
	q.channels["666"] = db.GuiChannel{
		RemotePubkey: remotePub, ChanID: "666", FundingTxid: "abcdef", Capacity: 1_000_000,
		Alias: "bob", IsActive: true, IsOpen: true, LocalFeeRate: 500, RemoteCltv: 80,
		RemoteBaseFee: 1000, RemoteFeeRate: 250, RemoteMinHtlcMsat: 1000, RemoteMaxHtlcMsat: 990_000_000,
		ArOutTarget: 75, ArInTarget: 90, ArAmtTarget: 30000, ArMaxCost: 65, AutoFees: true,
	}
	q.peerAliases[remotePub] = pgtype.Text{String: "bob", Valid: true}
	c := &fakeUCClient{
		channels: []*lnrpc.Channel{basicChannel(chanID)},
		blockHt:  800000,
		version:  "0.18.0-beta",
		chanInfo: map[uint64]*lnrpc.ChannelEdge{chanID: edge(chanID, 500, 250, false)},
	}
	require.NoError(t, UpdateChannels(context.Background(), q, c))
	// every open channel not listed by LND is closed in one write, keyed on the listed ids.
	require.Len(t, q.closeCalls, 1)
	assert.Equal(t, []string{"666"}, q.closeCalls[0].ListedChanIds)
}

// TestUpdateChannels_ForwardingAlias: a pending HTLC with a forwarding_channel uses
// the channel alias from the DB; unknown channels fall back to "---".
func TestUpdateChannels_ForwardingAlias(t *testing.T) {
	q := newFakeUCQ()
	q.channels["12345"] = db.GuiChannel{ChanID: "12345", Alias: "carol", RemoteCltv: 80}
	chanID := uint64(888)
	ch := basicChannel(chanID)
	ch.PendingHtlcs = []*lnrpc.HTLC{
		{Incoming: false, Amount: 100, HashLock: []byte{0x02}, ExpirationHeight: 900000, ForwardingChannel: 12345},
		{Incoming: false, Amount: 100, HashLock: []byte{0x03}, ExpirationHeight: 900000, ForwardingChannel: 7},
	}
	c := &fakeUCClient{
		channels: []*lnrpc.Channel{ch},
		blockHt:  800000,
		version:  "0.18.0-beta",
		chanInfo: map[uint64]*lnrpc.ChannelEdge{chanID: edge(chanID, 500, 250, false)},
	}
	require.NoError(t, UpdateChannels(context.Background(), q, c))
	require.Len(t, q.insertedHTLCs, 2)
	assert.Equal(t, "carol", q.insertedHTLCs[0].ForwardingAlias)
	assert.Equal(t, "12345", q.insertedHTLCs[0].ForwardingChannel)
	assert.Equal(t, "---", q.insertedHTLCs[1].ForwardingAlias)
}

func TestUpdateChannels_NoChannels(t *testing.T) {
	q := newFakeUCQ()
	c := &fakeUCClient{channels: nil, blockHt: 800000, version: "0.18.0-beta"}
	require.NoError(t, UpdateChannels(context.Background(), q, c))
	assert.True(t, q.pendingHTLCDeleted)
	assert.Empty(t, q.insertedChannels)
	assert.Empty(t, q.updatedChannels)
	// LND lists nothing -> every open channel in the DB gets closed.
	require.Len(t, q.closeCalls, 1)
	assert.Empty(t, q.closeCalls[0].ListedChanIds)
}
