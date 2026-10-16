package graphwatcher

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// The market log keeps the routing policies on all channels of our peers in
// market_policy, so the fees other nodes charge toward a peer, and which of
// their channels are disabled or announce a small max_htlc, can be followed over
// time. A gossip update is written when it changes a policy; once every
// marketSnapshotEvery a snapshot of all channels of every peer adds the full
// state. Rows older than marketRetention are deleted after each snapshot.
const (
	marketSnapshotEvery = 24 * time.Hour
	marketRetention     = 30 * 24 * time.Hour
	marketLogEvery      = time.Hour
)

// policyState is what a market row records of a policy, to skip unchanged updates.
type policyState struct {
	fee, base, minHtlc, maxHtlc int64
	inFee, inBase               int32
	timeLock                    uint32
	disabled                    bool
}

func stateOf(p *lnrpc.RoutingPolicy) policyState {
	return policyState{
		fee: p.GetFeeRateMilliMsat(), base: p.GetFeeBaseMsat(),
		minHtlc: p.GetMinHtlc(), maxHtlc: int64(p.GetMaxHtlcMsat()),
		inFee: p.GetInboundFeeRateMilliMsat(), inBase: p.GetInboundFeeBaseMsat(),
		timeLock: p.GetTimeLockDelta(), disabled: p.GetDisabled(),
	}
}

// marketLog holds our peers and the last written policy per channel and node.
// The stream loop and the snapshot goroutine share it under mu.
type marketLog struct {
	mu           sync.Mutex
	peers        map[string]struct{}
	last         map[string]policyState // "chan_id|policy_node"
	written      int
	lastLogLine  time.Time
	snapshotting bool
}

func newMarketLog(now time.Time) *marketLog {
	return &marketLog{peers: map[string]struct{}{}, last: map[string]policyState{}, lastLogLine: now}
}

func (m *marketLog) setPeers(pubkeys []string) {
	peers := make(map[string]struct{}, len(pubkeys))
	for _, pk := range pubkeys {
		peers[pk] = struct{}{}
	}
	m.mu.Lock()
	m.peers = peers
	m.mu.Unlock()
}

// marketRow is the market_policy row of node's policy on a channel of peer.
func marketRow(at time.Time, source, chanID string, capacity int64, peer, node string, p *lnrpc.RoutingPolicy) db.InsertMarketPoliciesParams {
	s := stateOf(p)
	row := db.InsertMarketPoliciesParams{
		Timestamp: pgtype.Timestamptz{Time: at, Valid: true}, Source: source, ChanID: chanID, Capacity: capacity,
		PeerPubkey: peer, PolicyNode: node, FeePpm: s.fee, BaseFeeMsat: s.base,
		InboundFeePpm: s.inFee, InboundBaseMsat: s.inBase, MinHtlcMsat: s.minHtlc, MaxHtlcMsat: s.maxHtlc,
		TimeLockDelta: int32(s.timeLock), Disabled: s.disabled,
	}
	if lu := p.GetLastUpdate(); lu > 0 {
		row.LastUpdate = pgtype.Timestamptz{Time: time.Unix(int64(lu), 0), Valid: true}
	}
	return row
}

// updateRows returns the rows a gossip update adds: one per peer of ours on the
// channel (two when both ends are peers), none when the advertised policy did
// not change since it was last written.
func (m *marketLog) updateRows(cu *lnrpc.ChannelEdgeUpdate, at time.Time) []db.InsertMarketPoliciesParams {
	p := cu.GetRoutingPolicy()
	if p == nil {
		return nil
	}
	adv, conn := cu.GetAdvertisingNode(), cu.GetConnectingNode()
	cid := strconv.FormatUint(cu.GetChanId(), 10)
	m.mu.Lock()
	defer m.mu.Unlock()
	var peers []string
	for _, n := range []string{conn, adv} {
		if _, ok := m.peers[n]; ok {
			peers = append(peers, n)
		}
	}
	if len(peers) == 0 {
		return nil
	}
	key, s := cid+"|"+adv, stateOf(p)
	if prev, ok := m.last[key]; ok && prev == s {
		return nil
	}
	m.last[key] = s
	rows := make([]db.InsertMarketPoliciesParams, 0, len(peers))
	for _, peer := range peers {
		rows = append(rows, marketRow(at, "update", cid, cu.GetCapacity(), peer, adv, p))
	}
	return rows
}

// recordUpdates writes the market rows of a graph update.
func (d Deps) recordUpdates(ctx context.Context, m *marketLog, update *lnrpc.GraphTopologyUpdate) error {
	var rows []db.InsertMarketPoliciesParams
	for _, cu := range update.GetChannelUpdates() {
		rows = append(rows, m.updateRows(cu, d.Now())...)
	}
	if len(rows) == 0 {
		return nil
	}
	if _, err := d.Q.InsertMarketPolicies(ctx, rows); err != nil {
		return err
	}
	m.mu.Lock()
	m.written += len(rows)
	m.mu.Unlock()
	return nil
}

// refreshMarket reloads our peers and runs marketTick.
func (d Deps) refreshMarket(ctx context.Context, m *marketLog) {
	peers, err := d.Q.ListOpenPeerPubkeys(ctx)
	if err != nil {
		gwLog(fmt.Sprintf("Market: %s", err))
		return
	}
	m.setPeers(peers)
	d.marketTick(ctx, m)
}

// marketTick runs from the stream loop: an hourly log line with the number of
// rows written, and a snapshot in the background when the last one is older
// than marketSnapshotEvery.
func (d Deps) marketTick(ctx context.Context, m *marketLog) {
	now := d.Now()
	m.mu.Lock()
	if now.Sub(m.lastLogLine) >= marketLogEvery {
		gwLog(fmt.Sprintf("Market: %d policy changes recorded since %s", m.written, m.lastLogLine.Format("15:04")))
		m.written, m.lastLogLine = 0, now
	}
	busy := m.snapshotting
	m.mu.Unlock()
	if busy {
		return
	}
	last, err := d.Q.LastMarketSnapshot(ctx)
	if err != nil {
		gwLog(fmt.Sprintf("Market: %s", err))
		return
	}
	if last.Valid && now.Sub(last.Time) < marketSnapshotEvery {
		return
	}
	m.mu.Lock()
	m.snapshotting = true
	m.mu.Unlock()
	go func() {
		defer func() {
			m.mu.Lock()
			m.snapshotting = false
			m.mu.Unlock()
		}()
		if serr := d.marketSnapshot(ctx, m); serr != nil && ctx.Err() == nil {
			gwLog(fmt.Sprintf("Market: snapshot error: %s", serr))
		}
	}()
}

// marketSnapshot writes the policies on all channels of every peer, then
// deletes rows older than marketRetention.
func (d Deps) marketSnapshot(ctx context.Context, m *marketLog) error {
	m.mu.Lock()
	peers := make([]string, 0, len(m.peers))
	for pk := range m.peers {
		peers = append(peers, pk)
	}
	m.mu.Unlock()
	at := d.Now()
	total := 0
	for _, peer := range peers {
		info, err := d.LN.GetNodeInfo(ctx, &lnrpc.NodeInfoRequest{PubKey: peer, IncludeChannels: true})
		if err != nil {
			gwLog(fmt.Sprintf("Market: node info %s: %s", peer, err))
			continue
		}
		var rows []db.InsertMarketPoliciesParams
		m.mu.Lock()
		for _, e := range info.GetChannels() {
			cid := strconv.FormatUint(e.GetChannelId(), 10)
			for _, side := range []struct {
				node string
				pol  *lnrpc.RoutingPolicy
			}{{e.GetNode1Pub(), e.GetNode1Policy()}, {e.GetNode2Pub(), e.GetNode2Policy()}} {
				if side.pol == nil {
					continue
				}
				m.last[cid+"|"+side.node] = stateOf(side.pol)
				rows = append(rows, marketRow(at, "snapshot", cid, e.GetCapacity(), peer, side.node, side.pol))
			}
		}
		m.mu.Unlock()
		if len(rows) == 0 {
			continue
		}
		if _, err := d.Q.InsertMarketPolicies(ctx, rows); err != nil {
			return err
		}
		total += len(rows)
	}
	deleted, err := d.Q.DeleteOldMarketPolicies(ctx, pgtype.Timestamptz{Time: at.Add(-marketRetention), Valid: true})
	if err != nil {
		return err
	}
	gwLog(fmt.Sprintf("Market: snapshot of %d peers, %d policies, %d old rows deleted", len(peers), total, deleted))
	return nil
}
