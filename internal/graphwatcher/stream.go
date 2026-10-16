package graphwatcher

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// probeClock tracks the last probe time per target and new peer (key
// "target|peer"). The mutex protects concurrent writes from the main loop and
// probe goroutines.
type probeClock struct {
	mu   sync.Mutex
	last map[string]time.Time
}

func newProbeClock() *probeClock { return &probeClock{last: map[string]time.Time{}} }

func (p *probeClock) get(pk string) time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.last[pk]
}
func (p *probeClock) set(pk string, t time.Time) {
	p.mu.Lock()
	p.last[pk] = t
	p.mu.Unlock()
}

// Run is the main entry point: checks the GW-Enabled setting, runs a graph
// subscription session, and resubscribes after any error or disconnect.
func (d Deps) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if ensureSetting(ctx, d.Q, "GW-Enabled", "0") == "1" {
			d.runSession(ctx)
		} else {
			d.Sleep(30 * time.Second)
		}
		d.Sleep(20 * time.Second) // finally
	}
}

func (d Deps) runSession(ctx context.Context) {
	gwLog("Starting graph subscription...")
	stream, err := d.LN.SubscribeChannelGraph(ctx, &lnrpc.GraphTopologySubscription{})
	if err != nil {
		gwLog(fmt.Sprintf("Error: %s", err))
		return
	}
	var chainHeight uint32
	selfPubkey := ""
	if info, ierr := d.LN.GetInfo(ctx, &lnrpc.GetInfoRequest{}); ierr == nil {
		chainHeight = info.GetBlockHeight()
		selfPubkey = info.GetIdentityPubkey()
	}
	arTargets, err := loadARTargets(ctx, d.Q)
	if err != nil {
		gwLog(fmt.Sprintf("Error: %s", err))
		return
	}
	lastRefresh := d.Now()
	cooldown := atoiOr(ensureSetting(ctx, d.Q, "GW-Cooldown", "300"), 300)
	clock := newProbeClock()
	gwLog(fmt.Sprintf("Watching %d AR target(s), chain height %d", len(arTargets), chainHeight))

	chanState := map[string]struct{}{} // "cid|adv"
	eventCount := 0

	market := newMarketLog(d.Now())
	d.refreshMarket(ctx, market)

	for {
		update, rerr := stream.Recv()
		if rerr != nil {
			if ctx.Err() == nil {
				gwLog(fmt.Sprintf("Error: %s", rerr))
			}
			return
		}

		now := d.Now()
		if now.Sub(lastRefresh).Seconds() > 60 {
			if getSetting(ctx, d.Q, "GW-Enabled", "0") != "1" {
				gwLog("Disabled, stopping stream")
				return
			}
			if at, aerr := loadARTargets(ctx, d.Q); aerr == nil {
				arTargets = at
			}
			cooldown = atoiOr(getSetting(ctx, d.Q, "GW-Cooldown", "300"), 300)
			d.refreshMarket(ctx, market)
			lastRefresh = now
		}

		if merr := d.recordUpdates(ctx, market, update); merr != nil {
			gwLog(fmt.Sprintf("Market: %s", merr))
		}

		for _, cu := range update.GetChannelUpdates() {
			if herr := d.handleChannelUpdate(ctx, cu, arTargets, chainHeight, chanState, cooldown, clock, selfPubkey, &eventCount); herr != nil {
				gwLog(fmt.Sprintf("Error: %s", herr))
				return
			}
		}
		for _, cc := range update.GetClosedChans() {
			if herr := d.handleClosedChan(ctx, cc, arTargets, chanState, &eventCount); herr != nil {
				gwLog(fmt.Sprintf("Error: %s", herr))
				return
			}
		}
		if eventCount >= 50 {
			if terr := d.Q.DeleteOldGraphEvents(ctx); terr != nil {
				gwLog(fmt.Sprintf("Error: %s", terr))
				return
			}
			eventCount = 0
		}
	}
}

// handleChannelUpdate processes a channel update event. It ignores channels
// that are older than 144 blocks or do not involve an AR target, deduplicates
// per (channel, advertiser) pair, and dispatches a probe right away when the new
// peer's policy toward the target is known and enabled, the cooldown for the
// pair (target, new peer) has elapsed and probeGate lets it through.
func (d Deps) handleChannelUpdate(ctx context.Context, cu *lnrpc.ChannelEdgeUpdate, arTargets map[string]struct{}, chainHeight uint32, chanState map[string]struct{}, cooldown int, clock *probeClock, selfPubkey string, eventCount *int) error {
	adv := cu.GetAdvertisingNode()
	conn := cu.GetConnectingNode()
	var targetPk, otherPk string
	if _, ok := arTargets[adv]; ok {
		targetPk, otherPk = adv, conn
	} else if _, ok := arTargets[conn]; ok {
		targetPk, otherPk = conn, adv
	} else {
		return nil
	}

	chanID := cu.GetChanId()
	cid := chanIDStr(chanID)
	advFeePpm := cu.GetRoutingPolicy().GetFeeRateMilliMsat()
	advBaseFee := cu.GetRoutingPolicy().GetFeeBaseMsat()
	advDisabled := cu.GetRoutingPolicy().GetDisabled()
	capacity := cu.GetCapacity()

	// Skip if chain height is unknown or the channel is 144+ blocks old.
	if chainHeight == 0 {
		return nil
	}
	fundingHeight := chanID >> 40
	age := int64(chainHeight) - int64(fundingHeight)
	if age >= 144 {
		return nil
	}

	stateKey := cid + "|" + adv
	if _, ok := chanState[stateKey]; ok {
		return nil
	}
	chanState[stateKey] = struct{}{}

	const eventType = "new_channel"
	gwLog(fmt.Sprintf("new_channel %s (height %d, age %d blocks)", cid, fundingHeight, age))

	// The fee that matters is the new peer's policy toward the target; an
	// update advertised by the target carries the target's policy instead.
	feePpm := advFeePpm
	baseFee := advBaseFee
	disabled := advDisabled
	policyKnown := adv == otherPk
	if !policyKnown {
		if info, e := d.LN.GetChanInfo(ctx, &lnrpc.ChanInfoRequest{ChanId: chanID}); e == nil {
			var pol *lnrpc.RoutingPolicy
			if info.GetNode1Pub() == otherPk {
				pol = info.GetNode1Policy()
			} else if info.GetNode2Pub() == otherPk {
				pol = info.GetNode2Policy()
			}
			if pol != nil {
				feePpm, baseFee, disabled = pol.GetFeeRateMilliMsat(), pol.GetFeeBaseMsat(), pol.GetDisabled()
				policyKnown = true
			}
		}
	}

	targetAlias := d.getAlias(ctx, targetPk)
	otherAlias := ""
	if otherPk != "" {
		otherAlias = d.getAlias(ctx, otherPk)
	}

	probeTriggered := false
	clockKey := targetPk + "|" + otherPk
	lastT := clock.get(clockKey)
	switch {
	case !policyKnown:
		gwLog(fmt.Sprintf("new_channel on %s (%s), policy of %s not known yet", targetAlias, cid, otherAlias))
	case disabled:
		gwLog(fmt.Sprintf("new_channel on %s (%s), %s disabled toward the target", targetAlias, cid, otherAlias))
	case d.Now().Sub(lastT).Seconds() < float64(cooldown):
		remaining := int(float64(cooldown) - d.Now().Sub(lastT).Seconds())
		gwLog(fmt.Sprintf("new_channel on %s (%s), cooldown %ds remaining", targetAlias, cid, remaining))
	default:
		plan, skip, gerr := d.probeGate(ctx, targetPk, feePpm)
		switch {
		case gerr != nil:
			gwLog(fmt.Sprintf("Probe error for %s: %s", targetAlias, gerr))
		case plan == nil:
			gwLog(fmt.Sprintf("new_channel on %s (%s) from %s, skip: %s", targetAlias, cid, otherAlias, skip))
		default:
			probeTriggered = true
			clock.set(clockKey, d.Now())
			go d.dispatchProbe(ctx, selfPubkey, plan, targetAlias, cid, eventType, clock, clockKey, otherPk, feePpm)
		}
	}

	feeLogged := pgtype.Int4{}
	if policyKnown {
		feeLogged = pgtype.Int4{Int32: int32(feePpm), Valid: true}
	}
	if err := d.Q.InsertGraphEvent(ctx, db.InsertGraphEventParams{
		Timestamp: pgtype.Timestamptz{Time: d.Now(), Valid: true}, EventType: eventType, ChanID: cid, Capacity: capacity,
		FeePpm: feeLogged, BaseFeeMsat: baseFee,
		TargetPubkey: targetPk, TargetAlias: targetAlias, OtherNode: otherPk, OtherAlias: otherAlias,
		Disabled: disabled, ProbeTriggered: probeTriggered, RoutesFound: 0, PolicyNode: adv,
	}); err != nil {
		return err
	}
	*eventCount++
	return nil
}

// handleClosedChan processes a channel-closed event. It removes the channel
// from the dedup state, looks up which AR target was involved, and records the event.
func (d Deps) handleClosedChan(ctx context.Context, cc *lnrpc.ClosedChannelUpdate, arTargets map[string]struct{}, chanState map[string]struct{}, eventCount *int) error {
	chanID := cc.GetChanId()
	cid := chanIDStr(chanID)
	capacity := cc.GetCapacity()
	for k := range chanState {
		if strings.HasPrefix(k, cid+"|") {
			delete(chanState, k)
		}
	}
	var targetPk, otherPk string
	if info, e := d.LN.GetChanInfo(ctx, &lnrpc.ChanInfoRequest{ChanId: chanID}); e == nil {
		if _, ok := arTargets[info.GetNode1Pub()]; ok {
			targetPk, otherPk = info.GetNode1Pub(), info.GetNode2Pub()
		} else if _, ok := arTargets[info.GetNode2Pub()]; ok {
			targetPk, otherPk = info.GetNode2Pub(), info.GetNode1Pub()
		}
	}
	if targetPk == "" {
		return nil
	}
	targetAlias := d.getAlias(ctx, targetPk)
	otherAlias := ""
	if otherPk != "" {
		otherAlias = d.getAlias(ctx, otherPk)
	}
	gwLog(fmt.Sprintf("chan_closed %s involving %s", cid, targetAlias))
	if err := d.Q.InsertGraphEvent(ctx, db.InsertGraphEventParams{
		Timestamp: pgtype.Timestamptz{Time: d.Now(), Valid: true}, EventType: "chan_closed", ChanID: cid, Capacity: capacity,
		FeePpm: pgtype.Int4{}, BaseFeeMsat: 0, TargetPubkey: targetPk, TargetAlias: targetAlias,
		OtherNode: otherPk, OtherAlias: otherAlias, Disabled: false, ProbeTriggered: false, RoutesFound: 0, PolicyNode: "",
	}); err != nil {
		return err
	}
	*eventCount++
	return nil
}

// dispatchProbe runs triggerProbe in a goroutine, updates the probe clock on
// completion, and writes the verified-route count back to the graph event record.
func (d Deps) dispatchProbe(ctx context.Context, selfPubkey string, plan *probePlan, targetAlias, cid, eventType string, clock *probeClock, clockKey, otherPk string, otherFeePpm int64) {
	routes, err := d.triggerProbe(ctx, selfPubkey, plan, otherPk, otherFeePpm, cid, d.Now())
	if err != nil {
		gwLog(fmt.Sprintf("Probe error for %s: %s", targetAlias, err))
		return
	}
	clock.set(clockKey, d.Now())
	if routes > 0 {
		if uerr := d.Q.UpdateGraphEventRoutesFound(ctx, db.UpdateGraphEventRoutesFoundParams{
			TargetPubkey: plan.targets[0].RemotePubkey, ChanID: cid, EventType: eventType, RoutesFound: int32(routes),
		}); uerr != nil {
			gwLog(fmt.Sprintf("Probe error for %s: %s", targetAlias, uerr))
		}
	}
}
