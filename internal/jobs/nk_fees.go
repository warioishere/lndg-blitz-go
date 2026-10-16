package jobs

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	af "github.com/warioishere/lndg-blitz-go/internal/af"
	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// NK job timing: it evaluates every nkEvery (the data loop runs more often),
// and deletes nk_fee_log rows older than nkLogRetention once an hour.
const (
	nkEvery        = time.Minute
	nkLogRetention = 30 * 24 * time.Hour
)

// nkQuerier is the DB access of the NK job.
type nkQuerier interface {
	settingsQuerier
	ListNkJobChannels(ctx context.Context) ([]db.GuiChannel, error)
	NkForwardFlows(ctx context.Context, arg db.NkForwardFlowsParams) ([]db.NkForwardFlowsRow, error)
	NkBalanceOutFlows(ctx context.Context, arg db.NkBalanceOutFlowsParams) ([]db.NkBalanceOutFlowsRow, error)
	UpdateChannelNkPolicy(ctx context.Context, arg db.UpdateChannelNkPolicyParams) error
	InsertNkFeeLog(ctx context.Context, arg db.InsertNkFeeLogParams) error
	DeleteOldNkFeeLogs(ctx context.Context, timestamp pgtype.Timestamptz) error
	InsertAutofee(ctx context.Context, arg db.InsertAutofeeParams) error
	InsertInboundFeeLog(ctx context.Context, arg db.InsertInboundFeeLogParams) error
}

// nkShadow is the fee NK would run on a channel group that is not in NK mode,
// carried from decision to decision.
type nkShadow struct {
	fee, inbound            int
	lastChange, lastPublish time.Time
}

// nkRunner keeps what the NK job carries between runs: the shadow fees, the
// last logged reason per channel (a decision is logged when it publishes or
// its reason changes) and the run and purge times.
type nkRunner struct {
	lastRun, lastPurge time.Time
	shadow             map[string]*nkShadow
	reason             map[string]string
}

var nkState = &nkRunner{shadow: map[string]*nkShadow{}, reason: map[string]string{}}

// NkFees runs the NK fee mode: NK channels get their decisions applied, the
// other auto-fee channels get shadow decisions when NK-Shadow is 1. Channels to
// the same peer are decided together (one fee, one inbound rate).
func NkFees(ctx context.Context, q nkQuerier, client policyClient) error {
	return nkState.run(ctx, q, client, time.Now())
}

// nkGroup is the channels to one peer in one mode.
type nkGroup struct {
	key      string
	nk       bool
	channels []db.GuiChannel
}

func (n *nkRunner) run(ctx context.Context, q nkQuerier, client policyClient, now time.Time) error {
	if now.Sub(n.lastRun) < nkEvery {
		return nil
	}
	n.lastRun = now
	p, err := af.LoadNkParams(ctx, q)
	if err != nil {
		return err
	}
	channels, err := q.ListNkJobChannels(ctx)
	if err != nil {
		return err
	}
	groups := map[string]*nkGroup{}
	var ids []string
	for _, ch := range channels {
		nk := ch.AfMode == "nk"
		if !nk && !p.Shadow {
			continue
		}
		key := ch.RemotePubkey + "|" + ch.AfMode
		g := groups[key]
		if g == nil {
			g = &nkGroup{key: key, nk: nk}
			groups[key] = g
		}
		g.channels = append(g.channels, ch)
		ids = append(ids, ch.ChanID)
	}
	if len(ids) == 0 {
		return nil
	}
	flows, err := af.LoadNkFlows(ctx, q, ids, p.LockFlowPct, now)
	if err != nil {
		return err
	}
	inboundOK := false
	if info, ierr := client.GetInfo(ctx, &lnrpc.GetInfoRequest{}); ierr == nil {
		inboundOK = versionFloat(info.Version) >= 0.18
	}
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		g := groups[k]
		in := af.NkGroupInput(g.channels, flows)
		if g.nk {
			d := af.NkDecide(in, p, now)
			n.apply(ctx, q, client, g, in, d, p, inboundOK, now)
			continue
		}
		s := n.shadow[g.key]
		if s == nil {
			s = &nkShadow{fee: in.Fee, inbound: in.Inbound}
			n.shadow[g.key] = s
		}
		in.Fee, in.Inbound, in.LastChange, in.LastPublish = s.fee, s.inbound, s.lastChange, s.lastPublish
		d := af.NkDecide(in, p, now)
		changed := d.Fee != s.fee || d.Inbound != s.inbound
		if d.Publish {
			s.lastPublish = now
		}
		if changed {
			s.lastChange = now
		}
		for _, ch := range g.channels {
			if changed || n.reason[ch.ChanID] != d.Reason {
				n.log(ctx, q, ch, false, in, d, s.fee, s.inbound, nkMaxHtlcFor(ch, d, p.MaxHtlcStep), now)
			}
		}
		s.fee, s.inbound = d.Fee, d.Inbound
	}
	if now.Sub(n.lastPurge) >= time.Hour {
		n.lastPurge = now
		if e := q.DeleteOldNkFeeLogs(ctx, ts(now.Add(-nkLogRetention))); e != nil {
			dataLog(fmt.Sprintf("NK: error deleting old log rows: %s", e))
		}
	}
	return nil
}

// nkMaxHtlcFor is the max_htlc the NK rules give a channel.
func nkMaxHtlcFor(ch db.GuiChannel, d af.NkDecision, step float64) int64 {
	avail := ch.LocalBalance - int64(ch.LocalChanReserve)
	return af.NkMaxHtlc(avail, ch.Capacity, ch.LocalMinHtlcMsat, step, d.Empty)
}

// apply sends a decision for the channels of an NK group to LND and records it.
// A channel that fails is logged and skipped.
func (n *nkRunner) apply(ctx context.Context, q nkQuerier, client policyClient, g *nkGroup, in af.NkInput, d af.NkDecision, p af.NkParams, inboundOK bool, now time.Time) {
	for _, ch := range g.channels {
		maxHtlc := nkMaxHtlcFor(ch, d, p.MaxHtlcStep)
		if !d.Publish {
			if n.reason[ch.ChanID] != d.Reason {
				n.log(ctx, q, ch, true, in, d, int(ch.LocalFeeRate), int(ch.LocalInboundFeeRate), ch.LocalMaxHtlcMsat, now)
			}
			continue
		}
		base := ch.LocalBaseFee
		if p.BaseMsat >= 0 {
			base = int32(p.BaseMsat)
		}
		inbound, inBase := ch.LocalInboundFeeRate, ch.LocalInboundBaseFee
		req := &lnrpc.PolicyUpdateRequest{
			Scope:         &lnrpc.PolicyUpdateRequest_ChanPoint{ChanPoint: channelPoint(ch.FundingTxid, ch.OutputIndex)},
			BaseFeeMsat:   int64(base),
			FeeRate:       float64(d.Fee) / 1000000,
			TimeLockDelta: uint32(ch.LocalCltv),
			MaxHtlcMsat:   uint64(maxHtlc),
		}
		if inboundOK {
			inbound = int32(d.Inbound)
			switch {
			case inbound < 0:
				inBase = -base
			case inbound == 0:
				inBase = 0
			}
			req.InboundFee = &lnrpc.InboundFee{BaseFeeMsat: inBase, FeeRatePpm: inbound}
		}
		if _, err := client.UpdateChannelPolicy(ctx, req); err != nil {
			dataLog(fmt.Sprintf("NK: error updating %s (%s): %s", ch.ChanID, ch.Alias, err))
			continue
		}
		feeChanged := int32(d.Fee) != ch.LocalFeeRate
		inboundChanged := inbound != ch.LocalInboundFeeRate
		lastChange := ch.NkLastChange
		if feeChanged || inboundChanged {
			lastChange = ts(now)
		}
		if err := q.UpdateChannelNkPolicy(ctx, db.UpdateChannelNkPolicyParams{
			ChanID: ch.ChanID, LocalFeeRate: int32(d.Fee), LocalBaseFee: base, LocalInboundFeeRate: inbound,
			LocalInboundBaseFee: inBase, LocalMaxHtlcMsat: maxHtlc, FeesUpdated: ts(now),
			NkLastChange: lastChange, NkLastPublish: ts(now),
		}); err != nil {
			dataLog(fmt.Sprintf("NK: error saving %s (%s): %s", ch.ChanID, ch.Alias, err))
			continue
		}
		setting := "NK [ " + d.Reason + " ]"
		if feeChanged {
			if err := q.InsertAutofee(ctx, db.InsertAutofeeParams{
				Timestamp: ts(now), ChanID: ch.ChanID, PeerAlias: ch.Alias, Setting: truncRunes(setting, 20),
				OldValue: ch.LocalFeeRate, NewValue: int32(d.Fee),
			}); err != nil {
				dataLog(fmt.Sprintf("NK: error logging %s: %s", ch.ChanID, err))
			}
		}
		if inboundChanged {
			if err := q.InsertInboundFeeLog(ctx, db.InsertInboundFeeLogParams{
				Timestamp: ts(now), ChanID: ch.ChanID, PeerAlias: ch.Alias, Setting: truncRunes(setting, 20),
				OldValue: ch.LocalInboundFeeRate, NewValue: inbound,
			}); err != nil {
				dataLog(fmt.Sprintf("NK: error logging %s: %s", ch.ChanID, err))
			}
		}
		applied := d
		applied.Inbound = int(inbound)
		n.log(ctx, q, ch, true, in, applied, int(ch.LocalFeeRate), int(ch.LocalInboundFeeRate), maxHtlc, now)
	}
}

// log writes one nk_fee_log row; feeOld/inboundOld are the values before the decision.
func (n *nkRunner) log(ctx context.Context, q nkQuerier, ch db.GuiChannel, applied bool, in af.NkInput, d af.NkDecision, feeOld, inboundOld int, maxHtlcNew int64, now time.Time) {
	n.reason[ch.ChanID] = d.Reason
	row := db.InsertNkFeeLogParams{
		Timestamp: ts(now), ChanID: ch.ChanID, PeerAlias: truncRunes(ch.Alias, 32), Applied: applied,
		Reason: truncRunes(d.Reason, 40), LocalPct: in.LocalPct, TargetPct: int32(in.TargetPct),
		FeeOld: int32(feeOld), FeeNew: int32(d.Fee), CapPpm: int32(d.Cap),
		MaxHtlcOld: ch.LocalMaxHtlcMsat, MaxHtlcNew: maxHtlcNew,
		InboundOld: int32(inboundOld), InboundNew: int32(d.Inbound),
	}
	if v, ok := in.Flow.Flow24(); ok {
		row.Flow24Ppm = pgtype.Int4{Int32: int32(v), Valid: true}
	}
	if v, ok := in.Flow.Flow72(); ok {
		row.Flow72Ppm = pgtype.Int4{Int32: int32(v), Valid: true}
	}
	if !d.LockUntil.IsZero() {
		row.LockUntil = ts(d.LockUntil)
	}
	if err := q.InsertNkFeeLog(ctx, row); err != nil {
		dataLog(fmt.Sprintf("NK: error writing log for %s: %s", ch.ChanID, err))
	}
}
