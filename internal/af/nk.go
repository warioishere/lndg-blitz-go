package af

import (
	"context"
	"math"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

// NK fee mode: a per-channel alternative to auto-fees (af_mode = 'nk'),
// modelled on the fee algorithm a peer operator described in detail:
//
//   - each channel has a local liquidity target, 100 - ar_in_target (the level
//     the rebalancer refills it to); below it the fee goes up,
//     above it the fee goes down, in percent of the current fee (up faster than
//     down);
//   - the price the channel actually sold at (24 h, else 72 h) sets a target
//     fee: NK-TargetUp x that price below the liquidity target, NK-TargetDown x
//     that price above it; a step never passes the target fee, and at the target
//     fee the fee holds; without a sale in 72 h there is no target fee;
//   - no decrease while a large outbound flow (forward or balance-out of at
//     least NK-LockFlowPct % of capacity) is recent; the lock lasts longer the
//     higher the fee and the emptier the channel;
//   - above NK-AnchorPct % of the sold price a decrease jumps straight there,
//     below the sold price it falls by NK-BelowFlowPct % of the normal step
//     (whole ppm only, so a small fee stays put);
//   - the cap is NK-CapFactor x the reference price (the sold price of 24 h,
//     72 h, else 30 days), at least NK-CapMin; NK-CapMin without a sale in 30
//     days;
//   - an empty channel gets max_htlc at its minimum, its fee follows the rules
//     below the target;
//   - max_htlc is the highest step of a ladder down from the capacity (each step
//     NK-MaxHtlcStep times smaller) that fits into the available liquidity, and
//     only goes out with a fee update;
//   - the inbound discount of a channel with dynamic inbound fees, while it is
//     below its target, is its rebalance budget with the price it sold at
//     (forwards of 6 h, else 24 h, else 72 h) as the reference, never more than
//     its own outbound fee; none at or above the target;
//   - changes go out at most every NK-MinInterval, the policy is resent every
//     NK-Republish even when unchanged.
//
// The rebalancer pays for an NK channel at most min(fee, reference price) less
// the fill margin, instead of fee x ar_max_cost, over all NK channels to the
// peer like the fee decision. The fill margin grows with the local share from
// NK-BudgetMarginMin ppm (empty) to NK-BudgetMarginMax % of the price (full);
// without a sale in 30 days min(fee, NK-CapMin) takes the place of the
// reference price. A raised fee never raises the budget.

// NkParams are the NK-* settings.
type NkParams struct {
	Shadow             bool
	StepUpPct          float64
	StepDownPct        float64
	TargetUp           float64
	TargetDown         float64
	BelowFlowPct       float64
	LockMin, LockMax   time.Duration
	LockFlowPct        float64
	LockFeeRef         int
	AnchorPct          float64
	CapFactor          float64
	CapMin             int
	MaxRate            int
	MinRate            int
	EmptyPct           float64
	MaxHtlcStep        float64
	MinInterval        time.Duration
	Republish          time.Duration
	BaseMsat           int // -1 keeps the channel's base fee
	BudgetMarginMin    int
	BudgetMarginMaxPct float64
}

type nkSettingsQuerier interface {
	GetOrCreateLocalSetting(ctx context.Context, arg db.GetOrCreateLocalSettingParams) (db.GuiLocalsetting, error)
}

// LoadNkParams reads the NK-* settings, creating missing ones with their defaults.
func LoadNkParams(ctx context.Context, q nkSettingsQuerier) (NkParams, error) {
	var p NkParams
	var firstErr error
	num := func(key, def string) float64 {
		row, err := q.GetOrCreateLocalSetting(ctx, db.GetOrCreateLocalSettingParams{Key: key, Value: def})
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			row.Value = def
		}
		v, perr := strconv.ParseFloat(row.Value, 64)
		if perr != nil {
			v, _ = strconv.ParseFloat(def, 64)
		}
		return v
	}
	minutes := func(key, def string) time.Duration { return time.Duration(num(key, def) * float64(time.Minute)) }
	p.Shadow = num("NK-Shadow", "1") == 1
	p.StepUpPct = num("NK-StepUp", "3")
	p.StepDownPct = num("NK-StepDown", "1")
	p.TargetUp = num("NK-TargetUp", "1.2")
	p.TargetDown = num("NK-TargetDown", "0.8")
	p.BelowFlowPct = num("NK-BelowFlowPct", "10")
	p.LockMin = minutes("NK-LockMin", "12")
	p.LockMax = minutes("NK-LockMax", "360")
	p.LockFlowPct = num("NK-LockFlowPct", "2")
	p.LockFeeRef = int(num("NK-LockFeeRef", "2000"))
	p.AnchorPct = num("NK-AnchorPct", "110")
	p.CapFactor = num("NK-CapFactor", "2")
	p.CapMin = int(num("NK-CapMin", "1000"))
	p.MaxRate = int(num("NK-MaxRate", "7000"))
	p.MinRate = int(num("NK-MinRate", "1"))
	p.EmptyPct = num("NK-EmptyPct", "2")
	p.MaxHtlcStep = num("NK-MaxHtlcStep", "1.2")
	p.MinInterval = minutes("NK-MinInterval", "5")
	p.Republish = minutes("NK-Republish", "30")
	p.BaseMsat = int(num("NK-BaseMsat", "1"))
	p.BudgetMarginMin = int(num("NK-BudgetMarginMin", "10"))
	p.BudgetMarginMaxPct = num("NK-BudgetMarginMax", "20")
	return p, firstErr
}

// NkFlow is a channel's outbound flow over the last 30 days. Fwd* are
// forwards (gross fee: inbound discount of the incoming channel included),
// All* are forwards plus balance-outs valued at the channel's fee then.
// LastBig is the latest single flow of at least NK-LockFlowPct % of capacity.
type NkFlow struct {
	Fwd6hAmt, Fwd6hFee, Fwd24hAmt, Fwd24hFee, Fwd72hAmt, Fwd72hFee   float64
	All24hAmt, All24hFee, All72hAmt, All72hFee, All30dAmt, All30dFee float64
	LastBig                                                          time.Time
}

// Add sums o into f, every window and the latest large flow, so several
// channels to one peer can be judged as one.
func (f *NkFlow) Add(o NkFlow) {
	f.Fwd6hAmt += o.Fwd6hAmt
	f.Fwd6hFee += o.Fwd6hFee
	f.Fwd24hAmt += o.Fwd24hAmt
	f.Fwd24hFee += o.Fwd24hFee
	f.Fwd72hAmt += o.Fwd72hAmt
	f.Fwd72hFee += o.Fwd72hFee
	f.All24hAmt += o.All24hAmt
	f.All24hFee += o.All24hFee
	f.All72hAmt += o.All72hAmt
	f.All72hFee += o.All72hFee
	f.All30dAmt += o.All30dAmt
	f.All30dFee += o.All30dFee
	if o.LastBig.After(f.LastBig) {
		f.LastBig = o.LastBig
	}
}

func ppm(fee, amt float64) (int, bool) {
	if amt <= 0 {
		return 0, false
	}
	return int(math.Round(fee * 1e6 / amt)), true
}

// Flow24 and Flow72 are the ppm the channel's outbound flow paid.
func (f NkFlow) Flow24() (int, bool) { return ppm(f.All24hFee, f.All24hAmt) }
func (f NkFlow) Flow72() (int, bool) { return ppm(f.All72hFee, f.All72hAmt) }

// Anchor is the price the channel sold at: 24 hours, else 72 hours.
func (f NkFlow) Anchor() (int, bool) {
	if v, ok := f.Flow24(); ok {
		return v, true
	}
	return f.Flow72()
}

// RefPrice is the price for the cap and the rebalance budget: the anchor,
// else the sold price of 30 days.
func (f NkFlow) RefPrice() (int, bool) {
	if v, ok := f.Anchor(); ok {
		return v, true
	}
	return ppm(f.All30dFee, f.All30dAmt)
}

// SoldPpm is the forward price for the inbound discount: 6 h, else 24 h, else 72 h.
func (f NkFlow) SoldPpm() (int, bool) {
	if v, ok := ppm(f.Fwd6hFee, f.Fwd6hAmt); ok {
		return v, true
	}
	if v, ok := ppm(f.Fwd24hFee, f.Fwd24hAmt); ok {
		return v, true
	}
	return ppm(f.Fwd72hFee, f.Fwd72hAmt)
}

type nkFlowQuerier interface {
	NkForwardFlows(ctx context.Context, arg db.NkForwardFlowsParams) ([]db.NkForwardFlowsRow, error)
	NkBalanceOutFlows(ctx context.Context, arg db.NkBalanceOutFlowsParams) ([]db.NkBalanceOutFlowsRow, error)
}

// LoadNkFlows returns the NkFlow of each given channel (channels without flow are missing).
func LoadNkFlows(ctx context.Context, q nkFlowQuerier, chanIDs []string, lockFlowPct float64, now time.Time) (map[string]NkFlow, error) {
	tz := func(d time.Duration) pgtype.Timestamptz { return pgtype.Timestamptz{Time: now.Add(-d), Valid: true} }
	fwd, err := q.NkForwardFlows(ctx, db.NkForwardFlowsParams{
		Since6h: tz(6 * time.Hour), Since24h: tz(24 * time.Hour), Since72h: tz(72 * time.Hour),
		BigPct: lockFlowPct, ChanIds: chanIDs, Since: tz(30 * 24 * time.Hour),
	})
	if err != nil {
		return nil, err
	}
	outs, err := q.NkBalanceOutFlows(ctx, db.NkBalanceOutFlowsParams{
		Since24h: tz(24 * time.Hour), Since72h: tz(72 * time.Hour),
		BigPct: lockFlowPct, ChanIds: chanIDs, Since: tz(30 * 24 * time.Hour),
	})
	if err != nil {
		return nil, err
	}
	m := make(map[string]NkFlow, len(fwd)+len(outs))
	later := func(a time.Time, b pgtype.Timestamptz) time.Time {
		if b.Valid && b.Time.After(a) {
			return b.Time
		}
		return a
	}
	for _, r := range fwd {
		f := m[r.ChanID]
		f.Fwd6hAmt, f.Fwd6hFee, f.Fwd24hAmt, f.Fwd24hFee, f.Fwd72hAmt, f.Fwd72hFee = r.Amt6h, r.Fee6h, r.Amt24h, r.Fee24h, r.Amt72h, r.Fee72h
		f.All24hAmt += r.Amt24h
		f.All24hFee += r.Fee24h
		f.All72hAmt += r.Amt72h
		f.All72hFee += r.Fee72h
		f.All30dAmt += r.Amt30d
		f.All30dFee += r.Fee30d
		f.LastBig = later(f.LastBig, r.LastBig)
		m[r.ChanID] = f
	}
	for _, r := range outs {
		f := m[r.ChanID]
		f.All24hAmt += r.Amt24h
		f.All24hFee += r.Fee24h
		f.All72hAmt += r.Amt72h
		f.All72hFee += r.Fee72h
		f.All30dAmt += r.Amt30d
		f.All30dFee += r.Fee30d
		f.LastBig = later(f.LastBig, r.LastBig)
		m[r.ChanID] = f
	}
	return m, nil
}

// NkInput is the state of a channel (or of a peer's channels together) the
// fee decision depends on.
type NkInput struct {
	Fee, Inbound            int
	LocalPct                float64
	TargetPct               int
	Flow                    NkFlow
	InboundDynamic          bool
	LastChange, LastPublish time.Time
}

// NkDecision is what the NK rules set. Publish is true when the policy goes out now.
type NkDecision struct {
	Fee, Inbound, Cap int
	Reason            string
	LockUntil         time.Time
	Empty, Publish    bool
}

// nkLock is how long a large outbound flow blocks decreases: from LockMin up to
// LockMax, longer for a higher fee and an emptier channel.
func nkLock(fee int, localPct float64, p NkParams) time.Duration {
	feeF := math.Min(1, math.Max(0, float64(fee)/float64(max(p.LockFeeRef, 1))))
	empt := 1 - math.Min(1, math.Max(0, localPct/100))
	return p.LockMin + time.Duration(float64(p.LockMax-p.LockMin)*feeF*empt)
}

// NkLockUntil is the end of the decrease lock the channel's last large
// outbound flow set (zero without one).
func NkLockUntil(f NkFlow, fee int, localPct float64, p NkParams) time.Time {
	if f.LastBig.IsZero() {
		return time.Time{}
	}
	return f.LastBig.Add(nkLock(fee, localPct, p))
}

// NkCap is the highest fee: NK-CapFactor x the reference price, at least
// NK-CapMin; NK-CapMin without a sale in 30 days; never above NK-MaxRate.
func NkCap(flow NkFlow, p NkParams) int {
	if ref, ok := flow.RefPrice(); ok {
		return min(max(int(p.CapFactor*float64(ref)), p.CapMin), p.MaxRate)
	}
	return min(p.CapMin, p.MaxRate)
}

// NkDecide applies the NK fee rules to one channel.
func NkDecide(in NkInput, p NkParams, now time.Time) NkDecision {
	d := NkDecision{Fee: in.Fee, Inbound: in.Inbound, Cap: NkCap(in.Flow, p), Empty: in.LocalPct < p.EmptyPct}
	anchor, hasAnchor := in.Flow.Anchor()
	fee := in.Fee
	if in.LocalPct < float64(in.TargetPct) {
		d.Fee, d.Reason = max(int(math.Ceil(float64(fee)*(1+p.StepUpPct/100))), fee+1), "below target"
		if d.Empty {
			d.Reason = "empty"
		}
		if target := int(math.Round(float64(anchor) * p.TargetUp)); hasAnchor && d.Fee > target {
			d.Fee, d.Reason = max(fee, target), d.Reason+", target fee"
		}
	} else {
		lock := NkLockUntil(in.Flow, fee, in.LocalPct, p)
		top := int(math.Round(float64(anchor) * p.AnchorPct / 100))
		floor := int(math.Round(float64(anchor) * p.TargetDown))
		switch {
		case now.Before(lock):
			d.LockUntil, d.Reason = lock, "lock"
		case hasAnchor && fee > top:
			d.Fee, d.Reason = top, "to anchor"
		case hasAnchor && fee <= floor:
			d.Reason = "above target, target fee"
		case hasAnchor && fee < anchor:
			d.Fee, d.Reason = max(int(math.Round(float64(fee)*(1-p.StepDownPct*p.BelowFlowPct/1e4))), floor), "above target, below flow"
		default:
			d.Fee, d.Reason = min(int(math.Floor(float64(fee)*(1-p.StepDownPct/100))), fee-1), "above target"
			if hasAnchor {
				d.Fee = max(d.Fee, floor)
			}
		}
	}
	if d.Fee > d.Cap {
		d.Fee, d.Reason = d.Cap, d.Reason+", cap"
	}
	d.Fee = max(d.Fee, p.MinRate)

	switch {
	case in.InboundDynamic && in.LocalPct < float64(in.TargetPct):
		if sold, ok := in.Flow.SoldPpm(); ok {
			d.Inbound = -min(max(nkBudget(in.Fee, sold, in.LocalPct, p), 0), d.Fee)
		}
	case in.Inbound < 0:
		d.Inbound = 0
	}

	changed := d.Fee != in.Fee || d.Inbound != in.Inbound
	due := now.Sub(in.LastChange) >= p.MinInterval
	if changed && !due {
		d.Fee, d.Inbound, d.Reason = in.Fee, in.Inbound, d.Reason+", wait"
		changed = false
	}
	d.Publish = changed || now.Sub(in.LastPublish) >= p.Republish
	return d
}

// NkMaxHtlc is the highest step of a ladder down from the capacity, each step
// step times smaller than the one above, that fits into the available
// liquidity; never under minMsat (or 1000 sats); minimum for an empty channel.
// A step of 1 or less is taken as 1.01, so the ladder always descends.
func NkMaxHtlc(availSat, capacitySat, minMsat int64, step float64, empty bool) int64 {
	floor := max(minMsat, 1_000_000)
	avail := float64(availSat * 1000)
	if empty || avail <= float64(floor) {
		return floor
	}
	step = max(step, 1.01)
	m := float64(capacitySat * 1000)
	for m > avail {
		m /= step
	}
	return max(int64(m), floor)
}

// NkTargetPct is the channel's NK local liquidity target in percent: 100 -
// ar_in_target, the local level the rebalancer refills the channel to.
func NkTargetPct(arInTarget int32) int {
	return int(100 - arInTarget)
}

// nkMargin is the fill margin on a price: NK-BudgetMarginMin ppm for an empty
// channel, rising with the local share to NK-BudgetMarginMax % of the price
// for a full one.
func nkMargin(price int, localPct float64, p NkParams) int {
	fill := math.Min(1, math.Max(0, localPct/100))
	minM := float64(p.BudgetMarginMin)
	return int(math.Round(minM + (p.BudgetMarginMaxPct/100*float64(price)-minM)*fill))
}

// nkBudget is min(fee, price) less the fill margin: the rebalance budget and
// the inbound discount of an NK channel, each with its own price.
func nkBudget(fee, price int, localPct float64, p NkParams) int {
	base := min(fee, price)
	return base - nkMargin(base, localPct, p)
}

// NkBudgetPpm is what the rebalancer may pay at most to refill an NK channel,
// before the source fee: nkBudget with the reference price; with NK-CapMin
// without a sale in 30 days.
func NkBudgetPpm(fee int, flow NkFlow, localPct float64, p NkParams) int {
	ref, ok := flow.RefPrice()
	if !ok {
		ref = p.CapMin
	}
	return nkBudget(fee, ref, localPct, p)
}

// NkGroupInput combines a peer's channels: fill over their total capacity,
// their flows summed, the highest fee, the latest change and publish times and
// the capacity-weighted target. The inbound rate and the dynamic flag are those
// of the first channel.
func NkGroupInput(chs []db.GuiChannel, flows map[string]NkFlow) NkInput {
	var local, capacity int64
	var targetW float64
	in := NkInput{Inbound: int(chs[0].LocalInboundFeeRate), InboundDynamic: chs[0].InboundOffset != 0}
	var f NkFlow
	for _, ch := range chs {
		local += ch.LocalBalance + ch.PendingOutbound
		capacity += ch.Capacity
		targetW += float64(NkTargetPct(ch.ArInTarget)) * float64(ch.Capacity)
		in.Fee = max(in.Fee, int(ch.LocalFeeRate))
		if ch.NkLastChange.Valid && ch.NkLastChange.Time.After(in.LastChange) {
			in.LastChange = ch.NkLastChange.Time
		}
		if ch.NkLastPublish.Valid && ch.NkLastPublish.Time.After(in.LastPublish) {
			in.LastPublish = ch.NkLastPublish.Time
		}
		f.Add(flows[ch.ChanID])
	}
	in.Flow = f
	if capacity > 0 {
		in.LocalPct = float64(local) * 100 / float64(capacity)
		in.TargetPct = int(math.Round(targetW / float64(capacity)))
	}
	return in
}

// TargetChannel is the part of a refill target the rebalancer budget depends on.
type TargetChannel struct {
	ChanID, RemotePubkey            string
	LocalFeeRate, ArMaxCost         int32
	ArInTarget                      int32
	Capacity, LocalBalance, PendOut int64
	AfMode                          string
}

// TargetOf returns the TargetChannel of a channel row.
func TargetOf(ch db.GuiChannel) TargetChannel {
	return TargetChannel{
		ChanID: ch.ChanID, RemotePubkey: ch.RemotePubkey, LocalFeeRate: ch.LocalFeeRate, ArMaxCost: ch.ArMaxCost,
		ArInTarget: ch.ArInTarget, Capacity: ch.Capacity, LocalBalance: ch.LocalBalance, PendOut: ch.PendingOutbound,
		AfMode: ch.AfMode,
	}
}

// NkBudgetQuerier is the DB access TargetBudget needs.
type NkBudgetQuerier interface {
	nkSettingsQuerier
	nkFlowQuerier
	ListNkPeerChannels(ctx context.Context, remotePubkey string) ([]db.GuiChannel, error)
}

// TargetBudget returns the fee rate and the max-cost percent the rebalancer
// multiplies into its budget for a refill target: the channel's fee and
// ar_max_cost, or for an NK channel the NK budget at 100 %, taken over the NK
// channels to the peer (NkGroupInput) or, when the NK job runs none of them,
// over the target alone.
func TargetBudget(ctx context.Context, q NkBudgetQuerier, t TargetChannel, now time.Time) (feeRate, maxCostPct int, err error) {
	if t.AfMode != "nk" {
		return int(t.LocalFeeRate), int(t.ArMaxCost), nil
	}
	p, err := LoadNkParams(ctx, q)
	if err != nil {
		return 0, 0, err
	}
	chs, err := q.ListNkPeerChannels(ctx, t.RemotePubkey)
	if err != nil {
		return 0, 0, err
	}
	if len(chs) == 0 {
		chs = []db.GuiChannel{{ChanID: t.ChanID, LocalFeeRate: t.LocalFeeRate, ArInTarget: t.ArInTarget,
			Capacity: t.Capacity, LocalBalance: t.LocalBalance, PendingOutbound: t.PendOut}}
	}
	ids := make([]string, len(chs))
	for i, ch := range chs {
		ids[i] = ch.ChanID
	}
	flows, err := LoadNkFlows(ctx, q, ids, p.LockFlowPct, now)
	if err != nil {
		return 0, 0, err
	}
	in := NkGroupInput(chs, flows)
	return NkBudgetPpm(in.Fee, in.Flow, in.LocalPct, p), 100, nil
}

// TargetBudgetPpm is fee x max cost % of a refill target (TargetBudget), the
// rebalancer's budget before the source fee. On an error it returns
// fee x ar_max_cost together with the error.
func TargetBudgetPpm(ctx context.Context, q NkBudgetQuerier, ch db.GuiChannel, now time.Time) (int, error) {
	fee, cost, err := TargetBudget(ctx, q, TargetOf(ch), now)
	if err != nil {
		fee, cost = int(ch.LocalFeeRate), int(ch.ArMaxCost)
	}
	return int(float64(fee) * (float64(cost) / 100)), err
}
