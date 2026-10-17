package jobs

import (
	"context"
	"encoding/hex"
	"strconv"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/rebalancer"
)

// ViaAttempt is the outcome of probing one source channel in ProbeVia.
// HopKeys is the forced route (via -> target -> self at the end); AmountSat and
// FeePpm (route fees, source fee not included) are set for a verified route.
type ViaAttempt struct {
	SourceChanID  string
	SourceFeeRate int
	HopKeys       [][]byte
	Verified      bool
	AmountSat     int64
	FeePpm        *float64
}

// probeFeeLimitSat is the fee budget for refilling target from a source channel
// with the given fee: the target's budget (af.TargetBudgetPpm) minus the source
// fee, capped at maxFeeRate, for ar_amt_target sats. It is the budget the
// rebalancer's opportunity-cost check allows, so a route probed above it is
// never paid. It is 0 when the source fee leaves no budget.
func probeFeeLimitSat(target db.GuiChannel, targetBudgetPpm, sourceFeeRate, maxFeeRate int) int64 {
	inner := targetBudgetPpm - sourceFeeRate
	budgetPpm := maxFeeRate
	if inner < budgetPpm {
		budgetPpm = inner
	}
	if budgetPpm <= 0 {
		return 0
	}
	return int64(float64(int64(budgetPpm)*target.ArAmtTarget) / 1000000)
}

// ProbeVia probes routes self -> ... -> via -> target -> self, so a refill of
// target runs over via's channel to target; targetBudgetPpm is the target's
// budget (af.TargetBudgetPpm). The leg to via comes from
// QueryRoutes (target ignored, so it appears only as the second-to-last hop).
// Sources come from probeSources over channels (the rebalancer's channel set),
// cheapest fee first, up to maxPerTarget of them that have a route to via;
// probing stops at the first verified route. No route is stored; each probe
// HTLC goes to route_attempt_log through log (nil: not recorded).
func ProbeVia(ctx context.Context, stub ProbeLightningClient, routerstub ProbeRouterClient, selfPubkey string, target db.GuiChannel, targetBudgetPpm int, viaPubkey string, channels []db.GuiChannel, maxFeeRate, maxPerTarget int, minAmountSat int64, log rebalancer.AttemptLogQuerier) ([]ViaAttempt, error) {
	selfKey, err := hex.DecodeString(selfPubkey)
	if err != nil {
		return nil, err
	}
	targetKey, err := hex.DecodeString(target.RemotePubkey)
	if err != nil {
		return nil, err
	}
	sources := probeSources(channels, target, targetBudgetPpm, maxFeeRate)

	var attempts []ViaAttempt
	for _, src := range sources {
		if len(attempts) >= maxPerTarget {
			break
		}
		srcID, perr := strconv.ParseUint(src.chanID, 10, 64)
		if perr != nil {
			return nil, perr
		}
		resp, qerr := stub.QueryRoutes(ctx, &lnrpc.QueryRoutesRequest{
			PubKey:            viaPubkey,
			Amt:               target.ArAmtTarget,
			OutgoingChanIds:   []uint64{srcID},
			IgnoredNodes:      [][]byte{targetKey},
			FeeLimit:          &lnrpc.FeeLimit{Limit: &lnrpc.FeeLimit_Fixed{Fixed: src.feeLimitSat}},
			UseMissionControl: true,
		})
		if qerr != nil || len(resp.GetRoutes()) == 0 || len(resp.GetRoutes()[0].GetHops()) == 0 {
			continue
		}
		var hopKeys [][]byte
		for _, h := range resp.GetRoutes()[0].GetHops() {
			b, e := hex.DecodeString(h.GetPubKey())
			if e != nil {
				return nil, e
			}
			hopKeys = append(hopKeys, b)
		}
		hopKeys = append(hopKeys, targetKey, selfKey)

		a := ViaAttempt{SourceChanID: src.chanID, SourceFeeRate: src.feeRate, HopKeys: hopKeys}
		found := probeWithBinarySearch(ctx, routerstub, src.chanID, hopKeys, rebalancer.FinalCltvDelta, target.ArAmtTarget, src.feeLimitSat, minAmountSat,
			attemptRecorder(ctx, log, target.RemotePubkey, src.chanID))
		if found.routeHex != nil && found.amountSat > 0 {
			a.Verified, a.AmountSat, a.FeePpm = true, found.amountSat, found.feePpm
		}
		attempts = append(attempts, a)
		if a.Verified {
			break
		}
	}
	return attempts, nil
}
