package jobs

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	af "github.com/warioishere/lndg-blitz-go/internal/af"
	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

// Fee rules in shadow mode: the jobs compute what the rule would set and write it
// to shadow_fee_log; nothing is applied. The rules use the channel's rebalance
// cost as the FLP floor computes it (af.AvgRebalanceCostPPM over the last
// FLP-Lookback rebalances, source fee included).
const (
	shadowCostMargin = 0.10 // margin around the rebalance cost
	shadowIdleDays   = 3    // no outbound forward for this long counts as no sales
)

type shadowLogQuerier interface {
	InsertShadowFeeLog(ctx context.Context, arg db.InsertShadowFeeLogParams) error
}

// logShadow writes one shadow decision; a failed write is logged and never stops a fee job.
func logShadow(ctx context.Context, q shadowLogQuerier, p db.InsertShadowFeeLogParams) {
	if err := q.InsertShadowFeeLog(ctx, p); err != nil {
		dataLog(fmt.Sprintf("Error writing shadow fee log for %s: %s", p.ChanID, err))
	}
}

// idleDropRate is the outbound rate of the idle-drop rule: a channel at or above
// its outbound target that sold nothing for shadowIdleDays goes straight down to
// its rebalance cost plus margin instead of AF's small steps, and never below it.
// ok is false when the rule does not apply or AF's rate is already at or below.
func idleDropRate(row *af.ChannelFeeRow, now time.Time) (rate int, ok bool) {
	if row.AvgRebalanceCost == nil || row.OutPercent < 100-row.ArInTarget {
		return 0, false
	}
	if row.LastForwardOut != nil && now.Sub(*row.LastForwardOut) < shadowIdleDays*24*time.Hour {
		return 0, false
	}
	floor := int(math.Ceil(float64(*row.AvgRebalanceCost) * (1 + shadowCostMargin)))
	if int(row.NewRate) <= floor {
		return 0, false
	}
	return floor, true
}

func hoursSince(t *time.Time, now time.Time) pgtype.Float8 {
	if t == nil {
		return pgtype.Float8{}
	}
	return pgtype.Float8{Float64: now.Sub(*t).Hours(), Valid: true}
}
