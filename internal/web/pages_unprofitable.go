package web

import (
	"math"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// Age bonus heuristic constants.
const (
	ucVeryNewDays     = 7.0
	ucEstablishedDays = 30.0
	ucMaxAgeBonus     = 2.0
)

// ucTimeframe represents one entry in the timeframe selector.
type ucTimeframe struct {
	key  string
	days int
	name string
}

var ucTimeframes = []ucTimeframe{
	{"1", 1, "1 Day"},
	{"7", 7, "7 Days"},
	{"30", 30, "30 Days"},
	{"90", 90, "90 Days"},
}

// roundEven rounds x to n decimal places using banker's rounding.
func roundEven(x float64, n int) float64 {
	factor := math.Pow(10, float64(n))
	return math.RoundToEven(x*factor) / factor
}

// ucChan holds the base channel data needed for the unprofitable channels view.
type ucChan struct {
	chanID        string
	capacity      int64
	localBalance  int64
	remoteBalance int64
	localFeeRate  int64
	alias         string
	initiator     bool
}

// ucMetrics holds aggregate metrics per channel.
type ucMetrics struct {
	routedOut        int64
	feeRevenue       float64
	lastRouting      *time.Time
	lastRoutingAmt   int64
	rebalancedOut    float64
	lastRebalance    *time.Time
	lastRebalanceAmt float64
	rebalanceFeeCost float64
	assistedRevenue  float64
}

// handleUnprofitableChannels renders the unprofitable channels page. For each
// active, open channel it computes activity/profit metrics and a heuristic
// "Stuck Index". Aggregates run in Postgres; scoring uses Go float64 arithmetic
// (RoundToEven, int64 truncation). GetInfo errors result in block height 0 rather
// than 500; DB errors return 500.
func (s *Server) handleUnprofitableChannels(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// timeframe: absent defaults to '30', otherwise the raw query value.
	q := r.URL.Query()
	timeframe := "30"
	if q.Has("timeframe") {
		timeframe = q.Get("timeframe")
	}
	selected := ucTimeframes[2] // default 30 days
	for _, o := range ucTimeframes {
		if o.key == timeframe {
			selected = o
			break
		}
	}
	filterDate := time.Now().AddDate(0, 0, -selected.days)

	// Active, open channels.
	chanRows, err := s.queryMaps(ctx, `SELECT chan_id, capacity, local_balance, remote_balance, local_fee_rate, COALESCE(alias,'') AS alias, initiator FROM gui_channels WHERE is_active = true AND is_open = true`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	channels := make([]ucChan, 0, len(chanRows))
	metrics := make(map[string]*ucMetrics, len(chanRows))
	for _, row := range chanRows {
		cid, _ := row["chan_id"].(string)
		capacity, _ := toInt64(row["capacity"])
		lb, _ := toInt64(row["local_balance"])
		rb, _ := toInt64(row["remote_balance"])
		fr, _ := toInt64(row["local_fee_rate"])
		al, _ := row["alias"].(string)
		init, _ := row["initiator"].(bool)
		channels = append(channels, ucChan{
			chanID: cid, capacity: capacity, localBalance: lb, remoteBalance: rb,
			localFeeRate: fr, alias: al, initiator: init,
		})
		metrics[cid] = &ucMetrics{}
	}

	// runAgg executes an aggregate query (parameter $1 = filterDate) and calls
	// apply for each row whose "cid" exists in the metrics map.
	runAgg := func(sql string, apply func(*ucMetrics, map[string]any)) bool {
		rows, qerr := s.queryMaps(ctx, sql, filterDate)
		if qerr != nil {
			http.Error(w, qerr.Error(), http.StatusInternalServerError)
			return false
		}
		for _, row := range rows {
			cid, _ := row["cid"].(string)
			if m, ok := metrics[cid]; ok {
				apply(m, row)
			}
		}
		return true
	}

	// 1. Outbound forwards: routed_out_sats (int(amt_out_msat/1000) per row) + fee_revenue.
	if !runAgg(`SELECT chan_id_out AS cid, COALESCE(sum(amt_out_msat/1000),0)::bigint AS routed_out, COALESCE(sum(fee),0)::float8 AS fee_revenue FROM gui_forwards WHERE forward_date >= $1 GROUP BY chan_id_out`,
		func(m *ucMetrics, row map[string]any) {
			m.routedOut, _ = toInt64(row["routed_out"])
			m.feeRevenue, _ = toFloat64(row["fee_revenue"])
		}) {
		return
	}

	// last_routing (date + amount) = most recent outbound forward per channel.
	// Tie-break by id ASC keeps the first encountered (lowest id) when dates match.
	if !runAgg(`SELECT DISTINCT ON (chan_id_out) chan_id_out AS cid, forward_date, (amt_out_msat/1000)::bigint AS amt FROM gui_forwards WHERE forward_date >= $1 ORDER BY chan_id_out, forward_date DESC, id ASC`,
		func(m *ucMetrics, row map[string]any) {
			if t, ok := toTime(row["forward_date"]); ok {
				m.lastRouting = &t
			}
			m.lastRoutingAmt, _ = toInt64(row["amt"])
		}) {
		return
	}

	// 2. Inbound forwards: assisted_revenue (sum of fees where this channel was inbound).
	if !runAgg(`SELECT chan_id_in AS cid, COALESCE(sum(fee),0)::float8 AS assisted FROM gui_forwards WHERE forward_date >= $1 GROUP BY chan_id_in`,
		func(m *ucMetrics, row map[string]any) {
			m.assistedRevenue, _ = toFloat64(row["assisted"])
		}) {
		return
	}

	// rebalance_fee_cost (for profit): cost of rebalances into this channel.
	// Invoice (settled, in period) joined to payment (rebalance, status=2, in period).
	if !runAgg(`SELECT i.chan_in AS cid, COALESCE(sum(p.fee),0)::float8 AS cost
                FROM gui_invoices i JOIN gui_payments p ON p.payment_hash = i.r_hash
                WHERE i.state = 1 AND i.settle_date >= $1
                  AND p.status = 2 AND p.rebal_chan IS NOT NULL AND p.creation_date >= $1
                GROUP BY i.chan_in`,
		func(m *ucMetrics, row map[string]any) {
			m.rebalanceFeeCost, _ = toFloat64(row["cost"])
		}) {
		return
	}

	// 3. Rebalance OUT (display only): rebalanced_out_sats + last_rebalance.
	if !runAgg(`SELECT chan_out AS cid, COALESCE(sum(value),0)::float8 AS rebal_out FROM gui_payments WHERE creation_date >= $1 AND status = 2 AND rebal_chan IS NOT NULL GROUP BY chan_out`,
		func(m *ucMetrics, row map[string]any) {
			m.rebalancedOut, _ = toFloat64(row["rebal_out"])
		}) {
		return
	}
	if !runAgg(`SELECT DISTINCT ON (chan_out) chan_out AS cid, creation_date, value FROM gui_payments WHERE creation_date >= $1 AND status = 2 AND rebal_chan IS NOT NULL ORDER BY chan_out, creation_date DESC, payment_hash ASC`,
		func(m *ucMetrics, row map[string]any) {
			if t, ok := toTime(row["creation_date"]); ok {
				m.lastRebalance = &t
			}
			m.lastRebalanceAmt, _ = toFloat64(row["value"])
		}) {
		return
	}

	// Block height — GetInfo error results in 0 rather than 500.
	var blockHeight int64
	if info, gErr := s.lnd.Lightning.GetInfo(ctx, &lnrpc.GetInfoRequest{}); gErr == nil {
		blockHeight = int64(info.GetBlockHeight())
	}

	// Normalisation maxima across all channels (max_outbound is computed but not
	// used in scoring, so it is omitted).
	maxOutboundRatio, maxAssisted, maxFeeRate := 1.0, 1.0, 1000.0
	if len(channels) > 0 {
		maxOutboundRatio, maxAssisted, maxFeeRate = 0.0, 0.0, 0.0
		for _, ch := range channels {
			m := metrics[ch.chanID]
			totalOutbound := float64(m.routedOut) + m.rebalancedOut
			capacity := ch.capacity
			if capacity == 0 { // capacity or 1
				capacity = 1
			}
			ratio := totalOutbound / float64(capacity)
			if ratio > maxOutboundRatio {
				maxOutboundRatio = ratio
			}
			if m.assistedRevenue > maxAssisted {
				maxAssisted = m.assistedRevenue
			}
			if float64(ch.localFeeRate) > maxFeeRate {
				maxFeeRate = float64(ch.localFeeRate)
			}
		}
	}
	highRatioThreshold := math.Max(maxOutboundRatio*0.7, 0.5)
	mediumRatioThreshold := math.Max(maxOutboundRatio*0.3, 0.2)
	highAssistedThreshold := maxAssisted * 0.7
	mediumAssistedThreshold := maxAssisted * 0.3
	highFeeThreshold := math.Max(maxFeeRate, 1000) * 0.7
	mediumFeeThreshold := math.Max(maxFeeRate, 1000) * 0.3

	result := make([]map[string]any, 0, len(channels))
	for _, ch := range channels {
		m := metrics[ch.chanID]
		profit := m.feeRevenue - m.rebalanceFeeCost

		var lastRouting any
		if m.lastRouting != nil {
			lastRouting = map[string]any{"date": *m.lastRouting, "amount": m.lastRoutingAmt}
		}
		var lastRebalance any
		if m.lastRebalance != nil {
			lastRebalance = map[string]any{"date": *m.lastRebalance, "amount": m.lastRebalanceAmt}
		}

		localRatio := 0.0
		if ch.capacity > 0 {
			localRatio = float64(ch.localBalance) / float64(ch.capacity)
		}
		localRatioPct := roundEven(localRatio*100, 2)

		totalOutbound := float64(m.routedOut) + m.rebalancedOut
		assistedRevenue := m.assistedRevenue
		outboundRatio := 0.0
		if ch.capacity > 0 {
			outboundRatio = totalOutbound / float64(ch.capacity)
		}
		localFeeRate := float64(ch.localFeeRate)

		priorityScore := 7.0
		switch {
		case outboundRatio > highRatioThreshold && localFeeRate > highFeeThreshold:
			priorityScore = 1.0
		case outboundRatio > highRatioThreshold && assistedRevenue > highAssistedThreshold:
			priorityScore = 2.0
		case outboundRatio > mediumRatioThreshold && localFeeRate > mediumFeeThreshold:
			priorityScore = 3.0
		case outboundRatio > mediumRatioThreshold && assistedRevenue > mediumAssistedThreshold:
			priorityScore = 4.0
		case outboundRatio > 0 && localFeeRate > 0:
			priorityScore = 5.0
		case outboundRatio > 0 && assistedRevenue > 0:
			priorityScore = 6.0
		}

		switch {
		case assistedRevenue > highAssistedThreshold:
			priorityScore = math.Max(1.0, priorityScore-1.5)
		case assistedRevenue > mediumAssistedThreshold:
			priorityScore = math.Max(1.0, priorityScore-0.75)
		case assistedRevenue > 0:
			priorityScore = math.Max(1.0, priorityScore-0.25)
		}

		const lowLocalThreshold = 0.2
		const highLocalThreshold = 0.8
		if localRatio <= lowLocalThreshold {
			adj := 3.0 * (1 - (localRatio / lowLocalThreshold))
			priorityScore = math.Max(1.0, priorityScore-adj)
		} else if localRatio >= highLocalThreshold {
			adj := 0.5 * ((localRatio - highLocalThreshold) / (1 - highLocalThreshold))
			priorityScore = math.Min(7.0, priorityScore+adj)
		}

		ageBonus := 0.0
		ageDays := -1.0
		if blockHeight > 0 && ch.chanID != "" {
			if cid, perr := strconv.ParseInt(ch.chanID, 10, 64); perr == nil {
				openingBlock := cid >> 40
				if openingBlock > 0 {
					ageBlocks := blockHeight - openingBlock
					if ageBlocks < 0 {
						ageBlocks = 0
					}
					ageDays = float64(ageBlocks) / 144.0
					if ageDays < ucVeryNewDays {
						ageBonus = ucMaxAgeBonus
					} else if ageDays < ucEstablishedDays {
						ageProgress := (ageDays - ucVeryNewDays) / (ucEstablishedDays - ucVeryNewDays)
						ageBonus = ucMaxAgeBonus * (1 - ageProgress)
					}
				}
			}
		}
		priorityScore = math.Max(1.0, priorityScore-ageBonus)
		smartStuckIndex := roundEven(priorityScore/7.0, 2)

		var ageDaysVal any = "N/A"
		if ageDays >= 0 {
			ageDaysVal = int64(math.RoundToEven(ageDays))
		}

		alias := ch.alias
		if alias == "" { // channel.alias or "---"
			alias = "---"
		}

		result = append(result, map[string]any{
			"chan_id":             ch.chanID,
			"alias":               alias,
			"routed_out_sats":     m.routedOut,
			"last_routing":        lastRouting,
			"rebalanced_out_sats": m.rebalancedOut,
			"last_rebalance":      lastRebalance,
			"profit":              profit,
			"assisted_revenue":    assistedRevenue,
			"initiator":           ch.initiator,
			"capacity_millions":   roundEven(float64(ch.capacity)/1000000, 1),
			"local_ratio":         localRatioPct,
			"stuck_index":         smartStuckIndex,
			"age_days":            ageDaysVal,
		})
	}

	// Sort by profit ascending (stable).
	sort.SliceStable(result, func(i, j int) bool {
		return result[i]["profit"].(float64) < result[j]["profit"].(float64)
	})

	tfOpts := make([]map[string]any, len(ucTimeframes))
	for i, o := range ucTimeframes {
		tfOpts[i] = map[string]any{"key": o.key, "name": o.name}
	}

	s.renderTemplate(w, r, "unprofitable_channels.html", map[string]any{
		"channels":          result,
		"timeframe":         timeframe,
		"timeframe_name":    selected.name,
		"timeframe_options": tfOpts,
	})
}
