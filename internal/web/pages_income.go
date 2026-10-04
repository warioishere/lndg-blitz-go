package web

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// windowSuffix maps windows w0..w4 to their context-key suffixes
// (order: lifetime, 90/30/7/1 days).
var windowSuffix = [5]string{"", "_90day", "_30day", "_7day", "_1day"}

// agg5 runs a 5-window aggregation: aggExpr (e.g. "sum(fee)" or "count(*)")
// lifetime plus FILTER clauses for each cutoff. cast (e.g. "::bigint"/"::float8")
// is applied around COALESCE. Returns [lifetime, 90d, 30d, 7d, 1d] as float64.
func (s *Server) agg5(ctx context.Context, table, dateCol, aggExpr, cast, baseWhere string, cutoffs [4]time.Time) ([5]float64, error) {
	col := func(filter string) string {
		e := aggExpr
		if filter != "" {
			e = aggExpr + " FILTER (WHERE " + dateCol + " >= " + filter + ")"
		}
		return "COALESCE(" + e + ",0)" + cast
	}
	where := ""
	if baseWhere != "" {
		where = " WHERE " + baseWhere
	}
	sql := fmt.Sprintf(`SELECT %s AS w0, %s AS w1, %s AS w2, %s AS w3, %s AS w4 FROM %s%s`,
		col(""), col("$1"), col("$2"), col("$3"), col("$4"), table, where)
	rows, err := s.queryMaps(ctx, sql, cutoffs[0], cutoffs[1], cutoffs[2], cutoffs[3])
	var out [5]float64
	if err != nil {
		return out, err
	}
	m := rows[0]
	for i, k := range []string{"w0", "w1", "w2", "w3", "w4"} {
		out[i], _ = toFloat64(m[k])
	}
	return out, nil
}

// handleIncome renders the P&L page with aggregations over 5 windows
// (lifetime/90/30/7/1d) from Forwards, Invoices, Payments, Onchain and Closures.
// RPC errors return 500.
func (s *Server) handleIncome(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	info, err := s.lnd.Lightning.GetInfo(ctx, &lnrpc.GetInfoRequest{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	bh := int64(info.GetBlockHeight())
	now := time.Now().UTC() // UTC: N days = N*24h like timedelta
	cut := [4]time.Time{now.AddDate(0, 0, -90), now.AddDate(0, 0, -30), now.AddDate(0, 0, -7), now.AddDate(0, 0, -1)}

	fail := func(e error) bool {
		if e != nil {
			http.Error(w, e.Error(), http.StatusInternalServerError)
			return true
		}
		return false
	}

	fwdCount, e1 := s.agg5(ctx, "gui_forwards", "forward_date", "count(*)", "", "", cut)
	fwdAmtMsat, e2 := s.agg5(ctx, "gui_forwards", "forward_date", "sum(amt_out_msat)", "::bigint", "", cut)
	fwdFee, e3 := s.agg5(ctx, "gui_forwards", "forward_date", "sum(fee)", "::float8", "", cut)
	invRecv, e4 := s.agg5(ctx, "gui_invoices", "settle_date", "sum(amt_paid)", "::bigint", "state = 1 AND is_revenue = true", cut)
	paySent, e5 := s.agg5(ctx, "gui_payments", "creation_date", "sum(value)", "::float8", "status = 2", cut)
	payFee, e6 := s.agg5(ctx, "gui_payments", "creation_date", "sum(fee)", "::float8", "status = 2", cut)
	onchFee, e7 := s.agg5(ctx, "gui_onchain", "time_stamp", "sum(fee)", "::bigint", "", cut)
	if fail(e1) || fail(e2) || fail(e3) || fail(e4) || fail(e5) || fail(e6) || fail(e7) {
		return
	}

	// Closures: windowed by close_height >= block_height - N (144 blocks/day).
	closeRows, err := s.queryMaps(ctx,
		`SELECT COALESCE(sum(closing_costs),0)::bigint AS w0,
                COALESCE(sum(closing_costs) FILTER (WHERE close_height >= $1),0)::bigint AS w1,
                COALESCE(sum(closing_costs) FILTER (WHERE close_height >= $2),0)::bigint AS w2,
                COALESCE(sum(closing_costs) FILTER (WHERE close_height >= $3),0)::bigint AS w3,
                COALESCE(sum(closing_costs) FILTER (WHERE close_height >= $4),0)::bigint AS w4
            FROM gui_closures`, bh-12960, bh-4320, bh-1008, bh-144)
	if fail(err) {
		return
	}
	var closeFee [5]float64
	for i, k := range []string{"w0", "w1", "w2", "w3", "w4"} {
		closeFee[i], _ = toFloat64(closeRows[0][k])
	}

	var forwardCount, forwardAmount, totalRevenue, totalRevenuePpm [5]int64
	var onchainCosts, totalFees, totalFeesPpm, percentCost, profits, profitsPpm [5]int64
	for i := 0; i < 5; i++ {
		forwardCount[i] = int64(fwdCount[i])
		forwardAmount[i] = int64(fwdAmtMsat[i] / 1000.0) // int(sum/1000)
		totalRevenue[i] = int64(fwdFee[i]) + int64(invRecv[i])
		totalFees[i] = int64(payFee[i])
		totalSent := int64(paySent[i])
		onchainCosts[i] = int64(onchFee[i]) + int64(closeFee[i])
		profits[i] = totalRevenue[i] - totalFees[i] - onchainCosts[i]
		if forwardAmount[i] != 0 {
			totalRevenuePpm[i] = int64(float64(totalRevenue[i]) / (float64(forwardAmount[i]) / 1000000))
			profitsPpm[i] = int64(float64(profits[i]) / (float64(forwardAmount[i]) / 1000000))
		}
		if totalSent != 0 {
			totalFeesPpm[i] = int64(float64(totalFees[i]) / (float64(totalSent) / 1000000))
		}
		if totalRevenue[i] != 0 {
			percentCost[i] = int64(((float64(totalFees[i]) + float64(onchainCosts[i])) / float64(totalRevenue[i])) * 100)
		}
	}

	c := map[string]any{
		"node_info": map[string]any{
			"identity_pubkey": info.GetIdentityPubkey(),
			"alias":           info.GetAlias(),
		},
	}
	set := func(name string, vals [5]int64) {
		for i, suf := range windowSuffix {
			c[name+suf] = vals[i]
		}
	}
	set("forward_count", forwardCount)
	set("forward_amount", forwardAmount)
	set("total_revenue", totalRevenue)
	set("total_revenue_ppm", totalRevenuePpm)
	set("onchain_costs", onchainCosts)
	set("total_fees", totalFees)
	set("total_fees_ppm", totalFeesPpm)
	set("percent_cost", percentCost)
	set("profits", profits)
	set("profits_ppm", profitsPpm)

	s.renderTemplate(w, r, "income.html", c)
}
