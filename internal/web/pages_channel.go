package web

import (
	"html/template"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/warioishere/lndg-blitz-go/internal/af"
	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/pyround"
)

// chanWinSuffix maps the 4 time windows to their context-key suffixes
// (order: lifetime, 30d, 7d, 1d).
var chanWinSuffix = [4]string{"", "_30day", "_7day", "_1day"}

// handleChannelDetail renders the channel detail page. It computes lifetime/30d/7d/1d
// aggregates from Forwards, Payments, Invoices, Rebalancer and FailedHTLCs using SQL
// FILTER aggregates and Go float64 arithmetic (int64 truncation, RoundToEven).
// af.Main supplies new_rate/adjustment/net_routed_7day. The chan_id is extracted
// from "?=<chan_id>" in the URL. DB errors return 500; missing channel renders
// channel.html with channel=nil.
func (s *Server) handleChannelDetail(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	chanID := singleQueryParam(r)

	chRows, err := s.queryMaps(ctx, `SELECT * FROM gui_channels WHERE chan_id = $1`, chanID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(chRows) == 0 {
		// Channel not found — render with channel=nil.
		s.renderTemplate(w, r, "channel.html", map[string]any{
			"chan_id": chanID, "channel": nil, "peer_info": map[string]any{},
			"autofees": []any{}, "incoming_htlcs": []any{}, "outgoing_htlcs": []any{},
		})
		return
	}
	ch := chRows[0]

	now := time.Now().UTC() // UTC: N days = N*24h like timedelta
	c30 := now.AddDate(0, 0, -30)
	c7 := now.AddDate(0, 0, -7)
	c1 := now.AddDate(0, 0, -1)

	fail := func(e error) bool {
		if e != nil {
			http.Error(w, e.Error(), http.StatusInternalServerError)
			return true
		}
		return false
	}
	// one executes a single-row aggregate query and returns the row.
	one := func(sql string, args ...any) (map[string]any, error) {
		rows, e := s.queryMaps(ctx, sql, args...)
		if e != nil {
			return nil, e
		}
		if len(rows) == 0 {
			return map[string]any{}, nil
		}
		return rows[0], nil
	}
	win4i := func(m map[string]any, p string) [4]int64 {
		var out [4]int64
		for i := 0; i < 4; i++ {
			out[i], _ = toInt64(m[p+strconv.Itoa(i)])
		}
		return out
	}
	win4f := func(m map[string]any, p string) [4]float64 {
		var out [4]float64
		for i := 0; i < 4; i++ {
			out[i], _ = toFloat64(m[p+strconv.Itoa(i)])
		}
		return out
	}

	// Float columns are summed exactly (::numeric), as pandas does; see channels.
	// --- Forwards (routing) per time window, split by out/in. amt = sum(amt_out_msat). ---
	fwdSQL := func(col string) string {
		return `SELECT
            count(*) AS c0, count(*) FILTER (WHERE forward_date>=$2) AS c1, count(*) FILTER (WHERE forward_date>=$3) AS c2, count(*) FILTER (WHERE forward_date>=$4) AS c3,
            COALESCE(sum(amt_out_msat),0)::bigint AS a0, COALESCE(sum(amt_out_msat) FILTER (WHERE forward_date>=$2),0)::bigint AS a1, COALESCE(sum(amt_out_msat) FILTER (WHERE forward_date>=$3),0)::bigint AS a2, COALESCE(sum(amt_out_msat) FILTER (WHERE forward_date>=$4),0)::bigint AS a3,
            COALESCE(sum(fee::numeric),0)::float8 AS f0, COALESCE(sum(fee::numeric) FILTER (WHERE forward_date>=$2),0)::float8 AS f1, COALESCE(sum(fee::numeric) FILTER (WHERE forward_date>=$3),0)::float8 AS f2, COALESCE(sum(fee::numeric) FILTER (WHERE forward_date>=$4),0)::float8 AS f3,
            COALESCE(sum(amt_out_msat) FILTER (WHERE forward_date>=$3 AND amt_out_msat>=1000000),0)::bigint AS a7f,
            COALESCE(sum(fee::numeric) FILTER (WHERE forward_date>=$3 AND amt_out_msat>=1000000),0)::float8 AS f7f
            FROM gui_forwards WHERE ` + col + ` = $1`
	}
	outRow, err := one(fwdSQL("chan_id_out"), chanID, c30, c7, c1)
	if fail(err) {
		return
	}
	inRow, err := one(fwdSQL("chan_id_in"), chanID, c30, c7, c1)
	if fail(err) {
		return
	}

	routedOut := win4i(outRow, "c")
	amtOutMsat := win4f(outRow, "a")
	feeOut := win4f(outRow, "f")
	routedIn := win4i(inRow, "c")
	amtInMsat := win4f(inRow, "a")
	feeIn := win4f(inRow, "f")
	amtOut7fMsat, _ := toFloat64(outRow["a7f"])
	fee7fOut, _ := toFloat64(outRow["f7f"])
	amtIn7fMsat, _ := toFloat64(inRow["a7f"])
	fee7fIn, _ := toFloat64(inRow["f7f"])

	var amtRoutedOut, amtRoutedIn, averageOut, averageIn, revenue, revenueAssist [4]int64
	for i := 0; i < 4; i++ {
		amtRoutedOut[i] = int64(amtOutMsat[i] / 1000) // int(sum/1000)
		amtRoutedIn[i] = int64(amtInMsat[i] / 1000)
		revenue[i] = int64(feeOut[i])
		revenueAssist[i] = int64(feeIn[i])
		if routedOut[i] != 0 {
			averageOut[i] = int64(float64(amtRoutedOut[i]) / float64(routedOut[i]))
		}
		if routedIn[i] != 0 {
			averageIn[i] = int64(float64(amtRoutedIn[i]) / float64(routedIn[i]))
		}
	}
	amtRoutedOut7fees := int64(amtOut7fMsat / 1000)
	revenue7fees := int64(fee7fOut)
	amtRoutedIn7fees := int64(amtIn7fMsat / 1000)
	revenueAssist7fees := int64(fee7fIn)

	// --- Rebalance OUT (Payments chan_out=chan, status2, rebal) ---
	payRow, err := one(`SELECT
            count(*) AS c0, count(*) FILTER (WHERE creation_date>=$2) AS c1, count(*) FILTER (WHERE creation_date>=$3) AS c2, count(*) FILTER (WHERE creation_date>=$4) AS c3,
            COALESCE(sum(value::numeric),0)::float8 AS v0, COALESCE(sum(value::numeric) FILTER (WHERE creation_date>=$2),0)::float8 AS v1, COALESCE(sum(value::numeric) FILTER (WHERE creation_date>=$3),0)::float8 AS v2, COALESCE(sum(value::numeric) FILTER (WHERE creation_date>=$4),0)::float8 AS v3
            FROM gui_payments WHERE status=2 AND chan_out=$1 AND rebal_chan IS NOT NULL`, chanID, c30, c7, c1)
	if fail(err) {
		return
	}
	rebalOut := win4i(payRow, "c")
	rebalOutVal := win4f(payRow, "v")
	var amtRebalOut [4]int64
	for i := 0; i < 4; i++ {
		amtRebalOut[i] = int64(rebalOutVal[i])
	}

	// --- Rebalance IN (Invoices chan_in=chan, state1, r_hash linked to rebalance payment) ---
	invRow, err := one(`SELECT
            count(*) AS c0, count(*) FILTER (WHERE settle_date>=$2) AS c1, count(*) FILTER (WHERE settle_date>=$3) AS c2, count(*) FILTER (WHERE settle_date>=$4) AS c3,
            COALESCE(sum(amt_paid),0)::bigint AS a0, COALESCE(sum(amt_paid) FILTER (WHERE settle_date>=$2),0)::bigint AS a1, COALESCE(sum(amt_paid) FILTER (WHERE settle_date>=$3),0)::bigint AS a2, COALESCE(sum(amt_paid) FILTER (WHERE settle_date>=$4),0)::bigint AS a3
            FROM gui_invoices WHERE state=1 AND chan_in=$1 AND r_hash IN (SELECT payment_hash FROM gui_payments WHERE status=2 AND rebal_chan=$1)`, chanID, c30, c7, c1)
	if fail(err) {
		return
	}
	rebalIn := win4i(invRow, "c")
	rebalInAmt := win4i(invRow, "a")
	var amtRebalIn [4]int64
	for i := 0; i < 4; i++ {
		amtRebalIn[i] = rebalInAmt[i]
	}

	// --- Costs (Invoice value>=1000 JOIN Payment value>=1000 rebal_chan=chan) ---
	costRow, err := one(`SELECT
            COALESCE(sum(p.fee::numeric),0)::float8 AS c0,
            COALESCE(sum(p.fee::numeric) FILTER (WHERE i.settle_date>=$2),0)::float8 AS c1,
            COALESCE(sum(p.fee::numeric) FILTER (WHERE i.settle_date>=$3),0)::float8 AS c2,
            COALESCE(sum(p.fee::numeric) FILTER (WHERE i.settle_date>=$4),0)::float8 AS c3
            FROM gui_invoices i JOIN gui_payments p ON p.payment_hash = i.r_hash
            WHERE i.state=1 AND i.chan_in=$1 AND i.value>=1000 AND p.status=2 AND p.rebal_chan=$1 AND p.value>=1000`, chanID, c30, c7, c1)
	if fail(err) {
		return
	}
	costFee := win4f(costRow, "c")
	var costs [4]int64
	for i := 0; i < 4; i++ {
		if rebalIn[i] != 0 {
			costs[i] = int64(costFee[i])
		}
	}

	// Closure costs are only added to the lifetime window.
	fundingTxid, _ := ch["funding_txid"].(string)
	outputIndex, _ := toInt64(ch["output_index"])
	closeRow, err := one(`SELECT COALESCE(closing_costs,0)::bigint AS cc FROM gui_closures WHERE funding_txid=$1 AND funding_index=$2 LIMIT 1`, fundingTxid, outputIndex)
	if fail(err) {
		return
	}
	if cc, ok := toInt64(closeRow["cc"]); ok {
		costs[0] += cc
	}

	var profits, profitsVol [4]int64
	for i := 0; i < 4; i++ {
		profits[i] = revenue[i] - costs[i]
		if amtRoutedOut[i] != 0 {
			profitsVol[i] = int64(float64(profits[i]) / (float64(amtRoutedOut[i]) / 1000000))
		}
	}

	// --- Rebalancer attempts per time window (keyed by stop date) ---
	remotePubkey, _ := ch["remote_pubkey"].(string)
	rebRow, err := one(`SELECT
            count(*) FILTER (WHERE status>=2 AND status<400) AS att0,
            count(*) FILTER (WHERE status>=2 AND status<400 AND stop>=$2) AS att1,
            count(*) FILTER (WHERE status>=2 AND status<400 AND stop>=$3) AS att2,
            count(*) FILTER (WHERE status>=2 AND status<400 AND stop>=$4) AS att3,
            count(*) FILTER (WHERE status=2) AS suc0,
            count(*) FILTER (WHERE status=2 AND stop>=$2) AS suc1,
            count(*) FILTER (WHERE status=2 AND stop>=$3) AS suc2,
            count(*) FILTER (WHERE status=2 AND stop>=$4) AS suc3
            FROM gui_rebalancer WHERE last_hop_pubkey=$1`, remotePubkey, c30, c7, c1)
	if fail(err) {
		return
	}
	attempts := win4i(rebRow, "att")
	success := win4i(rebRow, "suc")
	var successRate [4]int64
	for i := 0; i < 4; i++ {
		if attempts[i] != 0 {
			successRate[i] = int64((float64(success[i]) / float64(attempts[i])) * 100)
		}
	}

	// --- Failed HTLCs due to insufficient outbound liquidity per time window ---
	fhRow, err := one(`SELECT
            count(*) FILTER (WHERE chan_id_out=$1 AND wire_failure=15 AND failure_detail=6 AND amount > chan_out_liq + chan_out_pending) AS f0,
            count(*) FILTER (WHERE chan_id_out=$1 AND wire_failure=15 AND failure_detail=6 AND amount > chan_out_liq + chan_out_pending AND timestamp>=$2) AS f1,
            count(*) FILTER (WHERE chan_id_out=$1 AND wire_failure=15 AND failure_detail=6 AND amount > chan_out_liq + chan_out_pending AND timestamp>=$3) AS f2,
            count(*) FILTER (WHERE chan_id_out=$1 AND wire_failure=15 AND failure_detail=6 AND amount > chan_out_liq + chan_out_pending AND timestamp>=$4) AS f3
            FROM gui_failedhtlcs WHERE wire_failure<>99 AND (chan_id_in=$1 OR chan_id_out=$1)`, chanID, c30, c7, c1)
	if fail(err) {
		return
	}
	failedOut := win4i(fhRow, "f")

	// start_date = earliest OUT forward time (used for lifetime APY).
	sdRow, err := one(`SELECT min(forward_date) AS sd FROM gui_forwards WHERE chan_id_out=$1`, chanID)
	if fail(err) {
		return
	}
	startDate, hasStart := toTime(sdRow["sd"])

	// Node-wide totals (raw local_balance/capacity across open channels).
	nodeRow, err := one(`SELECT COALESCE(sum(local_balance),0)::bigint AS nout, COALESCE(sum(capacity),0)::bigint AS ncap FROM gui_channels WHERE is_open=true`)
	if fail(err) {
		return
	}
	nodeOut, _ := toInt64(nodeRow["nout"])
	nodeCap, _ := toInt64(nodeRow["ncap"])

	// Channel base values.
	capacity, _ := toInt64(ch["capacity"])
	localBalance, _ := toInt64(ch["local_balance"])
	localBalance += i64(ch["pending_outbound"])
	remoteBalance, _ := toInt64(ch["remote_balance"])
	remoteBalance += i64(ch["pending_inbound"])
	localFeeRate := i64(ch["local_fee_rate"])
	remoteFeeRate := i64(ch["remote_fee_rate"])
	arInTarget := i64(ch["ar_in_target"])
	arMaxCost := i64(ch["ar_max_cost"])
	arAmtTarget := i64(ch["ar_amt_target"])

	outPercent := int64(math.RoundToEven(float64(localBalance) / float64(capacity) * 100))
	inPercent := int64(math.RoundToEven(float64(remoteBalance) / float64(capacity) * 100))
	openBlock := int64(0)
	if cid, perr := strconv.ParseInt(chanID, 10, 64); perr == nil {
		openBlock = cid >> 40
	}

	// 7-day metrics and fee logic.
	var outRate, rebalPpm int64
	if amtRoutedOut7fees > 0 {
		outRate = int64((float64(revenue7fees) / float64(amtRoutedOut7fees)) * 1000000)
	}
	if amtRebalIn[2] > 0 {
		rebalPpm = int64((float64(costs[2]) / float64(amtRebalIn[2])) * 1000000)
	}
	var assistedRatio any
	if revenue7fees == 0 {
		assistedRatio = int64(revenueAssist7fees) // round(int,2) -> int
	} else {
		assistedRatio = pyround.NumPy(float64(revenueAssist7fees)/float64(revenue7fees), 2)
	}

	var feeRatio int64 = 100
	if localFeeRate != 0 {
		feeRatio = int64(math.RoundToEven((float64(remoteFeeRate) / float64(localFeeRate)) * 1000 / 10))
	}
	inboundCan := ((float64(remoteBalance) * 100) / float64(capacity)) / float64(arInTarget)
	var feeCheck int64 = 1
	if arMaxCost != 0 {
		feeCheck = int64(math.RoundToEven((float64(feeRatio) / float64(arMaxCost)) * 1000 / 10))
	}
	var steps int64
	if inboundCan >= 1 {
		steps = int64(((float64(inPercent) - float64(arInTarget)) / ((float64(arAmtTarget) / float64(capacity)) * 100)) + 0.999)
	}

	// APY / iAPY / CV (all default to 0.0).
	var apy, assistedApy, cv [4]float64
	if nodeCap > 0 {
		outboundRatio := float64(nodeOut) / float64(nodeCap)
		capf := float64(capacity)
		if hasStart {
			daysRouting := float64(int64(now.Sub(startDate)/time.Second)) / 86400.0
			apy[0] = pyround.NumPy(((float64(profits[0])/daysRouting)*36500)/(capf*outboundRatio), 2)
			assistedApy[0] = pyround.NumPy(((float64(revenueAssist[0])/daysRouting)*36500)/(capf*(1-outboundRatio)), 2)
			cv[0] = pyround.NumPy(((float64(revenue[0])/daysRouting)*36500)/(capf*outboundRatio)+assistedApy[0], 2)
		}
		factor := [4]float64{0, 1216.6667, 5214.2857, 36500} // index 1=30d,2=7d,3=1d
		for i := 1; i < 4; i++ {
			apy[i] = pyround.NumPy((float64(profits[i])*factor[i])/(capf*outboundRatio), 2)
			assistedApy[i] = pyround.NumPy((float64(revenueAssist[i])*factor[i])/(capf*(1-outboundRatio)), 2)
			cv[i] = pyround.NumPy((float64(revenue[i])*factor[i])/(capf*outboundRatio)+assistedApy[i], 2)
		}
	}

	// af.Main provides new_rate/adjustment/net_routed_7day.
	var newRate, adjustment, netRouted7day float64
	var eligible bool
	if guiCh, gerr := s.queries.GetChannel(ctx, chanID); gerr == nil {
		if rows, aerr := af.Main(ctx, s.queries, []db.GuiChannel{guiCh}, time.Now()); aerr == nil {
			for _, rr := range rows {
				if rr.ChanID == chanID {
					newRate = rr.NewRate
					adjustment = rr.Adjustment
					netRouted7day = rr.NetRouted7day
					eligible = rr.Eligible
				}
			}
		}
	}

	// Build the channel map from model columns plus computed fields.
	channel := ch
	channel["local_balance"] = localBalance
	channel["remote_balance"] = remoteBalance
	channel["out_percent"] = outPercent
	channel["in_percent"] = inPercent
	channel["open_block"] = openBlock
	channel["out_rate"] = outRate
	channel["rebal_ppm"] = rebalPpm
	channel["assisted_ratio"] = assistedRatio
	channel["fee_ratio"] = feeRatio
	channel["inbound_can"] = inboundCan
	channel["fee_check"] = feeCheck
	channel["steps"] = steps
	channel["new_rate"] = newRate
	channel["adjustment"] = adjustment
	channel["net_routed_7day"] = netRouted7day
	channel["eligible"] = eligible // derived from af result
	// Carry the _fees columns (used by the template).
	channel["amt_routed_out_7day_fees"] = amtRoutedOut7fees
	channel["revenue_7day_fees"] = revenue7fees
	channel["amt_routed_in_7day_fees"] = amtRoutedIn7fees
	channel["revenue_assist_7day_fees"] = revenueAssist7fees
	setWin := func(name string, v [4]int64) {
		for i, suf := range chanWinSuffix {
			channel[name+suf] = v[i]
		}
	}
	setWinF := func(name string, v [4]float64) {
		for i, suf := range chanWinSuffix {
			channel[name+suf] = v[i]
		}
	}
	setWin("routed_out", routedOut)
	setWin("amt_routed_out", amtRoutedOut)
	setWin("average_out", averageOut)
	setWin("revenue", revenue)
	setWin("routed_in", routedIn)
	setWin("amt_routed_in", amtRoutedIn)
	setWin("average_in", averageIn)
	setWin("revenue_assist", revenueAssist)
	setWin("rebal_out", rebalOut)
	setWin("amt_rebal_out", amtRebalOut)
	setWin("rebal_in", rebalIn)
	setWin("amt_rebal_in", amtRebalIn)
	setWin("costs", costs)
	setWin("profits", profits)
	setWin("profits_vol", profitsVol)
	setWin("attempts", attempts)
	setWin("success", success)
	setWin("success_rate", successRate)
	setWin("failed_out", failedOut)
	setWinF("apy", apy)
	setWinF("assisted_apy", assistedApy)
	setWinF("cv", cv)

	// peer_info, autofees, HTLCs.
	peerInfo := map[string]any{}
	if rows, e := s.queryMaps(ctx, `SELECT * FROM gui_peers WHERE pubkey=$1 LIMIT 1`, remotePubkey); e == nil && len(rows) > 0 {
		peerInfo = rows[0]
	} else if e != nil {
		http.Error(w, e.Error(), http.StatusInternalServerError)
		return
	}

	autofees, err := s.queryMaps(ctx, `SELECT * FROM gui_autofees WHERE chan_id=$1 AND timestamp>=$2 ORDER BY id DESC`, chanID, c30)
	if fail(err) {
		return
	}
	for _, log := range autofees {
		oldV := i64(log["old_value"])
		newV := i64(log["new_value"])
		log["old_value"] = oldV
		log["new_value"] = newV
		if oldV == 0 {
			log["change"] = int64(0)
		} else {
			log["change"] = pyround.Round(float64(newV-oldV)*100/float64(oldV), 1)
		}
	}

	incoming, err := s.queryMaps(ctx, `SELECT * FROM gui_pendinghtlcs WHERE chan_id=$1 AND incoming=true ORDER BY hash_lock`, chanID)
	if fail(err) {
		return
	}
	outgoing, err := s.queryMaps(ctx, `SELECT * FROM gui_pendinghtlcs WHERE chan_id=$1 AND incoming=false ORDER BY hash_lock`, chanID)
	if fail(err) {
		return
	}

	// Note: the target:"_blank" attribute (colon instead of equals) is intentional to preserve
	// the existing behaviour; autoescape is disabled via template.HTML.
	rebalURL := template.HTML(`<a href="/rebalances?last_hop_pubkey=` + remotePubkey + `" target:"_blank"> Rebalances</a>`)

	s.renderTemplate(w, r, "channel.html", map[string]any{
		"chan_id":        chanID,
		"channel":        channel,
		"peer_info":      peerInfo,
		"autofees":       autofees,
		"incoming_htlcs": incoming,
		"outgoing_htlcs": outgoing,
		"rebal_url":      rebalURL,
	})
}

// i64 is a convenience wrapper around toInt64 that discards the ok return value
// (missing/NULL values become 0).
func i64(v any) int64 {
	n, _ := toInt64(v)
	return n
}
