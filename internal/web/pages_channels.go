package web

import (
	"math"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/warioishere/lndg-blitz-go/internal/pyround"
)

// Aggregate row types for the channels page.
type chFwdAgg struct {
	cnt7, cnt30   int64
	amt7, amt30   int64   // sum(amt_out_msat) — also used for inbound
	fee7, fee30   float64 // sum(fee)
	ifee7, ifee30 float64 // sum(inbound_fee)
}
type chPayAgg struct {
	cnt7, cnt30 int64
	val7, val30 float64 // sum(value)
}
type chInvAgg struct {
	cnt7, cnt30   int64
	amt7, amt30   int64   // sum(amt_paid)
	cost7, cost30 float64 // sum(associated payment.fee) — rebalance costs
}

// chChannelCalc holds per-channel intermediate values until APY/CV are computed.
type chChannelCalc struct {
	row        map[string]any
	capacity   int64
	profits7   int64
	profits30  int64
	revenue7   int64
	revenue30  int64
	assist7    int64
	assist30   int64
	numUpdates int64
}

// handleChannels renders the channel performance table with 7d/30d activity,
// revenue/profit, APY and CV (Channel Value). Aggregates run via SQL FILTER clauses
// for 7d/30d windows and an Invoice-JOIN-Payment for rebalance costs. int64
// truncation and RoundToEven match the expected arithmetic. DB errors return 500.
func (s *Server) handleChannels(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	now := time.Now().UTC() // UTC: N days = N*24h like timedelta
	cut7 := now.AddDate(0, 0, -7)
	cut30 := now.AddDate(0, 0, -30)

	channels, err := s.queryMaps(ctx, `SELECT chan_id, short_chan_id, remote_pubkey, COALESCE(alias,'') AS alias,
                funding_txid, output_index, capacity, local_balance, remote_balance,
                pending_outbound, pending_inbound, num_updates, initiator
            FROM gui_channels WHERE is_open = true AND private = false ORDER BY chan_id`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(channels) == 0 {
		s.renderTemplate(w, r, "channels.html", map[string]any{"channels": []any{}, "apy_7day": int64(0), "apy_30day": int64(0)})
		return
	}

	// Float columns are summed exactly (::numeric): pandas' sums carry no float
	// error, Postgres' float8 sum does, and int() of 1780.9999999999995 is off by one.

	// Forwards (30d base, 7d via FILTER) grouped by out or in channel.
	fwdSQL := func(col string) string {
		return `SELECT ` + col + ` AS cid,
                count(*) FILTER (WHERE forward_date >= $1) AS cnt7,
                count(*) AS cnt30,
                COALESCE(sum(amt_out_msat) FILTER (WHERE forward_date >= $1),0)::bigint AS amt7,
                COALESCE(sum(amt_out_msat),0)::bigint AS amt30,
                COALESCE(sum(fee::numeric) FILTER (WHERE forward_date >= $1),0)::float8 AS fee7,
                COALESCE(sum(fee::numeric),0)::float8 AS fee30,
                COALESCE(sum(inbound_fee::numeric) FILTER (WHERE forward_date >= $1),0)::float8 AS ifee7,
                COALESCE(sum(inbound_fee::numeric),0)::float8 AS ifee30
            FROM gui_forwards WHERE forward_date >= $2 GROUP BY ` + col
	}
	outFwd := map[string]chFwdAgg{}
	inFwd := map[string]chFwdAgg{}
	loadFwd := func(sql string, dst map[string]chFwdAgg) error {
		rows, qerr := s.queryMaps(ctx, sql, cut7, cut30)
		if qerr != nil {
			return qerr
		}
		for _, row := range rows {
			cid, _ := row["cid"].(string)
			a := chFwdAgg{}
			a.cnt7, _ = toInt64(row["cnt7"])
			a.cnt30, _ = toInt64(row["cnt30"])
			a.amt7, _ = toInt64(row["amt7"])
			a.amt30, _ = toInt64(row["amt30"])
			a.fee7, _ = toFloat64(row["fee7"])
			a.fee30, _ = toFloat64(row["fee30"])
			a.ifee7, _ = toFloat64(row["ifee7"])
			a.ifee30, _ = toFloat64(row["ifee30"])
			dst[cid] = a
		}
		return nil
	}
	if err := loadFwd(fwdSQL("chan_id_out"), outFwd); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := loadFwd(fwdSQL("chan_id_in"), inFwd); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Rebalance payments (status=2, rebal_chan set, 30d) grouped by chan_out.
	payAgg := map[string]chPayAgg{}
	payRows, err := s.queryMaps(ctx, `SELECT chan_out AS cid,
                count(*) FILTER (WHERE creation_date >= $1) AS cnt7,
                count(*) AS cnt30,
                COALESCE(sum(value::numeric) FILTER (WHERE creation_date >= $1),0)::float8 AS val7,
                COALESCE(sum(value::numeric),0)::float8 AS val30
            FROM gui_payments WHERE status = 2 AND rebal_chan IS NOT NULL AND creation_date >= $2 GROUP BY chan_out`, cut7, cut30)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for _, row := range payRows {
		cid, _ := row["cid"].(string)
		a := chPayAgg{}
		a.cnt7, _ = toInt64(row["cnt7"])
		a.cnt30, _ = toInt64(row["cnt30"])
		a.val7, _ = toFloat64(row["val7"])
		a.val30, _ = toFloat64(row["val30"])
		payAgg[cid] = a
	}

	// Rebalance invoices (state=1, r_hash linked to 30d rebalance payment) grouped
	// by chan_in; cost7/30 = sum(payment.fee) via JOIN — these are the rebalance costs.
	invAgg := map[string]chInvAgg{}
	invRows, err := s.queryMaps(ctx, `SELECT i.chan_in AS cid,
                count(*) FILTER (WHERE i.settle_date >= $1) AS cnt7,
                count(*) AS cnt30,
                COALESCE(sum(i.amt_paid) FILTER (WHERE i.settle_date >= $1),0)::bigint AS amt7,
                COALESCE(sum(i.amt_paid),0)::bigint AS amt30,
                COALESCE(sum(p.fee::numeric) FILTER (WHERE i.settle_date >= $1 AND p.creation_date >= $1),0)::float8 AS cost7,
                COALESCE(sum(p.fee::numeric),0)::float8 AS cost30
            FROM gui_invoices i JOIN gui_payments p ON p.payment_hash = i.r_hash
            WHERE i.state = 1 AND p.status = 2 AND p.rebal_chan IS NOT NULL AND p.creation_date >= $2
            GROUP BY i.chan_in`, cut7, cut30)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for _, row := range invRows {
		cid, _ := row["cid"].(string)
		a := chInvAgg{}
		a.cnt7, _ = toInt64(row["cnt7"])
		a.cnt30, _ = toInt64(row["cnt30"])
		a.amt7, _ = toInt64(row["amt7"])
		a.amt30, _ = toInt64(row["amt30"])
		a.cost7, _ = toFloat64(row["cost7"])
		a.cost30, _ = toFloat64(row["cost30"])
		invAgg[cid] = a
	}

	// amtDiv computes int(sum/divisor)/10 for a channel with entries in the
	// window, int64(0) otherwise; amtColumns then gives each column one type,
	// as pandas does (float as soon as one row is).
	amtDiv := func(sum float64, divisor float64, count int64) any {
		if count == 0 {
			return int64(0)
		}
		return float64(int64(sum/divisor)) / 10.0
	}
	amtColumns := []string{"amt_routed_in_7day", "amt_routed_out_7day", "amt_routed_in_30day", "amt_routed_out_30day",
		"amt_rebal_in_30day", "amt_rebal_out_30day", "amt_rebal_in_7day", "amt_rebal_out_7day"}

	calcs := make([]chChannelCalc, 0, len(channels))
	var sumProfits7, sumProfits30, sumLocal, sumCapacity, sumUpdates int64

	for _, ch := range channels {
		chanID, _ := ch["chan_id"].(string)
		capacity, _ := toInt64(ch["capacity"])
		localBal, _ := toInt64(ch["local_balance"])
		remoteBal, _ := toInt64(ch["remote_balance"])
		pendOut, _ := toInt64(ch["pending_outbound"])
		pendIn, _ := toInt64(ch["pending_inbound"])
		numUpdates, _ := toInt64(ch["num_updates"])
		initiator, _ := ch["initiator"].(bool)

		localBalance := localBal + pendOut
		remoteBalance := remoteBal + pendIn

		out, hasOut := outFwd[chanID]
		in, hasIn := inFwd[chanID]
		pay := payAgg[chanID]
		inv := invAgg[chanID]

		// inbound_fee sums (int truncation); zero when no entry.
		ifeeOut7 := int64(out.ifee7)
		ifeeOut30 := int64(out.ifee30)
		ifeeIn7 := int64(in.ifee7)
		ifeeIn30 := int64(in.ifee30)

		var revenue7, revenue30, assist7, assist30 int64
		if hasOut {
			revenue7 = int64(out.fee7) + ifeeOut7
			revenue30 = int64(out.fee30) + ifeeOut30
		}
		if hasIn {
			assist7 = int64(in.fee7) - ifeeIn7
			assist30 = int64(in.fee30) - ifeeIn30
		}
		// costs = int(sum payment.fee) + inbound_fee_in (rebalance costs + inbound fee costs).
		costs7 := int64(inv.cost7) + ifeeIn7
		costs30 := int64(inv.cost30) + ifeeIn30
		profits7 := revenue7 - costs7
		profits30 := revenue30 - costs30

		openBlock := int64(0)
		if cid, perr := strconv.ParseInt(chanID, 10, 64); perr == nil {
			openBlock = cid >> 40
		}

		row := map[string]any{
			"chan_id":              chanID,
			"short_chan_id":        ch["short_chan_id"],
			"remote_pubkey":        ch["remote_pubkey"],
			"alias":                ch["alias"],
			"funding_txid":         ch["funding_txid"],
			"output_index":         ch["output_index"],
			"local_balance":        localBalance,
			"remote_balance":       remoteBalance,
			"mil_capacity":         pyround.Round(float64(capacity)/1000000, 1),
			"routed_in_7day":       in.cnt7,
			"routed_out_7day":      out.cnt7,
			"routed_in_30day":      in.cnt30,
			"routed_out_30day":     out.cnt30,
			"amt_routed_in_7day":   amtDiv(float64(in.amt7), 100000000, in.cnt7),
			"amt_routed_out_7day":  amtDiv(float64(out.amt7), 100000000, out.cnt7),
			"amt_routed_in_30day":  amtDiv(float64(in.amt30), 100000000, in.cnt30),
			"amt_routed_out_30day": amtDiv(float64(out.amt30), 100000000, out.cnt30),
			"rebal_in_30day":       inv.cnt30,
			"rebal_out_30day":      pay.cnt30,
			"rebal_in_7day":        inv.cnt7,
			"rebal_out_7day":       pay.cnt7,
			"amt_rebal_in_30day":   amtDiv(float64(inv.amt30), 100000, inv.cnt30),
			"amt_rebal_out_30day":  amtDiv(pay.val30, 100000, pay.cnt30),
			"amt_rebal_in_7day":    amtDiv(float64(inv.amt7), 100000, inv.cnt7),
			"amt_rebal_out_7day":   amtDiv(pay.val7, 100000, pay.cnt7),
			"revenue_7day":         revenue7,
			"revenue_30day":        revenue30,
			"revenue_assist_7day":  assist7,
			"revenue_assist_30day": assist30,
			"profits_7day":         profits7,
			"profits_30day":        profits30,
			"open_block":           openBlock,
			"num_updates":          numUpdates,
			"initiator":            initiator,
		}

		calcs = append(calcs, chChannelCalc{
			row: row, capacity: capacity, profits7: profits7, profits30: profits30,
			revenue7: revenue7, revenue30: revenue30, assist7: assist7, assist30: assist30,
			numUpdates: numUpdates,
		})
		sumProfits7 += profits7
		sumProfits30 += profits30
		sumLocal += localBalance
		sumCapacity += capacity
		sumUpdates += numUpdates
	}

	for _, col := range amtColumns {
		float := false
		for _, c := range calcs {
			if _, ok := c.row[col].(float64); ok {
				float = true
				break
			}
		}
		if float {
			for _, c := range calcs {
				if _, ok := c.row[col].(int64); ok {
					c.row[col] = 0.0
				}
			}
		}
	}

	// Node-level APY — divided by sum(local_balance).
	apy7Node := pyround.NumPy((float64(sumProfits7)*5214.2857)/float64(sumLocal), 2)
	apy30Node := pyround.NumPy((float64(sumProfits30)*1216.6667)/float64(sumLocal), 2)

	nodeCapacity := sumCapacity
	var outboundRatio float64
	if nodeCapacity > 0 {
		outboundRatio = float64(sumLocal) / float64(nodeCapacity)
	}

	for i := range calcs {
		c := &calcs[i]
		// updates%
		var updates int64
		if sumUpdates != 0 {
			updates = int64(math.RoundToEven((float64(c.numUpdates) / float64(sumUpdates)) * 100))
		}
		c.row["updates"] = updates

		if nodeCapacity > 0 {
			capf := float64(c.capacity)
			c.row["apy_7day"] = pyround.Round((float64(c.profits7)*5214.2857)/(capf*outboundRatio), 2)
			c.row["apy_30day"] = pyround.Round((float64(c.profits30)*1216.6667)/(capf*outboundRatio), 2)
			c.row["cv_7day"] = pyround.Round((float64(c.revenue7)*5214.2857)/(capf*outboundRatio)+(float64(c.assist7)*5214.2857)/(capf*(1-outboundRatio)), 2)
			c.row["cv_30day"] = pyround.Round((float64(c.revenue30)*1216.6667)/(capf*outboundRatio)+(float64(c.assist30)*1216.6667)/(capf*(1-outboundRatio)), 2)
		} else {
			c.row["apy_7day"] = 0.0
			c.row["apy_30day"] = 0.0
			c.row["cv_7day"] = 0.0
			c.row["cv_30day"] = 0.0
		}
	}

	// Sort by cv_30day descending (stable).
	sort.SliceStable(calcs, func(i, j int) bool {
		ci, _ := calcs[i].row["cv_30day"].(float64)
		cj, _ := calcs[j].row["cv_30day"].(float64)
		return ci > cj
	})

	result := make([]map[string]any, len(calcs))
	for i := range calcs {
		result[i] = calcs[i].row
	}

	s.renderTemplate(w, r, "channels.html", map[string]any{
		"channels":  result,
		"apy_7day":  apy7Node,
		"apy_30day": apy30Node,
	})
}
