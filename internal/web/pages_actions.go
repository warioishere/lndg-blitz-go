package web

import (
	"math"
	"net/http"
	"time"
)

// handleActions evaluates active, open, public channels and suggests AR actions
// (Enable/Disable AR) based on 7-day routing balance (o7D/i7D) and liquidity
// percentages. outbound/inbound_percent are integer-division values from Postgres;
// o7D/i7D are computed as int(sum/1e7)/100. Case 6 ("Peer Fee Too High") is
// skipped via continue and never shown. DB errors return 500.
func (s *Server) handleActions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	filter7day := time.Now().UTC().AddDate(0, 0, -7)

	channels, err := s.queryMaps(ctx, `SELECT chan_id, funding_txid, output_index, short_chan_id, remote_pubkey, COALESCE(alias,'') AS alias,
                capacity, local_balance, remote_balance, pending_outbound, pending_inbound,
                unsettled_balance, local_base_fee, local_fee_rate, remote_base_fee, remote_fee_rate,
                auto_rebalance,
                ((local_balance+pending_outbound)*1000)/capacity AS outbound_percent,
                ((remote_balance+pending_inbound)*1000)/capacity AS inbound_percent
            FROM gui_channels WHERE is_active = true AND is_open = true AND private = false`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 7-day forward aggregates per channel: count + volume, split by in/out.
	type fwdAgg struct {
		count int64
		vol   int64
	}
	inAgg := map[string]fwdAgg{}
	outAgg := map[string]fwdAgg{}
	loadAgg := func(sql string, dst map[string]fwdAgg) bool {
		rows, qerr := s.queryMaps(ctx, sql, filter7day)
		if qerr != nil {
			http.Error(w, qerr.Error(), http.StatusInternalServerError)
			return false
		}
		for _, row := range rows {
			cid, _ := row["cid"].(string)
			c, _ := toInt64(row["cnt"])
			v, _ := toInt64(row["vol"])
			dst[cid] = fwdAgg{count: c, vol: v}
		}
		return true
	}
	if !loadAgg(`SELECT chan_id_in AS cid, count(*) AS cnt, COALESCE(sum(amt_in_msat),0)::bigint AS vol FROM gui_forwards WHERE forward_date >= $1 GROUP BY chan_id_in`, inAgg) {
		return
	}
	if !loadAgg(`SELECT chan_id_out AS cid, count(*) AS cnt, COALESCE(sum(amt_out_msat),0)::bigint AS vol FROM gui_forwards WHERE forward_date >= $1 GROUP BY chan_id_out`, outAgg) {
		return
	}

	actionList := make([]map[string]any, 0)
	for _, ch := range channels {
		chanID, _ := ch["chan_id"].(string)
		localFeeRate, _ := toInt64(ch["local_fee_rate"])
		remoteFeeRate, _ := toInt64(ch["remote_fee_rate"])
		localBalance, _ := toInt64(ch["local_balance"])
		pendingOutbound, _ := toInt64(ch["pending_outbound"])
		remoteBalance, _ := toInt64(ch["remote_balance"])
		pendingInbound, _ := toInt64(ch["pending_inbound"])
		outboundAnnot, _ := toInt64(ch["outbound_percent"])
		inboundAnnot, _ := toInt64(ch["inbound_percent"])
		autoRebalance, _ := ch["auto_rebalance"].(bool)

		op := int64(math.RoundToEven(float64(outboundAnnot) / 10.0)) // int(round(x/10, 0))
		ip := int64(math.RoundToEven(float64(inboundAnnot) / 10.0))

		in := inAgg[chanID]
		out := outAgg[chanID]

		// i7D/o7D = 0 (int) when no forwards; otherwise float int(sum/1e7)/100.
		var i7Dval, o7Dval float64
		var i7Ddisp, o7Ddisp any = int64(0), int64(0)
		if in.count != 0 {
			i7Dval = float64(int64(float64(in.vol)/10000000.0)) / 100.0
			i7Ddisp = i7Dval
		}
		if out.count != 0 {
			o7Dval = float64(int64(float64(out.vol)/10000000.0)) / 100.0
			o7Ddisp = o7Dval
		}

		// Decision logic: only Case 2 (Enable AR) and Case 3 (Disable AR) are appended;
		// Cases 1/4/5 and Case 6 are skipped.
		var output, reason string
		switch {
		case o7Dval > i7Dval*1.10 && op > 75:
			continue // Case 1
		case o7Dval > i7Dval*1.10 && ip > 75 && !autoRebalance:
			if localFeeRate <= remoteFeeRate {
				continue // Case 6: Peer Fee Too High (skipped)
			}
			output = "Enable AR" // Case 2
			reason = "o7D > i7D AND Inbound Liq > 75%"
		case o7Dval < i7Dval*1.10 && op > 75 && autoRebalance:
			output = "Disable AR" // Case 3
			reason = "o7D < i7D AND Outbound Liq > 75%"
		case o7Dval < i7Dval*1.10 && ip > 75:
			continue // Case 4
		default:
			continue // Case 5
		}

		actionList = append(actionList, map[string]any{
			"chan_id":           chanID,
			"short_chan_id":     ch["short_chan_id"],
			"remote_pubkey":     ch["remote_pubkey"],
			"alias":             ch["alias"],
			"capacity":          ch["capacity"],
			"local_balance":     localBalance + pendingOutbound,
			"remote_balance":    remoteBalance + pendingInbound,
			"outbound_percent":  op,
			"inbound_percent":   ip,
			"unsettled_balance": ch["unsettled_balance"],
			"local_base_fee":    ch["local_base_fee"],
			"local_fee_rate":    localFeeRate,
			"remote_base_fee":   ch["remote_base_fee"],
			"remote_fee_rate":   remoteFeeRate,
			"routed_in_7day":    in.count,
			"routed_out_7day":   out.count,
			"i7D":               i7Ddisp,
			"o7D":               o7Ddisp,
			"auto_rebalance":    autoRebalance,
			"output":            output,
			"reason":            reason,
			// Include the actual channel point (funding_txid + output_index).
			"funding_txid": ch["funding_txid"],
			"output_index": ch["output_index"],
		})
	}

	s.renderTemplate(w, r, "action_list.html", map[string]any{"action_list": actionList})
}
