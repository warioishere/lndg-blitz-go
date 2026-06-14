package web

import (
	"math"
	"net/http"

	"github.com/warioishere/lndg-blitz-go/internal/lnd"
)

// advancedSQL selects open channels with outbound_percent/inbound_percent computed
// as ((bal+pending)*1000)/capacity using integer division in Postgres, ordered by
// is_active descending then outbound_percent ascending.
const advancedSQL = `SELECT *,
        ((local_balance+pending_outbound)*1000)/capacity AS outbound_percent,
        ((remote_balance+pending_inbound)*1000)/capacity AS inbound_percent
    FROM gui_channels WHERE is_open = true
    ORDER BY is_active DESC, outbound_percent ASC`

// handleAdvanced renders the Advanced Channel Settings table with per-row computed
// fields (out/in_percent, fee_ratio, local_min/max_htlc, pending-adjusted balances)
// plus node cache statistics and local settings (AF-/AR-/GUI-/LND-/NODE_CACHE).
func (s *Server) handleAdvanced(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	rows, err := s.queryMaps(ctx, advancedSQL)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for _, row := range rows {
		op, _ := toInt64(row["outbound_percent"])
		ip, _ := toInt64(row["inbound_percent"])
		// out_percent = int(round(outbound_percent/10, 0)) — banker's rounding.
		row["out_percent"] = int(math.RoundToEven(float64(op) / 10.0))
		row["in_percent"] = int(math.RoundToEven(float64(ip) / 10.0))

		lb, _ := toInt64(row["local_balance"])
		po, _ := toInt64(row["pending_outbound"])
		rb, _ := toInt64(row["remote_balance"])
		pi, _ := toInt64(row["pending_inbound"])
		row["local_balance"] = lb + po
		row["remote_balance"] = rb + pi

		lfr, _ := toInt64(row["local_fee_rate"])
		if lfr == 0 {
			row["fee_ratio"] = 100
		} else {
			rfr, _ := toInt64(row["remote_fee_rate"])
			row["fee_ratio"] = int(math.RoundToEven((float64(rfr) / float64(lfr)) * 1000.0 / 10.0))
		}

		minMsat, _ := toInt64(row["local_min_htlc_msat"])
		maxMsat, _ := toInt64(row["local_max_htlc_msat"])
		row["local_min_htlc"] = float64(minMsat) / 1000.0
		row["local_max_htlc"] = float64(maxMsat) / 1000.0
	}

	entries, bytesUsed := lnd.CacheStats()
	mb := math.RoundToEven(float64(bytesUsed)/1024/1024*100) / 100

	s.renderTemplate(w, r, "advanced.html", map[string]any{
		"channels":           rows,
		"local_settings":     s.getLocalSettings(ctx, "AF-", "AR-", "GUI-", "LND-", "NODE_CACHE"),
		"node_cache_entries": entries,
		"node_cache_mb":      mb,
	})
}

// handleAdvancedRebalancing renders the Advanced Rebalancing page. For each
// AR-enabled source channel it lists eligible target peers (open+auto_rebalance,
// oRate >= source.oRate+ppm_diff, excluding self and sibling channels), annotated
// with AllowedTarget status. POST (Rebalance/Save Targets/Apply All) is handled
// separately in Layer 6.
func (s *Server) handleAdvancedRebalancing(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	channels, err := s.queryMaps(ctx,
		`SELECT * FROM gui_channels WHERE is_open = true AND auto_rebalance = true`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	atRows, err := s.queryMaps(ctx, `SELECT source_chan_id, target_pubkey FROM gui_allowedtarget`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	allowedByChan := make(map[string]map[string]bool)
	for _, at := range atRows {
		src, _ := at["source_chan_id"].(string)
		tp, _ := at["target_pubkey"].(string)
		if allowedByChan[src] == nil {
			allowedByChan[src] = make(map[string]bool)
		}
		allowedByChan[src][tp] = true
	}

	sources := make([]map[string]any, 0, len(channels))
	for _, ch := range channels {
		chanID, _ := ch["chan_id"].(string)
		remotePubkey, _ := ch["remote_pubkey"].(string)
		diff, _ := toInt64(ch["ar_source_ppm_diff"]) // ar_source_ppm_diff or 0 (NOT NULL)
		localFeeRate, _ := toInt64(ch["local_fee_rate"])
		allowed := allowedByChan[chanID]

		targets := make([]map[string]any, 0)
		seen := make(map[string]bool)
		for _, t := range channels {
			tChanID, _ := t["chan_id"].(string)
			tPubkey, _ := t["remote_pubkey"].(string)
			if tChanID == chanID || tPubkey == remotePubkey {
				continue
			}
			tlfr, _ := toInt64(t["local_fee_rate"])
			if tlfr < localFeeRate+diff {
				continue
			}
			if seen[tPubkey] {
				continue
			}
			seen[tPubkey] = true
			targets = append(targets, map[string]any{"obj": t, "allowed": allowed[tPubkey]})
		}

		arMaxCost, _ := toInt64(ch["ar_max_cost"])
		limitPpm := int64(float64(localFeeRate) * (float64(arMaxCost) / 100.0))
		outPercent := 0
		if capacity, _ := toInt64(ch["capacity"]); capacity != 0 {
			lb, _ := toInt64(ch["local_balance"])
			po, _ := toInt64(ch["pending_outbound"])
			outPercent = int(math.RoundToEven(float64(lb+po) / float64(capacity) * 100))
		}
		sources = append(sources, map[string]any{
			"channel":     ch,
			"targets":     targets,
			"limit":       limitPpm,
			"out_percent": outPercent,
		})
	}
	s.renderTemplate(w, r, "advanced_rebalancing.html", map[string]any{"sources": sources})
}
