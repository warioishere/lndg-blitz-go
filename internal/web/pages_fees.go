package web

import (
	"net/http"
	"sort"
	"time"

	"github.com/warioishere/lndg-blitz-go/internal/af"
	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

// handleFees runs the autofee analysis (af.Main) over open, public channels and
// renders the suggested fee rates sorted by out_percent. local_settings = AF-.
func (s *Server) handleFees(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	open, err := s.queries.ListOpenChannels(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// filter(is_open=True, private=False)
	channels := make([]db.GuiChannel, 0, len(open))
	byID := make(map[string]db.GuiChannel, len(open))
	for _, ch := range open {
		if ch.Private {
			continue
		}
		channels = append(channels, ch)
		byID[ch.ChanID] = ch
	}

	rows, err := af.Main(ctx, s.queries, channels, time.Now())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	records := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		records = append(records, feeRecord(row, byID[row.ChanID]))
	}
	// Sort ascending by out_percent (stable).
	sort.SliceStable(records, func(i, j int) bool {
		return records[i]["out_percent"].(int) < records[j]["out_percent"].(int)
	})

	s.renderTemplate(w, r, "fee_rates.html", map[string]any{
		"channels":       records,
		"local_settings": s.getLocalSettings(ctx, "AF-"),
	})
}

// feeRecord builds the template context row for a single channel: af-computed
// metrics from ChannelFeeRow combined with display fields from the channel record.
// local_balance/remote_balance come from the row (af adds pending_outbound/inbound).
func feeRecord(row *af.ChannelFeeRow, ch db.GuiChannel) map[string]any {
	return map[string]any{
		"funding_txid":            ch.FundingTxid,
		"output_index":            ch.OutputIndex,
		"short_chan_id":           ch.ShortChanID,
		"alias":                   ch.Alias,
		"private":                 ch.Private,
		"auto_fees":               ch.AutoFees,
		"chan_id":                 row.ChanID,
		"remote_pubkey":           row.RemotePubkey,
		"local_balance":           row.LocalBalance,
		"remote_balance":          row.RemoteBalance,
		"capacity":                row.Capacity,
		"out_percent":             row.OutPercent,
		"in_percent":              row.InPercent,
		"net_routed_7day":         row.NetRouted7day,
		"adjustment":              row.Adjustment,
		"new_rate":                row.NewRate,
		"inbound_adjustment":      row.InboundAdjustment,
		"new_inbound_rate":        row.NewInboundRate,
		"local_fee_rate":          row.LocalFeeRate,
		"local_inbound_fee_rate":  row.LocalInboundFeeRate,
		"remote_fee_rate":         row.RemoteFeeRate,
		"remote_inbound_fee_rate": row.RemoteInboundFeeRate,
		"ar_max_cost":             row.ArMaxCost,
		"auto_rebalance":          row.AutoRebalance,
		"eligible":                row.Eligible,
		"fees_updated":            row.FeesUpdated,
	}
}
