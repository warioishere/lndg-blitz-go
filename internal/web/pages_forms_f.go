package web

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// handleAutoMaxhtlcPost sets maxhtlc_percent and mx_liq_* thresholds for a channel,
// then pushes the resulting max HTLC value via UpdateChannelPolicy. Redirects to
// the referrer.
func (s *Server) handleAutoMaxhtlcPost(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	f := &flasher{}
	_ = r.ParseForm()
	chanIDInt, e1 := strconv.ParseInt(r.PostForm.Get("chan_id"), 10, 64)
	percent, e2 := strconv.Atoi(r.PostForm.Get("percent"))
	if e1 != nil || e2 != nil {
		f.add(invalidRequest)
		s.redirect(w, r, refererOr(r, "/"), f)
		return
	}
	// Optional fields (required=False) default to 0 when absent or empty.
	atoiOr0 := func(key string) int64 {
		v, _ := strconv.ParseInt(r.PostForm.Get(key), 10, 64)
		return v
	}
	mxThreshold := atoiOr0("mx_liq_threshold")
	mxValue := atoiOr0("mx_liq_value")
	mxUpper := atoiOr0("mx_liq_upper")

	chanID := strconv.FormatInt(chanIDInt, 10)
	ch, err := s.queries.GetChannel(ctx, chanID)
	if err != nil {
		f.add(invalidRequest)
		s.redirect(w, r, refererOr(r, "/"), f)
		return
	}

	outbound := int64(ch.LocalBalance) + int64(ch.PendingOutbound)
	target := outbound * int64(100-percent) / 100 // int(outbound*(100-percent)/100)
	maxHtlcMsat := target * 1000

	// LND/DB errors on failure flash 'Error updating channel: {e}'.
	if _, err := s.lnd.Lightning.UpdateChannelPolicy(ctx, &lnrpc.PolicyUpdateRequest{
		Scope:         &lnrpc.PolicyUpdateRequest_ChanPoint{ChanPoint: channelPoint(ch.FundingTxid, ch.OutputIndex)},
		BaseFeeMsat:   int64(ch.LocalBaseFee),
		FeeRate:       float64(ch.LocalFeeRate) / 1000000,
		TimeLockDelta: uint32(ch.LocalCltv),
		MaxHtlcMsat:   uint64(maxHtlcMsat),
	}); err != nil {
		f.add("Error updating channel: " + err.Error())
		s.redirect(w, r, refererOr(r, "/"), f)
		return
	}
	if _, err := s.db.Exec(ctx, `UPDATE gui_channels SET maxhtlc_percent=$2, mx_liq_threshold=$3, mx_liq_value=$4, mx_liq_upper=$5, local_max_htlc_msat=$6, maxhtlc_updated=now() WHERE chan_id=$1`,
		chanID, int32(percent), mxThreshold, mxValue, mxUpper, maxHtlcMsat); err != nil {
		f.add("Error updating channel: " + err.Error())
		s.redirect(w, r, refererOr(r, "/"), f)
		return
	}
	f.add(fmt.Sprintf("Max HTLC for channel %s (%s) updated to a value of: %d", ch.Alias, chanID, target))
	s.redirect(w, r, refererOr(r, "/"), f)
}

// handleGraphWatcherPost handles the graph watcher POST: action=purge_events deletes
// all GraphEvents. Redirects to /graphwatcher.
func (s *Server) handleGraphWatcherPost(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	_ = r.ParseForm()
	if r.PostForm.Get("action") != "purge_events" {
		s.redirect(w, r, "/", nil) // any other action redirects home
		return
	}
	f := &flasher{}
	var count int64
	if err := s.db.QueryRow(ctx, `SELECT count(*) FROM gui_graphevent`).Scan(&count); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := s.db.Exec(ctx, `DELETE FROM gui_graphevent`); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	f.add(fmt.Sprintf("Purged %d graph watcher events.", count))
	s.redirect(w, r, "/graphwatcher", f)
}
