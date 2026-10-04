package web

import (
	"fmt"
	"net/http"
	"strconv"
)

// ffaChannel holds the channel fields needed for full_fee_adj (including remote_pubkey).
type ffaChannel struct {
	chanID, alias, remotePubkey, fundingTxid              string
	localFeeRate, localBaseFee, localCltv, inboundOffset  int32
	localInboundBaseFee, localInboundFeeRate, outputIndex int32
}

// handleFullFeeAdjPost adjusts the outbound fee rate of all (or selected) channels
// by delta_ppm, recalculates the inbound offset where applicable, and syncs sibling
// channels. LND/DB errors return 500. Redirects to /full-fee-adj/.
func (s *Server) handleFullFeeAdjPost(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	f := &flasher{}
	_ = r.ParseForm()

	deltaPpm := 0
	if r.PostForm.Has("delta_ppm") {
		v, err := strconv.Atoi(r.PostForm.Get("delta_ppm"))
		if err != nil {
			f.add("Invalid PPM delta.")
			s.redirect(w, r, "/full-fee-adj/", f)
			return
		}
		deltaPpm = v
	}

	selected := r.PostForm["channels"]
	var sql string
	var args []any
	if len(selected) > 0 {
		sql = `SELECT ` + ffaColumns + ` FROM gui_channels WHERE chan_id = ANY($1) ORDER BY chan_id`
		args = []any{selected}
	} else {
		sql = `SELECT ` + ffaColumns + ` FROM gui_channels WHERE is_open = true ORDER BY chan_id`
	}
	rows, err := s.db.Query(ctx, sql, args...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	channels, err := scanFFAChannels(rows)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fail := func(e error) bool {
		if e != nil {
			http.Error(w, e.Error(), http.StatusInternalServerError)
			return true
		}
		return false
	}

	processed := map[string]bool{}
	updated := 0
	for _, ch := range channels {
		if processed[ch.chanID] {
			continue
		}
		processed[ch.chanID] = true

		newRate := ch.localFeeRate + int32(deltaPpm)
		if newRate < 0 {
			newRate = 0
		}
		if err := s.applyOutboundFee(ctx, ch, float64(newRate)); fail(err) {
			return
		}
		siblings, updatedSiblings, err := s.syncPeerOutboundFee(ctx, ch.remotePubkey, ch.chanID, newRate)
		if fail(err) {
			return
		}
		for _, sib := range siblings {
			processed[sib] = true
		}
		updated += 1 + updatedSiblings
	}

	f.add(fmt.Sprintf("Outbound fee rate adjusted by %+d ppm for %d channel(s).", deltaPpm, updated))
	s.redirect(w, r, "/full-fee-adj/", f)
}
