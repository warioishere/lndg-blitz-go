package web

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
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
	const cols = `chan_id, COALESCE(alias,''), remote_pubkey, local_fee_rate, local_base_fee, local_cltv,
		inbound_offset, local_inbound_base_fee, local_inbound_fee_rate, funding_txid, output_index`
	var sql string
	var args []any
	if len(selected) > 0 {
		sql = `SELECT ` + cols + ` FROM gui_channels WHERE chan_id = ANY($1) ORDER BY chan_id`
		args = []any{selected}
	} else {
		sql = `SELECT ` + cols + ` FROM gui_channels WHERE is_open = true ORDER BY chan_id`
	}
	rows, err := s.db.Query(ctx, sql, args...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var channels []ffaChannel
	for rows.Next() {
		var c ffaChannel
		if err := rows.Scan(&c.chanID, &c.alias, &c.remotePubkey, &c.localFeeRate, &c.localBaseFee, &c.localCltv,
			&c.inboundOffset, &c.localInboundBaseFee, &c.localInboundFeeRate, &c.fundingTxid, &c.outputIndex); err != nil {
			rows.Close()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		channels = append(channels, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
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
		req := &lnrpc.PolicyUpdateRequest{
			Scope:         &lnrpc.PolicyUpdateRequest_ChanPoint{ChanPoint: channelPoint(ch.fundingTxid, ch.outputIndex)},
			BaseFeeMsat:   int64(ch.localBaseFee),
			FeeRate:       float64(newRate) / 1000000,
			TimeLockDelta: uint32(ch.localCltv),
		}
		hasInbound := false
		var inboundTarget int32
		if ch.inboundOffset != 0 {
			balance := newRate + ch.inboundOffset
			if balance > 0 {
				inboundTarget = -balance
			}
			hasInbound = true
			req.InboundFee = &lnrpc.InboundFee{BaseFeeMsat: ch.localInboundBaseFee, FeeRatePpm: inboundTarget}
		}
		if _, err := s.lnd.Lightning.UpdateChannelPolicy(ctx, req); fail(err) {
			return
		}

		now := time.Now()
		oldRate := ch.localFeeRate
		var execErr error
		if hasInbound {
			_, execErr = s.db.Exec(ctx, `UPDATE gui_channels SET local_fee_rate=$2, fees_updated=$3, local_inbound_fee_rate=$4, offset_updated=$5 WHERE chan_id=$1`,
				ch.chanID, newRate, now, inboundTarget, now)
		} else {
			_, execErr = s.db.Exec(ctx, `UPDATE gui_channels SET local_fee_rate=$2, fees_updated=$3 WHERE chan_id=$1`,
				ch.chanID, newRate, now)
		}
		if fail(execErr) {
			return
		}
		if _, err := s.db.Exec(ctx, `INSERT INTO gui_autofees (timestamp, chan_id, peer_alias, setting, old_value, new_value) VALUES ($1,$2,$3,$4,$5,$6)`,
			now, ch.chanID, ch.alias, "Manual", oldRate, newRate); fail(err) {
			return
		}
		if hasInbound && ch.localInboundFeeRate != inboundTarget {
			if _, err := s.db.Exec(ctx, `INSERT INTO gui_inboundfeelog (timestamp, chan_id, peer_alias, setting, old_value, new_value) VALUES ($1,$2,$3,$4,$5,$6)`,
				now, ch.chanID, ch.alias, "Fee Adj Offset", ch.localInboundFeeRate, inboundTarget); fail(err) {
				return
			}
		}

		updatedSiblings, err := s.syncPeerOutboundFee(ctx, ch.remotePubkey, ch.chanID, newRate)
		if fail(err) {
			return
		}
		// Mark all siblings (open, same peer) as processed so they are not touched again.
		sibRows, err := s.db.Query(ctx, `SELECT chan_id FROM gui_channels WHERE remote_pubkey=$1 AND is_open=true AND chan_id<>$2`, ch.remotePubkey, ch.chanID)
		if fail(err) {
			return
		}
		for sibRows.Next() {
			var sib string
			if err := sibRows.Scan(&sib); err != nil {
				sibRows.Close()
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			processed[sib] = true
		}
		sibRows.Close()
		if err := sibRows.Err(); fail(err) {
			return
		}
		updated += 1 + updatedSiblings
	}

	f.add(fmt.Sprintf("Outbound fee rate adjusted by %+d ppm for %d channel(s).", deltaPpm, updated))
	s.redirect(w, r, "/full-fee-adj/", f)
}
