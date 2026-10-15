package web

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// handleInboundOffsetPost handles single or bulk inbound offset adjustments via
// UpdateChannelPolicy (inbound_fee, LND v0.18+) and logs changes to InboundFeeLog.
func (s *Server) handleInboundOffsetPost(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	f := &flasher{}
	_ = r.ParseForm()

	if r.PostForm.Get("bulk") == "1" {
		s.inboundOffsetBulk(ctx, w, r, f)
		return
	}
	s.inboundOffsetSingle(ctx, w, r, f)
}

func (s *Server) inboundOffsetBulk(ctx context.Context, w http.ResponseWriter, r *http.Request, f *flasher) {
	delta, err := strconv.Atoi(r.PostForm.Get("delta_offset"))
	if r.PostForm.Get("delta_offset") == "" {
		delta = 0 // request.POST.get('delta_offset', 0)
	} else if err != nil {
		f.add("Invalid offset delta.")
		s.redirect(w, r, "/inbound-offset/", f)
		return
	}

	selected := r.PostForm["channels"]
	var rows pgx.Rows
	var qerr error
	if len(selected) > 0 {
		rows, qerr = s.db.Query(ctx, `SELECT `+ffaColumns+` FROM gui_channels WHERE chan_id = ANY($1) ORDER BY chan_id`, selected)
	} else {
		rows, qerr = s.db.Query(ctx, `SELECT `+ffaColumns+` FROM gui_channels WHERE is_open = true ORDER BY chan_id`)
	}
	if qerr != nil {
		http.Error(w, qerr.Error(), http.StatusInternalServerError)
		return
	}
	channels, err := scanFFAChannels(rows)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Version check (once).
	info, gerr := s.lnd.Lightning.GetInfo(ctx, &lnrpc.GetInfoRequest{})
	if gerr != nil {
		f.add("Error checking LND version: " + gerr.Error())
		s.redirect(w, r, "/inbound-offset/", f)
		return
	}
	if !versionAtLeast(info.GetVersion(), 0.18) {
		f.add("LND version too low to set inbound fees, update to v0.18+")
		s.redirect(w, r, "/inbound-offset/", f)
		return
	}

	updated := 0
	for _, ch := range channels {
		newOffset := ch.inboundOffset + int32(delta)
		if newOffset > 0 {
			newOffset = 0
		}
		withOffset := ch
		withOffset.inboundOffset = newOffset
		targetFeeRate, _ := s.inboundOffsetRate(ctx, withOffset, int(ch.localFeeRate))
		if _, err := s.lnd.Lightning.UpdateChannelPolicy(ctx, &lnrpc.PolicyUpdateRequest{
			Scope:         &lnrpc.PolicyUpdateRequest_ChanPoint{ChanPoint: channelPoint(ch.fundingTxid, ch.outputIndex)},
			BaseFeeMsat:   int64(ch.localBaseFee),
			FeeRate:       float64(ch.localFeeRate) / 1000000,
			TimeLockDelta: uint32(ch.localCltv),
			InboundFee:    &lnrpc.InboundFee{BaseFeeMsat: ch.localInboundBaseFee, FeeRatePpm: targetFeeRate},
		}); err != nil {
			f.add("Error updating " + ch.alias + ": " + err.Error())
			continue
		}
		now := time.Now()
		oldRate := ch.localInboundFeeRate
		if _, err := s.db.Exec(ctx, `UPDATE gui_channels SET inbound_offset=$2, local_inbound_fee_rate=$3, offset_updated=$4 WHERE chan_id=$1`,
			ch.chanID, newOffset, targetFeeRate, now); err != nil {
			f.add("Error updating " + ch.alias + ": " + err.Error())
			continue
		}
		if oldRate != targetFeeRate {
			if _, err := s.db.Exec(ctx, `INSERT INTO gui_inboundfeelog (timestamp, chan_id, peer_alias, setting, old_value, new_value) VALUES ($1,$2,$3,$4,$5,$6)`,
				now, ch.chanID, ch.alias, "Bulk Offset", oldRate, targetFeeRate); err != nil {
				f.add("Error updating " + ch.alias + ": " + err.Error())
				continue
			}
		}
		updated++
	}
	f.add(fmt.Sprintf("Inbound offset adjusted by %+d for %d channel(s).", delta, updated))
	s.redirect(w, r, "/inbound-offset/", f)
}

func (s *Server) inboundOffsetSingle(ctx context.Context, w http.ResponseWriter, r *http.Request, f *flasher) {
	chanIDInt, e1 := strconv.ParseInt(r.PostForm.Get("chan_id"), 10, 64)
	offset, e2 := strconv.Atoi(r.PostForm.Get("offset"))
	if e1 != nil || e2 != nil {
		f.add(invalidRequest)
		s.redirect(w, r, refererOr(r, "/"), f)
		return
	}
	chanID := strconv.FormatInt(chanIDInt, 10)
	rows, err := s.db.Query(ctx, `SELECT `+ffaColumns+` FROM gui_channels WHERE chan_id=$1`, chanID)
	var found []ffaChannel
	if err == nil {
		found, err = scanFFAChannels(rows)
	}
	if err != nil || len(found) == 0 {
		f.add(invalidRequest)
		s.redirect(w, r, refererOr(r, "/"), f)
		return
	}
	ch := found[0]
	withOffset := ch
	withOffset.inboundOffset = int32(offset)
	targetFeeRate, _ := s.inboundOffsetRate(ctx, withOffset, int(ch.localFeeRate))
	// GetInfo/LND/DB errors flash 'Error updating channel: {e}'.
	fail := func(e error) {
		f.add("Error updating channel: " + e.Error())
		s.redirect(w, r, refererOr(r, "/"), f)
	}
	info, err := s.lnd.Lightning.GetInfo(ctx, &lnrpc.GetInfoRequest{})
	if err != nil {
		fail(err)
		return
	}
	if !versionAtLeast(info.GetVersion(), 0.18) {
		f.add("LND version too low to set inbound fees, update to v0.18+")
		s.redirect(w, r, refererOr(r, "/"), f)
		return
	}
	if _, err := s.lnd.Lightning.UpdateChannelPolicy(ctx, &lnrpc.PolicyUpdateRequest{
		Scope:         &lnrpc.PolicyUpdateRequest_ChanPoint{ChanPoint: channelPoint(ch.fundingTxid, ch.outputIndex)},
		BaseFeeMsat:   int64(ch.localBaseFee),
		FeeRate:       float64(ch.localFeeRate) / 1000000,
		TimeLockDelta: uint32(ch.localCltv),
		InboundFee:    &lnrpc.InboundFee{BaseFeeMsat: ch.localInboundBaseFee, FeeRatePpm: targetFeeRate},
	}); err != nil {
		fail(err)
		return
	}
	now := time.Now()
	oldRate := ch.localInboundFeeRate
	if _, err := s.db.Exec(ctx, `UPDATE gui_channels SET inbound_offset=$2, local_inbound_fee_rate=$3, offset_updated=$4 WHERE chan_id=$1`,
		chanID, int32(offset), targetFeeRate, now); err != nil {
		fail(err)
		return
	}
	if _, err := s.db.Exec(ctx, `INSERT INTO gui_inboundfeelog (timestamp, chan_id, peer_alias, setting, old_value, new_value) VALUES ($1,$2,$3,$4,$5,$6)`,
		now, chanID, ch.alias, "Manual Offset", oldRate, targetFeeRate); err != nil {
		fail(err)
		return
	}
	f.add(fmt.Sprintf("Inbound fee rate for channel %s (%s) updated to a value of: %d", ch.alias, chanID, targetFeeRate))
	s.redirect(w, r, refererOr(r, "/"), f)
}
