package web

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/routerrpc"
)

// pyBool formats a bool as "True" or "False".
func pyBool(b bool) string {
	if b {
		return "True"
	}
	return "False"
}

// handleUpdateChannel updates a single channel field identified by update_target
// (codes 0..23). Fee/HTLC/CLTV/state codes call LND (UpdateChannelPolicy/
// UpdateChanStatus); the rest write to the DB only. Requires a valid form and an
// existing channel. LND/DB errors return 500. Redirects to the referrer.
func (s *Server) handleUpdateChannel(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	f := &flasher{}
	_ = r.ParseForm()
	chanIDInt, errC := strconv.ParseInt(r.PostForm.Get("chan_id"), 10, 64)
	target, errT := strconv.ParseFloat(r.PostForm.Get("target"), 64)
	ut, errU := strconv.Atoi(r.PostForm.Get("update_target"))

	if errC != nil || errT != nil || errU != nil || ut < 0 || ut > 23 {
		f.add(invalidRequest)
		s.redirect(w, r, refererOr(r, "/"), f)
		return
	}
	chanID := strconv.FormatInt(chanIDInt, 10)
	ch, err := s.queries.GetChannel(ctx, chanID)
	if err != nil {
		f.add(invalidRequest)
		s.redirect(w, r, refererOr(r, "/"), f)
		return
	}
	alias := ch.Alias
	cp := channelPoint(ch.FundingTxid, ch.OutputIndex)
	fail := func(e error) bool {
		if e != nil {
			http.Error(w, e.Error(), http.StatusInternalServerError)
			return true
		}
		return false
	}
	upd := func(sql string, args ...any) bool {
		_, e := s.db.Exec(ctx, sql, args...)
		return !fail(e)
	}

	switch ut {
	case 0: // base fee (LND)
		tbf := int64(math.RoundToEven(target))
		if _, err := s.lnd.Lightning.UpdateChannelPolicy(ctx, &lnrpc.PolicyUpdateRequest{
			Scope: &lnrpc.PolicyUpdateRequest_ChanPoint{ChanPoint: cp}, BaseFeeMsat: tbf,
			FeeRate: float64(ch.LocalFeeRate) / 1000000, TimeLockDelta: uint32(ch.LocalCltv),
		}); fail(err) {
			return
		}
		if !upd(`UPDATE gui_channels SET local_base_fee=$2 WHERE chan_id=$1`, chanID, int32(tbf)) {
			return
		}
		f.add(fmt.Sprintf("Base fee for channel %s (%s) updated to a value of: %d", alias, chanID, tbf))
	case 1: // fee rate (LND + sibling sync)
		if !s.updateChannelFeeRate(ctx, w, f, ch, cp, target) {
			return
		}
	case 12: // inbound base fee (LND, v0.18+)
		info, err := s.lnd.Lightning.GetInfo(ctx, &lnrpc.GetInfoRequest{})
		if fail(err) {
			return
		}
		if versionAtLeast(info.GetVersion(), 0.18) {
			tbf := int64(math.RoundToEven(target))
			if _, err := s.lnd.Lightning.UpdateChannelPolicy(ctx, &lnrpc.PolicyUpdateRequest{
				Scope: &lnrpc.PolicyUpdateRequest_ChanPoint{ChanPoint: cp}, BaseFeeMsat: int64(ch.LocalBaseFee),
				FeeRate: float64(ch.LocalFeeRate) / 1000000, TimeLockDelta: uint32(ch.LocalCltv),
				InboundFee: &lnrpc.InboundFee{BaseFeeMsat: int32(tbf), FeeRatePpm: ch.LocalInboundFeeRate},
			}); fail(err) {
				return
			}
			if !upd(`UPDATE gui_channels SET local_inbound_base_fee=$2 WHERE chan_id=$1`, chanID, int32(tbf)) {
				return
			}
			f.add(fmt.Sprintf("Inbound base fee for channel %s (%s) updated to a value of: %d", alias, chanID, tbf))
		} else {
			f.add("LND version too low to set inbound fees, update to v0.18+")
		}
	case 13: // inbound fee rate (LND, v0.18+)
		info, err := s.lnd.Lightning.GetInfo(ctx, &lnrpc.GetInfoRequest{})
		if fail(err) {
			return
		}
		if versionAtLeast(info.GetVersion(), 0.18) {
			tfr := int64(math.RoundToEven(target))
			if _, err := s.lnd.Lightning.UpdateChannelPolicy(ctx, &lnrpc.PolicyUpdateRequest{
				Scope: &lnrpc.PolicyUpdateRequest_ChanPoint{ChanPoint: cp}, BaseFeeMsat: int64(ch.LocalBaseFee),
				FeeRate: float64(ch.LocalFeeRate) / 1000000, TimeLockDelta: uint32(ch.LocalCltv),
				InboundFee: &lnrpc.InboundFee{BaseFeeMsat: ch.LocalInboundBaseFee, FeeRatePpm: int32(tfr)},
			}); fail(err) {
				return
			}
			// a manual rate wins over the offset automation, which would otherwise
			// recompute this channel on the next outbound fee change or offset job
			if !upd(`UPDATE gui_channels SET local_inbound_fee_rate=$2, inbound_offset=0 WHERE chan_id=$1`, chanID, int32(tfr)) {
				return
			}
			f.add(fmt.Sprintf("Inbound fee rate for channel %s (%s) updated to a value of: %d", alias, chanID, tfr))
			if ch.InboundOffset != 0 {
				f.add(fmt.Sprintf("Inbound offset for channel %s (%s) cleared, the manual rate now wins.", alias, chanID))
			}
		} else {
			f.add("LND version too low to set inbound fees, update to v0.18+")
		}
	case 2:
		if !upd(`UPDATE gui_channels SET ar_amt_target=$2 WHERE chan_id=$1`, chanID, int64(target)) {
			return
		}
		f.add(fmt.Sprintf("Auto rebalancer target amount for channel %s (%s) updated to a value of: %s", alias, chanID, pyFloatString(target)))
	case 3:
		if !upd(`UPDATE gui_channels SET ar_in_target=$2 WHERE chan_id=$1`, chanID, int32(target)) {
			return
		}
		f.add(fmt.Sprintf("Auto rebalancer inbound target for channel %s (%s) updated to a value of: %s%%", alias, chanID, pyFloatString(target)))
	case 4:
		if !upd(`UPDATE gui_channels SET ar_out_target=$2 WHERE chan_id=$1`, chanID, int32(target)) {
			return
		}
		f.add(fmt.Sprintf("Auto rebalancer outbound target for channel %s (%s) updated to a value of: %s%%", alias, chanID, pyFloatString(target)))
	case 5:
		newVal := !ch.AutoRebalance
		if !upd(`UPDATE gui_channels SET auto_rebalance=$2 WHERE chan_id=$1`, chanID, newVal) {
			return
		}
		f.add(fmt.Sprintf("Auto rebalancer status for channel %s (%s) updated to a value of: %s", alias, chanID, pyBool(newVal)))
	case 6:
		if !upd(`UPDATE gui_channels SET ar_max_cost=$2 WHERE chan_id=$1`, chanID, int32(target)) {
			return
		}
		f.add(fmt.Sprintf("Auto rebalancer max cost for channel %s (%s) updated to a value of: %s%%", alias, chanID, pyFloatString(target)))
	case 7: // channel state (LND router)
		action := routerrpc.ChanStatusAction(1) // DISABLE
		newDisabled := true
		if target == 1 {
			action = routerrpc.ChanStatusAction(0) // ENABLE
			newDisabled = false
		}
		if _, err := s.lnd.Router.UpdateChanStatus(ctx, &routerrpc.UpdateChanStatusRequest{ChanPoint: cp, Action: action}); fail(err) {
			return
		}
		if !upd(`UPDATE gui_channels SET local_disabled=$2 WHERE chan_id=$1`, chanID, newDisabled) {
			return
		}
		state := "Disabled"
		if target == 1 {
			state = "Enabled"
		}
		f.add(fmt.Sprintf("Toggled channel state for channel %s (%s) to a value of: %s", alias, chanID, state))
		if target == 0 {
			f.add("Use with caution, while a channel is disabled (local fees highlighted in red) it will not route out.")
		}
	case 8:
		newVal := !ch.AutoFees
		if !upd(`UPDATE gui_channels SET auto_fees=$2 WHERE chan_id=$1`, chanID, newVal) {
			return
		}
		f.add(fmt.Sprintf("Auto fees status for channel %s (%s) updated to a value of: %s", alias, chanID, pyBool(newVal)))
	case 9: // cltv (LND)
		if _, err := s.lnd.Lightning.UpdateChannelPolicy(ctx, &lnrpc.PolicyUpdateRequest{
			Scope: &lnrpc.PolicyUpdateRequest_ChanPoint{ChanPoint: cp}, BaseFeeMsat: int64(ch.LocalBaseFee),
			FeeRate: float64(ch.LocalFeeRate) / 1000000, TimeLockDelta: uint32(int64(target)),
		}); fail(err) {
			return
		}
		if !upd(`UPDATE gui_channels SET local_cltv=$2 WHERE chan_id=$1`, chanID, int32(target)) {
			return
		}
		f.add(fmt.Sprintf("CLTV for channel %s (%s) updated to a value of: %s", alias, chanID, pyFloatString(target)))
	case 10: // min htlc (LND)
		minMsat := int64(target * 1000)
		if _, err := s.lnd.Lightning.UpdateChannelPolicy(ctx, &lnrpc.PolicyUpdateRequest{
			Scope: &lnrpc.PolicyUpdateRequest_ChanPoint{ChanPoint: cp}, BaseFeeMsat: int64(ch.LocalBaseFee),
			FeeRate: float64(ch.LocalFeeRate) / 1000000, TimeLockDelta: uint32(ch.LocalCltv),
			MinHtlcMsatSpecified: true, MinHtlcMsat: uint64(minMsat),
		}); fail(err) {
			return
		}
		if !upd(`UPDATE gui_channels SET local_min_htlc_msat=$2 WHERE chan_id=$1`, chanID, minMsat) {
			return
		}
		f.add(fmt.Sprintf("Min HTLC for channel %s (%s) updated to a value of: %s", alias, chanID, pyFloatString(target)))
	case 11: // max htlc (LND)
		maxMsat := int64(target * 1000)
		if _, err := s.lnd.Lightning.UpdateChannelPolicy(ctx, &lnrpc.PolicyUpdateRequest{
			Scope: &lnrpc.PolicyUpdateRequest_ChanPoint{ChanPoint: cp}, BaseFeeMsat: int64(ch.LocalBaseFee),
			FeeRate: float64(ch.LocalFeeRate) / 1000000, TimeLockDelta: uint32(ch.LocalCltv),
			MaxHtlcMsat: uint64(maxMsat),
		}); fail(err) {
			return
		}
		if !upd(`UPDATE gui_channels SET local_max_htlc_msat=$2 WHERE chan_id=$1`, chanID, maxMsat) {
			return
		}
		f.add(fmt.Sprintf("Max HTLC for channel %s (%s) updated to a value of: %s", alias, chanID, pyFloatString(target)))
	case 14:
		newVal := !ch.ArSource
		if !upd(`UPDATE gui_channels SET ar_source=$2 WHERE chan_id=$1`, chanID, newVal) {
			return
		}
		f.add(fmt.Sprintf("AR source for channel %s (%s) set to: %s", alias, chanID, pyBool(newVal)))
	case 15:
		if !upd(`UPDATE gui_channels SET ar_source_ppm_diff=$2 WHERE chan_id=$1`, chanID, int32(target)) {
			return
		}
		f.add(fmt.Sprintf("Source ppm diff for channel %s (%s) updated to: %s", alias, chanID, pyFloatString(target)))
	case 16:
		newVal := !ch.EpEnabled
		if !upd(`UPDATE gui_channels SET ep_enabled=$2 WHERE chan_id=$1`, chanID, newVal) {
			return
		}
		f.add(fmt.Sprintf("Emergency fees status for channel %s (%s) set to: %s", alias, chanID, pyBool(newVal)))
	case 17:
		if !upd(`UPDATE gui_channels SET ep_target=$2 WHERE chan_id=$1`, chanID, int32(target)) {
			return
		}
		f.add(fmt.Sprintf("Emergency target for channel %s (%s) updated to: %s", alias, chanID, pyFloatString(target)))
	case 18:
		if !upd(`UPDATE gui_channels SET ep_inc_pct=$2 WHERE chan_id=$1`, chanID, target) {
			return
		}
		f.add(fmt.Sprintf("Emergency increase %% for channel %s (%s) updated to: %s", alias, chanID, pyFloatString(target)))
	case 19:
		if !upd(`UPDATE gui_channels SET ep_cooldown=$2 WHERE chan_id=$1`, chanID, int32(target)) {
			return
		}
		f.add(fmt.Sprintf("Emergency cooldown for channel %s (%s) updated to: %s min", alias, chanID, pyFloatString(target)))
	case 20:
		if !upd(`UPDATE gui_channels SET ep_live_threshold=$2 WHERE chan_id=$1`, chanID, int32(target)) {
			return
		}
		f.add(fmt.Sprintf("Live threshold for channel %s (%s) updated to: %s%%", alias, chanID, pyFloatString(target)))
	case 21:
		if !upd(`UPDATE gui_channels SET ep_live_inc_pct=$2 WHERE chan_id=$1`, chanID, target) {
			return
		}
		f.add(fmt.Sprintf("Live increase %% for channel %s (%s) updated to: %s", alias, chanID, pyFloatString(target)))
	case 22:
		newVal := !ch.FlpEnabled
		if !upd(`UPDATE gui_channels SET flp_enabled=$2 WHERE chan_id=$1`, chanID, newVal) {
			return
		}
		f.add(fmt.Sprintf("Fee limit protection for channel %s (%s) set to: %s", alias, chanID, pyBool(newVal)))
	case 23:
		newVal := int32(target)
		if !upd(`UPDATE gui_channels SET flp_safety=$2 WHERE chan_id=$1`, chanID, newVal) {
			return
		}
		f.add(fmt.Sprintf("FLP safety for channel %s (%s) updated to: %d", alias, chanID, newVal))
	default:
		f.add("Invalid target code. Please try again.")
	}
	s.redirect(w, r, refererOr(r, "/"), f)
}

// updateChannelFeeRate handles update_target==1: pushes UpdateChannelPolicy with
// optional inbound offset recalculation, updates the DB, writes autofees/inbound
// fee log entries, and syncs sibling channels. Returns false (with 500 set) on error.
func (s *Server) updateChannelFeeRate(ctx context.Context, w http.ResponseWriter, f *flasher, ch db.GuiChannel, cp *lnrpc.ChannelPoint, target float64) bool {
	req := &lnrpc.PolicyUpdateRequest{
		Scope: &lnrpc.PolicyUpdateRequest_ChanPoint{ChanPoint: cp}, BaseFeeMsat: int64(ch.LocalBaseFee),
		FeeRate: target / 1000000, TimeLockDelta: uint32(ch.LocalCltv),
	}
	hasInbound := false
	var inboundTarget int32
	if ch.InboundOffset != 0 {
		balance := target + float64(ch.InboundOffset)
		if balance > 0 {
			inboundTarget = int32(math.RoundToEven(-balance))
		}
		hasInbound = true
		req.InboundFee = &lnrpc.InboundFee{BaseFeeMsat: ch.LocalInboundBaseFee, FeeRatePpm: inboundTarget}
	}
	if _, err := s.lnd.Lightning.UpdateChannelPolicy(ctx, req); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return false
	}
	now := time.Now()
	oldFeeRate := ch.LocalFeeRate
	newRate := int32(target)
	var execErr error
	if hasInbound {
		_, execErr = s.db.Exec(ctx, `UPDATE gui_channels SET local_fee_rate=$2, fees_updated=$3, local_inbound_fee_rate=$4, offset_updated=$5 WHERE chan_id=$1`,
			ch.ChanID, newRate, now, inboundTarget, now)
	} else {
		_, execErr = s.db.Exec(ctx, `UPDATE gui_channels SET local_fee_rate=$2, fees_updated=$3 WHERE chan_id=$1`,
			ch.ChanID, newRate, now)
	}
	if execErr != nil {
		http.Error(w, execErr.Error(), http.StatusInternalServerError)
		return false
	}
	if _, err := s.db.Exec(ctx, `INSERT INTO gui_autofees (timestamp, chan_id, peer_alias, setting, old_value, new_value) VALUES ($1,$2,$3,$4,$5,$6)`,
		now, ch.ChanID, ch.Alias, "Manual", oldFeeRate, newRate); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return false
	}
	if hasInbound && ch.LocalInboundFeeRate != inboundTarget {
		if _, err := s.db.Exec(ctx, `INSERT INTO gui_inboundfeelog (timestamp, chan_id, peer_alias, setting, old_value, new_value) VALUES ($1,$2,$3,$4,$5,$6)`,
			now, ch.ChanID, ch.Alias, "Fee Adj Offset", ch.LocalInboundFeeRate, inboundTarget); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return false
		}
	}
	updated, err := s.syncPeerOutboundFee(ctx, ch.RemotePubkey, ch.ChanID, newRate)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return false
	}
	if updated > 0 {
		f.add(fmt.Sprintf("Synced outbound fee with %d sibling channel(s).", updated))
	}
	f.add(fmt.Sprintf("Fee rate for channel %s (%s) updated to a value of: %s", ch.Alias, ch.ChanID, pyFloatString(target)))
	return true
}
