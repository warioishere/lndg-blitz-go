package web

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// fetchOpenFFAChannels loads all open channels with the fields required by
// update_setting and full_fee_adj, ordered by chan_id.
func (s *Server) fetchOpenFFAChannels(ctx context.Context) ([]ffaChannel, error) {
	const cols = `chan_id, COALESCE(alias,''), remote_pubkey, local_fee_rate, local_base_fee, local_cltv,
		inbound_offset, local_inbound_base_fee, local_inbound_fee_rate, funding_txid, output_index`
	rows, err := s.db.Query(ctx, `SELECT `+cols+` FROM gui_channels WHERE is_open = true ORDER BY chan_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ffaChannel
	for rows.Next() {
		var c ffaChannel
		if err := rows.Scan(&c.chanID, &c.alias, &c.remotePubkey, &c.localFeeRate, &c.localBaseFee, &c.localCltv,
			&c.inboundOffset, &c.localInboundBaseFee, &c.localInboundFeeRate, &c.fundingTxid, &c.outputIndex); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// handleUpdateSetting is a multipurpose endpoint. ALL-* keys broadcast fee/policy
// changes across all open channels (LND + DB; ALL-oRate includes sibling sync) or
// bulk-update DB fields. RR-/QR-/GW- keys upsert LocalSettings. Parse/LND/DB errors
// return 500.
func (s *Server) handleUpdateSetting(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	f := &flasher{}
	_ = r.ParseForm()
	key := r.PostForm.Get("key")
	value := r.PostForm.Get("value")
	if key == "" || value == "" || len(key) > 20 || len(value) > 2000 {
		f.add("Invalid Request Form. Please try again.")
		s.redirect(w, r, refererOr(r, "/"), f)
		return
	}

	fail := func(e error) bool {
		if e != nil {
			http.Error(w, e.Error(), http.StatusInternalServerError)
			return true
		}
		return false
	}
	// intVal/floatVal parse the form value; a parse error returns 500.
	intVal := func() (int, bool) {
		v, err := strconv.Atoi(value)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return 0, false
		}
		return v, true
	}
	floatVal := func() (float64, bool) {
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return 0, false
		}
		return v, true
	}
	setLS := func(k, v string) bool {
		_, e := s.db.Exec(ctx, `INSERT INTO gui_localsettings (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value=$2`, k, v)
		return !fail(e)
	}
	bulkChan := func(sql string, arg any) bool {
		_, e := s.db.Exec(ctx, sql, arg)
		return !fail(e)
	}
	onOff := func(enabled bool, on, off string) string {
		if enabled {
			return on
		}
		return off
	}

	switch key {
	case "ALL-oRate":
		target, ok := intVal()
		if !ok {
			return
		}
		channels, err := s.fetchOpenFFAChannels(ctx)
		if fail(err) {
			return
		}
		processed := map[string]bool{}
		totalSynced := 0
		for _, ch := range channels {
			if processed[ch.chanID] {
				continue
			}
			processed[ch.chanID] = true
			if _, err := s.lnd.Lightning.UpdateChannelPolicy(ctx, &lnrpc.PolicyUpdateRequest{
				Scope:       &lnrpc.PolicyUpdateRequest_ChanPoint{ChanPoint: channelPoint(ch.fundingTxid, ch.outputIndex)},
				BaseFeeMsat: int64(ch.localBaseFee), FeeRate: float64(target) / 1000000, TimeLockDelta: uint32(ch.localCltv),
			}); fail(err) {
				return
			}
			now := time.Now()
			oldRate := ch.localFeeRate
			if _, err := s.db.Exec(ctx, `UPDATE gui_channels SET local_fee_rate=$2, fees_updated=$3 WHERE chan_id=$1`, ch.chanID, int32(target), now); fail(err) {
				return
			}
			if _, err := s.db.Exec(ctx, `INSERT INTO gui_autofees (timestamp, chan_id, peer_alias, setting, old_value, new_value) VALUES ($1,$2,$3,$4,$5,$6)`,
				now, ch.chanID, ch.alias, "Manual", oldRate, int32(target)); fail(err) {
				return
			}
			updatedSiblings, err := s.syncPeerOutboundFee(ctx, ch.remotePubkey, ch.chanID, int32(target))
			if fail(err) {
				return
			}
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
			if fail(sibRows.Err()) {
				return
			}
			totalSynced += updatedSiblings
		}
		if totalSynced > 0 {
			f.add("Synced outbound fee with " + strconv.Itoa(totalSynced) + " sibling channel(s).")
		}
		f.add("Fee rate for all open channels updated to a value of: " + strconv.Itoa(target))
	case "ALL-oBase":
		target, ok := intVal()
		if !ok {
			return
		}
		channels, err := s.fetchOpenFFAChannels(ctx)
		if fail(err) {
			return
		}
		for _, ch := range channels {
			if _, err := s.lnd.Lightning.UpdateChannelPolicy(ctx, &lnrpc.PolicyUpdateRequest{
				Scope:       &lnrpc.PolicyUpdateRequest_ChanPoint{ChanPoint: channelPoint(ch.fundingTxid, ch.outputIndex)},
				BaseFeeMsat: int64(target), FeeRate: float64(ch.localFeeRate) / 1000000, TimeLockDelta: uint32(ch.localCltv),
			}); fail(err) {
				return
			}
			if _, err := s.db.Exec(ctx, `UPDATE gui_channels SET local_base_fee=$2 WHERE chan_id=$1`, ch.chanID, int32(target)); fail(err) {
				return
			}
		}
		f.add("Base fee for all channels updated to a value of: " + strconv.Itoa(target))
	case "ALL-iRate", "ALL-iBase":
		target, ok := intVal()
		if !ok {
			return
		}
		info, err := s.lnd.Lightning.GetInfo(ctx, &lnrpc.GetInfoRequest{})
		if fail(err) {
			return
		}
		if !versionAtLeast(info.GetVersion(), 0.18) {
			f.add("LND version too low to set inbound fees, update to v0.18+")
			break
		}
		channels, err := s.fetchOpenFFAChannels(ctx)
		if fail(err) {
			return
		}
		clearedOffsets := 0
		for _, ch := range channels {
			var inbound *lnrpc.InboundFee
			var col string
			if key == "ALL-iRate" {
				inbound = &lnrpc.InboundFee{BaseFeeMsat: ch.localInboundBaseFee, FeeRatePpm: int32(target)}
				// same as the per channel update: a manual rate wins over the offset
				col = "local_inbound_fee_rate=$2, inbound_offset=0"
				if ch.inboundOffset != 0 {
					clearedOffsets++
				}
			} else {
				inbound = &lnrpc.InboundFee{BaseFeeMsat: int32(target), FeeRatePpm: ch.localInboundFeeRate}
				col = "local_inbound_base_fee=$2"
			}
			if _, err := s.lnd.Lightning.UpdateChannelPolicy(ctx, &lnrpc.PolicyUpdateRequest{
				Scope:       &lnrpc.PolicyUpdateRequest_ChanPoint{ChanPoint: channelPoint(ch.fundingTxid, ch.outputIndex)},
				BaseFeeMsat: int64(ch.localBaseFee), FeeRate: float64(ch.localFeeRate) / 1000000, TimeLockDelta: uint32(ch.localCltv),
				InboundFee: inbound,
			}); fail(err) {
				return
			}
			if _, err := s.db.Exec(ctx, `UPDATE gui_channels SET `+col+` WHERE chan_id=$1`, ch.chanID, int32(target)); fail(err) {
				return
			}
		}
		if key == "ALL-iRate" {
			f.add("Inbound fee rate for all open channels updated to a value of: " + strconv.Itoa(target))
			if clearedOffsets > 0 {
				f.add("Inbound offset cleared on " + strconv.Itoa(clearedOffsets) + " channel(s), the manual rate now wins.")
			}
		} else {
			f.add("Inbound base fee for all channels updated to a value of: " + strconv.Itoa(target))
		}
	case "ALL-CLTV":
		target, ok := intVal()
		if !ok {
			return
		}
		channels, err := s.fetchOpenFFAChannels(ctx)
		if fail(err) {
			return
		}
		for _, ch := range channels {
			if _, err := s.lnd.Lightning.UpdateChannelPolicy(ctx, &lnrpc.PolicyUpdateRequest{
				Scope:       &lnrpc.PolicyUpdateRequest_ChanPoint{ChanPoint: channelPoint(ch.fundingTxid, ch.outputIndex)},
				BaseFeeMsat: int64(ch.localBaseFee), FeeRate: float64(ch.localFeeRate) / 1000000, TimeLockDelta: uint32(target),
			}); fail(err) {
				return
			}
			if _, err := s.db.Exec(ctx, `UPDATE gui_channels SET local_cltv=$2 WHERE chan_id=$1`, ch.chanID, int32(target)); fail(err) {
				return
			}
		}
		f.add("CLTV for all channels updated to a value of: " + strconv.Itoa(target))
	case "ALL-minHTLC":
		fv, ok := floatVal()
		if !ok {
			return
		}
		target := int64(fv * 1000)
		channels, err := s.fetchOpenFFAChannels(ctx)
		if fail(err) {
			return
		}
		for _, ch := range channels {
			if _, err := s.lnd.Lightning.UpdateChannelPolicy(ctx, &lnrpc.PolicyUpdateRequest{
				Scope:       &lnrpc.PolicyUpdateRequest_ChanPoint{ChanPoint: channelPoint(ch.fundingTxid, ch.outputIndex)},
				BaseFeeMsat: int64(ch.localBaseFee), FeeRate: float64(ch.localFeeRate) / 1000000, TimeLockDelta: uint32(ch.localCltv),
				MinHtlcMsatSpecified: true, MinHtlcMsat: uint64(target),
			}); fail(err) {
				return
			}
			if _, err := s.db.Exec(ctx, `UPDATE gui_channels SET local_min_htlc_msat=$2 WHERE chan_id=$1`, ch.chanID, target); fail(err) {
				return
			}
		}
		f.add("Min HTLC for all channels updated to a value of: " + pyFloatString(fv))
	case "ALL-Amts":
		target, ok := intVal()
		if !ok {
			return
		}
		if !bulkChan(`UPDATE gui_channels SET ar_amt_target=$1 WHERE is_open=true`, int64(target)) {
			return
		}
		f.add("AR target amounts for all channels updated to a value of: " + strconv.Itoa(target))
	case "ALL-MaxCost":
		target, ok := intVal()
		if !ok {
			return
		}
		if !bulkChan(`UPDATE gui_channels SET ar_max_cost=$1 WHERE is_open=true`, int32(target)) {
			return
		}
		f.add("AR max cost for all channels updated to a value of: " + strconv.Itoa(target))
	case "ALL-oTarget":
		target, ok := intVal()
		if !ok {
			return
		}
		if !bulkChan(`UPDATE gui_channels SET ar_out_target=$1 WHERE is_open=true`, int32(target)) {
			return
		}
		f.add("AR outbound liquidity target for all channels updated to a value of: " + strconv.Itoa(target))
	case "ALL-iTarget":
		target, ok := intVal()
		if !ok {
			return
		}
		if !bulkChan(`UPDATE gui_channels SET ar_in_target=$1 WHERE is_open=true`, int32(target)) {
			return
		}
		f.add("AR inbound liquidity target for all channels updated to a value of: " + strconv.Itoa(target))
	case "ALL-AR":
		target, ok := intVal()
		if !ok {
			return
		}
		if !bulkChan(`UPDATE gui_channels SET auto_rebalance=$1 WHERE is_open=true`, target != 0) {
			return
		}
		f.add("Auto-Rebalance targeting for all channels updated to a value of: " + strconv.Itoa(target))
	case "ALL-AF":
		target, ok := intVal()
		if !ok {
			return
		}
		if _, err := s.db.Exec(ctx, `UPDATE gui_channels SET auto_fees=$1 WHERE is_open=true AND private=false`, target != 0); fail(err) {
			return
		}
		f.add("Auto Fees setting for all channels updated to a value of: " + strconv.Itoa(target))
		if !setLS("AF-Enabled", strconv.Itoa(target)) {
			return
		}
		f.add("Auto Fees globally " + onOff(target != 0, "enabled", "disabled"))
	case "RR-RouteLimit":
		target, ok := intVal()
		if !ok {
			return
		}
		if !setLS("RR-RouteLimit", strconv.Itoa(target)) {
			return
		}
		f.add("Route limit updated to: " + strconv.Itoa(target))
	case "RR-CollectRoutes":
		norm := onOff(value == "1", "1", "0")
		if !setLS("RR-CollectRoutes", norm) {
			return
		}
		f.add("Route collecting " + onOff(norm == "1", "enabled", "disabled"))
	case "RR-UseSavedRoutes":
		norm := onOff(value == "1", "1", "0")
		if !setLS("RR-UseSavedRoutes", norm) {
			return
		}
		f.add("Saved route usage " + onOff(norm == "1", "enabled", "disabled"))
	case "QR-Enabled":
		norm := onOff(value == "1", "1", "0")
		if !setLS("QR-Enabled", norm) {
			return
		}
		f.add("Route probing " + onOff(norm == "1", "enabled", "disabled"))
	case "QR-UpdateHours":
		fv, ok := floatVal()
		if !ok {
			return
		}
		if !setLS("QR-UpdateHours", pyFloatString(fv)) {
			return
		}
		f.add("Probe interval updated to: " + pyFloatString(fv) + "h")
	case "QR-MaxPerTarget":
		target, ok := intVal()
		if !ok {
			return
		}
		if !setLS("QR-MaxPerTarget", strconv.Itoa(target)) {
			return
		}
		f.add("Max probes per target updated to: " + strconv.Itoa(target))
	case "QR-LastProbe":
		if !setLS("QR-LastProbe", value) {
			return
		}
	case "GW-Enabled":
		norm := onOff(value == "1", "1", "0")
		if !setLS("GW-Enabled", norm) {
			return
		}
		f.add("Graph Watcher " + onOff(norm == "1", "enabled", "disabled"))
	case "GW-Cooldown":
		target, ok := intVal()
		if !ok {
			return
		}
		if !setLS("GW-Cooldown", strconv.Itoa(target)) {
			return
		}
		f.add("Graph Watcher cooldown updated to: " + strconv.Itoa(target) + "s")
	case "GW-Exclude":
		if !setLS("GW-Exclude", value) {
			return
		}
	default:
		f.add("Invalid Request. Please try again. [" + key + "]")
	}

	s.redirect(w, r, refererOr(r, "/"), f)
}
