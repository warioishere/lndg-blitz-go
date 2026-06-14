package web

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/routerrpc"
)

// channelPoint builds a ChannelPoint from a funding txid and output index,
// using the string variant of the funding_txid oneof.
func channelPoint(fundingTxid string, outputIndex int32) *lnrpc.ChannelPoint {
	return &lnrpc.ChannelPoint{
		FundingTxid: &lnrpc.ChannelPoint_FundingTxidStr{FundingTxidStr: fundingTxid},
		OutputIndex: uint32(outputIndex),
	}
}

// handleChanPolicy updates channel policy (fees, HTLC limits, CLTV, inbound fees)
// and optionally enables or disables the channel, then writes DB records and
// syncs sibling channels. Returns the updated fields as a flat object.
func (s *Server) handleChanPolicy(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ChanID         *string  `json:"chan_id"`
		BaseFee        *int64   `json:"base_fee"`
		FeeRate        *int64   `json:"fee_rate"`
		InboundBaseFee *int64   `json:"inbound_base_fee"`
		InboundFeeRate *int64   `json:"inbound_fee_rate"`
		Disabled       *int64   `json:"disabled"`
		Cltv           *int64   `json:"cltv"`
		MinHtlc        *float64 `json:"min_htlc"`
		MaxHtlc        *float64 `json:"max_htlc"`
	}
	// chan_id is a CharField with max_length=20; the channel must exist.
	if !decodeJSON(r, &body) || body.ChanID == nil || len(*body.ChanID) > 20 {
		writeAPIError(w, "Invalid request!")
		return
	}
	ctx := r.Context()
	ch, err := s.queries.GetChannel(ctx, *body.ChanID)
	if err != nil {
		writeAPIError(w, "Invalid request!") // Channel does not exist
		return
	}
	cp := channelPoint(ch.FundingTxid, ch.OutputIndex)
	returnResp := newOrderedMap()

	// Policy block runs when ANY fee/HTLC/CLTV field is present.
	if body.BaseFee != nil || body.FeeRate != nil || body.Cltv != nil || body.MinHtlc != nil ||
		body.MaxHtlc != nil || body.InboundBaseFee != nil || body.InboundFeeRate != nil {

		baseFeeMsat := int64(ch.LocalBaseFee)
		if body.BaseFee != nil {
			baseFeeMsat = *body.BaseFee
		}
		feeRate := float64(ch.LocalFeeRate) / 1000000
		if body.FeeRate != nil {
			feeRate = float64(*body.FeeRate) / 1000000
		}
		inboundBaseFeeMsat := int64(ch.LocalInboundBaseFee)
		if body.InboundBaseFee != nil {
			inboundBaseFeeMsat = *body.InboundBaseFee
		}
		inboundFeeRate := int64(ch.LocalInboundFeeRate)
		if body.InboundFeeRate != nil {
			inboundFeeRate = *body.InboundFeeRate
		}
		timeLockDelta := int64(ch.LocalCltv)
		if body.Cltv != nil {
			timeLockDelta = *body.Cltv
		}
		minHtlcMsat := ch.LocalMinHtlcMsat
		if body.MinHtlc != nil {
			minHtlcMsat = int64(*body.MinHtlc * 1000)
		}
		maxHtlcMsat := ch.LocalMaxHtlcMsat
		if body.MaxHtlc != nil {
			maxHtlcMsat = int64(*body.MaxHtlc * 1000)
		}

		info, err := s.lnd.Lightning.GetInfo(ctx, &lnrpc.GetInfoRequest{})
		if err != nil {
			writeAPIError(w, "Channel policy update failed! Error: "+grpcErrorMsg(err))
			return
		}

		req := &lnrpc.PolicyUpdateRequest{
			Scope:                &lnrpc.PolicyUpdateRequest_ChanPoint{ChanPoint: cp},
			BaseFeeMsat:          baseFeeMsat,
			FeeRate:              feeRate,
			TimeLockDelta:        uint32(timeLockDelta),
			MinHtlcMsatSpecified: true,
			MinHtlcMsat:          uint64(minHtlcMsat),
			MaxHtlcMsat:          uint64(maxHtlcMsat),
		}
		// Include inbound fees only when either inbound field is non-zero.
		if (body.InboundBaseFee != nil && *body.InboundBaseFee != 0) ||
			(body.InboundFeeRate != nil && *body.InboundFeeRate != 0) {
			if versionAtLeast(info.GetVersion(), 0.18) {
				req.InboundFee = &lnrpc.InboundFee{
					BaseFeeMsat: int32(inboundBaseFeeMsat),
					FeeRatePpm:  int32(inboundFeeRate),
				}
			} else {
				writeAPIError(w, "LND version too low to set inbound fees, update to v0.18+")
				return
			}
		}

		if _, err := s.lnd.Lightning.UpdateChannelPolicy(ctx, req); err != nil {
			writeAPIError(w, "Channel policy update failed! Error: "+grpcErrorMsg(err))
			return
		}

		now := time.Now()
		if body.BaseFee != nil {
			if err := s.execChanPolicy(ctx, w, `UPDATE gui_channels SET local_base_fee=$2 WHERE chan_id=$1`, ch.ChanID, *body.BaseFee); err != nil {
				return
			}
			returnResp.Set("base_fee", *body.BaseFee)
		}
		if body.FeeRate != nil {
			oldFeeRate := ch.LocalFeeRate
			newRate := int32(*body.FeeRate)
			if err := s.execChanPolicy(ctx, w, `UPDATE gui_channels SET local_fee_rate=$2, fees_updated=$3 WHERE chan_id=$1`, ch.ChanID, newRate, now); err != nil {
				return
			}
			returnResp.Set("fee_rate", *body.FeeRate)
			if _, err := s.db.Exec(ctx, `INSERT INTO gui_autofees (timestamp, chan_id, peer_alias, setting, old_value, new_value) VALUES ($1,$2,$3,$4,$5,$6)`,
				now, ch.ChanID, ch.Alias, "Manual", oldFeeRate, newRate); err != nil {
				writeAPIError(w, "Channel policy update failed! Error: "+err.Error())
				return
			}
			updated, err := s.syncPeerOutboundFee(ctx, ch.RemotePubkey, ch.ChanID, newRate)
			if err != nil {
				writeAPIError(w, "Channel policy update failed! Error: "+grpcErrorMsg(err))
				return
			}
			if updated > 0 {
				returnResp.Set("synced_siblings", updated)
			}
		}
		if body.InboundBaseFee != nil {
			if err := s.execChanPolicy(ctx, w, `UPDATE gui_channels SET local_inbound_base_fee=$2 WHERE chan_id=$1`, ch.ChanID, int32(*body.InboundBaseFee)); err != nil {
				return
			}
			returnResp.Set("inbound_base_fee", *body.InboundBaseFee)
		}
		if body.InboundFeeRate != nil {
			if err := s.execChanPolicy(ctx, w, `UPDATE gui_channels SET local_inbound_fee_rate=$2 WHERE chan_id=$1`, ch.ChanID, int32(*body.InboundFeeRate)); err != nil {
				return
			}
			returnResp.Set("inbound_fee_rate", *body.InboundFeeRate)
		}
		if body.Cltv != nil {
			if err := s.execChanPolicy(ctx, w, `UPDATE gui_channels SET local_cltv=$2 WHERE chan_id=$1`, ch.ChanID, int32(*body.Cltv)); err != nil {
				return
			}
			returnResp.Set("cltv", *body.Cltv)
		}
		if body.MinHtlc != nil {
			if err := s.execChanPolicy(ctx, w, `UPDATE gui_channels SET local_min_htlc_msat=$2 WHERE chan_id=$1`, ch.ChanID, int64(*body.MinHtlc*1000)); err != nil {
				return
			}
			returnResp.Set("min_htlc", *body.MinHtlc)
		}
		if body.MaxHtlc != nil {
			if err := s.execChanPolicy(ctx, w, `UPDATE gui_channels SET local_max_htlc_msat=$2 WHERE chan_id=$1`, ch.ChanID, int64(*body.MaxHtlc*1000)); err != nil {
				return
			}
			returnResp.Set("max_htlc", *body.MaxHtlc)
		}
	}

	if body.Disabled != nil {
		action := routerrpc.ChanStatusAction(0) // ENABLE
		if *body.Disabled != 0 {
			action = routerrpc.ChanStatusAction(1) // DISABLE
		}
		if _, err := s.lnd.Router.UpdateChanStatus(ctx, &routerrpc.UpdateChanStatusRequest{ChanPoint: cp, Action: action}); err != nil {
			writeAPIError(w, "Channel policy update failed! Error: "+grpcErrorMsg(err))
			return
		}
		if _, err := s.db.Exec(ctx, `UPDATE gui_channels SET local_disabled=$2 WHERE chan_id=$1`, ch.ChanID, *body.Disabled != 0); err != nil {
			writeAPIError(w, "Channel policy update failed! Error: "+err.Error())
			return
		}
		// Bug fix: the disabled field is set to the disabled value, not the base_fee value.
		returnResp.Set("disabled", *body.Disabled)
	}

	writeJSON(w, http.StatusOK, returnResp)
}

// execChanPolicy executes an UPDATE statement and writes an error response on failure.
func (s *Server) execChanPolicy(ctx context.Context, w http.ResponseWriter, sql string, args ...any) error {
	if _, err := s.db.Exec(ctx, sql, args...); err != nil {
		writeAPIError(w, "Channel policy update failed! Error: "+err.Error())
		return err
	}
	return nil
}

// versionAtLeast returns true when the first 4 characters of version parse as a
// float >= min. This matches the version check used by the LND node info endpoint.
func versionAtLeast(version string, min float64) bool {
	v := version
	if len(v) > 4 {
		v = v[:4]
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return false
	}
	return f >= min
}

// syncPeerOutboundFee propagates a manual outbound fee change to all other open
// channels with the same peer (sibling channels). Each sibling gets an
// UpdateChannelPolicy call and DB writes for both the fee rate and the autofees
// log. If the sibling has an inbound offset configured, the inbound fee rate is
// adjusted accordingly. Returns the number of siblings actually updated.
func (s *Server) syncPeerOutboundFee(ctx context.Context, remotePubkey, excludeChanID string, newRate int32) (int, error) {
	rows, err := s.db.Query(ctx,
		`SELECT chan_id, local_fee_rate, local_base_fee, local_cltv, inbound_offset,
		        local_inbound_base_fee, local_inbound_fee_rate, alias, funding_txid, output_index
		 FROM gui_channels WHERE remote_pubkey=$1 AND is_open=true AND chan_id<>$2`,
		remotePubkey, excludeChanID)
	if err != nil {
		return 0, err
	}
	type sibling struct {
		chanID                                                string
		localFeeRate, localBaseFee, localCltv, inboundOffset  int32
		localInboundBaseFee, localInboundFeeRate, outputIndex int32
		alias, fundingTxid                                    string
	}
	var siblings []sibling
	for rows.Next() {
		var sb sibling
		if err := rows.Scan(&sb.chanID, &sb.localFeeRate, &sb.localBaseFee, &sb.localCltv, &sb.inboundOffset,
			&sb.localInboundBaseFee, &sb.localInboundFeeRate, &sb.alias, &sb.fundingTxid, &sb.outputIndex); err != nil {
			rows.Close()
			return 0, err
		}
		siblings = append(siblings, sb)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	updated := 0
	for _, sb := range siblings {
		if sb.localFeeRate == newRate {
			continue
		}
		req := &lnrpc.PolicyUpdateRequest{
			Scope:         &lnrpc.PolicyUpdateRequest_ChanPoint{ChanPoint: channelPoint(sb.fundingTxid, sb.outputIndex)},
			BaseFeeMsat:   int64(sb.localBaseFee),
			FeeRate:       float64(newRate) / 1000000,
			TimeLockDelta: uint32(sb.localCltv),
		}
		hasInbound := false
		var inboundTarget int32
		if sb.inboundOffset != 0 {
			balance := newRate + sb.inboundOffset
			if balance > 0 {
				inboundTarget = -balance
			} else {
				inboundTarget = 0
			}
			hasInbound = true
			req.InboundFee = &lnrpc.InboundFee{BaseFeeMsat: sb.localInboundBaseFee, FeeRatePpm: inboundTarget}
		}
		if _, err := s.lnd.Lightning.UpdateChannelPolicy(ctx, req); err != nil {
			return updated, err
		}

		now := time.Now()
		oldRate := sb.localFeeRate
		if hasInbound {
			if _, err := s.db.Exec(ctx, `UPDATE gui_channels SET local_fee_rate=$2, fees_updated=$3, local_inbound_fee_rate=$4, offset_updated=$5 WHERE chan_id=$1`,
				sb.chanID, newRate, now, inboundTarget, now); err != nil {
				return updated, err
			}
		} else {
			if _, err := s.db.Exec(ctx, `UPDATE gui_channels SET local_fee_rate=$2, fees_updated=$3 WHERE chan_id=$1`,
				sb.chanID, newRate, now); err != nil {
				return updated, err
			}
		}
		if _, err := s.db.Exec(ctx, `INSERT INTO gui_autofees (timestamp, chan_id, peer_alias, setting, old_value, new_value) VALUES ($1,$2,$3,$4,$5,$6)`,
			now, sb.chanID, sb.alias, "Manual", oldRate, newRate); err != nil {
			return updated, err
		}
		if hasInbound {
			oldInbound := sb.localInboundFeeRate
			if oldInbound != inboundTarget {
				if _, err := s.db.Exec(ctx, `INSERT INTO gui_inboundfeelog (timestamp, chan_id, peer_alias, setting, old_value, new_value) VALUES ($1,$2,$3,$4,$5,$6)`,
					now, sb.chanID, sb.alias, "Fee Adj Offset", oldInbound, inboundTarget); err != nil {
					return updated, err
				}
			}
		}
		updated++
	}
	return updated, nil
}
