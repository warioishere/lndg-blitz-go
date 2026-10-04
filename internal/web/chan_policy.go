package web

import (
	"context"
	"net/http"
	"strconv"

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
		// An explicitly sent inbound fee goes to LND even when it is 0 (that is how
		// it gets reset); otherwise a new outbound rate re-applies the inbound offset.
		var offsetTarget *int32
		if body.InboundBaseFee != nil || body.InboundFeeRate != nil {
			if versionAtLeast(info.GetVersion(), 0.18) {
				req.InboundFee = &lnrpc.InboundFee{
					BaseFeeMsat: int32(inboundBaseFeeMsat),
					FeeRatePpm:  int32(inboundFeeRate),
				}
			} else {
				writeAPIError(w, "LND version too low to set inbound fees, update to v0.18+")
				return
			}
		} else if body.FeeRate != nil && versionAtLeast(info.GetVersion(), 0.18) {
			if t, ok := offsetInboundFee(ch.InboundOffset, float64(*body.FeeRate)); ok {
				offsetTarget = &t
				req.InboundFee = &lnrpc.InboundFee{BaseFeeMsat: ch.LocalInboundBaseFee, FeeRatePpm: t}
			}
		}

		if _, err := s.lnd.Lightning.UpdateChannelPolicy(ctx, req); err != nil {
			writeAPIError(w, "Channel policy update failed! Error: "+grpcErrorMsg(err))
			return
		}

		if body.BaseFee != nil {
			if err := s.execChanPolicy(ctx, w, `UPDATE gui_channels SET local_base_fee=$2 WHERE chan_id=$1`, ch.ChanID, *body.BaseFee); err != nil {
				return
			}
			returnResp.Set("base_fee", *body.BaseFee)
		}
		if body.FeeRate != nil {
			newRate := int32(*body.FeeRate)
			if err := s.recordOutboundFee(ctx, ffaChannelFrom(ch), newRate, offsetTarget); err != nil {
				writeAPIError(w, "Channel policy update failed! Error: "+err.Error())
				return
			}
			returnResp.Set("fee_rate", *body.FeeRate)
			_, updated, err := s.syncPeerOutboundFee(ctx, ch.RemotePubkey, ch.ChanID, newRate)
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
			// same as the UI: an explicitly set rate wins over the offset automation
			if err := s.execChanPolicy(ctx, w, `UPDATE gui_channels SET local_inbound_fee_rate=$2, inbound_offset=0 WHERE chan_id=$1`, ch.ChanID, int32(*body.InboundFeeRate)); err != nil {
				return
			}
			returnResp.Set("inbound_fee_rate", *body.InboundFeeRate)
			returnResp.Set("cleared_inbound_offset", ch.InboundOffset != 0)
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
