package web

import (
	"context"
	"math"
	"time"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// One place for a manual outbound fee change: update_channel, full_fee_adj,
// ALL-oRate, chan_policy and the sibling sync all go through these helpers, so
// the inbound offset is applied the same way on every path.

// ffaChannelFrom picks the fee fields of a full channel row.
func ffaChannelFrom(ch db.GuiChannel) ffaChannel {
	return ffaChannel{
		chanID: ch.ChanID, alias: ch.Alias, remotePubkey: ch.RemotePubkey,
		localFeeRate: ch.LocalFeeRate, localBaseFee: ch.LocalBaseFee, localCltv: ch.LocalCltv,
		inboundOffset: ch.InboundOffset, localInboundBaseFee: ch.LocalInboundBaseFee,
		localInboundFeeRate: ch.LocalInboundFeeRate, fundingTxid: ch.FundingTxid, outputIndex: ch.OutputIndex,
	}
}

// offsetInboundFee returns the inbound fee rate the channel's inbound offset
// implies for a new outbound rate; ok is false when the channel has no offset.
func offsetInboundFee(inboundOffset int32, newRate float64) (target int32, ok bool) {
	if inboundOffset == 0 {
		return 0, false
	}
	if balance := newRate + float64(inboundOffset); balance > 0 {
		return int32(math.RoundToEven(-balance)), true
	}
	return 0, true
}

// recordOutboundFee is the DB side of a manual outbound fee change already
// pushed to LND: targeted write plus the autofees / inbound fee logs.
// inboundTarget is nil when the inbound fee was not recomputed.
func (s *Server) recordOutboundFee(ctx context.Context, ch ffaChannel, newRate int32, inboundTarget *int32) error {
	now := time.Now()
	var err error
	if inboundTarget != nil {
		_, err = s.db.Exec(ctx, `UPDATE gui_channels SET local_fee_rate=$2, fees_updated=$3, local_inbound_fee_rate=$4, offset_updated=$3 WHERE chan_id=$1`,
			ch.chanID, newRate, now, *inboundTarget)
	} else {
		_, err = s.db.Exec(ctx, `UPDATE gui_channels SET local_fee_rate=$2, fees_updated=$3 WHERE chan_id=$1`, ch.chanID, newRate, now)
	}
	if err != nil {
		return err
	}
	if _, err := s.db.Exec(ctx, `INSERT INTO gui_autofees (timestamp, chan_id, peer_alias, setting, old_value, new_value) VALUES ($1,$2,$3,$4,$5,$6)`,
		now, ch.chanID, ch.alias, "Manual", ch.localFeeRate, newRate); err != nil {
		return err
	}
	if inboundTarget != nil && ch.localInboundFeeRate != *inboundTarget {
		if _, err := s.db.Exec(ctx, `INSERT INTO gui_inboundfeelog (timestamp, chan_id, peer_alias, setting, old_value, new_value) VALUES ($1,$2,$3,$4,$5,$6)`,
			now, ch.chanID, ch.alias, "Fee Adj Offset", ch.localInboundFeeRate, *inboundTarget); err != nil {
			return err
		}
	}
	return nil
}

// applyOutboundFee sets a new outbound fee rate on LND and in the DB. A channel
// with an inbound offset gets its inbound fee recomputed in the same update.
func (s *Server) applyOutboundFee(ctx context.Context, ch ffaChannel, newRate float64) error {
	req := &lnrpc.PolicyUpdateRequest{
		Scope:       &lnrpc.PolicyUpdateRequest_ChanPoint{ChanPoint: channelPoint(ch.fundingTxid, ch.outputIndex)},
		BaseFeeMsat: int64(ch.localBaseFee), FeeRate: newRate / 1000000, TimeLockDelta: uint32(ch.localCltv),
	}
	var inboundTarget *int32
	if t, ok := offsetInboundFee(ch.inboundOffset, newRate); ok {
		inboundTarget = &t
		req.InboundFee = &lnrpc.InboundFee{BaseFeeMsat: ch.localInboundBaseFee, FeeRatePpm: t}
	}
	if _, err := s.lnd.Lightning.UpdateChannelPolicy(ctx, req); err != nil {
		return err
	}
	return s.recordOutboundFee(ctx, ch, int32(newRate), inboundTarget)
}

// syncPeerOutboundFee mirrors a manual outbound fee change onto the other open
// channels to the same peer. Returns all sibling chan_ids (processed) and how
// many of them actually changed.
func (s *Server) syncPeerOutboundFee(ctx context.Context, remotePubkey, excludeChanID string, newRate int32) (processed []string, updated int, err error) {
	rows, err := s.db.Query(ctx,
		`SELECT `+ffaColumns+` FROM gui_channels WHERE remote_pubkey=$1 AND is_open=true AND chan_id<>$2`,
		remotePubkey, excludeChanID)
	if err != nil {
		return nil, 0, err
	}
	siblings, err := scanFFAChannels(rows)
	if err != nil {
		return nil, 0, err
	}
	for _, sb := range siblings {
		processed = append(processed, sb.chanID)
		if sb.localFeeRate == newRate {
			continue
		}
		if err := s.applyOutboundFee(ctx, sb, float64(newRate)); err != nil {
			return processed, updated, err
		}
		updated++
	}
	return processed, updated, nil
}
