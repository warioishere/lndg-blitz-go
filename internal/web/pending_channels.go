package web

import (
	"net/http"
	"time"

	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// handlePendingChannels calls the PendingChannels RPC and returns a data object
// keyed by the categories that are present. Returns null data when no category
// exists and limbo balance is zero. On error, returns 200 + {"error":...}.
func (s *Server) handlePendingChannels(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	resp, err := s.lnd.Lightning.PendingChannels(ctx, &lnrpc.PendingChannelsRequest{})
	if err != nil {
		writeAPIError(w, "Failed to get pending channels! Error: "+grpcErrorMsg(err))
		return
	}

	hasAny := len(resp.GetPendingOpenChannels()) > 0 ||
		len(resp.GetPendingForceClosingChannels()) > 0 ||
		len(resp.GetWaitingCloseChannels()) > 0 ||
		resp.GetTotalLimboBalance() != 0
	if !hasAny {
		writeSuccess(w, nil)
		return
	}

	target := newOrderedMap()

	if len(resp.GetPendingOpenChannels()) > 0 {
		items := make([]any, 0)
		for _, oc := range resp.GetPendingOpenChannels() {
			ch := oc.GetChannel()
			items = append(items, newOrderedMap().
				Set("alias", s.peerAlias(ctx, ch.GetRemoteNodePub(), nil)).
				Set("remote_node_pub", ch.GetRemoteNodePub()).
				Set("channel_point", ch.GetChannelPoint()).
				Set("capacity", ch.GetCapacity()).
				Set("local_balance", ch.GetLocalBalance()).
				Set("remote_balance", ch.GetRemoteBalance()).
				Set("local_chan_reserve_sat", ch.GetLocalChanReserveSat()).
				Set("remote_chan_reserve_sat", ch.GetRemoteChanReserveSat()).
				Set("initiator", int32(ch.GetInitiator())).
				Set("commitment_type", int32(ch.GetCommitmentType())).
				Set("commit_fee", oc.GetCommitFee()).
				Set("commit_weight", oc.GetCommitWeight()).
				Set("fee_per_kw", oc.GetFeePerKw()))
		}
		target.Set("pending_open", items)
	}

	if len(resp.GetPendingForceClosingChannels()) > 0 {
		items := make([]any, 0)
		for _, fc := range resp.GetPendingForceClosingChannels() {
			ch := fc.GetChannel()
			blocks := fc.GetBlocksTilMaturity()
			maturity := time.Now().Add(time.Duration(10*blocks) * time.Minute)
			item := newOrderedMap().
				Set("remote_node_pub", ch.GetRemoteNodePub()).
				Set("channel_point", ch.GetChannelPoint()).
				Set("capacity", ch.GetCapacity()).
				Set("local_balance", ch.GetLocalBalance()).
				Set("remote_balance", ch.GetRemoteBalance()).
				Set("initiator", int32(ch.GetInitiator())).
				Set("commitment_type", int32(ch.GetCommitmentType())).
				Set("closing_txid", fc.GetClosingTxid()).
				Set("limbo_balance", fc.GetLimboBalance()).
				Set("maturity_height", fc.GetMaturityHeight()).
				Set("blocks_til_maturity", fc.GetBlocksTilMaturity()).
				Set("maturity_datetime", isoformatUTC(maturity))
			s.addPendingChannelDetails(ctx, item, ch.GetChannelPoint())
			items = append(items, item)
		}
		target.Set("pending_force_closing", items)
	}

	if len(resp.GetWaitingCloseChannels()) > 0 {
		items := make([]any, 0)
		for _, wc := range resp.GetWaitingCloseChannels() {
			ch := wc.GetChannel()
			item := newOrderedMap().
				Set("remote_node_pub", ch.GetRemoteNodePub()).
				Set("channel_point", ch.GetChannelPoint()).
				Set("capacity", ch.GetCapacity()).
				Set("local_balance", ch.GetLocalBalance()).
				Set("remote_balance", ch.GetRemoteBalance()).
				Set("local_chan_reserve_sat", ch.GetLocalChanReserveSat()).
				Set("remote_chan_reserve_sat", ch.GetRemoteChanReserveSat()).
				Set("initiator", int32(ch.GetInitiator())).
				Set("commitment_type", int32(ch.GetCommitmentType())).
				Set("limbo_balance", wc.GetLimboBalance())
			s.addPendingChannelDetails(ctx, item, ch.GetChannelPoint())
			items = append(items, item)
		}
		target.Set("waiting_close", items)
	}

	if resp.GetTotalLimboBalance() != 0 {
		target.Set("total_limbo_balance", resp.GetTotalLimboBalance())
	}

	writeSuccess(w, target)
}
