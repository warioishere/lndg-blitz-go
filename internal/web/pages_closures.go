package web

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// setPendingDetails resolves short_chan_id/chan_id/alias from gui_channels by
// funding_txid+output_index for a pending-channel map entry; sets nil on miss.
func (s *Server) setPendingDetails(ctx context.Context, m map[string]any, channelPoint string) {
	fundingTxid, outputIndex := splitChannelPoint(channelPoint)
	if idx, err := strconv.Atoi(outputIndex); err == nil {
		var sc, ci, al string
		if e := s.db.QueryRow(ctx,
			`SELECT short_chan_id, chan_id, alias FROM gui_channels WHERE funding_txid=$1 AND output_index=$2`,
			fundingTxid, idx).Scan(&sc, &ci, &al); e == nil {
			m["short_chan_id"], m["chan_id"], m["alias"] = sc, ci, al
			return
		}
	}
	m["short_chan_id"], m["chan_id"], m["alias"] = nil, nil, nil
}

// shortChanIDFromChanID derives a human-readable short channel ID
// (BlockxTxxOutput) from a numeric chan_id via bit shifts.
func shortChanIDFromChanID(v any) string {
	s, _ := v.(string)
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return s
	}
	return fmt.Sprintf("%dx%dx%d", n>>40, (n>>16)&0xFFFFFF, n&0xFFFF)
}

// handleClosures renders the closures page, showing pending/force/waiting-close
// channels from the PendingChannels RPC and the closures table (LEFT JOIN channels,
// with short_chan_id derived from chan_id when missing). Results are ordered by
// close_height. RPC errors render error.html.
func (s *Server) handleClosures(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	resp, err := s.lnd.Lightning.PendingChannels(ctx, &lnrpc.PendingChannelsRequest{})
	if err != nil {
		s.renderError(w, r, grpcCodeString(err))
		return
	}

	var pendingClosed, pendingForceClosed, waitingForClose []map[string]any
	for _, cc := range resp.GetPendingClosingChannels() {
		ch := cc.GetChannel()
		item := map[string]any{
			"remote_node_pub":         ch.GetRemoteNodePub(),
			"channel_point":           ch.GetChannelPoint(),
			"capacity":                ch.GetCapacity(),
			"local_balance":           ch.GetLocalBalance(),
			"remote_balance":          ch.GetRemoteBalance(),
			"local_chan_reserve_sat":  ch.GetLocalChanReserveSat(),
			"remote_chan_reserve_sat": ch.GetRemoteChanReserveSat(),
			"initiator":               int32(ch.GetInitiator()),
			"commitment_type":         int32(ch.GetCommitmentType()),
			"local_commit_fee_sat":    int64(0),
			"limbo_balance":           int64(0),
			"closing_txid":            cc.GetClosingTxid(),
		}
		s.setPendingDetails(ctx, item, ch.GetChannelPoint())
		pendingClosed = append(pendingClosed, item)
	}
	for _, fc := range resp.GetPendingForceClosingChannels() {
		ch := fc.GetChannel()
		blocks := fc.GetBlocksTilMaturity()
		if blocks <= 0 {
			blocks = findNextBlockMaturity(fc)
		}
		item := map[string]any{
			"remote_node_pub":     ch.GetRemoteNodePub(),
			"channel_point":       ch.GetChannelPoint(),
			"capacity":            ch.GetCapacity(),
			"local_balance":       ch.GetLocalBalance(),
			"remote_balance":      ch.GetRemoteBalance(),
			"initiator":           int32(ch.GetInitiator()),
			"commitment_type":     int32(ch.GetCommitmentType()),
			"closing_txid":        fc.GetClosingTxid(),
			"limbo_balance":       fc.GetLimboBalance(),
			"maturity_height":     fc.GetMaturityHeight(),
			"blocks_til_maturity": blocks,
			"maturity_datetime":   time.Now().Add(time.Duration(10*blocks) * time.Minute),
		}
		s.setPendingDetails(ctx, item, ch.GetChannelPoint())
		pendingForceClosed = append(pendingForceClosed, item)
	}
	for _, wc := range resp.GetWaitingCloseChannels() {
		ch := wc.GetChannel()
		item := map[string]any{
			"remote_node_pub":         ch.GetRemoteNodePub(),
			"channel_point":           ch.GetChannelPoint(),
			"capacity":                ch.GetCapacity(),
			"local_balance":           ch.GetLocalBalance(),
			"remote_balance":          ch.GetRemoteBalance(),
			"local_chan_reserve_sat":  ch.GetLocalChanReserveSat(),
			"remote_chan_reserve_sat": ch.GetRemoteChanReserveSat(),
			"initiator":               int32(ch.GetInitiator()),
			"commitment_type":         int32(ch.GetCommitmentType()),
			"local_commit_fee_sat":    wc.GetCommitments().GetLocalCommitFeeSat(),
			"limbo_balance":           wc.GetLimboBalance(),
			"closing_txid":            wc.GetClosingTxid(),
		}
		s.setPendingDetails(ctx, item, ch.GetChannelPoint())
		waitingForClose = append(waitingForClose, item)
	}

	closures, err := s.queryMaps(ctx,
		`SELECT c.*, COALESCE(ch.alias, '') AS alias, COALESCE(ch.short_chan_id, '') AS short_chan_id
            FROM gui_closures c LEFT JOIN gui_channels ch ON c.chan_id = ch.chan_id
            ORDER BY c.close_height DESC`)
	if err != nil {
		s.renderError(w, r, err.Error())
		return
	}
	for _, cl := range closures {
		if sc, _ := cl["short_chan_id"].(string); sc == "" {
			cl["short_chan_id"] = shortChanIDFromChanID(cl["chan_id"])
		}
	}

	s.renderTemplate(w, r, "closures.html", map[string]any{
		"pending_closed":       pendingClosed,
		"pending_force_closed": pendingForceClosed,
		"waiting_for_close":    waitingForClose,
		"closures":             closures,
	})
}
