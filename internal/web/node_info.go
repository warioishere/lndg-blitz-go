package web

import (
	"context"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/pyround"
)

// handleNodeInfo returns node status including pending channels and wallet balance.
// An RPC error produces a JSON {"detail":...} 500 response. The db_size field
// reflects the local file size of the channel database at LND_DATABASE_PATH only;
// remote filesystem paths are not supported.
func (s *Server) handleNodeInfo(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	info, err := s.lnd.Lightning.GetInfo(ctx, &lnrpc.GetInfoRequest{})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": grpcErrorMsg(err)})
		return
	}
	balances, err := s.lnd.Lightning.WalletBalance(ctx, &lnrpc.WalletBalanceRequest{})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": grpcErrorMsg(err)})
		return
	}
	pending, err := s.lnd.Lightning.PendingChannels(ctx, &lnrpc.PendingChannelsRequest{})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": grpcErrorMsg(err)})
		return
	}

	limbo := pending.GetTotalLimboBalance()
	var pendingOpen, pendingForceClosed, waitingForClose any
	var pendingOpenBalance, pendingClosingBalance int64

	if len(pending.GetPendingOpenChannels()) > 0 {
		inboundSetting := s.settingIntDefault(ctx, "AR-Inbound%", 90)
		outboundSetting := s.settingIntDefault(ctx, "AR-Outbound%", 75)
		amtSetting := s.settingFloatDefault(ctx, "AR-Target%", 3)
		costSetting := s.settingIntDefault(ctx, "AR-MaxCost%", 65)
		autoFeesSetting := s.settingIntDefault(ctx, "AF-Enabled", 0)

		items := make([]any, 0, len(pending.GetPendingOpenChannels()))
		for _, oc := range pending.GetPendingOpenChannels() {
			ch := oc.GetChannel()
			pendingOpenBalance += ch.GetLocalBalance()
			fundingTxid, outputIndex := splitChannelPoint(ch.GetChannelPoint())
			pc, updated := s.pendingChannelRow(ctx, fundingTxid, outputIndex)

			item := newOrderedMap().
				Set("alias", s.peerAlias(ctx, ch.GetRemoteNodePub(), "")).
				Set("remote_node_pub", ch.GetRemoteNodePub()).
				Set("channel_point", ch.GetChannelPoint()).
				Set("funding_txid", fundingTxid).
				Set("output_index", outputIndex).
				Set("capacity", ch.GetCapacity()).
				Set("local_balance", ch.GetLocalBalance()).
				Set("remote_balance", ch.GetRemoteBalance()).
				Set("local_chan_reserve_sat", ch.GetLocalChanReserveSat()).
				Set("remote_chan_reserve_sat", ch.GetRemoteChanReserveSat()).
				Set("initiator", int32(ch.GetInitiator())).
				Set("commitment_type", int32(ch.GetCommitmentType())).
				Set("commit_fee", oc.GetCommitFee()).
				Set("commit_weight", oc.GetCommitWeight()).
				Set("fee_per_kw", oc.GetFeePerKw())

			// When a pending channel row exists, use its stored values (which may be NULL);
			// otherwise emit an empty string to indicate no override is set.
			if updated {
				item.Set("local_base_fee", int4Val(pc.LocalBaseFee))
				item.Set("local_fee_rate", int4Val(pc.LocalFeeRate))
				item.Set("local_cltv", int4Val(pc.LocalCltv))
			} else {
				item.Set("local_base_fee", "")
				item.Set("local_fee_rate", "")
				item.Set("local_cltv", "")
			}
			// AR/AF fields: use the stored value if available, otherwise fall back to the global setting default.
			if updated && pc.AutoRebalance.Valid {
				item.Set("auto_rebalance", pc.AutoRebalance.Bool)
			} else {
				item.Set("auto_rebalance", false)
			}
			if updated && pc.ArAmtTarget.Valid {
				item.Set("ar_amt_target", pc.ArAmtTarget.Int64)
			} else {
				item.Set("ar_amt_target", int64(amtSetting/100*float64(ch.GetCapacity())))
			}
			if updated && pc.ArInTarget.Valid {
				item.Set("ar_in_target", pc.ArInTarget.Int32)
			} else {
				item.Set("ar_in_target", inboundSetting)
			}
			if updated && pc.ArOutTarget.Valid {
				item.Set("ar_out_target", pc.ArOutTarget.Int32)
			} else {
				item.Set("ar_out_target", outboundSetting)
			}
			if updated && pc.ArMaxCost.Valid {
				item.Set("ar_max_cost", pc.ArMaxCost.Int32)
			} else {
				item.Set("ar_max_cost", costSetting)
			}
			if updated && pc.AutoFees.Valid {
				item.Set("auto_fees", pc.AutoFees.Bool)
			} else {
				item.Set("auto_fees", autoFeesSetting != 0)
			}
			items = append(items, item)
		}
		pendingOpen = items
	}

	if len(pending.GetPendingForceClosingChannels()) > 0 {
		items := make([]any, 0)
		for _, fc := range pending.GetPendingForceClosingChannels() {
			ch := fc.GetChannel()
			blocks := fc.GetBlocksTilMaturity()
			if blocks <= 0 {
				blocks = findNextBlockMaturity(fc)
			}
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
				Set("blocks_til_maturity", blocks).
				Set("maturity_datetime", isoformatUTC(maturity))
			s.addPendingChannelDetails(ctx, item, ch.GetChannelPoint())
			items = append(items, item)
		}
		pendingForceClosed = items
	}

	if len(pending.GetWaitingCloseChannels()) > 0 {
		items := make([]any, 0)
		for _, wc := range pending.GetWaitingCloseChannels() {
			ch := wc.GetChannel()
			pendingClosingBalance += wc.GetLimboBalance()
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
				Set("local_commit_fee_sat", wc.GetCommitments().GetLocalCommitFeeSat()).
				Set("limbo_balance", wc.GetLimboBalance()).
				Set("closing_txid", wc.GetClosingTxid())
			s.addPendingChannelDetails(ctx, item, ch.GetChannelPoint())
			items = append(items, item)
		}
		waitingForClose = items
	}

	limbo -= pendingClosingBalance

	chains := make([]string, 0, len(info.GetChains()))
	for _, c := range info.GetChains() {
		chains = append(chains, c.GetChain()+"-"+c.GetNetwork())
	}

	balance := newOrderedMap().
		Set("limbo", limbo).
		Set("onchain", balances.GetTotalBalance()).
		Set("confirmed", balances.GetConfirmedBalance()).
		Set("unconfirmed", balances.GetUnconfirmedBalance()).
		Set("total", balances.GetTotalBalance()+pendingOpenBalance+limbo)

	resp := newOrderedMap().
		Set("version", info.GetVersion()).
		Set("num_peers", info.GetNumPeers()).
		Set("synced_to_graph", info.GetSyncedToGraph()).
		Set("synced_to_chain", info.GetSyncedToChain()).
		Set("num_active_channels", info.GetNumActiveChannels()).
		Set("num_inactive_channels", info.GetNumInactiveChannels()).
		Set("chains", chains).
		Set("block", newOrderedMap().Set("hash", info.GetBlockHash()).Set("height", info.GetBlockHeight())).
		Set("balance", balance).
		Set("pending_open", pendingOpen).
		Set("pending_force_closed", pendingForceClosed).
		Set("waiting_for_close", waitingForClose).
		Set("db_size", channelDBSize(s.cfg.LND_DATABASE_PATH))
	writeJSON(w, http.StatusOK, resp)
}

// findNextBlockMaturity returns the number of blocks until maturity for a force-closed
// channel: blocks_til_maturity if positive, otherwise the first HTLC with a positive
// value, otherwise -1.
func findNextBlockMaturity(fc *lnrpc.PendingChannelsResponse_ForceClosedChannel) int32 {
	if fc.GetBlocksTilMaturity() > 0 {
		return fc.GetBlocksTilMaturity()
	}
	for _, htlc := range fc.GetPendingHtlcs() {
		if htlc.GetBlocksTilMaturity() > 0 {
			return htlc.GetBlocksTilMaturity()
		}
	}
	return -1
}

// pendingChannelRow loads the gui_pendingchannels override row for the given
// funding_txid and output_index. Returns updated=false when no row exists.
func (s *Server) pendingChannelRow(ctx context.Context, fundingTxid, outputIndex string) (db_GuiPendingchannel, bool) {
	idx, err := strconv.Atoi(outputIndex)
	if err != nil {
		return db_GuiPendingchannel{}, false
	}
	var pc db_GuiPendingchannel
	row := s.db.QueryRow(ctx,
		`SELECT local_base_fee, local_fee_rate, local_cltv, auto_rebalance,
		        ar_amt_target, ar_in_target, ar_out_target, ar_max_cost, auto_fees
		 FROM gui_pendingchannels WHERE funding_txid=$1 AND output_index=$2`, fundingTxid, idx)
	err = row.Scan(&pc.LocalBaseFee, &pc.LocalFeeRate, &pc.LocalCltv, &pc.AutoRebalance,
		&pc.ArAmtTarget, &pc.ArInTarget, &pc.ArOutTarget, &pc.ArMaxCost, &pc.AutoFees)
	if err != nil {
		return db_GuiPendingchannel{}, false
	}
	return pc, true
}

// db_GuiPendingchannel holds the subset of override columns from gui_pendingchannels.
type db_GuiPendingchannel struct {
	LocalBaseFee  pgtype.Int4
	LocalFeeRate  pgtype.Int4
	LocalCltv     pgtype.Int4
	AutoRebalance pgtype.Bool
	ArAmtTarget   pgtype.Int8
	ArInTarget    pgtype.Int4
	ArOutTarget   pgtype.Int4
	ArMaxCost     pgtype.Int4
	AutoFees      pgtype.Bool
}

// peerAlias returns the alias of a peer from gui_peers (nil when the column
// is NULL), or missing when the peer does not exist.
func (s *Server) peerAlias(ctx context.Context, pubkey string, missing any) any {
	var alias pgtype.Text
	if err := s.db.QueryRow(ctx, `SELECT alias FROM gui_peers WHERE pubkey=$1`, pubkey).Scan(&alias); err != nil {
		return missing
	}
	if !alias.Valid {
		return nil
	}
	return alias.String
}

// addPendingChannelDetails looks up short_chan_id, chan_id, and alias from
// gui_channels by funding_txid and output_index, setting null on the map when
// no matching row is found.
func (s *Server) addPendingChannelDetails(ctx context.Context, m *orderedMap, channelPoint string) {
	fundingTxid, outputIndex := splitChannelPoint(channelPoint)
	idx, err := strconv.Atoi(outputIndex)
	if err == nil {
		var short, chanID, alias string
		qerr := s.db.QueryRow(ctx,
			`SELECT short_chan_id, chan_id, alias FROM gui_channels
			 WHERE funding_txid=$1 AND output_index=$2`, fundingTxid, idx).Scan(&short, &chanID, &alias)
		if qerr == nil {
			m.Set("short_chan_id", short).Set("chan_id", chanID).Set("alias", alias)
			return
		}
		if qerr != pgx.ErrNoRows {
			// On unexpected errors, fall through and emit null fields.
			_ = qerr
		}
	}
	m.Set("short_chan_id", nil).Set("chan_id", nil).Set("alias", nil)
}

func (s *Server) settingIntDefault(ctx context.Context, key string, def int) int {
	ls, err := s.queries.GetLocalSetting(ctx, key)
	if err != nil {
		return def
	}
	n, err := strconv.Atoi(ls.Value)
	if err != nil {
		return def
	}
	return n
}

func (s *Server) settingFloatDefault(ctx context.Context, key string, def float64) float64 {
	ls, err := s.queries.GetLocalSetting(ctx, key)
	if err != nil {
		return def
	}
	f, err := strconv.ParseFloat(ls.Value, 64)
	if err != nil {
		return def
	}
	return f
}

func splitChannelPoint(cp string) (txid, index string) {
	if i := strings.IndexByte(cp, ':'); i >= 0 {
		return cp[:i], cp[i+1:]
	}
	return cp, ""
}

func int4Val(v pgtype.Int4) any {
	if v.Valid {
		return v.Int32
	}
	return nil
}

// channelDBSize returns the channel database file size in gigabytes, rounded
// to 3 decimal places; the integer 0, as in Python, if the file cannot be stat'd.
func channelDBSize(path string) any {
	fi, err := os.Stat(expandHome(path))
	if err != nil {
		return 0
	}
	gb := float64(fi.Size()) * 0.000000001
	return pyround.Round(gb, 3)
}

func expandHome(p string) string {
	if strings.HasPrefix(p, "~") {
		if home, err := os.UserHomeDir(); err == nil {
			return home + p[1:]
		}
	}
	return p
}
