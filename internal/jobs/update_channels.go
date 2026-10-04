package jobs

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// updateChannelsClient is the LND subset required by UpdateChannels.
type updateChannelsClient interface {
	ListChannels(ctx context.Context, in *lnrpc.ListChannelsRequest, opts ...grpc.CallOption) (*lnrpc.ListChannelsResponse, error)
	GetInfo(ctx context.Context, in *lnrpc.GetInfoRequest, opts ...grpc.CallOption) (*lnrpc.GetInfoResponse, error)
	GetChanInfo(ctx context.Context, in *lnrpc.ChanInfoRequest, opts ...grpc.CallOption) (*lnrpc.ChannelEdge, error)
	UpdateChannelPolicy(ctx context.Context, in *lnrpc.PolicyUpdateRequest, opts ...grpc.CallOption) (*lnrpc.PolicyUpdateResponse, error)
	DisconnectPeer(ctx context.Context, in *lnrpc.DisconnectPeerRequest, opts ...grpc.CallOption) (*lnrpc.DisconnectPeerResponse, error)
	GetNodeInfo(ctx context.Context, in *lnrpc.NodeInfoRequest, opts ...grpc.CallOption) (*lnrpc.NodeInfo, error)
}

// updateChannelsQuerier bundles all DB access required by UpdateChannels,
// including settings, node cache, channels, peers, pending HTLCs, and autofees.
type updateChannelsQuerier interface {
	// settings + node_cache
	GetLocalSetting(ctx context.Context, key string) (db.GuiLocalsetting, error)
	GetOrCreateLocalSetting(ctx context.Context, arg db.GetOrCreateLocalSettingParams) (db.GuiLocalsetting, error)
	GetNodeCache(ctx context.Context, pubkey string) (db.GuiNodecache, error)
	UpsertNodeCache(ctx context.Context, arg db.UpsertNodeCacheParams) error
	// channels
	GetChannel(ctx context.Context, chanID string) (db.GuiChannel, error)
	GetChannelAlias(ctx context.Context, chanID string) (string, error)
	InsertChannel(ctx context.Context, arg db.InsertChannelParams) error
	UpdateChannelSync(ctx context.Context, arg db.UpdateChannelSyncParams) error
	SyncChannelLocalPolicy(ctx context.Context, arg db.SyncChannelLocalPolicyParams) (int64, error)
	FillChannelDefaults(ctx context.Context, arg db.FillChannelDefaultsParams) error
	CloseMissingChannels(ctx context.Context, arg db.CloseMissingChannelsParams) error
	// peers
	GetPeer(ctx context.Context, pubkey string) (db.GuiPeer, error)
	GetPeerAlias(ctx context.Context, pubkey string) (pgtype.Text, error)
	SetPeerConnected(ctx context.Context, arg db.SetPeerConnectedParams) error
	SetPeerLastReconnected(ctx context.Context, arg db.SetPeerLastReconnectedParams) error
	// pending htlcs / channels
	DeleteAllPendingHTLCs(ctx context.Context) error
	InsertPendingHTLC(ctx context.Context, arg db.InsertPendingHTLCParams) error
	GetPendingChannelByFunding(ctx context.Context, arg db.GetPendingChannelByFundingParams) (db.GuiPendingchannel, error)
	DeletePendingChannel(ctx context.Context, arg db.DeletePendingChannelParams) error
	// events / autofees
	InsertPeerEvent(ctx context.Context, arg db.InsertPeerEventParams) error
	InsertAutofee(ctx context.Context, arg db.InsertAutofeeParams) error
}

func ptr64(v int64) *int64 { return &v }

// peerAliasGetter is the narrow DB subset required by peerAliasOpt.
type peerAliasGetter interface {
	GetPeerAlias(ctx context.Context, pubkey string) (pgtype.Text, error)
}

// peerAliasOpt returns the peer alias for pubkey. Returns ("", true, nil) when
// the peer does not exist or its alias column is NULL.
func peerAliasOpt(ctx context.Context, q peerAliasGetter, pubkey string) (string, bool, error) {
	a, err := q.GetPeerAlias(ctx, pubkey)
	if isNoRows(err) {
		return "", true, nil
	}
	if err != nil {
		return "", false, err
	}
	if !a.Valid {
		return "", true, nil
	}
	return a.String, false, nil
}

// UpdateChannels syncs ListChannels into the DB, records PeerEvents for
// connection and policy changes, parses routing policies via GetChanInfo,
// processes pending HTLCs and channels, and marks channels closed when they
// are no longer returned by LND.
func UpdateChannels(ctx context.Context, q updateChannelsQuerier, client updateChannelsClient) error {
	chanList := []string{}
	var channelsToCreate []db.GuiChannel
	var channelsToUpdate []db.GuiChannel
	var pendingHTLCs []db.InsertPendingHTLCParams

	resp, err := client.ListChannels(ctx, &lnrpc.ListChannelsRequest{})
	if err != nil {
		return err
	}
	channels := resp.GetChannels()
	if err := q.DeleteAllPendingHTLCs(ctx); err != nil {
		return err
	}
	getInfo, err := client.GetInfo(ctx, &lnrpc.GetInfoRequest{})
	if err != nil {
		return err
	}
	blockHeight := getInfo.GetBlockHeight()
	version := getInfo.GetVersion()

	for _, channel := range channels {
		isNew := false
		chanIDStr := formatChanID(channel.GetChanId())
		var ch, loaded db.GuiChannel
		var pendingChannel *db.GuiPendingchannel

		existing, gerr := q.GetChannel(ctx, chanIDStr)
		switch {
		case gerr == nil:
			ch = existing
			loaded = existing
			pa, isNone, perr := peerAliasOpt(ctx, q, channel.GetRemotePubkey())
			if perr != nil {
				return perr
			}
			if !isNone && pa != ch.Alias {
				ch.Alias = pa
			}
		case isNoRows(gerr):
			isNew = true
			alias := nodeAlias(ctx, q, client, channel.GetRemotePubkey())
			cp := channel.GetChannelPoint()
			parts := strings.Split(cp, ":")
			if len(parts) != 2 {
				return fmt.Errorf("invalid channel_point %q", cp)
			}
			idx, aerr := strconv.Atoi(parts[1])
			if aerr != nil {
				return aerr
			}
			cid := channel.GetChanId()
			now := time.Now()
			ch = db.GuiChannel{
				RemotePubkey: channel.GetRemotePubkey(),
				ChanID:       chanIDStr,
				ShortChanID:  fmt.Sprintf("%dx%dx%d", cid>>40, (cid>>16)&0xFFFFFF, cid&0xFFFF),
				Initiator:    channel.GetInitiator(),
				Alias:        alias,
				FundingTxid:  parts[0],
				OutputIndex:  int32(idx),
				Capacity:     channel.GetCapacity(),
				Private:      channel.GetPrivate(),
				PushAmt:      int64(channel.GetPushAmountSat()),
				CloseAddress: channel.GetCloseAddress(),
				// Field defaults for new channels.
				EpTarget:        50,
				EpIncPct:        10,
				EpCooldown:      10,
				EpLiveThreshold: 40,
				EpLiveIncPct:    5,
				FeesUpdated:     ts(now),
				EpUpdated:       ts(now),
				// offset_updated, maxhtlc_updated, htlc_boost_checked left NULL
			}
			pc, pcerr := q.GetPendingChannelByFunding(ctx, db.GetPendingChannelByFundingParams{FundingTxid: parts[0], OutputIndex: int32(idx)})
			if pcerr == nil {
				pendingChannel = &pc
			} else if !isNoRows(pcerr) {
				return pcerr
			}
		default:
			return gerr
		}

		// Update basic channel data
		ch.LocalBalance = channel.GetLocalBalance()
		ch.RemoteBalance = channel.GetRemoteBalance()
		ch.UnsettledBalance = channel.GetUnsettledBalance()
		ch.LocalCommit = int32(channel.GetCommitFee())
		ch.LocalChanReserve = int32(channel.GetLocalChanReserveSat())
		ch.NumUpdates = int32(channel.GetNumUpdates())
		ch.IsOpen = true
		ch.TotalSent = channel.GetTotalSatoshisSent()
		ch.TotalReceived = channel.GetTotalSatoshisReceived()

		var pendingOut, pendingIn int64
		htlcCounter := int32(0)
		for _, htlc := range channel.GetPendingHtlcs() {
			hashHex := hex.EncodeToString(htlc.GetHashLock())
			fwdAlias := "---"
			if a, aerr := q.GetChannelAlias(ctx, formatChanID(htlc.GetForwardingChannel())); aerr == nil {
				fwdAlias = a
			} else if !isNoRows(aerr) {
				return aerr
			}
			pendingHTLCs = append(pendingHTLCs, db.InsertPendingHTLCParams{
				ChanID:            ch.ChanID,
				Alias:             ch.Alias,
				Incoming:          htlc.GetIncoming(),
				Amount:            htlc.GetAmount(),
				HashLock:          hashHex,
				ExpirationHeight:  int32(htlc.GetExpirationHeight()),
				ForwardingChannel: formatChanID(htlc.GetForwardingChannel()),
				ForwardingAlias:   fwdAlias,
			})
			if htlc.GetIncoming() {
				pendingIn += htlc.GetAmount()
			} else {
				pendingOut += htlc.GetAmount()
			}
			htlcCounter++
			// HTLC expiring within 13 blocks -> disconnect peer to resolve.
			if int64(htlc.GetExpirationHeight())-int64(blockHeight) <= 13 {
				peer, perr := q.GetPeer(ctx, channel.GetRemotePubkey())
				hasPeer := false
				if perr == nil {
					hasPeer = true
				} else if !isNoRows(perr) {
					return perr
				}
				if hasPeer && (!peer.LastReconnected.Valid || int(time.Since(peer.LastReconnected.Time).Seconds()/60) > 10) {
					dataLog(fmt.Sprintf("HTLC expiring at %d and within 13 blocks of %d, disconnecting peer %s to resolve HTLC: %s ", htlc.GetExpirationHeight(), blockHeight, channel.GetRemotePubkey(), hashHex))
					disconnectPeer(ctx, q, client, peer)
					if e := q.SetPeerLastReconnected(ctx, db.SetPeerLastReconnectedParams{Pubkey: peer.Pubkey, LastReconnected: ts(time.Now())}); e != nil {
						return e
					}
				} else {
					dataLog(fmt.Sprintf("Could not find peer %s with expiring HTLC: %s", channel.GetRemotePubkey(), hashHex))
				}
			}
		}
		ch.PendingOutbound = pendingOut
		ch.PendingInbound = pendingIn
		ch.HtlcCount = htlcCounter

		outLiq := ch.LocalBalance + ch.PendingOutbound
		emit := func(event string, oldVal *int64, newVal int64) error {
			var ov pgtype.Int8
			if oldVal != nil {
				ov = pgtype.Int8{Int64: *oldVal, Valid: true}
			}
			return q.InsertPeerEvent(ctx, db.InsertPeerEventParams{
				Timestamp: ts(time.Now()),
				ChanID:    ch.ChanID,
				PeerAlias: ch.Alias,
				Event:     event,
				OldValue:  ov,
				NewValue:  newVal,
				OutLiq:    outLiq,
			})
		}

		// Emit a Connection event on first sync (isNew) or when active state changes.
		if isNew || ch.IsActive != channel.GetActive() {
			ch.LastUpdate = ts(time.Now())
			pa, isNone, perr := peerAliasOpt(ctx, q, ch.RemotePubkey)
			if perr != nil {
				return perr
			}
			if isNone {
				ch.Alias = ""
			} else {
				ch.Alias = pa
			}
			switch {
			case isNew:
				nv := int64(0)
				if channel.GetActive() {
					nv = 1
				}
				if e := emit("Connection", nil, nv); e != nil {
					return e
				}
			case channel.GetActive():
				if e := emit("Connection", ptr64(0), 1); e != nil {
					return e
				}
			default:
				if e := emit("Connection", ptr64(1), 0); e != nil {
					return e
				}
			}
			ch.IsActive = channel.GetActive()
		}

		var oldFeeRate *int
		var localFeeRatePolicy int64
		chanData, cierr := client.GetChanInfo(ctx, &lnrpc.ChanInfoRequest{ChanId: channel.GetChanId()})
		if cierr == nil {
			var local, remote *lnrpc.RoutingPolicy
			if chanData.GetNode1Pub() == channel.GetRemotePubkey() {
				local = chanData.GetNode2Policy()
				remote = chanData.GetNode1Policy()
			} else {
				local = chanData.GetNode1Policy()
				remote = chanData.GetNode2Policy()
			}
			if isNew {
				z := 0
				oldFeeRate = &z
			}
			ch.LocalBaseFee = int32(local.GetFeeBaseMsat())
			ch.LocalFeeRate = int32(local.GetFeeRateMilliMsat())
			localFeeRatePolicy = local.GetFeeRateMilliMsat()
			ch.LocalCltv = int32(local.GetTimeLockDelta())
			ch.LocalDisabled = local.GetDisabled()
			ch.LocalMinHtlcMsat = local.GetMinHtlc()
			ch.LocalMaxHtlcMsat = int64(local.GetMaxHtlcMsat())
			if versionFloat(version) >= 0.18 {
				ch.LocalInboundBaseFee = local.GetInboundFeeBaseMsat()
				ch.LocalInboundFeeRate = local.GetInboundFeeRateMilliMsat()
			} else {
				ch.LocalInboundBaseFee = 0
				ch.LocalInboundFeeRate = 0
			}

			if !isNew && ch.RemoteCltv == -1 {
				// First time remote policy data is available; emit all remote fields
				// with old_value=nil.
				if e := emit("BaseFee", nil, remote.GetFeeBaseMsat()); e != nil {
					return e
				}
				ch.RemoteBaseFee = int32(remote.GetFeeBaseMsat())
				if e := emit("FeeRate", nil, remote.GetFeeRateMilliMsat()); e != nil {
					return e
				}
				ch.RemoteFeeRate = int32(remote.GetFeeRateMilliMsat())
				if remote.GetDisabled() {
					if e := emit("Disabled", nil, 1); e != nil {
						return e
					}
				} else {
					if e := emit("Disabled", nil, 0); e != nil {
						return e
					}
				}
				ch.RemoteDisabled = remote.GetDisabled()
				if e := emit("CLTV", nil, int64(remote.GetTimeLockDelta())); e != nil {
					return e
				}
				ch.RemoteCltv = int32(remote.GetTimeLockDelta())
				if e := emit("MinHTLC", nil, remote.GetMinHtlc()); e != nil {
					return e
				}
				ch.RemoteMinHtlcMsat = remote.GetMinHtlc()
				if e := emit("MaxHTLC", nil, int64(remote.GetMaxHtlcMsat())); e != nil {
					return e
				}
				ch.RemoteMaxHtlcMsat = int64(remote.GetMaxHtlcMsat())
				if versionFloat(version) >= 0.18 {
					if e := emit("IncomingBaseFee", nil, int64(remote.GetInboundFeeBaseMsat())); e != nil {
						return e
					}
					ch.RemoteInboundBaseFee = remote.GetInboundFeeBaseMsat()
					if e := emit("IncomingFeeRate", nil, int64(remote.GetInboundFeeRateMilliMsat())); e != nil {
						return e
					}
					ch.RemoteInboundFeeRate = remote.GetInboundFeeRateMilliMsat()
				} else {
					ch.RemoteInboundBaseFee = 0
					ch.RemoteInboundFeeRate = 0
				}
			} else {
				// Detect changes in remote policy fields and emit events for each change.
				if isNew || int64(ch.RemoteBaseFee) != remote.GetFeeBaseMsat() {
					var old *int64
					if !isNew {
						old = ptr64(int64(ch.RemoteBaseFee))
					}
					if e := emit("BaseFee", old, remote.GetFeeBaseMsat()); e != nil {
						return e
					}
					ch.RemoteBaseFee = int32(remote.GetFeeBaseMsat())
				}
				if isNew || int64(ch.RemoteFeeRate) != remote.GetFeeRateMilliMsat() {
					var old *int64
					if !isNew {
						old = ptr64(int64(ch.RemoteFeeRate))
					}
					if e := emit("FeeRate", old, remote.GetFeeRateMilliMsat()); e != nil {
						return e
					}
					ch.RemoteFeeRate = int32(remote.GetFeeRateMilliMsat())
				}
				if isNew || ch.RemoteDisabled != remote.GetDisabled() {
					switch {
					case isNew:
						nv := int64(0)
						if remote.GetDisabled() {
							nv = 1
						}
						if e := emit("Disabled", nil, nv); e != nil {
							return e
						}
					case remote.GetDisabled():
						if e := emit("Disabled", ptr64(0), 1); e != nil {
							return e
						}
					default:
						if e := emit("Disabled", ptr64(1), 0); e != nil {
							return e
						}
					}
					ch.RemoteDisabled = remote.GetDisabled()
				}
				if isNew || int64(ch.RemoteCltv) != int64(remote.GetTimeLockDelta()) {
					var old *int64
					if !isNew {
						old = ptr64(int64(ch.RemoteCltv))
					}
					if e := emit("CLTV", old, int64(remote.GetTimeLockDelta())); e != nil {
						return e
					}
					ch.RemoteCltv = int32(remote.GetTimeLockDelta())
				}
				if isNew || ch.RemoteMinHtlcMsat != remote.GetMinHtlc() {
					var old *int64
					if !isNew {
						old = ptr64(ch.RemoteMinHtlcMsat)
					}
					if e := emit("MinHTLC", old, remote.GetMinHtlc()); e != nil {
						return e
					}
					ch.RemoteMinHtlcMsat = remote.GetMinHtlc()
				}
				if isNew || ch.RemoteMaxHtlcMsat != int64(remote.GetMaxHtlcMsat()) {
					var old *int64
					if !isNew {
						old = ptr64(ch.RemoteMaxHtlcMsat)
					}
					if e := emit("MaxHTLC", old, int64(remote.GetMaxHtlcMsat())); e != nil {
						return e
					}
					ch.RemoteMaxHtlcMsat = int64(remote.GetMaxHtlcMsat())
				}
				if versionFloat(version) >= 0.18 {
					if isNew || ch.RemoteInboundBaseFee != remote.GetInboundFeeBaseMsat() {
						var old *int64
						if !isNew {
							old = ptr64(int64(ch.RemoteInboundBaseFee))
						}
						if e := emit("IncomingBaseFee", old, int64(remote.GetInboundFeeBaseMsat())); e != nil {
							return e
						}
						ch.RemoteInboundBaseFee = remote.GetInboundFeeBaseMsat()
					}
					if isNew || ch.RemoteInboundFeeRate != remote.GetInboundFeeRateMilliMsat() {
						var old *int64
						if !isNew {
							old = ptr64(int64(ch.RemoteInboundFeeRate))
						}
						if e := emit("IncomingFeeRate", old, int64(remote.GetInboundFeeRateMilliMsat())); e != nil {
							return e
						}
						ch.RemoteInboundFeeRate = remote.GetInboundFeeRateMilliMsat()
					}
				} else {
					ch.RemoteInboundBaseFee = 0
					ch.RemoteInboundFeeRate = 0
				}
			}
		} else {
			// LND has not yet added the channel to its graph.
			dataLog(fmt.Sprintf("Error getting graph data for channel %s: %s", ch.ChanID, grpcErrMsg(cierr)))
			if pendingChannel != nil {
				dataLog(fmt.Sprintf("Waiting for pending channel %s to be added to the graph...", ch.ChanID))
				continue
			}
			oldFeeRate = nil
			// Initialize policy fields to -1/false for new channels not yet in the graph.
			if isNew {
				ch.LocalBaseFee = -1
				ch.LocalFeeRate = -1
				ch.LocalCltv = -1
				ch.LocalDisabled = false
				ch.LocalMinHtlcMsat = -1
				ch.LocalMaxHtlcMsat = -1
				ch.RemoteBaseFee = -1
				ch.RemoteFeeRate = -1
				ch.RemoteCltv = -1
				ch.RemoteDisabled = false
				ch.RemoteMinHtlcMsat = -1
				ch.RemoteMaxHtlcMsat = -1
				ch.LocalInboundBaseFee = -1
				ch.LocalInboundFeeRate = -1
				ch.RemoteInboundBaseFee = -1
				ch.RemoteInboundFeeRate = -1
			}
		}

		// Apply pending channel settings (only set for new channels with a pending record).
		autoFeesIsNone := isNew
		if pendingChannel != nil {
			pc := pendingChannel
			pbf := pc.LocalBaseFee.Valid && pc.LocalBaseFee.Int32 != 0
			pfr := pc.LocalFeeRate.Valid && pc.LocalFeeRate.Int32 != 0
			pcltv := pc.LocalCltv.Valid && pc.LocalCltv.Int32 != 0
			if pbf || pfr || pcltv {
				baseFee := ch.LocalBaseFee
				if pbf {
					baseFee = pc.LocalBaseFee.Int32
				}
				feeRate := ch.LocalFeeRate
				if pfr {
					feeRate = pc.LocalFeeRate.Int32
				}
				cltv := ch.LocalCltv
				if pcltv {
					cltv = pc.LocalCltv.Int32
				}
				if _, e := client.UpdateChannelPolicy(ctx, &lnrpc.PolicyUpdateRequest{
					Scope:         &lnrpc.PolicyUpdateRequest_ChanPoint{ChanPoint: channelPoint(ch.FundingTxid, ch.OutputIndex)},
					BaseFeeMsat:   int64(baseFee),
					FeeRate:       float64(feeRate) / 1000000,
					TimeLockDelta: uint32(cltv),
				}); e != nil {
					return e
				}
				ch.LocalBaseFee = baseFee
				ch.LocalFeeRate = feeRate
				ch.LocalCltv = cltv
				ch.FeesUpdated = ts(time.Now())
			}
			if pc.AutoRebalance.Valid {
				ch.AutoRebalance = pc.AutoRebalance.Bool
			}
			if pc.ArAmtTarget.Valid && pc.ArAmtTarget.Int64 != 0 {
				ch.ArAmtTarget = pc.ArAmtTarget.Int64
			}
			if pc.ArInTarget.Valid && pc.ArInTarget.Int32 != 0 {
				ch.ArInTarget = pc.ArInTarget.Int32
			}
			if pc.ArOutTarget.Valid && pc.ArOutTarget.Int32 != 0 {
				ch.ArOutTarget = pc.ArOutTarget.Int32
			}
			if pc.ArMaxCost.Valid && pc.ArMaxCost.Int32 != 0 {
				ch.ArMaxCost = pc.ArMaxCost.Int32
			}
			if pc.AutoFees.Valid {
				ch.AutoFees = pc.AutoFees.Bool
				autoFeesIsNone = false
			}
			if e := q.DeletePendingChannel(ctx, db.DeletePendingChannelParams{FundingTxid: ch.FundingTxid, OutputIndex: ch.OutputIndex}); e != nil {
				return e
			}
		}

		if e := applyChannelDefaults(ctx, q, &ch, autoFeesIsNone); e != nil {
			return e
		}
		if isNew {
			if oldFeeRate != nil && int64(*oldFeeRate) != localFeeRatePolicy {
				if e := logExtFeeChange(ctx, q, ch, int32(*oldFeeRate)); e != nil {
					return e
				}
			}
			channelsToCreate = append(channelsToCreate, ch)
		} else {
			if e := syncLocalPolicy(ctx, q, ch, loaded); e != nil {
				return e
			}
			if ch.ArOutTarget != loaded.ArOutTarget || ch.ArInTarget != loaded.ArInTarget ||
				ch.ArAmtTarget != loaded.ArAmtTarget || ch.ArMaxCost != loaded.ArMaxCost {
				if e := q.FillChannelDefaults(ctx, db.FillChannelDefaultsParams{
					ChanID: ch.ChanID, ArOutTarget: ch.ArOutTarget, ArInTarget: ch.ArInTarget,
					ArAmtTarget: ch.ArAmtTarget, ArMaxCost: ch.ArMaxCost,
				}); e != nil {
					return e
				}
			}
			channelsToUpdate = append(channelsToUpdate, ch)
		}
		chanList = append(chanList, chanIDStr)
	}

	// Mark channels closed: open in the DB but absent from the LND channel list.
	if e := q.CloseMissingChannels(ctx, db.CloseMissingChannelsParams{
		LastUpdate: ts(time.Now()), ListedChanIds: chanList,
	}); e != nil {
		return e
	}

	// Write all pending inserts and updates.
	for _, p := range pendingHTLCs {
		if e := q.InsertPendingHTLC(ctx, p); e != nil {
			return e
		}
	}
	for i := range channelsToCreate {
		if e := q.InsertChannel(ctx, insertChannelParamsFrom(channelsToCreate[i])); e != nil {
			return e
		}
	}
	for i := range channelsToUpdate {
		if e := q.UpdateChannelSync(ctx, updateChannelSyncParamsFrom(channelsToUpdate[i])); e != nil {
			return e
		}
	}
	return nil
}

// insertChannelParamsFrom builds the INSERT params for a new channel row.
func insertChannelParamsFrom(ch db.GuiChannel) db.InsertChannelParams {
	return db.InsertChannelParams{
		ChanID:               ch.ChanID,
		RemotePubkey:         ch.RemotePubkey,
		FundingTxid:          ch.FundingTxid,
		OutputIndex:          ch.OutputIndex,
		Capacity:             ch.Capacity,
		LocalBalance:         ch.LocalBalance,
		RemoteBalance:        ch.RemoteBalance,
		UnsettledBalance:     ch.UnsettledBalance,
		Initiator:            ch.Initiator,
		Alias:                ch.Alias,
		LocalBaseFee:         ch.LocalBaseFee,
		LocalFeeRate:         ch.LocalFeeRate,
		IsActive:             ch.IsActive,
		IsOpen:               ch.IsOpen,
		AutoRebalance:        ch.AutoRebalance,
		RemoteBaseFee:        ch.RemoteBaseFee,
		RemoteFeeRate:        ch.RemoteFeeRate,
		LocalCommit:          ch.LocalCommit,
		LocalChanReserve:     ch.LocalChanReserve,
		ArInTarget:           ch.ArInTarget,
		NumUpdates:           ch.NumUpdates,
		ArAmtTarget:          ch.ArAmtTarget,
		ArOutTarget:          ch.ArOutTarget,
		ArMaxCost:            ch.ArMaxCost,
		LastUpdate:           ch.LastUpdate,
		LocalDisabled:        ch.LocalDisabled,
		RemoteDisabled:       ch.RemoteDisabled,
		HtlcCount:            ch.HtlcCount,
		PendingInbound:       ch.PendingInbound,
		PendingOutbound:      ch.PendingOutbound,
		Private:              ch.Private,
		TotalReceived:        ch.TotalReceived,
		TotalSent:            ch.TotalSent,
		FeesUpdated:          ch.FeesUpdated,
		AutoFees:             ch.AutoFees,
		LocalCltv:            ch.LocalCltv,
		RemoteCltv:           ch.RemoteCltv,
		LocalMaxHtlcMsat:     ch.LocalMaxHtlcMsat,
		LocalMinHtlcMsat:     ch.LocalMinHtlcMsat,
		RemoteMaxHtlcMsat:    ch.RemoteMaxHtlcMsat,
		RemoteMinHtlcMsat:    ch.RemoteMinHtlcMsat,
		ShortChanID:          ch.ShortChanID,
		Notes:                ch.Notes,
		CloseAddress:         ch.CloseAddress,
		PushAmt:              ch.PushAmt,
		LocalInboundBaseFee:  ch.LocalInboundBaseFee,
		LocalInboundFeeRate:  ch.LocalInboundFeeRate,
		RemoteInboundBaseFee: ch.RemoteInboundBaseFee,
		RemoteInboundFeeRate: ch.RemoteInboundFeeRate,
		ArSource:             ch.ArSource,
		ArSourcePpmDiff:      ch.ArSourcePpmDiff,
		InboundOffset:        ch.InboundOffset,
		OffsetUpdated:        ch.OffsetUpdated,
		MaxhtlcPercent:       ch.MaxhtlcPercent,
		MaxhtlcUpdated:       ch.MaxhtlcUpdated,
		MxLiqThreshold:       ch.MxLiqThreshold,
		MxLiqValue:           ch.MxLiqValue,
		MxLiqUpper:           ch.MxLiqUpper,
		EpTarget:             ch.EpTarget,
		EpUpdated:            ch.EpUpdated,
		EpEnabled:            ch.EpEnabled,
		EpIncPct:             ch.EpIncPct,
		EpCooldown:           ch.EpCooldown,
		EpLiveThreshold:      ch.EpLiveThreshold,
		EpLiveIncPct:         ch.EpLiveIncPct,
		FlpEnabled:           ch.FlpEnabled,
		FlpSafety:            ch.FlpSafety,
		HtlcBoostChecked:     ch.HtlcBoostChecked,
	}
}

// updateChannelSyncParamsFrom builds the UPDATE params for a channel sync (LND state only).
func updateChannelSyncParamsFrom(ch db.GuiChannel) db.UpdateChannelSyncParams {
	return db.UpdateChannelSyncParams{
		ChanID:               ch.ChanID,
		RemotePubkey:         ch.RemotePubkey,
		ShortChanID:          ch.ShortChanID,
		FundingTxid:          ch.FundingTxid,
		OutputIndex:          ch.OutputIndex,
		Capacity:             ch.Capacity,
		LocalBalance:         ch.LocalBalance,
		RemoteBalance:        ch.RemoteBalance,
		UnsettledBalance:     ch.UnsettledBalance,
		LocalCommit:          ch.LocalCommit,
		LocalChanReserve:     ch.LocalChanReserve,
		NumUpdates:           ch.NumUpdates,
		Initiator:            ch.Initiator,
		Alias:                ch.Alias,
		TotalSent:            ch.TotalSent,
		TotalReceived:        ch.TotalReceived,
		Private:              ch.Private,
		PendingOutbound:      ch.PendingOutbound,
		PendingInbound:       ch.PendingInbound,
		HtlcCount:            ch.HtlcCount,
		RemoteBaseFee:        ch.RemoteBaseFee,
		RemoteFeeRate:        ch.RemoteFeeRate,
		RemoteInboundBaseFee: ch.RemoteInboundBaseFee,
		RemoteInboundFeeRate: ch.RemoteInboundFeeRate,
		RemoteDisabled:       ch.RemoteDisabled,
		RemoteCltv:           ch.RemoteCltv,
		RemoteMinHtlcMsat:    ch.RemoteMinHtlcMsat,
		RemoteMaxHtlcMsat:    ch.RemoteMaxHtlcMsat,
		PushAmt:              ch.PushAmt,
		CloseAddress:         ch.CloseAddress,
		IsActive:             ch.IsActive,
		IsOpen:               ch.IsOpen,
		LastUpdate:           ch.LastUpdate,
	}
}

// syncLocalPolicy persists the policy LND reports for an existing channel only while
// the DB still holds what this sync loaded: a UI / auto-fees write made meanwhile wins.
// An external fee change is logged once, when it is actually written.
func syncLocalPolicy(ctx context.Context, q updateChannelsQuerier, ch, loaded db.GuiChannel) error {
	if ch.LocalBaseFee == loaded.LocalBaseFee && ch.LocalFeeRate == loaded.LocalFeeRate &&
		ch.LocalInboundBaseFee == loaded.LocalInboundBaseFee && ch.LocalInboundFeeRate == loaded.LocalInboundFeeRate &&
		ch.LocalCltv == loaded.LocalCltv && ch.LocalMinHtlcMsat == loaded.LocalMinHtlcMsat &&
		ch.LocalMaxHtlcMsat == loaded.LocalMaxHtlcMsat && ch.LocalDisabled == loaded.LocalDisabled {
		return nil
	}
	feeChanged := ch.LocalFeeRate != loaded.LocalFeeRate
	n, err := q.SyncChannelLocalPolicy(ctx, db.SyncChannelLocalPolicyParams{
		ChanID:                 ch.ChanID,
		LocalBaseFee:           ch.LocalBaseFee,
		LocalFeeRate:           ch.LocalFeeRate,
		LocalInboundBaseFee:    ch.LocalInboundBaseFee,
		LocalInboundFeeRate:    ch.LocalInboundFeeRate,
		LocalCltv:              ch.LocalCltv,
		LocalMinHtlcMsat:       ch.LocalMinHtlcMsat,
		LocalMaxHtlcMsat:       ch.LocalMaxHtlcMsat,
		LocalDisabled:          ch.LocalDisabled,
		FeeChanged:             feeChanged,
		Now:                    ts(time.Now()),
		OldLocalBaseFee:        loaded.LocalBaseFee,
		OldLocalFeeRate:        loaded.LocalFeeRate,
		OldLocalInboundBaseFee: loaded.LocalInboundBaseFee,
		OldLocalInboundFeeRate: loaded.LocalInboundFeeRate,
		OldLocalCltv:           loaded.LocalCltv,
		OldLocalMinHtlcMsat:    loaded.LocalMinHtlcMsat,
		OldLocalMaxHtlcMsat:    loaded.LocalMaxHtlcMsat,
		OldLocalDisabled:       loaded.LocalDisabled,
	})
	if err != nil {
		return err
	}
	if n > 0 && feeChanged {
		return logExtFeeChange(ctx, q, ch, loaded.LocalFeeRate)
	}
	return nil
}

// logExtFeeChange records a fee change made outside LNDg in the autofees log.
func logExtFeeChange(ctx context.Context, q updateChannelsQuerier, ch db.GuiChannel, oldFeeRate int32) error {
	dataLog(fmt.Sprintf("Ext fee change detected on %s for peer %s: fee updated from %d to %d", ch.ChanID, ch.Alias, oldFeeRate, ch.LocalFeeRate))
	return q.InsertAutofee(ctx, db.InsertAutofeeParams{
		Timestamp: ts(time.Now()),
		ChanID:    ch.ChanID,
		PeerAlias: ch.Alias,
		Setting:   "Ext",
		OldValue:  oldFeeRate,
		NewValue:  ch.LocalFeeRate,
	})
}
