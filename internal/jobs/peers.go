package jobs

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	lnd "github.com/warioishere/lndg-blitz-go/internal/lnd"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// peerClient is the LND subset required by the peer jobs (including GetNodeInfo
// for node cache lookups).
type peerClient interface {
	ListPeers(ctx context.Context, in *lnrpc.ListPeersRequest, opts ...grpc.CallOption) (*lnrpc.ListPeersResponse, error)
	ConnectPeer(ctx context.Context, in *lnrpc.ConnectPeerRequest, opts ...grpc.CallOption) (*lnrpc.ConnectPeerResponse, error)
	DisconnectPeer(ctx context.Context, in *lnrpc.DisconnectPeerRequest, opts ...grpc.CallOption) (*lnrpc.DisconnectPeerResponse, error)
	GetNodeInfo(ctx context.Context, in *lnrpc.NodeInfoRequest, opts ...grpc.CallOption) (*lnrpc.NodeInfo, error)
}

// peerQuerier bundles peer DB access and node cache queries.
type peerQuerier interface {
	// node cache (lnd.GetNodeInfoCached)
	GetLocalSetting(ctx context.Context, key string) (db.GuiLocalsetting, error)
	GetNodeCache(ctx context.Context, pubkey string) (db.GuiNodecache, error)
	UpsertNodeCache(ctx context.Context, arg db.UpsertNodeCacheParams) error
	// peers
	GetPeer(ctx context.Context, pubkey string) (db.GuiPeer, error)
	InsertPeer(ctx context.Context, arg db.InsertPeerParams) error
	UpdatePeerData(ctx context.Context, arg db.UpdatePeerDataParams) error
	DisconnectStalePeers(ctx context.Context, pubkeys []string) error
	ListPeersNoAlias(ctx context.Context) ([]db.GuiPeer, error)
	UpdatePeerAlias(ctx context.Context, arg db.UpdatePeerAliasParams) error
	SetPeerConnected(ctx context.Context, arg db.SetPeerConnectedParams) error
	SetPeerLastReconnected(ctx context.Context, arg db.SetPeerLastReconnectedParams) error
	ListInactivePeerPubkeys(ctx context.Context) ([]string, error)
}

// getNodeAlias is a convenience wrapper around nodeAlias for peer-scoped callers.
func getNodeAlias(ctx context.Context, q peerQuerier, client peerClient, pubkey string) string {
	return nodeAlias(ctx, q, client, pubkey)
}

// aliasOrNone returns the alias string, or the literal "None" when the value is NULL.
func aliasOrNone(a pgtype.Text) string {
	if !a.Valid {
		return "None"
	}
	return a.String
}

// UpdatePeers syncs the connected peer list from LND into the DB and marks
// peers no longer connected as disconnected.
func UpdatePeers(ctx context.Context, q peerQuerier, client peerClient) error {
	resp, err := client.ListPeers(ctx, &lnrpc.ListPeersRequest{LatestError: true})
	if err != nil {
		return err
	}
	peerList := make([]string, 0, len(resp.Peers))
	for _, peer := range resp.Peers {
		pingTime := int64(math.RoundToEven(float64(peer.PingTime) / 1000))
		existing, gerr := q.GetPeer(ctx, peer.PubKey)
		switch {
		case gerr == nil:
			alias := getNodeAlias(ctx, q, client, peer.PubKey)
			newAlias := existing.Alias
			if alias != "" && (!existing.Alias.Valid || existing.Alias.String == "" || existing.Alias.String != alias) {
				newAlias = pgtype.Text{String: alias, Valid: true}
			}
			if e := q.UpdatePeerData(ctx, db.UpdatePeerDataParams{
				Pubkey: peer.PubKey, Address: peer.Address, SatSent: peer.SatSent, SatRecv: peer.SatRecv,
				Inbound: peer.Inbound, PingTime: pingTime, Alias: newAlias, Connected: true,
			}); e != nil {
				return e
			}
		case isNoRows(gerr):
			alias := getNodeAlias(ctx, q, client, peer.PubKey)
			if e := q.InsertPeer(ctx, db.InsertPeerParams{
				Pubkey: peer.PubKey, Address: peer.Address, SatSent: peer.SatSent, SatRecv: peer.SatRecv,
				Inbound: peer.Inbound, PingTime: pingTime, Alias: pgtype.Text{String: alias, Valid: true}, Connected: true,
			}); e != nil {
				return e
			}
		default:
			return gerr
		}
		peerList = append(peerList, peer.PubKey)
	}
	return q.DisconnectStalePeers(ctx, peerList)
}

// RefreshPeerAliases resolves missing aliases for peers that have no alias set.
func RefreshPeerAliases(ctx context.Context, q peerQuerier, client peerClient) error {
	peers, err := q.ListPeersNoAlias(ctx)
	if err != nil {
		return err
	}
	for _, peer := range peers {
		alias := getNodeAlias(ctx, q, client, peer.Pubkey)
		if alias != "" {
			if e := q.UpdatePeerAlias(ctx, db.UpdatePeerAliasParams{
				Pubkey: peer.Pubkey, Alias: pgtype.Text{String: alias, Valid: true},
			}); e != nil {
				return e
			}
		}
	}
	return nil
}

// disconnectQuerier is the narrow DB subset required by disconnectPeer.
type disconnectQuerier interface {
	SetPeerConnected(ctx context.Context, arg db.SetPeerConnectedParams) error
}

// disconnectClient is the narrow LND subset required by disconnectPeer.
type disconnectClient interface {
	DisconnectPeer(ctx context.Context, in *lnrpc.DisconnectPeerRequest, opts ...grpc.CallOption) (*lnrpc.DisconnectPeerResponse, error)
}

// disconnectPeer disconnects a peer via LND and marks it as disconnected in the DB.
func disconnectPeer(ctx context.Context, q disconnectQuerier, client disconnectClient, peer db.GuiPeer) {
	if _, err := client.DisconnectPeer(ctx, &lnrpc.DisconnectPeerRequest{PubKey: peer.Pubkey}); err != nil {
		dataLog(fmt.Sprintf("Error disconnecting peer %s %s: %s", aliasOrNone(peer.Alias), peer.Pubkey, grpcErrMsg(err)))
		return
	}
	dataLog(fmt.Sprintf("Disconnected peer %s %s", aliasOrNone(peer.Alias), peer.Pubkey))
	_ = q.SetPeerConnected(ctx, db.SetPeerConnectedParams{Pubkey: peer.Pubkey, Connected: false})
}

// ReconnectPeers attempts to reconnect peers whose channels are inactive and
// that have not been reconnected within the last two minutes.
func ReconnectPeers(ctx context.Context, q peerQuerier, client peerClient) error {
	pubkeys, err := q.ListInactivePeerPubkeys(ctx)
	if err != nil {
		return err
	}
	now := time.Now()
	for _, pk := range pubkeys {
		peer, gerr := q.GetPeer(ctx, pk)
		if isNoRows(gerr) {
			continue
		}
		if gerr != nil {
			return gerr
		}
		// Skip peers reconnected within the last 2 minutes.
		if peer.LastReconnected.Valid && int(now.Sub(peer.LastReconnected.Time).Seconds()/60) <= 2 {
			continue
		}
		dataLog(fmt.Sprintf("Reconnecting peer %s %s, last reconnected at %s", aliasOrNone(peer.Alias), peer.Pubkey, lastReconnectedStr(peer.LastReconnected)))
		if peer.Connected {
			dataLog(fmt.Sprintf("Inactive channel is still connected to peer, disconnecting peer %s %s", aliasOrNone(peer.Alias), pk))
			disconnectPeer(ctx, q, client, peer)
		}
		host := peer.Address
		info, ierr := lnd.GetNodeInfoCached(ctx, q, client, pk)
		if ierr == nil && info.GetNode() != nil && len(info.GetNode().GetAddresses()) > 0 {
			host = info.GetNode().GetAddresses()[0].GetAddr()
		} else {
			dataLog(fmt.Sprintf("Unable to find node info on graph, using last known value for %s %s at %s", aliasOrNone(peer.Alias), peer.Pubkey, peer.Address))
			host = peer.Address
		}
		dataLog(fmt.Sprintf("Attempting connection to %s %s at %s", aliasOrNone(peer.Alias), pk, host))
		if _, e := client.ConnectPeer(ctx, &lnrpc.ConnectPeerRequest{
			Addr: &lnrpc.LightningAddress{Pubkey: pk, Host: host}, Perm: true, Timeout: 5,
		}); e != nil {
			dataLog(fmt.Sprintf("Error reconnecting %s %s: %s", aliasOrNone(peer.Alias), pk, grpcErrMsg(e)))
		} else {
			addrPrefix := peer.Address
			if len(addrPrefix) > 9 {
				addrPrefix = addrPrefix[:9]
			}
			if host != peer.Address && addrPrefix != "127.0.0.1" {
				if _, e2 := client.ConnectPeer(ctx, &lnrpc.ConnectPeerRequest{
					Addr: &lnrpc.LightningAddress{Pubkey: pk, Host: peer.Address}, Perm: true, Timeout: 5,
				}); e2 != nil {
					dataLog(fmt.Sprintf("Error reconnecting %s %s: %s", aliasOrNone(peer.Alias), pk, grpcErrMsg(e2)))
				}
			}
		}
		if e := q.SetPeerLastReconnected(ctx, db.SetPeerLastReconnectedParams{Pubkey: pk, LastReconnected: ts(now)}); e != nil {
			return e
		}
	}
	return nil
}

func lastReconnectedStr(t pgtype.Timestamptz) string {
	if !t.Valid {
		return "None"
	}
	return t.Time.Format("2006-01-02 15:04:05")
}
