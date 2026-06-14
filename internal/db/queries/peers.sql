-- Peers-Queries fuer update_peers / refresh_peer_aliases / reconnect_peers.

-- name: GetPeer :one
SELECT * FROM gui_peers WHERE pubkey = $1;

-- name: InsertPeer :exec
INSERT INTO gui_peers (pubkey, address, sat_sent, sat_recv, inbound, ping_time, alias, connected)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8);

-- name: UpdatePeerData :exec
UPDATE gui_peers SET address = $2, sat_sent = $3, sat_recv = $4, inbound = $5,
  ping_time = $6, alias = $7, connected = $8 WHERE pubkey = $1;

-- name: DisconnectStalePeers :exec
-- Peers.filter(connected=True).exclude(pubkey__in=peer_list).update(connected=False)
UPDATE gui_peers SET connected = false
WHERE connected = true AND pubkey <> ALL($1::varchar[]);

-- name: ListPeersNoAlias :many
SELECT * FROM gui_peers WHERE alias IS NULL OR alias = '';

-- name: UpdatePeerAlias :exec
UPDATE gui_peers SET alias = $2 WHERE pubkey = $1;

-- name: SetPeerConnected :exec
UPDATE gui_peers SET connected = $2 WHERE pubkey = $1;

-- name: SetPeerLastReconnected :exec
UPDATE gui_peers SET last_reconnected = $2 WHERE pubkey = $1;

-- name: ListInactivePeerPubkeys :many
SELECT DISTINCT remote_pubkey FROM gui_channels
WHERE is_open = true AND is_active = false AND private = false;
