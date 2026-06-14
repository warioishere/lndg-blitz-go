-- graph_watcher.py — Channel-Graph-Watcher Queries.

-- name: ListAutoRebalancePeerPubkeys :many
-- _load_ar_targets: remote_pubkeys offener AR-Channels.
SELECT DISTINCT remote_pubkey FROM gui_channels WHERE is_open = true AND auto_rebalance = true;

-- name: ListOpenARChannelsByPubkey :many
-- _trigger_probe targets: offene AR-Channels zu remote_pubkey.
SELECT * FROM gui_channels
WHERE is_open = true AND auto_rebalance = true AND remote_pubkey = $1;

-- name: ListGraphOutboundCans :many
-- outbound_cans: is_open, exclude(auto_rebalance=True, ar_source=False).
SELECT chan_id FROM gui_channels
WHERE is_open = true AND NOT (auto_rebalance = true AND ar_source = false)
ORDER BY chan_id;

-- name: ListRecentRoutesForTarget :many
-- recent_routes: RebalanceRoute.filter(target_pubkey).order_by('-id')[:10].
SELECT route, last_fee_ppm, outgoing_chan_id FROM gui_rebalanceroute
WHERE target_pubkey = $1 ORDER BY id DESC LIMIT 10;

-- name: HasActiveRebalanceForPubkey :one
-- _schedule_rebalance: existiert ein status in {0,1} Rebalance fuer den Ziel-Peer?
SELECT EXISTS (
    SELECT 1 FROM gui_rebalancer WHERE last_hop_pubkey = $1 AND status IN (0, 1)
) AS active;

-- name: InsertGraphProbeLog :exec
-- GraphProbeLog.objects.create(...); timestamp = now (Django-Default).
INSERT INTO gui_graphprobelog
    (timestamp, target_pubkey, target_alias, target_fee, target_max_cost, trigger_chan_id,
     other_pubkey, other_alias, other_fee_ppm, budget_ppm, sources_tried, routes_new,
     routes_existing, errors, routes_via_new_peer, rebalance_scheduled, details)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17);

-- name: InsertGraphEvent :exec
-- GraphEvent.objects.create(...); timestamp = now (Django-Default).
INSERT INTO gui_graphevent
    (timestamp, event_type, chan_id, capacity, fee_ppm, base_fee_msat, target_pubkey,
     target_alias, other_node, other_alias, disabled, probe_triggered, routes_found, policy_node)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14);

-- name: UpdateGraphEventRoutesFound :exec
-- _dispatch_probe: nachtraegliche routes_found-Aktualisierung des Events.
UPDATE gui_graphevent SET routes_found = $4
WHERE target_pubkey = $1 AND chan_id = $2 AND event_type = $3
  AND probe_triggered = true AND routes_found = 0;

-- name: DeleteOldGraphEvents :exec
-- _trim_events: nur die neuesten 500 Events behalten.
DELETE FROM gui_graphevent WHERE id IN (
    SELECT id FROM gui_graphevent ORDER BY timestamp DESC OFFSET 500
);
