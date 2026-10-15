-- graph_watcher.py — Channel-Graph-Watcher Queries.

-- name: ListAutoRebalancePeerPubkeys :many
-- _load_ar_targets: remote_pubkeys offener AR-Channels.
SELECT DISTINCT remote_pubkey FROM gui_channels WHERE is_open = true AND auto_rebalance = true;

-- name: ListOpenARChannelsByPubkey :many
-- _trigger_probe targets: offene AR-Channels zu remote_pubkey. ORDER BY chan_id wie
-- Djangos targets.first() (Primaerschluessel); targets[0] bestimmt Fee und Budget.
SELECT * FROM gui_channels
WHERE is_open = true AND auto_rebalance = true AND remote_pubkey = $1
ORDER BY chan_id;

-- name: InsertGraphProbeLog :exec
-- One forced-route probe through the new peer; rebalance_scheduled marks a
-- purchase over the verified route. routes_new and routes_existing belong to
-- the generic probe and stay 0.
INSERT INTO gui_graphprobelog
    (timestamp, target_pubkey, target_alias, target_fee, target_max_cost, trigger_chan_id,
     other_pubkey, other_alias, other_fee_ppm, budget_ppm, sources_tried, routes_new,
     routes_existing, errors, routes_via_new_peer, rebalance_scheduled, details,
     buy_ppm, via_fee_ppm, via_amount_sat, buy_status)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 0,
        0, $12, $13, $14, $15,
        $16, $17, $18, $19);

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
