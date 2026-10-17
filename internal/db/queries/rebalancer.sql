-- rebalancer.py — Route-Record- und Purge-Queries.

-- name: UpdateRebalanceRoute :exec
-- route_obj.save() (Full-Row der mutable Felder) in update_route. Gekeyt auf den
-- unique_together-Schluessel (target_pubkey, outgoing_chan_id, route), damit nach
-- get_or_create-Insert keine id zurueckgelesen werden muss.
UPDATE gui_rebalanceroute
SET final_cltv_delta = $4, route_hex = $5, success_count = $6, failure_count = $7,
    last_success = $8, last_failure = $9, last_fee_ppm = $10, last_failure_hop = $11
WHERE target_pubkey = $1 AND outgoing_chan_id = $2 AND route = $3;

-- name: UpdateRebalanceRouteFee :exec
-- update_route_fee: save(update_fields=['last_fee_ppm']).
UPDATE gui_rebalanceroute SET last_fee_ppm = $2 WHERE id = $1;

-- name: MarkRebalanceRouteFailure :exec
-- mark_route_failure: failure_count += 1; last_failure = now (DB-seitiges Inkrement).
UPDATE gui_rebalanceroute SET failure_count = failure_count + 1, last_failure = $2 WHERE id = $1;

-- name: DeleteRoutesNotOpenOutgoing :execrows
-- purge_stale_routes: Routen mit nicht (mehr) offenem outgoing-Channel loeschen.
DELETE FROM gui_rebalanceroute WHERE outgoing_chan_id <> ALL($1::varchar[]);

-- name: DeleteUnconfirmedRoutes :exec
-- Never-paid routes the route prober has not verified since cutoff.
DELETE FROM gui_rebalanceroute
WHERE success_count = 0 AND failure_count = 0 AND (last_probed IS NULL OR last_probed < $1);

-- name: DeleteRoutesOfNonTargets :execrows
-- Routes to peers without an open auto-rebalance channel.
DELETE FROM gui_rebalanceroute r
WHERE NOT EXISTS (
    SELECT 1 FROM gui_channels c WHERE c.remote_pubkey = r.target_pubkey AND c.is_open AND c.auto_rebalance
);

-- name: DeleteStaleTestedRoutes :exec
-- Tested routes whose latest evidence (payment, failure or prober verification)
-- is older than the cutoff. A route that failed part way stays until then: the
-- part before the failing node is material for combined routes.
DELETE FROM gui_rebalanceroute
WHERE GREATEST(last_success, last_failure, last_probed) < sqlc.arg(cutoff)::timestamptz
  AND NOT (success_count = 0 AND failure_count = 0);

-- name: ListOpenChannelIDs :many
SELECT chan_id FROM gui_channels WHERE is_open = true;

-- name: ListChannelAliases :many
-- _refresh_alias_cache: dict(Channels.objects.values_list('chan_id', 'alias')).
SELECT chan_id, alias FROM gui_channels;

-- run_rebalancer Orchestrator-Queries.

-- name: ListActiveOpenPublicChannels :many
-- auto_rebalance_channels Basis: Channels.objects.filter(is_active=True, is_open=True, private=False).
-- Annotation (percent_outbound/inbound_can) wird in Go gerechnet (annotateChannel).
SELECT * FROM gui_channels WHERE is_active = true AND is_open = true AND private = false;

-- name: ListChannelFeeAndDiffByIDs :many
-- get_source_fee_map + get_source_diff_map (chan_id__in). Beide Maps aus einer Query.
SELECT chan_id, local_fee_rate, ar_source_ppm_diff FROM gui_channels
WHERE chan_id = ANY($1::varchar[]);

-- name: GetTargetChannelInfo :one
-- get_target_info: erstes is_open+auto_rebalance-Channel zu remote_pubkey. ORDER BY
-- chan_id wie Djangos .first() (Primaerschluessel), sonst ist die Wahl bei mehreren
-- Channels zum selben Peer zufaellig.
SELECT chan_id, local_fee_rate, ar_max_cost, ar_in_target, capacity, local_balance, pending_outbound,
    af_mode
FROM gui_channels
WHERE is_open = true AND auto_rebalance = true AND remote_pubkey = $1
ORDER BY chan_id
LIMIT 1;

-- name: ListRemainingDrainChannels :many
-- _remaining_drain: alle offenen AR-Channels zu remote_pubkey.
SELECT remote_balance, pending_inbound, capacity, ar_in_target FROM gui_channels
WHERE remote_pubkey = $1 AND is_open = true AND auto_rebalance = true;

-- name: ListAllowedTargetSources :many
-- get_allowed_sources_for_target: source_chan_id-Menge fuer target_pubkey.
SELECT source_chan_id FROM gui_allowedtarget WHERE target_pubkey = $1;

-- name: GetChannelBalances :one
-- rebalancer update_channels: Channels.objects.filter(chan_id=...).first() (alte Werte fuer Log).
SELECT local_balance, remote_balance FROM gui_channels WHERE chan_id = $1;

-- name: SetChannelBalances :exec
-- rebalancer update_channels: db_channel.local_balance/remote_balance = ...; save().
UPDATE gui_channels SET local_balance = $2, remote_balance = $3 WHERE chan_id = $1;

-- auto_schedule / auto_enable Queries.

-- name: ListActiveRebalancePubkeys :many
-- get_active_rebalance_pubkeys / already_scheduled: status in {0,1}, last_hop != ''.
SELECT DISTINCT last_hop_pubkey FROM gui_rebalancer
WHERE status IN (0, 1) AND last_hop_pubkey <> '';

-- name: ListAllAllowedTargets :many
-- allowed_map: AllowedTarget.objects.select_related('source_chan').all() (nach id).
SELECT source_chan_id, target_pubkey FROM gui_allowedtarget ORDER BY id;

-- name: GetLastRebalanceForPubkey :one
-- letzter abgeschlossener Rebalance zu pub (status != 0, neueste id).
SELECT status, start, stop, duration FROM gui_rebalancer
WHERE last_hop_pubkey = $1 AND status <> 0
ORDER BY id DESC LIMIT 1;

-- name: AggForwardsInSince :many
-- auto_enable: pro chan_id_in Anzahl + Summe amt_in_msat seit filter_day.
SELECT chan_id_in, COUNT(*) AS cnt, COALESCE(SUM(amt_in_msat), 0)::bigint AS sum_msat
FROM gui_forwards WHERE forward_date >= $1 GROUP BY chan_id_in;

-- name: AggForwardsOutSince :many
-- auto_enable: pro chan_id_out Anzahl + Summe amt_out_msat seit filter_day.
SELECT chan_id_out, COUNT(*) AS cnt, COALESCE(SUM(amt_out_msat), 0)::bigint AS sum_msat
FROM gui_forwards WHERE forward_date >= $1 GROUP BY chan_id_out;

-- name: SetChannelAutoRebalance :exec
-- auto_enable: peer_channel.auto_rebalance = ...; save().
UPDATE gui_channels SET auto_rebalance = $2 WHERE chan_id = $1;

-- name: InsertAutopilot :exec
-- auto_enable: Autopilot(chan_id, peer_alias, setting, old_value, new_value).save().
INSERT INTO gui_autopilot (timestamp, chan_id, peer_alias, setting, old_value, new_value)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: ListPendingRebalances :many
-- get_pending_rebals: Rebalancer.objects.filter(status=0).order_by('id').
SELECT * FROM gui_rebalancer WHERE status = 0 ORDER BY id;

-- name: MarkInFlightRebalancesError :exec
-- main(): beim Start alle status=1 (in-flight) auf 400 setzen + stop=now.
UPDATE gui_rebalancer SET status = 400, stop = $1 WHERE status = 1;

-- name: UpdateRebalancerRecord :exec
-- save_record(rebalance) auf bestehendem Datensatz (record.save(), gekeyt auf id).
UPDATE gui_rebalancer
SET value = $2, fee_limit = $3, outgoing_chan_ids = $4, last_hop_pubkey = $5,
    target_alias = $6, duration = $7, start = $8, stop = $9, status = $10,
    payment_hash = $11, manual = $12, fees_paid = $13
WHERE id = $1;

-- name: InsertRebalancerRecord :one
-- RapidFire: Rebalancer(value, fee_limit, outgoing_chan_ids, last_hop_pubkey,
-- target_alias, duration=1, status=1).save(); requested=now (Django-Default).
INSERT INTO gui_rebalancer
    (requested, value, fee_limit, outgoing_chan_ids, last_hop_pubkey, target_alias,
     duration, start, stop, status, payment_hash, manual, fees_paid)
VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, NULL, $8, NULL, false, NULL)
RETURNING id;

-- name: ListRoutesForTarget :many
-- Saved routes of a target from the given sources; a route that failed after
-- cutoff is left out.
SELECT * FROM gui_rebalanceroute
WHERE target_pubkey = sqlc.arg(target_pubkey)
  AND outgoing_chan_id = ANY(sqlc.arg(chan_ids)::varchar[])
  AND (last_failure < sqlc.arg(cutoff) OR last_failure IS NULL)
ORDER BY id;

-- name: InsertRouteAttempt :exec
INSERT INTO route_attempt_log
    (timestamp, rebalance_id, route_id, target_pubkey, outgoing_chan_id, list, position,
     evidence_age_s, fee_ppm, value_sat, outcome, failure_code, failure_hop, route, duration_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15);

-- name: ListSentRouteAttempts :many
-- Attempts that went out since $1 with their path: the evidence for the
-- success chance and the speed of each node pair.
SELECT "timestamp", route, outcome, failure_hop, duration_ms
FROM route_attempt_log
WHERE "timestamp" >= $1 AND route IS NOT NULL
  AND outcome IN ('success', 'success_smaller', 'failed', 'timeout')
ORDER BY "timestamp";

-- name: ListCombinableRoutes :many
-- Saved routes with a stored route, the material the rebalancer combines new
-- routes from.
SELECT * FROM gui_rebalanceroute WHERE route_hex IS NOT NULL AND route_hex <> '' ORDER BY id;

-- name: DeleteOldRouteAttempts :exec
DELETE FROM route_attempt_log WHERE "timestamp" < $1;
