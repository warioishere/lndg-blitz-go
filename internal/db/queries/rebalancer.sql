-- rebalancer.py — Route-Record-, NodeReputation- und Purge-Queries.

-- name: UpdateRebalanceRoute :exec
-- route_obj.save() (Full-Row der mutable Felder) in update_route. Gekeyt auf den
-- unique_together-Schluessel (target_pubkey, outgoing_chan_id, route), damit nach
-- get_or_create-Insert keine id zurueckgelesen werden muss.
UPDATE gui_rebalanceroute
SET final_cltv_delta = $4, route_hex = $5, success_count = $6, failure_count = $7,
    last_success = $8, last_failure = $9, last_fee_ppm = $10
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

-- name: DeleteStaleTestedRoutes :exec
-- getestete Routen ohne kuerzlichen Erfolg loeschen; ungetestete (probed) behalten.
DELETE FROM gui_rebalanceroute
WHERE (last_success < $1 OR last_success IS NULL)
  AND NOT (success_count = 0 AND failure_count = 0);

-- name: DeleteStaleNodeReputations :exec
DELETE FROM gui_nodereputation
WHERE (last_success < $1 OR last_success IS NULL)
  AND (last_failure < $1 OR last_failure IS NULL);

-- name: IncrNodeReputationSuccess :exec
-- get_or_create(pubkey) + success_count += 1 + last_success = now.
INSERT INTO gui_nodereputation (pubkey, success_count, failure_count, last_success)
VALUES ($1, 1, 0, $2)
ON CONFLICT (pubkey) DO UPDATE
SET success_count = gui_nodereputation.success_count + 1, last_success = $2;

-- name: IncrNodeReputationFailure :exec
INSERT INTO gui_nodereputation (pubkey, success_count, failure_count, last_failure)
VALUES ($1, 0, 1, $2)
ON CONFLICT (pubkey) DO UPDATE
SET failure_count = gui_nodereputation.failure_count + 1, last_failure = $2;

-- name: ListNodeReputations :many
SELECT pubkey, success_count, failure_count FROM gui_nodereputation
WHERE pubkey = ANY($1::varchar[]);

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
SELECT local_fee_rate, ar_max_cost FROM gui_channels
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

-- get_saved_routes Kandidaten-Selects (rebalancer.py:616-627). Gemeinsamer Filter:
-- target_pubkey + outgoing_chan_id IN chan_ids + (last_failure < cutoff OR NULL).

-- name: ListUntestedNoFeeRoutes :many
-- truly_untested: success_count=0 AND failure_count=0 AND last_fee_ppm IS NULL,
-- order_by('?') == ORDER BY random() (nicht-deterministisch, siehe PORTING_NOTES).
SELECT id, target_pubkey, outgoing_chan_id, route, final_cltv_delta, success_count,
       failure_count, last_success, last_failure, route_hex, last_fee_ppm
FROM gui_rebalanceroute
WHERE target_pubkey = sqlc.arg(target_pubkey)
  AND outgoing_chan_id = ANY(sqlc.arg(chan_ids)::varchar[])
  AND (last_failure < sqlc.arg(cutoff) OR last_failure IS NULL)
  AND success_count = 0 AND failure_count = 0 AND last_fee_ppm IS NULL
ORDER BY random() LIMIT sqlc.arg(fetch_limit);

-- name: ListUntestedFeeKnownRoutes :many
-- fee_known_untested: success_count=0 AND failure_count=0 AND last_fee_ppm IS NOT NULL.
SELECT id, target_pubkey, outgoing_chan_id, route, final_cltv_delta, success_count,
       failure_count, last_success, last_failure, route_hex, last_fee_ppm
FROM gui_rebalanceroute
WHERE target_pubkey = sqlc.arg(target_pubkey)
  AND outgoing_chan_id = ANY(sqlc.arg(chan_ids)::varchar[])
  AND (last_failure < sqlc.arg(cutoff) OR last_failure IS NULL)
  AND success_count = 0 AND failure_count = 0 AND last_fee_ppm IS NOT NULL
ORDER BY last_fee_ppm LIMIT sqlc.arg(fetch_limit);

-- name: ListTestedRoutes :many
-- tested: NOT(success_count=0 AND failure_count=0), order_by('-weighted_ratio').
-- weighted_ratio inline = ((sc+1)/(sc+fc+2)) * ((sc+fc)/(sc+fc+10)).
SELECT id, target_pubkey, outgoing_chan_id, route, final_cltv_delta, success_count,
       failure_count, last_success, last_failure, route_hex, last_fee_ppm
FROM gui_rebalanceroute
WHERE target_pubkey = sqlc.arg(target_pubkey)
  AND outgoing_chan_id = ANY(sqlc.arg(chan_ids)::varchar[])
  AND (last_failure < sqlc.arg(cutoff) OR last_failure IS NULL)
  AND NOT (success_count = 0 AND failure_count = 0)
ORDER BY ((success_count + 1.0) / (success_count + failure_count + 2.0))
       * ((success_count + failure_count) / (success_count + failure_count + 10.0)) DESC
LIMIT sqlc.arg(fetch_limit);
