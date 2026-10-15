-- Probe-Subsystem (jobs.py:1064-1316): QueryRoutes-Binaersuche fuer AR-Targets,
-- RebalanceRoute get_or_create und ProbeLog (gui_probelog = Schema A).

-- name: GetRebalanceRoute :one
SELECT * FROM gui_rebalanceroute
WHERE target_pubkey = $1 AND outgoing_chan_id = $2 AND route = $3;

-- name: InsertRebalanceRoute :exec
-- get_or_create defaults: final_cltv_delta/route_hex/last_fee_ppm gesetzt;
-- success_count/failure_count = 0, last_success/last_failure = NULL.
INSERT INTO gui_rebalanceroute
    (target_pubkey, outgoing_chan_id, route, final_cltv_delta, route_hex, last_fee_ppm, success_count, failure_count)
VALUES ($1, $2, $3, $4, $5, $6, 0, 0);

-- name: UpdateRebalanceRouteHex :exec
-- obj.save(update_fields=['route_hex', 'last_fee_ppm'])
UPDATE gui_rebalanceroute SET route_hex = $2, last_fee_ppm = $3 WHERE id = $1;

-- name: InsertProbeLog :exec
INSERT INTO gui_probelog
    (timestamp, targets_scanned, routes_found, routes_existing, errors, duration_ms, details)
VALUES ($1, $2, $3, $4, $5, $6, $7);

-- name: DeleteOldProbeLogs :exec
-- ProbeLog.objects.order_by('-timestamp')[100:].delete() -- behalte die neuesten 100.
DELETE FROM gui_probelog WHERE id IN (
    SELECT id FROM gui_probelog ORDER BY timestamp DESC OFFSET 100
);
