-- Probe-Subsystem (jobs.py:1064-1316): QueryRoutes-Binaersuche fuer AR-Targets,
-- RebalanceRoute get_or_create und ProbeLog (gui_probelog = Schema A).

-- name: GetRebalanceRoute :one
SELECT * FROM gui_rebalanceroute
WHERE target_pubkey = $1 AND outgoing_chan_id = $2 AND route = $3;

-- name: InsertRebalanceRoute :exec
-- A new route with success_count/failure_count = 0; last_probed and
-- probed_amount_sat are set when the route prober verified it.
INSERT INTO gui_rebalanceroute
    (target_pubkey, outgoing_chan_id, route, final_cltv_delta, route_hex, last_fee_ppm, success_count, failure_count,
     last_probed, probed_amount_sat)
VALUES ($1, $2, $3, $4, $5, $6, 0, 0, $7, $8);

-- name: UpdateRebalanceRouteProbe :exec
-- The route prober verified a known route again.
UPDATE gui_rebalanceroute SET route_hex = $2, last_fee_ppm = $3, last_probed = $4, probed_amount_sat = $5
WHERE id = $1;

-- name: InsertProbeLog :exec
INSERT INTO gui_probelog
    (timestamp, targets_scanned, routes_found, routes_existing, errors, duration_ms, details)
VALUES ($1, $2, $3, $4, $5, $6, $7);

-- name: DeleteOldProbeLogs :exec
-- ProbeLog.objects.order_by('-timestamp')[100:].delete() -- behalte die neuesten 100.
DELETE FROM gui_probelog WHERE id IN (
    SELECT id FROM gui_probelog ORDER BY timestamp DESC OFFSET 100
);
