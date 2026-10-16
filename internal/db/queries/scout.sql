-- name: UpsertScoutPath :exec
-- A path the route scout verified; a path seen again gets the new route and time.
INSERT INTO scout_path (target_pubkey, outgoing_chan_id, route, route_hex, value_sat, last_verified)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (target_pubkey, outgoing_chan_id, route)
DO UPDATE SET route_hex = EXCLUDED.route_hex, value_sat = EXCLUDED.value_sat, last_verified = EXCLUDED.last_verified;

-- name: ListScoutPaths :many
-- Paths the route scout verified since $1.
SELECT * FROM scout_path WHERE last_verified >= $1 ORDER BY id;

-- name: DeleteOldScoutPaths :exec
DELETE FROM scout_path WHERE last_verified < $1;

-- name: MedianRebalanceValue :one
-- The median amount of the rebalances paid since $1 (0 without one).
SELECT coalesce(percentile_cont(0.5) WITHIN GROUP (ORDER BY value), 0)::float8 AS median
FROM gui_rebalancer WHERE status = 2 AND stop >= $1;
