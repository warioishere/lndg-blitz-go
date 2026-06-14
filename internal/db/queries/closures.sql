-- Closure-Queries fuer update_closures.

-- name: CountClosures :one
SELECT count(*) FROM gui_closures;

-- name: InsertClosure :exec
INSERT INTO gui_closures (chan_id, funding_txid, funding_index, closing_tx, remote_pubkey,
  capacity, close_height, settled_balance, time_locked_balance, close_type, open_initiator,
  close_initiator, resolution_count, closing_costs)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 0);

-- name: DeleteClosureByFunding :exec
DELETE FROM gui_closures WHERE funding_txid = $1 AND funding_index = $2;

-- name: UpdateClosureCosts :exec
UPDATE gui_closures SET closing_costs = $3 WHERE funding_txid = $1 AND funding_index = $2;

-- name: DeleteResolutionsByChan :exec
DELETE FROM gui_resolutions WHERE chan_id = $1;

-- name: ExistsResolutionBySweep :one
SELECT EXISTS (SELECT 1 FROM gui_resolutions WHERE sweep_txid = $1) AS does_exist;

-- name: InsertResolution :exec
INSERT INTO gui_resolutions (chan_id, resolution_type, outcome, outpoint_tx, outpoint_index, amount_sat, sweep_txid)
VALUES ($1, $2, $3, $4, $5, $6, $7);
