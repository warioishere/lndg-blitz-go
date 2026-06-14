-- Queries fuer agg_failed_htlcs / agg_htlcs.

-- name: SelectBalanceFailedIDs :many
SELECT id FROM gui_failedhtlcs WHERE timestamp <= $1 AND failure_detail = 6 LIMIT 100;

-- name: SelectDownstreamFailedIDs :many
SELECT id FROM gui_failedhtlcs WHERE timestamp <= $1 AND failure_detail = 99 LIMIT 100;

-- name: SelectOtherFailedIDs :many
SELECT id FROM gui_failedhtlcs WHERE timestamp <= $1 AND failure_detail NOT IN (6, 99) LIMIT 100;

-- name: AggregateFailedHTLCs :many
SELECT date_trunc('day', timestamp)::date AS day, chan_id_in, chan_id_out,
       SUM(amount)::bigint AS amount, SUM(missed_fee)::double precision AS fee,
       COALESCE(AVG(chan_out_liq), 0)::double precision AS liq,
       COALESCE(AVG(chan_out_pending), 0)::double precision AS pending,
       COUNT(id)::integer AS count,
       COALESCE(MAX(chan_in_alias), '')::varchar AS chan_in_alias,
       COALESCE(MAX(chan_out_alias), '')::varchar AS chan_out_alias
FROM gui_failedhtlcs WHERE id = ANY($1::bigint[])
GROUP BY day, chan_id_in, chan_id_out;

-- name: GetHistFailedHTLC :one
SELECT * FROM gui_histfailedhtlc WHERE date = $1 AND chan_id_in = $2 AND chan_id_out = $3;

-- name: InsertHistFailedHTLC :exec
INSERT INTO gui_histfailedhtlc (date, chan_id_in, chan_id_out, chan_in_alias, chan_out_alias,
  htlc_count, amount_sum, fee_sum, liq_avg, pending_avg, balance_count, downstream_count, other_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);

-- name: UpdateHistFailedHTLC :exec
UPDATE gui_histfailedhtlc SET htlc_count = $4, amount_sum = $5, fee_sum = $6, liq_avg = $7,
  pending_avg = $8, balance_count = $9, downstream_count = $10, other_count = $11
WHERE date = $1 AND chan_id_in = $2 AND chan_id_out = $3;

-- name: DeleteAggregatedFailedHTLCs :exec
DELETE FROM gui_failedhtlcs
WHERE id = ANY($1::bigint[]) AND chan_id_in = $2 AND chan_id_out = $3
  AND date_trunc('day', timestamp)::date = sqlc.arg(day)::date;
