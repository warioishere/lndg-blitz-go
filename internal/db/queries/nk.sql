-- name: ListNkJobChannels :many
-- Channels the NK job evaluates: the auto-fee channels of every mode (NK ones
-- are applied, the others get shadow decisions).
SELECT * FROM gui_channels
WHERE is_open = true AND is_active = true AND private = false AND auto_fees = true
ORDER BY chan_id;

-- name: NkForwardFlows :many
-- Forwards out of the given channels since @since (72 hours) with their gross
-- fee (the inbound discount the incoming channel gave included), per window;
-- last_big is the latest forward of at least @big_pct percent of capacity.
SELECT f.chan_id_out AS chan_id,
    COALESCE(SUM(CASE WHEN f.forward_date >= @since6h THEN f.amt_out_msat / 1000.0 END), 0)::float8 AS amt_6h,
    COALESCE(SUM(CASE WHEN f.forward_date >= @since6h THEN f.fee + f.inbound_fee END), 0)::float8 AS fee_6h,
    COALESCE(SUM(CASE WHEN f.forward_date >= @since24h THEN f.amt_out_msat / 1000.0 END), 0)::float8 AS amt_24h,
    COALESCE(SUM(CASE WHEN f.forward_date >= @since24h THEN f.fee + f.inbound_fee END), 0)::float8 AS fee_24h,
    COALESCE(SUM(f.amt_out_msat / 1000.0), 0)::float8 AS amt_72h,
    COALESCE(SUM(f.fee + f.inbound_fee), 0)::float8 AS fee_72h,
    MAX(CASE WHEN f.amt_out_msat / 10.0 >= c.capacity * @big_pct::float8 THEN f.forward_date END)::timestamptz AS last_big
FROM gui_forwards f JOIN gui_channels c ON c.chan_id = f.chan_id_out
WHERE f.chan_id_out = ANY(@chan_ids::varchar[]) AND f.forward_date >= @since
GROUP BY f.chan_id_out;

-- name: NkBalanceOutFlows :many
-- Balance-outs of the given channels since @since: rebalances that took
-- liquidity from the channel, valued at the channel's fee rate then (the
-- liquidity is sold at that price like a forward), per window; last_big as in
-- NkForwardFlows.
SELECT p.chan_out::varchar AS chan_id,
    COALESCE(SUM(CASE WHEN p.creation_date >= @since24h THEN p.value END), 0)::float8 AS amt_24h,
    COALESCE(SUM(CASE WHEN p.creation_date >= @since24h THEN p.value * COALESCE(p.source_fee_rate, 0) / 1000000.0 END), 0)::float8 AS fee_24h,
    COALESCE(SUM(p.value), 0)::float8 AS amt_72h,
    COALESCE(SUM(p.value * COALESCE(p.source_fee_rate, 0) / 1000000.0), 0)::float8 AS fee_72h,
    MAX(CASE WHEN p.value * 100 >= c.capacity * @big_pct::float8 THEN p.creation_date END)::timestamptz AS last_big
FROM gui_payments p JOIN gui_channels c ON c.chan_id = p.chan_out
WHERE p.status = 2 AND p.rebal_chan IS NOT NULL
  AND p.chan_out = ANY(@chan_ids::varchar[]) AND p.creation_date >= @since
GROUP BY p.chan_out;

-- name: UpdateChannelNkPolicy :exec
UPDATE gui_channels SET local_fee_rate = $2, local_base_fee = $3, local_inbound_fee_rate = $4,
    local_inbound_base_fee = $5, local_max_htlc_msat = $6, fees_updated = $7,
    nk_last_change = $8, nk_last_publish = $9
WHERE chan_id = $1;

-- name: InsertNkFeeLog :exec
INSERT INTO nk_fee_log ("timestamp", chan_id, peer_alias, applied, reason, local_pct, target_pct,
    flow24_ppm, flow72_ppm, lock_until, fee_old, fee_new, cap_ppm, max_htlc_old, max_htlc_new,
    inbound_old, inbound_new)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17);

-- name: DeleteOldNkFeeLogs :exec
DELETE FROM nk_fee_log WHERE "timestamp" < $1;
