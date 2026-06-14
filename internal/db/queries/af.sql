-- Queries fuer den Auto-Fee-Lauf (af.py). Forwards-Aggregate, Rebalance-Payments
-- fuer avg-cost, FailedHTLCs. amt_out_msat >= 1000000 entspricht dem Basisfilter
-- aus af.py:88.

-- name: ListOpenChannels :many
SELECT * FROM gui_channels WHERE is_open = true ORDER BY chan_id;

-- name: RebalPaymentsForChannel :many
-- Payments.objects.filter(status=2, rebal_chan=chan_id).order_by('-creation_date')[:lookback]
SELECT fee, value FROM gui_payments
WHERE status = 2 AND rebal_chan = $1
ORDER BY creation_date DESC
LIMIT $2;

-- name: ForwardsInSumFee :many
SELECT chan_id_in,
       SUM(amt_out_msat)::bigint AS amt_out_msat,
       SUM(fee)::double precision AS fee
FROM gui_forwards
WHERE forward_date >= $1 AND amt_out_msat >= 1000000
GROUP BY chan_id_in;

-- name: ForwardsInSum :many
SELECT chan_id_in, SUM(amt_out_msat)::bigint AS amt_out_msat
FROM gui_forwards
WHERE forward_date >= $1 AND amt_out_msat >= 1000000
GROUP BY chan_id_in;

-- name: ForwardsOutSumFee :many
SELECT chan_id_out,
       SUM(amt_out_msat)::bigint AS amt_out_msat,
       SUM(fee)::double precision AS fee
FROM gui_forwards
WHERE forward_date >= $1 AND amt_out_msat >= 1000000
GROUP BY chan_id_out;

-- name: ForwardsOutSum :many
SELECT chan_id_out, SUM(amt_out_msat)::bigint AS amt_out_msat
FROM gui_forwards
WHERE forward_date >= $1 AND amt_out_msat >= 1000000
GROUP BY chan_id_out;

-- name: ForwardsLastOut :many
SELECT chan_id_out, MAX(forward_date)::timestamptz AS last_out
FROM gui_forwards
WHERE forward_date >= $1 AND amt_out_msat >= 1000000
GROUP BY chan_id_out;

-- name: ForwardsLastIn :many
SELECT chan_id_in, MAX(forward_date)::timestamptz AS last_in
FROM gui_forwards
WHERE forward_date >= $1 AND amt_out_msat >= 1000000
GROUP BY chan_id_in;

-- name: FailedHTLCsForAF :many
-- FailedHTLCs.objects.filter(timestamp>=cutoff, wire_failure=15, failure_detail=6).values()
SELECT chan_id_out, amount, chan_out_liq, chan_out_pending
FROM gui_failedhtlcs
WHERE timestamp >= $1 AND wire_failure = 15 AND failure_detail = 6;
