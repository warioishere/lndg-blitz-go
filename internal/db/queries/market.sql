-- name: InsertMarketPolicies :copyfrom
INSERT INTO market_policy ("timestamp", source, chan_id, capacity, peer_pubkey, policy_node,
    fee_ppm, base_fee_msat, inbound_fee_ppm, inbound_base_msat, min_htlc_msat, max_htlc_msat,
    time_lock_delta, disabled, last_update)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15);

-- name: LastMarketSnapshot :one
-- Time of the latest graph snapshot (zero time when there is none).
SELECT COALESCE(MAX("timestamp"), 'epoch'::timestamptz)::timestamptz AS last
FROM market_policy WHERE source = 'snapshot';

-- name: DeleteOldMarketPolicies :execrows
DELETE FROM market_policy WHERE "timestamp" < $1;

-- name: ListOpenPeerPubkeys :many
SELECT DISTINCT remote_pubkey FROM gui_channels WHERE is_open = true;
