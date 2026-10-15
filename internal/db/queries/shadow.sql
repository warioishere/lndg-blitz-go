-- name: InsertShadowFeeLog :exec
INSERT INTO shadow_fee_log ("timestamp", rule, chan_id, peer_alias, local_fee_rate, local_inbound_fee_rate,
    avg_rebalance_cost, out_percent, hours_since_out, current_value, proposed_value)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);
