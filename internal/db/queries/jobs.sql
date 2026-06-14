-- Queries fuer die [Data]-Jobs (jobs.py).

-- name: ListEpEnabledChannels :many
SELECT * FROM gui_channels WHERE is_open = true AND ep_enabled = true ORDER BY chan_id;

-- name: InsertAutofee :exec
-- Autofees(chan_id, peer_alias, setting, old_value, new_value). timestamp = auto_now_add.
INSERT INTO gui_autofees (timestamp, chan_id, peer_alias, setting, old_value, new_value)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: InsertInboundFeeLog :exec
INSERT INTO gui_inboundfeelog (timestamp, chan_id, peer_alias, setting, old_value, new_value)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: UpdateChannelEmergencyFee :exec
-- emergency_fee_job: ch.local_fee_rate / fees_updated / ep_updated (ch.save()).
UPDATE gui_channels SET local_fee_rate = $2, fees_updated = $3, ep_updated = $4
WHERE chan_id = $1;

-- name: UpdateChannelMaxHtlc :exec
-- auto_maxhtlc_job: targeted update (jobs.py:877-880).
UPDATE gui_channels SET local_max_htlc_msat = $2, maxhtlc_updated = $3
WHERE chan_id = $1;

-- name: UpdateChannelInboundOffset :exec
-- inbound_offsets: targeted update (jobs.py:819-822).
UPDATE gui_channels SET local_inbound_fee_rate = $2, offset_updated = $3
WHERE chan_id = $1;

-- name: UpdateChannelFeeRate :exec
-- failed_htlc_boost_job / auto_fees: local_fee_rate + fees_updated (ch.save()).
UPDATE gui_channels SET local_fee_rate = $2, fees_updated = $3
WHERE chan_id = $1;

-- name: SetChannelHtlcBoostChecked :exec
UPDATE gui_channels SET htlc_boost_checked = $2 WHERE chan_id = $1;

-- name: CountFailedHTLCBoost :one
-- failed_htlc_boost_job: failed HTLCs for a channel's outbound in the interval.
SELECT count(*) FROM gui_failedhtlcs
WHERE chan_id_out = $1 AND timestamp >= $2 AND wire_failure = 15
  AND failure_detail = 6 AND amount > 0;

-- name: GetChannel :one
SELECT * FROM gui_channels WHERE chan_id = $1;

-- name: ListInboundOffsetChannels :many
-- inbound_offsets: is_open=true, inbound_offset != 0 (jobs.py:780).
SELECT * FROM gui_channels WHERE is_open = true AND inbound_offset <> 0 ORDER BY chan_id;

-- name: ListAutoFeesChannels :many
-- auto_fees: is_open + is_active + private=false + auto_fees=true (jobs.py:716).
SELECT * FROM gui_channels
WHERE is_open = true AND is_active = true AND private = false AND auto_fees = true
ORDER BY chan_id;

-- name: UpdateChannelAutoFees :exec
-- auto_fees: ch.local_fee_rate / local_inbound_fee_rate / fees_updated (ch.save()).
UPDATE gui_channels SET local_fee_rate = $2, local_inbound_fee_rate = $3, fees_updated = $4
WHERE chan_id = $1;

-- name: DeleteOnchainZeroBlock :exec
DELETE FROM gui_onchain WHERE block_height = 0;

-- name: NextOnchainBlockHeight :one
-- last_block = 0 if no rows else max(block_height)+1 (jobs.py:591).
SELECT COALESCE(MAX(block_height) + 1, 0)::integer AS next_block FROM gui_onchain;

-- name: InsertOnchain :exec
INSERT INTO gui_onchain (tx_hash, time_stamp, amount, fee, block_hash, block_height, label)
VALUES ($1, $2, $3, $4, $5, $6, $7);

-- name: ListPaymentsToClean :many
-- clean_payments: exclude status=1, cleaned=false, creation_date<=cutoff, order by index, top 10.
SELECT payment_hash, status, index FROM gui_payments
WHERE status <> 1 AND cleaned = false AND creation_date <= $1
ORDER BY index LIMIT 10;

-- name: SetPaymentCleaned :exec
UPDATE gui_payments SET cleaned = true WHERE payment_hash = $1;
