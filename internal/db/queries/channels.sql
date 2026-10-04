-- name: InsertChannel :exec
INSERT INTO gui_channels (chan_id, remote_pubkey, funding_txid, output_index, capacity, local_balance, remote_balance, unsettled_balance, initiator, alias, local_base_fee, local_fee_rate, is_active, is_open, auto_rebalance, remote_base_fee, remote_fee_rate, local_commit, local_chan_reserve, ar_in_target, num_updates, ar_amt_target, ar_out_target, ar_max_cost, last_update, local_disabled, remote_disabled, htlc_count, pending_inbound, pending_outbound, private, total_received, total_sent, fees_updated, auto_fees, local_cltv, remote_cltv, local_max_htlc_msat, local_min_htlc_msat, remote_max_htlc_msat, remote_min_htlc_msat, short_chan_id, notes, close_address, push_amt, local_inbound_base_fee, local_inbound_fee_rate, remote_inbound_base_fee, remote_inbound_fee_rate, ar_source, ar_source_ppm_diff, inbound_offset, offset_updated, maxhtlc_percent, maxhtlc_updated, mx_liq_threshold, mx_liq_value, mx_liq_upper, ep_target, ep_updated, ep_enabled, ep_inc_pct, ep_cooldown, ep_live_threshold, ep_live_inc_pct, flp_enabled, flp_safety, htlc_boost_checked)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59, $60, $61, $62, $63, $64, $65, $66, $67, $68);

-- name: UpdateChannelSync :exec
-- LND state only. Our own policy goes through SyncChannelLocalPolicy and UI-owned
-- settings are never written here, so a concurrent UI write is not reverted.
UPDATE gui_channels SET remote_pubkey = $2, short_chan_id = $3, funding_txid = $4, output_index = $5, capacity = $6, local_balance = $7, remote_balance = $8, unsettled_balance = $9, local_commit = $10, local_chan_reserve = $11, num_updates = $12, initiator = $13, alias = $14, total_sent = $15, total_received = $16, private = $17, pending_outbound = $18, pending_inbound = $19, htlc_count = $20, remote_base_fee = $21, remote_fee_rate = $22, remote_inbound_base_fee = $23, remote_inbound_fee_rate = $24, remote_disabled = $25, remote_cltv = $26, remote_min_htlc_msat = $27, remote_max_htlc_msat = $28, push_amt = $29, close_address = $30, is_active = $31, is_open = $32, last_update = $33 WHERE chan_id = $1;

-- name: SyncChannelLocalPolicy :execrows
-- Writes the policy LND reports only while the row still holds the values the sync
-- loaded; 0 rows means a UI / auto-fees write came in between and wins.
UPDATE gui_channels SET local_base_fee = @local_base_fee, local_fee_rate = @local_fee_rate,
  local_inbound_base_fee = @local_inbound_base_fee, local_inbound_fee_rate = @local_inbound_fee_rate,
  local_cltv = @local_cltv, local_min_htlc_msat = @local_min_htlc_msat,
  local_max_htlc_msat = @local_max_htlc_msat, local_disabled = @local_disabled,
  fees_updated = CASE WHEN @fee_changed::boolean THEN @now::timestamptz ELSE fees_updated END
WHERE chan_id = @chan_id
  AND local_base_fee = @old_local_base_fee AND local_fee_rate = @old_local_fee_rate
  AND local_inbound_base_fee = @old_local_inbound_base_fee AND local_inbound_fee_rate = @old_local_inbound_fee_rate
  AND local_cltv = @old_local_cltv AND local_min_htlc_msat = @old_local_min_htlc_msat
  AND local_max_htlc_msat = @old_local_max_htlc_msat AND local_disabled = @old_local_disabled;

-- name: FillChannelDefaults :exec
-- Fills auto-rebalance defaults only where a value is still unset (0).
UPDATE gui_channels SET
  ar_out_target = CASE WHEN ar_out_target = 0 THEN @ar_out_target::integer ELSE ar_out_target END,
  ar_in_target = CASE WHEN ar_in_target = 0 THEN @ar_in_target::integer ELSE ar_in_target END,
  ar_amt_target = CASE WHEN ar_amt_target = 0 THEN @ar_amt_target::bigint ELSE ar_amt_target END,
  ar_max_cost = CASE WHEN ar_max_cost = 0 THEN @ar_max_cost::integer ELSE ar_max_cost END
WHERE chan_id = @chan_id;

-- name: DeleteAllPendingHTLCs :exec
DELETE FROM gui_pendinghtlcs;

-- name: InsertPendingHTLC :exec
INSERT INTO gui_pendinghtlcs (chan_id, alias, incoming, amount, hash_lock, expiration_height, forwarding_channel, forwarding_alias)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8);

-- name: GetPeerAlias :one
SELECT alias FROM gui_peers WHERE pubkey = $1;

-- name: GetPendingChannelByFunding :one
SELECT * FROM gui_pendingchannels WHERE funding_txid = $1 AND output_index = $2;

-- name: DeletePendingChannel :exec
DELETE FROM gui_pendingchannels WHERE funding_txid = $1 AND output_index = $2;

-- name: InsertPeerEvent :exec
INSERT INTO gui_peerevents (timestamp, chan_id, peer_alias, event, old_value, new_value, out_liq)
VALUES ($1, $2, $3, $4, $5, $6, $7);

-- name: CloseMissingChannels :exec
-- Channels still open in the DB but no longer listed by LND.
UPDATE gui_channels SET is_active = false, is_open = false, last_update = @last_update
WHERE is_open = true AND chan_id <> ALL(@listed_chan_ids::varchar[]);
