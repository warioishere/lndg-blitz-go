-- name: InsertChannel :exec
INSERT INTO gui_channels (chan_id, remote_pubkey, funding_txid, output_index, capacity, local_balance, remote_balance, unsettled_balance, initiator, alias, local_base_fee, local_fee_rate, is_active, is_open, auto_rebalance, remote_base_fee, remote_fee_rate, local_commit, local_chan_reserve, ar_in_target, num_updates, ar_amt_target, ar_out_target, ar_max_cost, last_update, local_disabled, remote_disabled, htlc_count, pending_inbound, pending_outbound, private, total_received, total_sent, fees_updated, auto_fees, local_cltv, remote_cltv, local_max_htlc_msat, local_min_htlc_msat, remote_max_htlc_msat, remote_min_htlc_msat, short_chan_id, notes, close_address, push_amt, local_inbound_base_fee, local_inbound_fee_rate, remote_inbound_base_fee, remote_inbound_fee_rate, ar_source, ar_source_ppm_diff, inbound_offset, offset_updated, maxhtlc_percent, maxhtlc_updated, mx_liq_threshold, mx_liq_value, mx_liq_upper, ep_target, ep_updated, ep_enabled, ep_inc_pct, ep_cooldown, ep_live_threshold, ep_live_inc_pct, flp_enabled, flp_safety, htlc_boost_checked)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59, $60, $61, $62, $63, $64, $65, $66, $67, $68);

-- name: UpdateChannelSync :exec
UPDATE gui_channels SET remote_pubkey = $2, short_chan_id = $3, funding_txid = $4, output_index = $5, capacity = $6, local_balance = $7, remote_balance = $8, unsettled_balance = $9, local_commit = $10, local_chan_reserve = $11, num_updates = $12, initiator = $13, alias = $14, total_sent = $15, total_received = $16, private = $17, pending_outbound = $18, pending_inbound = $19, htlc_count = $20, local_base_fee = $21, local_inbound_base_fee = $22, inbound_offset = $23, local_disabled = $24, local_cltv = $25, local_min_htlc_msat = $26, local_max_htlc_msat = $27, remote_base_fee = $28, remote_fee_rate = $29, remote_inbound_base_fee = $30, remote_inbound_fee_rate = $31, remote_disabled = $32, remote_cltv = $33, remote_min_htlc_msat = $34, remote_max_htlc_msat = $35, push_amt = $36, close_address = $37, is_active = $38, is_open = $39, last_update = $40, auto_rebalance = $41, ar_amt_target = $42, ar_in_target = $43, ar_out_target = $44, ar_max_cost = $45, ar_source = $46, ar_source_ppm_diff = $47, auto_fees = $48, notes = $49 WHERE chan_id = $1;

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

-- name: CountOpenChannels :one
SELECT count(*) FROM gui_channels WHERE is_open = true;

-- name: ListOpenChannelsNotIn :many
SELECT * FROM gui_channels WHERE is_open = true AND chan_id <> ALL($1::varchar[]);
