-- htlc_stream.py — Failed-HTLC-Insert.

-- name: InsertFailedHtlc :exec
-- FailedHTLCs(...).save(); timestamp = now (Django-Default timezone.now).
INSERT INTO gui_failedhtlcs
    (timestamp, amount, chan_id_in, chan_id_out, chan_in_alias, chan_out_alias,
     chan_out_liq, chan_out_pending, wire_failure, failure_detail, missed_fee)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);
