-- Payment-Queries fuer update_payments / update_payment.

-- name: GetPayment :one
SELECT * FROM gui_payments WHERE payment_hash = $1;

-- name: ListInflightPayments :many
SELECT creation_date, payment_hash, index FROM gui_payments WHERE status = 1 ORDER BY index;

-- name: MaxPaymentIndex :one
SELECT COALESCE(MAX(index), 0)::integer AS max FROM gui_payments;

-- name: InsertPayment :exec
INSERT INTO gui_payments (creation_date, payment_hash, value, fee, status, index, cleaned)
VALUES ($1, $2, $3, $4, $5, $6, false);

-- name: SetPaymentStatus :exec
UPDATE gui_payments SET status = $2 WHERE payment_hash = $1;

-- name: UpdatePaymentBasic :exec
UPDATE gui_payments SET creation_date = $2, value = $3, fee = $4, status = $5, index = $6
WHERE payment_hash = $1;

-- name: UpdatePaymentHopResults :exec
-- chan_out / chan_out_alias / keysend_preimage / message / rebal_chan nach Hop-Verarbeitung.
UPDATE gui_payments SET chan_out = $2, chan_out_alias = $3, keysend_preimage = $4,
  message = $5, rebal_chan = $6 WHERE payment_hash = $1;

-- name: DeletePaymentHops :exec
DELETE FROM gui_paymenthops WHERE payment_hash_id = $1;

-- name: InsertPaymentHop :exec
INSERT INTO gui_paymenthops (attempt_id, step, chan_id, alias, chan_capacity, node_pubkey, amt, fee, payment_hash_id, cost_to)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);
