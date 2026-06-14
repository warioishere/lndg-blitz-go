-- Invoice-Queries fuer update_invoices / update_invoice.

-- name: ListOpenInvoices :many
SELECT r_hash, index FROM gui_invoices WHERE state = 0 ORDER BY index;

-- name: MaxInvoiceIndex :one
SELECT COALESCE(MAX(index), 0)::integer AS max FROM gui_invoices;

-- name: InsertInvoice :exec
INSERT INTO gui_invoices (creation_date, r_hash, value, amt_paid, state, index, is_revenue)
VALUES ($1, $2, $3, $4, $5, $6, false);

-- name: SetInvoiceState :exec
UPDATE gui_invoices SET state = $2 WHERE r_hash = $1;

-- name: UpdateInvoiceSettled :exec
UPDATE gui_invoices SET state = $2, amt_paid = $3, settle_date = $4, chan_in = $5,
  chan_in_alias = $6, keysend_preimage = $7, message = $8, sender = $9, sender_alias = $10
WHERE r_hash = $1;

-- name: GetChannelAlias :one
SELECT alias FROM gui_channels WHERE chan_id = $1;
