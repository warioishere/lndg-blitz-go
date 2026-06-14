-- Forwards-Queries fuer update_forwards / emergency_forward_check.

-- name: LatestForward :one
SELECT forward_date, id FROM gui_forwards ORDER BY forward_date DESC, id DESC LIMIT 1;

-- name: CountForwardsAtDate :one
SELECT count(*) FROM gui_forwards WHERE forward_date = $1;

-- name: GetChannelsByIDs :many
SELECT * FROM gui_channels WHERE chan_id = ANY($1::varchar[]);

-- name: ListEpEnabledChannelsByIDs :many
SELECT * FROM gui_channels WHERE chan_id = ANY($1::varchar[]) AND ep_enabled = true;

-- name: InsertForward :exec
INSERT INTO gui_forwards (forward_date, chan_id_in, chan_id_out, chan_in_alias, chan_out_alias, amt_in_msat, amt_out_msat, fee, inbound_fee)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
