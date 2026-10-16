DROP INDEX IF EXISTS payments_chan_out_date_idx;
DROP TABLE IF EXISTS nk_fee_log;
ALTER TABLE gui_channels
    DROP COLUMN IF EXISTS af_mode,
    DROP COLUMN IF EXISTS nk_target,
    DROP COLUMN IF EXISTS nk_budget,
    DROP COLUMN IF EXISTS nk_last_change,
    DROP COLUMN IF EXISTS nk_last_publish;
