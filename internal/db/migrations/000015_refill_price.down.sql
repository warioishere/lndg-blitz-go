ALTER TABLE gui_channels
    DROP COLUMN IF EXISTS refill_ppm,
    DROP COLUMN IF EXISTS refill_amount_sat,
    DROP COLUMN IF EXISTS refill_checked;
