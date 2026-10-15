ALTER TABLE gui_graphprobelog
    DROP COLUMN IF EXISTS buy_ppm,
    DROP COLUMN IF EXISTS via_fee_ppm,
    DROP COLUMN IF EXISTS via_amount_sat,
    DROP COLUMN IF EXISTS buy_status;
