ALTER TABLE gui_channels
    DROP COLUMN IF EXISTS nk_shadow_fee,
    DROP COLUMN IF EXISTS nk_shadow_inbound,
    DROP COLUMN IF EXISTS nk_shadow_change;
