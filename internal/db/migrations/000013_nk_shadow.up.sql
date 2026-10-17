-- The fee NK would run on an auto-fee channel that is not in NK mode (the
-- shadow decisions), its inbound rate and the time of its last change. The NK
-- job carries them from decision to decision; NULL starts the shadow at the
-- channel's own fee.
ALTER TABLE gui_channels
    ADD COLUMN nk_shadow_fee     integer,
    ADD COLUMN nk_shadow_inbound integer,
    ADD COLUMN nk_shadow_change  timestamp with time zone;
