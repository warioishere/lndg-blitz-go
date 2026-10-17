-- An NK channel's local liquidity target is 100 - ar_in_target, the level the
-- rebalancer refills it to. A target set for an NK channel becomes its
-- ar_in_target; the targets of the other channels stay with their ar_in_target.
UPDATE gui_channels SET ar_in_target = LEAST(GREATEST(100 - nk_target, 1), 100)
WHERE af_mode = 'nk' AND nk_target IS NOT NULL;
ALTER TABLE gui_channels DROP COLUMN nk_target;
