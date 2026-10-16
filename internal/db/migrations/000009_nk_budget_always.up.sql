-- NK channels always use the NK rebalance budget, so the per-channel flag goes.
ALTER TABLE gui_channels DROP COLUMN IF EXISTS nk_budget;
