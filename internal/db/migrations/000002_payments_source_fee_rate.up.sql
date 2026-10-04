-- Outbound ppm of the rebalance source channel(s) at import time (opportunity cost).
-- IF NOT EXISTS: a database migrated over from the Django app already has the column.
ALTER TABLE gui_payments ADD COLUMN IF NOT EXISTS source_fee_rate integer;
