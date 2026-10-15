DROP TABLE IF EXISTS route_attempt_log;
ALTER TABLE gui_rebalanceroute
    DROP COLUMN IF EXISTS last_probed,
    DROP COLUMN IF EXISTS probed_amount_sat;
