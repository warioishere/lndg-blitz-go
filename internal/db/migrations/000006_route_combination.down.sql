DELETE FROM route_attempt_log WHERE route_id IS NULL;
ALTER TABLE route_attempt_log ALTER COLUMN route_id SET NOT NULL;
ALTER TABLE gui_rebalanceroute DROP COLUMN IF EXISTS last_failure_hop;
