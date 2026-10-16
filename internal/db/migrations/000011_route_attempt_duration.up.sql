-- How long a sent attempt took until it succeeded or failed (or until the
-- rebalancer stopped waiting), in milliseconds. The rebalancer spreads it over
-- the route's hops to find slow node pairs.
ALTER TABLE route_attempt_log ADD COLUMN duration_ms integer;
