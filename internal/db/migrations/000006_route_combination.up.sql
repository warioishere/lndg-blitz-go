-- Failure source index of a saved route's latest failure: the part of the
-- route before that node worked and can be combined with other routes.
ALTER TABLE gui_rebalanceroute ADD COLUMN last_failure_hop integer;

-- A combined route is tried before it is saved, so its attempt has no route row.
ALTER TABLE route_attempt_log ALTER COLUMN route_id DROP NOT NULL;
