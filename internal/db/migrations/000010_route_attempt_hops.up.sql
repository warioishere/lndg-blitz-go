-- The hop pubkeys of every attempted route joined by "-" (the node itself last), so
-- an attempt keeps its path for combined and QueryRoutes routes and after a
-- saved route is deleted.
ALTER TABLE route_attempt_log ADD COLUMN route text;
