SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Validates the CHECK 20261030200000 added NOT VALID on
-- tfl_line_routes.step_seconds (DS-206). VALIDATE CONSTRAINT takes only
-- SHARE UPDATE EXCLUSIVE, so the api's reads and the writer's daily
-- replace carry on while it scans (about a hundred rows).
-- -------------------------------------------------------------------------

ALTER TABLE tfl_line_routes VALIDATE CONSTRAINT tfl_line_routes_step_seconds_check;
