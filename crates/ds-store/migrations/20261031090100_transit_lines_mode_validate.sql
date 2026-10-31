SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Validates the CHECK 20261031090000 added NOT VALID on transit_lines.mode
-- (the TfL transit design §6). VALIDATE CONSTRAINT takes only SHARE UPDATE
-- EXCLUSIVE, so the api's reads and the writer's activations carry on
-- while it scans (a few dozen rows).
-- -------------------------------------------------------------------------

ALTER TABLE transit_lines VALIDATE CONSTRAINT transit_lines_mode_check;
