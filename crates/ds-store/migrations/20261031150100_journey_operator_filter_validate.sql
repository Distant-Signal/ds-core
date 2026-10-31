SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Validates the two CHECKs 20261031150000 added NOT VALID on journeys'
-- operator filter columns (the planner operator filtering design §6.9).
-- VALIDATE CONSTRAINT takes only SHARE UPDATE EXCLUSIVE, so the api's
-- reads and writes carry on while it scans; every existing row has no
-- filter, which passes both.
-- -------------------------------------------------------------------------

ALTER TABLE journeys VALIDATE CONSTRAINT journeys_operator_filter_mode_check;
ALTER TABLE journeys VALIDATE CONSTRAINT journeys_operator_filter_consistent;
