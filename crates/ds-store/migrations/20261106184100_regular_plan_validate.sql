SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Validates the CHECK 20261106184000_regular_plan.sql added NOT VALID.
-- VALIDATE CONSTRAINT takes SHARE UPDATE EXCLUSIVE on journeys (reads and
-- writes carry on). Every existing row has NULL in the new columns, so
-- none can fail.
-- -------------------------------------------------------------------------

ALTER TABLE journeys VALIDATE CONSTRAINT journeys_regular_plan_check;
