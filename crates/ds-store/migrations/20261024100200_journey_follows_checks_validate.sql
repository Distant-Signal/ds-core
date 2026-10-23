SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Validates the CHECK 20261024100100 added NOT VALID. VALIDATE CONSTRAINT
-- takes only SHARE UPDATE EXCLUSIVE, so the api keeps writing while the
-- rows are scanned. Every row was written under the constraint already, so
-- this cannot fail.
-- -------------------------------------------------------------------------

ALTER TABLE journey_follows VALIDATE CONSTRAINT journey_follows_one_share_check;
