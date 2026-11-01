SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Validates the CHECKs 20261101130000 added NOT VALID for timetabled
-- untracked legs (DS-237). VALIDATE CONSTRAINT takes only SHARE UPDATE
-- EXCLUSIVE, so the api's reads and writes carry on while it scans; every
-- existing row has none of the new columns set, which passes both.
-- -------------------------------------------------------------------------

ALTER TABLE journey_legs VALIDATE CONSTRAINT journey_legs_timetable_check;
ALTER TABLE journey_revision_legs VALIDATE CONSTRAINT journey_revision_legs_timetable_check;
