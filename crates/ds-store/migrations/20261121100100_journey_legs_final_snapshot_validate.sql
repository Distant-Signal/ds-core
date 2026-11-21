SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Validates the CHECK 20261121100000_journey_legs_final_snapshot.sql added
-- NOT VALID. VALIDATE CONSTRAINT takes SHARE UPDATE EXCLUSIVE on
-- journey_legs (reads and writes carry on). Every existing row has NULL in
-- final_snapshot, so none can fail.
-- -------------------------------------------------------------------------

ALTER TABLE journey_legs VALIDATE CONSTRAINT journey_legs_final_snapshot_check;
