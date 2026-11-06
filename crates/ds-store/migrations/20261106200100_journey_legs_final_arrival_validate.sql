SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Validates the CHECK 20261106200000_journey_legs_final_arrival.sql added
-- NOT VALID. VALIDATE CONSTRAINT takes SHARE UPDATE EXCLUSIVE on
-- journey_legs (reads and writes carry on). Every existing row has NULL in
-- the new columns, so none can fail.
-- -------------------------------------------------------------------------

ALTER TABLE journey_legs VALIDATE CONSTRAINT journey_legs_final_arrival_check;
