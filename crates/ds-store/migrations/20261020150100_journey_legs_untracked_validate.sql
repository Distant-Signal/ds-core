SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Validates the two CHECKs the previous migration added NOT VALID
-- (`journey_legs_kind_check`, `journey_legs_untracked_shape_check`;
-- docs/superpowers/specs/2026-10-20-untracked-legs-and-tracked-train-states-design.md
-- §2.1). VALIDATE CONSTRAINT takes only SHARE UPDATE EXCLUSIVE, so the api
-- and the notifier keep writing legs while the rows are scanned. Every row
-- written before the columns existed is a train leg with them NULL, so
-- this cannot fail.
-- -------------------------------------------------------------------------
ALTER TABLE journey_legs
    VALIDATE CONSTRAINT journey_legs_kind_check;

ALTER TABLE journey_legs
    VALIDATE CONSTRAINT journey_legs_untracked_shape_check;
