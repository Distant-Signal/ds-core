SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Validates `journey_legs_stop_ids_check`, which the previous migration
-- added NOT VALID (docs/superpowers/specs/2026-10-20-tfl-stop-lookup-design.md
-- §4). VALIDATE CONSTRAINT takes only SHARE UPDATE EXCLUSIVE, so the api
-- and the notifier keep writing legs while the rows are scanned. Every row
-- written before the columns existed has them NULL, so this cannot fail.
-- -------------------------------------------------------------------------
ALTER TABLE journey_legs
    VALIDATE CONSTRAINT journey_legs_stop_ids_check;
