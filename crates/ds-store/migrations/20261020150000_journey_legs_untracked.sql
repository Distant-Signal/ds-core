SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Untracked journey legs (docs/superpowers/specs/
-- 2026-10-20-untracked-legs-and-tracked-train-states-design.md §2.1): a
-- leg for a Tube ride, a bus, a walk or anything else we have no live or
-- timetable data for. It has a mode, optional free-text places and line,
-- and planned times; its status comes from the clock alone.
--
-- Additive only. Every new column is nullable or has a constant default,
-- so ADD COLUMN changes only the catalog (no table rewrite), and every
-- existing row is a train leg (`kind = 'train'`, the new columns NULL).
-- Existing readers never select the new columns and keep working.
--
-- An untracked leg also has `train_subscription_id IS NULL` and
-- `match_mode = 'manual'`: it is entered by hand and has nothing to search
-- for, so the notifier's template sweep (`match_mode = 'unmatched'` only)
-- never sees it. `service_date` is the London date of `planned_departure`.
--
-- The two CHECKs are added NOT VALID (a brief ACCESS EXCLUSIVE for the
-- catalog only, hence the timeout; new writes are checked at once) and
-- validated by the next migration under SHARE UPDATE EXCLUSIVE. Every
-- existing row is a train leg with the new columns NULL, so neither can
-- fail. The mode list is checked by the api, not here, so adding a mode
-- needs no migration.
-- -------------------------------------------------------------------------
ALTER TABLE journey_legs
    ADD COLUMN kind TEXT NOT NULL DEFAULT 'train',
    ADD COLUMN transit_mode TEXT,
    ADD COLUMN origin_place TEXT,
    ADD COLUMN destination_place TEXT,
    ADD COLUMN line_name TEXT,
    ADD COLUMN planned_departure TIMESTAMPTZ,
    ADD COLUMN planned_arrival TIMESTAMPTZ;

ALTER TABLE journey_legs
    ADD CONSTRAINT journey_legs_kind_check
    CHECK (kind IN ('train', 'untracked'))
    NOT VALID;

ALTER TABLE journey_legs
    ADD CONSTRAINT journey_legs_untracked_shape_check
    CHECK (
        (kind = 'train'
            AND transit_mode IS NULL
            AND origin_place IS NULL
            AND destination_place IS NULL
            AND line_name IS NULL
            AND planned_departure IS NULL
            AND planned_arrival IS NULL)
        OR
        (kind = 'untracked'
            AND transit_mode ~ '^[a-z_]{1,20}$'
            AND planned_departure IS NOT NULL
            AND planned_arrival IS NOT NULL
            AND planned_arrival >= planned_departure
            AND planned_arrival <= planned_departure + INTERVAL '24 hours'
            AND train_subscription_id IS NULL
            AND match_mode = 'manual'
            AND origin_crs IS NULL
            AND destination_crs IS NULL
            AND (origin_place IS NULL OR char_length(origin_place) BETWEEN 1 AND 100)
            AND (destination_place IS NULL OR char_length(destination_place) BETWEEN 1 AND 100)
            AND (line_name IS NULL OR char_length(line_name) BETWEEN 1 AND 60))
    )
    NOT VALID;
