SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- A train leg's train and timetable, kept after the train's own data is gone
-- (DS-534, a sibling of DS-379's final_* outcome columns).
-- ds-workers' retention deletes `trains` (and with it `train_current_state`,
-- `train_movement_events` and `train_reasons`) about 30 days after the
-- service date, and `train_subscriptions.trains_id` goes NULL
-- (ON DELETE SET NULL). After that a matched leg had no train uid and no
-- timetable left anywhere, so the journey page could show nothing but
-- "matched to a timetabled train". These columns outlive `trains`
-- retention: the api snapshots the train's uid and a compact timetable and
-- outcome onto the leg while the train still exists, and renders from the
-- snapshot afterwards.
--
-- Expand-only: three nullable columns with no default (no rewrite) and a
-- CHECK added NOT VALID, validated by
-- 20261121100100_journey_legs_final_snapshot_validate.sql. Every existing
-- row has NULL in all three. Old code never reads them.
--
-- No backfill here. The api's hourly leg outcome sweep (`data::leg_outcomes`)
-- fills them for every leg whose subscription is resolved and whose `trains`
-- row still exists, and re-snapshots a leg once its train has completed. No
-- index: the sweep walks legs by id in batches, as it does for
-- `final_recorded_at`.
--
-- No grants change: journey_legs is already api SIUD (the sweep) and
-- notifier SIU table-wide; the writer's grant is column-limited and does
-- not cover them.
-- -------------------------------------------------------------------------

ALTER TABLE journey_legs
    -- The matched train's `trains.train_uid`, copied while the `trains` row
    -- exists. NULL: not snapshotted yet, or the leg never matched a train.
    ADD COLUMN final_train_uid   TEXT,
    -- A compact JSON object: the train's timetable (calling points with
    -- booked and actual times) and outcome, as the journey page needs them.
    -- Its shape belongs to the api and may grow; readers ignore unknown
    -- keys. NULL: not snapshotted yet.
    ADD COLUMN final_snapshot    JSONB,
    -- When the sweep last wrote the two columns above. NULL: never. Legs to
    -- snapshot are those with this NULL; legs to re-snapshot after
    -- completion are found by comparing it with when the train finished.
    ADD COLUMN final_snapshot_at TIMESTAMPTZ;

ALTER TABLE journey_legs
    ADD CONSTRAINT journey_legs_final_snapshot_check CHECK (
        final_snapshot IS NULL OR jsonb_typeof(final_snapshot) = 'object'
    ) NOT VALID;
