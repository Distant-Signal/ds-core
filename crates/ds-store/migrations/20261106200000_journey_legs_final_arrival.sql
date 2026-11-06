SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- A train leg's outcome, kept after the train's own data is gone (DS-379).
-- ds-workers' retention deletes `trains`, `train_current_state` and the
-- movements about 30 days after the service date (`trainsRetentionDays`),
-- but regular journeys' 90-day rule (distant-signal's
-- docs/superpowers/specs/2026-11-06-regular-journeys-design.md, decision 11
-- and §4.2) keeps an occurrence whose train arrived at least 15 minutes
-- late or was cancelled. The api's hourly leg outcome sweep
-- (`data::leg_outcomes`) records it here once the train is over, while
-- its data is still held; the retention rule reads these columns.
--
-- Expand-only: three nullable columns (no rewrite) and a CHECK added NOT
-- VALID, validated by 20261106200100_journey_legs_final_arrival_validate.sql.
-- Every existing row has NULL in them. Old code never reads them.
--
-- No grants change: journey_legs is already api SIUD (the sweep) and
-- notifier SIU table-wide; the writer's grant is column-limited and does
-- not cover them.
-- -------------------------------------------------------------------------

ALTER TABLE journey_legs
    -- The train's public delay at the leg's own destination, in minutes,
    -- as the journey page shows it: measured once it reported there, else
    -- the last forecast. NULL: not known (no train, or no report).
    ADD COLUMN final_arrival_delay_minutes INT,
    -- Whether the train was cancelled (`train_current_state.status`), in
    -- whole or in part. NULL: not known.
    ADD COLUMN final_train_cancelled       BOOLEAN,
    -- When the sweep recorded the two columns above. NULL: not recorded
    -- yet; the sweep writes each leg once.
    ADD COLUMN final_recorded_at           TIMESTAMPTZ;

ALTER TABLE journey_legs
    ADD CONSTRAINT journey_legs_final_arrival_check CHECK (
        final_recorded_at IS NOT NULL
        OR (final_arrival_delay_minutes IS NULL AND final_train_cancelled IS NULL)
    ) NOT VALID;
