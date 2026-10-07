-- -------------------------------------------------------------------------
-- Option C (event-time monotonicity guard) of
-- docs/superpowers/specs/2026-09-07-shared-train-status-write-race-design.md.
-- `trust-consumer` and `trust-backlog-consumer` both write
-- `train_current_state` for the same `trains_id`, via the shared
-- `upsert_train_movement` function, with no ordering guarantee between them
-- -- a lagging writer's stale event can silently overwrite a fresher one
-- (design doc §1.2/§2). This column is the guard's storage: each row now
-- carries the real-world time of the last event actually applied to it, so
-- a future write can be compared against it before being allowed to win.
-- -------------------------------------------------------------------------

ALTER TABLE train_current_state ADD COLUMN event_time TIMESTAMPTZ;

-- One-off backfill for rows that already existed before this column did.
-- `updated_at` (when Postgres last wrote the row) is an approximation of
-- "the real-world time of the last event applied" for these rows, not an
-- exact value -- there is no other historical event-time signal to backfill
-- from. See the design doc's Open Question 1: this is accepted as good
-- enough without further measurement, not re-litigated here.
UPDATE train_current_state SET event_time = updated_at WHERE event_time IS NULL;
