-- ---------------------------------------------------------------------
-- Schedule-first resolution for tracked-train pins (Decisions 3-5 of
-- docs/superpowers/specs/2026-09-05-schedule-first-train-tracking-design.md):
-- a new 'schedule_matched' resolution_status waypoint between 'pending'
-- and 'resolved', plus nullable columns for the schedule-match snapshot
-- this plan's Open Question 5 resolves in favor of a hybrid
-- snapshot-the-expensive-part / re-derive-the-cheap-name-part shape (see
-- the plan's own writeup -- schedule_destination_crs is resolved to a
-- display name via a LEFT JOIN at read time, the same way
-- pin_destination_crs already is; schedule_calling_points is NOT
-- re-derived at read time, since that would mean a live JOIN into a
-- whole line's whole-day schedule_line_population JSONB on every GET).
-- ---------------------------------------------------------------------

-- `IF EXISTS`/re-add rather than a bare rename: this is the standard,
-- unnamed-inline-CHECK-constraint auto-generated name Postgres assigns
-- (`{table}_{column}_check`), confirmed against
-- crates/api/migrations/20260828120000_train_tracking.sql's original,
-- un-named `CHECK (resolution_status IN (...))` clause -- `IF EXISTS`
-- is defensive only, in case that ever proves wrong on a real database
-- (see this task's own verification step).
ALTER TABLE tracked_trains DROP CONSTRAINT IF EXISTS tracked_trains_resolution_status_check;
ALTER TABLE tracked_trains ADD CONSTRAINT tracked_trains_resolution_status_check
    CHECK (resolution_status IN ('pending', 'schedule_matched', 'resolved', 'unresolved'));

-- Which candidate line's schedule_line_population produced the match --
-- an audit/debugging column only. Nothing re-queries
-- schedule_line_population using it at read time.
ALTER TABLE tracked_trains ADD COLUMN matched_line_id TEXT;

-- Snapshot of the matched entry's calling_points at match time, already
-- camelCase-shaped on write (see schedule_matching::ScheduleCallingPointDto,
-- Task 6) so this crate can relay it to the frontend as opaque JSON with
-- no read-time conversion.
ALTER TABLE tracked_trains ADD COLUMN schedule_calling_points JSONB;

-- The matched schedule's own terminus CRS, resolved once at match time --
-- same "store the stable code, derive the display name via a read-time
-- LEFT JOIN stations" pattern pin_origin_crs/pin_destination_crs already
-- use (TRACKED_TRAIN_STATE_SELECT).
ALTER TABLE tracked_trains ADD COLUMN schedule_destination_crs TEXT;

-- Parallels resolved_at, but independent of it: this is set the moment a
-- schedule match happens (Decision 3 step 4), NOT when TRUST later
-- confirms the pin live -- resolved_at keeps meaning exactly what it
-- means today.
ALTER TABLE tracked_trains ADD COLUMN schedule_matched_at TIMESTAMPTZ;
