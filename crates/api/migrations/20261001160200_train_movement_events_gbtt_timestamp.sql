SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- train_movement_events.gbtt_timestamp: TRUST's public-timetable (GBTT)
-- time for the movement, as sent on the 0003 message alongside
-- planned_timestamp (the working-timetable time). NULL for a pass or any
-- non-public event (TRUST sends it empty), and for every row written
-- before this column existed: raw_body has always been stored as `{}`, so
-- there is nothing to backfill from. Public-time delay history starts
-- with this migration. See
-- docs/superpowers/specs/2026-10-01-working-vs-public-times-design.md (P3).
--
-- Nullable, no default, catalog-only.
-- -------------------------------------------------------------------------
ALTER TABLE train_movement_events ADD COLUMN gbtt_timestamp TIMESTAMPTZ;
