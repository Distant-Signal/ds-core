SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- trust_event_backlog.gbtt_timestamp: TRUST's public-timetable (GBTT) time
-- for a Movement row, under the same timestamp correction as
-- planned_timestamp. trust-backlog-consumer is the primary writer of
-- train_movement_events (through POST /private/trust-event-backlog), and
-- until now it never carried this field, so every
-- train_movement_events.gbtt_timestamp it wrote was NULL (6.8M of 6.8M
-- rows on 2026-10-07). The backlog row needs its own copy so the
-- backlog-match replay and the uid-less replay can carry it too.
--
-- NULL for a pass, an Activation/Cancellation/Reinstatement, an empty
-- GBTT time, and every row written before this column existed.
--
-- Nullable, no default, catalog-only.
-- -------------------------------------------------------------------------
ALTER TABLE trust_event_backlog ADD COLUMN gbtt_timestamp TIMESTAMPTZ;
