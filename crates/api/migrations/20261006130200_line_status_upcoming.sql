SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- `line_status.upcoming`: future disruption notices for the line (user
-- decision 2026-10-06): an unplanned incident with a high-confidence,
-- ongoing period that has not started yet -- industrial action, or any
-- dated period starting within 14 days. A note beside the line's status,
-- never part of its severity: before 2026-10-06 such a notice either showed
-- as the line's current status (first rail day) or not at all.
--
-- A JSON array of `{"from", "to", "summary", "incident_id"}` objects
-- (`common::UpcomingDisruption`), written by the aggregator every cycle and
-- rendered by `GET /public/lines/...` as `upcoming`. `poller-tfl`'s rows
-- never write it and keep the default.
--
-- NOT NULL with a constant default: catalog-only since PostgreSQL 11,
-- nothing is rewritten (crates/api/tests/migration_index_locking.rs).
-- -------------------------------------------------------------------------
ALTER TABLE line_status ADD COLUMN upcoming JSONB NOT NULL DEFAULT '[]'::jsonb;
