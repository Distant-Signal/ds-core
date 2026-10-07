-- no-transaction
-- -------------------------------------------------------------------------
-- Index the diff publish's key staging table for
-- `schedule_calling_points_full`. Same shape, same risk and same column-
-- order reasoning as
-- 20260926220000_schedule_destination_departures_publish_keys_index.sql:
-- `publish_id` leads (serving `summarize` and `drop_publish`), followed by
-- the `delete_missing` anti-join's equality columns so a nested-loop plan's
-- inner side is an index probe rather than a seq scan of every staged key.
--
-- CONCURRENTLY and alone in its file: see
-- crates/api/tests/migration_index_locking.rs.
-- -------------------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS schedule_calling_points_full_publish_keys_probe
    ON schedule_calling_points_full_publish_keys (publish_id, service_date, uid, seq);
