-- no-transaction
-- -------------------------------------------------------------------------
-- The notifier's leave_reminder cycle reads, every minute, the journeys
-- that have a station buffer (20261106190000). Partial: most journeys have
-- none.
--
-- CONCURRENTLY and alone in its file: see
-- crates/ds-store/tests/migration_index_locking.rs. If the build is
-- interrupted it leaves an INVALID index, which `ds_store::migrate` drops
-- before the retry (crates/ds-store/src/migrate.rs).
-- -------------------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS journeys_station_buffer
    ON journeys (user_id)
    WHERE station_buffer_minutes IS NOT NULL;
