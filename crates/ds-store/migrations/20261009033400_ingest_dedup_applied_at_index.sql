-- no-transaction
-- -------------------------------------------------------------------------
-- Supports the ingest-writer's hourly prune of `ingest_dedup`
-- (`ingest_writer::dedup::prune`: `DELETE ... WHERE applied_at < now() -
-- 48 h`), so it is an index range scan rather than a scan of two days of
-- keys.
--
-- CONCURRENTLY and alone in its file: see
-- crates/api/tests/migration_index_locking.rs. If the build is interrupted
-- it leaves an INVALID index that IF NOT EXISTS would then skip; recovery
-- is `DROP INDEX CONCURRENTLY ingest_dedup_applied_at;` and a restart.
-- -------------------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS ingest_dedup_applied_at
    ON ingest_dedup (applied_at);
