-- no-transaction
-- -------------------------------------------------------------------------
-- Supports the aggregator's retention prune of `line_status_history`
-- (`crates/aggregator/src/queries.rs::prune_history`), which deletes
-- `WHERE computed_at < NOW() - <retention>` every cycle. The existing
-- `line_status_history_line_time (line_id, computed_at DESC)` is not led by
-- `computed_at`, so the prune was a sequential scan every cycle (DB review
-- F10). With this index it is a MIN probe plus an index range scan.
--
-- CONCURRENTLY and alone in its file: see
-- crates/api/tests/migration_index_locking.rs. If the build is interrupted
-- it leaves an INVALID index that IF NOT EXISTS would then skip; recovery
-- is `DROP INDEX CONCURRENTLY line_status_history_computed_at;` and a
-- restart.
-- -------------------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS line_status_history_computed_at
    ON line_status_history (computed_at);
