-- no-transaction
-- -------------------------------------------------------------------------
-- Supports the aggregator's retention prunes of `trains`
-- (`crates/aggregator/src/queries.rs::prune_trains` and
-- `crates/aggregator/src/archive.rs::archive_and_prune_trains`), which select
-- `WHERE service_date < CURRENT_DATE - <retention>` every cycle (~1/min).
-- The only indexes containing `service_date` had it as their SECOND column,
-- so with the real 14-day untracked retention and `LIMIT 1000` the planner
-- chose a full sequential scan of `trains` (prod 2026-09-27: ~425k tuples
-- per cycle; DB review part 2, DB2-6).
--
-- CONCURRENTLY and alone in its file: see
-- crates/api/tests/migration_index_locking.rs. If the build is interrupted
-- it leaves an INVALID index that IF NOT EXISTS would then skip; recovery
-- is `DROP INDEX CONCURRENTLY trains_service_date;` and a restart.
-- -------------------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS trains_service_date
    ON trains (service_date);
