-- no-transaction
-- -------------------------------------------------------------------------
-- Step 2 of 3: the unique index that becomes the new primary key
-- (line_id, service_date) in step 3.
--
-- Built CONCURRENTLY, alone in its file, so writes keep flowing while it
-- builds -- see crates/api/tests/migration_index_locking.rs. The current
-- primary key on line_id alone guarantees the existing rows are unique on
-- (line_id, service_date) too, so this cannot fail on duplicates.
--
-- If the build is ever interrupted it leaves an INVALID index behind that
-- IF NOT EXISTS would then skip, and step 3 would refuse it; recovery is
-- `DROP INDEX CONCURRENTLY full_coverage_line_stats_line_id_service_date_key;`
-- and a restart.
-- -------------------------------------------------------------------------
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS full_coverage_line_stats_line_id_service_date_key
    ON full_coverage_line_stats (line_id, service_date);
