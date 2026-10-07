-- -------------------------------------------------------------------------
-- Step 1 of 3: keep full_coverage_line_stats history per rail day.
--
-- WHY. The table was keyed by `line_id` alone -- one row per line,
-- overwritten every minute and at every rail-day rollover -- so a day's
-- final row could never be audited afterwards. When restarts of
-- full-coverage-consumer corrupted rail days 2026-09-25 and 2026-09-26
-- (every restart wiped the day's in-memory state, and every train seen
-- before it was then counted as cancelled), nothing survived to check.
-- Steps 2 and 3 re-key it by (line_id, service_date).
--
-- `service_date` already exists and is NOT NULL on every row (it was the
-- freshness guard of the original design), so no date column needs adding
-- or backfilling: each existing row keeps the date it was written for.
--
-- This step adds `partial`: the producer did not see every event of that
-- day (it started mid-day and could not replay the day's start), so unseen
-- services were left out rather than counted as cancelled, and the row
-- never reads 'available'.
--
-- BACKFILL. Every row written before this change came from a consumer
-- that rebuilt nothing on restart, so none of them can be trusted to be
-- complete: they are marked partial. Done without an UPDATE or a table
-- rewrite: the column is added with DEFAULT true (a constant default is
-- stored in the catalog and applies to every existing row), then the
-- default for NEW rows is switched to false. A full-coverage-consumer that
-- predates this change omits the field, which the api reads as false --
-- the meaning its rows have always had.
--
-- The lock_timeout makes a wait behind a long-running transaction fail
-- fast; the migration then reruns on the next api start.
-- -------------------------------------------------------------------------
SET LOCAL lock_timeout = '5s';

ALTER TABLE full_coverage_line_stats
    ADD COLUMN IF NOT EXISTS partial BOOLEAN NOT NULL DEFAULT true;

ALTER TABLE full_coverage_line_stats
    ALTER COLUMN partial SET DEFAULT false;
