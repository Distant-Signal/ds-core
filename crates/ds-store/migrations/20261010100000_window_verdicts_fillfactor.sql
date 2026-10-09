SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- `full_coverage_window_verdicts` fillfactor 50.
--
-- The same change, for the same reason, as
-- 20261009150000_window_stats_fillfactor.sql: aggregator re-evaluates every
-- line's current 15-minute bucket on each tick and upserts its row
-- (ON CONFLICT (line_id, bucket_start) DO UPDATE), and no indexed column
-- changes on those updates. At the default fillfactor of 100 the rows sit on
-- full pages, so most updates are non-HOT and write a new entry in every
-- index. Leaving half of each page free keeps them on-page (HOT). Rows,
-- values and readers are unchanged.
--
-- Only pages written after this take the new setting. Every bucket inserts
-- fresh rows and rows are pruned after 14 days, so the table turns over on
-- its own; no rewrite is needed. Cost: roughly twice the table size. Revert
-- with SET (fillfactor = 100).
--
-- SET (fillfactor) takes SHARE UPDATE EXCLUSIVE, which does not block reads
-- or writes.
-- -------------------------------------------------------------------------
ALTER TABLE full_coverage_window_verdicts SET (fillfactor = 50);
