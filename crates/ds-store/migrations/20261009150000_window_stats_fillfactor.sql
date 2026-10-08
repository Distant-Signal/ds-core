SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- `full_coverage_line_window_stats` fillfactor 50.
--
-- full-coverage-consumer rewrites every row of the current 15-minute bucket
-- about once a minute (~392 updates a minute in prod, 2026-10-08). At the
-- default fillfactor of 100 each bucket's rows are inserted onto full pages,
-- so ~77% of those updates were non-HOT: the new row version lands on another
-- page and every index gets a new entry, although no indexed column changes.
-- Leaving half of each page free lets the updates stay on-page (HOT), with no
-- index writes and far less bloat. Rows, values and readers are unchanged.
--
-- Only pages written after this take the new setting. Every bucket inserts
-- fresh rows and rows are pruned after 14 days, so the table turns over on
-- its own; no rewrite is needed. Cost: roughly twice the table size
-- (~+125 MB at 14-day retention). Revert with SET (fillfactor = 100).
--
-- SET (fillfactor) takes SHARE UPDATE EXCLUSIVE, which does not block reads
-- or writes.
-- -------------------------------------------------------------------------
ALTER TABLE full_coverage_line_window_stats SET (fillfactor = 50);
