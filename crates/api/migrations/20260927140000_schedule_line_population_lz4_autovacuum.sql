SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- schedule_line_population: lz4 TOAST compression and autovacuum tuning
-- (F4 / DQ8, option D).
--
-- Each row is one line's population for one service date, stored as a JSONB
-- blob of up to ~3 MB, so nearly all of the table is TOAST: in production on
-- 2026-09-27 the heap was 208 kB and the TOAST table 1,292 MB for 1,765
-- rows. The daily schedule publish replaces roughly one service date's rows
-- in one go and the aggregator prunes one, yet the TOAST table had never
-- been autovacuumed (autovacuum_count = 0, 44.5k dead chunks), because the
-- default 20% scale factor is larger than one day's turnover.
--
-- * population SET COMPRESSION lz4: lz4 decompresses several times faster
--   than the default pglz, which every read of a blob pays (the
--   full-coverage consumer's conditional GET, and the schedule-match reads).
--   Only values written from now on use it; existing rows keep pglz until
--   the next publish rewrites them, which happens within the retention
--   window. Catalog-only: it takes ACCESS EXCLUSIVE, but only for the
--   instant of the catalog update, and the table's readers go through the
--   api that is running this migration at startup.
--
-- * autovacuum: 0.05 for the heap and for its TOAST table, so each daily
--   publish/prune (about 1/8 of the rows) crosses the threshold. SET
--   (storage_parameter) is catalog-only under SHARE UPDATE EXCLUSIVE.
--
-- Measured before choosing D over a uid side table: pg_stat_statements
-- over 2h17m on 2026-09-27 recorded zero calls of the population-scanning
-- fallback (`population @> ...`) and of the per-line entry reads, with no
-- pending pins at all.
-- -------------------------------------------------------------------------

ALTER TABLE schedule_line_population ALTER COLUMN population SET COMPRESSION lz4;

ALTER TABLE schedule_line_population SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.05,
    toast.autovacuum_vacuum_scale_factor = 0.05
);
