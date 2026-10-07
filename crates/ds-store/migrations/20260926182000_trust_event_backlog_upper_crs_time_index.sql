-- no-transaction
-- -------------------------------------------------------------------------
-- Replaces `trust_event_backlog_crs_time (crs, planned_timestamp)` with an
-- index its one intended query can actually use.
--
-- That query is `trust_event_backlog_match::find_backlog_match` (the
-- late-tracking pin's CRS+time lookup, run per pending pin by the api's
-- periodic `run_backlog_match_sweep` and on pin creation), which filters
-- `UPPER(crs) = UPPER($1) AND planned_timestamp BETWEEN $2 AND $3`. A btree
-- on the bare `crs` column cannot serve an `UPPER(crs)` predicate, so every
-- call has been a sequential scan of the whole backlog: production showed
-- `trust_event_backlog_crs_time` at 39 MB with 0 scans, and EXPLAIN ANALYZE
-- on 560k synthetic rows (Postgres 18, which even has skip scan) chose a
-- parallel seq scan at ~45 ms. With this expression index the same query is
-- a bitmap index scan at ~0.1 ms, and the planner proves the partial
-- predicate `crs IS NOT NULL` from the strict `UPPER(crs) = ...` clause.
--
-- Indexing the expression, rather than rewriting the query to `crs =
-- UPPER($1)`, keeps the query's case-insensitive semantics exactly: nothing
-- upstream (trust-backlog-consumer's STANOX->CRS table, fed from a CSV and
-- from api's stanox_crs table) normalises case before insert.
--
-- CONCURRENTLY and alone in its file: see
-- crates/api/tests/migration_index_locking.rs and
-- 20260925222000_trains_train_id_service_date_unique.sql for why. The old
-- index is dropped by the next migration, once this one exists.
-- -------------------------------------------------------------------------
CREATE INDEX CONCURRENTLY trust_event_backlog_upper_crs_time
    ON trust_event_backlog (UPPER(crs), planned_timestamp)
    WHERE crs IS NOT NULL;
