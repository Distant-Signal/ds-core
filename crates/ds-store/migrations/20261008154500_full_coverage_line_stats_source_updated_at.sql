SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- `full_coverage_line_stats.source_updated_at` (ingest plan 3a.5, spec
-- §7.4 and §7.8, decision D13): the observed time the stream writer's
-- ordering guard compares, so a redelivered older `full-coverage-stats/1`
-- snapshot (an XAUTOCLAIM after a crash can reorder) does not overwrite a
-- newer one. The `/1` body carries no time of its own, so the handler
-- (3a.6) sets it from the envelope's `produced_at`, clamped to the writer's
-- `now() + 2 min`.
--
-- Expand only. Nullable with no default: rows written before this column,
-- and by the api's HTTP route until it is retired, have NULL, which the
-- guard treats as older than any snapshot (`t.source_updated_at IS NULL OR
-- ...`). A nullable column with no default is catalog-only: nothing is
-- rewritten (crates/api/tests/migration_index_locking.rs).
-- -------------------------------------------------------------------------
ALTER TABLE full_coverage_line_stats ADD COLUMN source_updated_at TIMESTAMPTZ;
