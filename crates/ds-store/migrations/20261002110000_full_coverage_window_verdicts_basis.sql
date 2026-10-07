SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Which rule produced each full-coverage `escalate` verdict (user decision,
-- 2026-10-02; the "Decisions (2026-10-02, sparse windows)" section of
-- docs/superpowers/specs/2026-09-27-full-coverage-windowed-stats-design.md):
--
-- * 'rate': the rate tiers over a window of at least
--   full_coverage_min_sample_size (6) evaluable trains;
-- * 'sparse_all_cancelled': rule A, a window below the sample size in which
--   every train (at least 2) was explicitly cancelled. aggregator enforces
--   it under its own allowlist (FULL_COVERAGE_WINDOW_SPARSE_ENFORCE_LINES,
--   empty by default: shadow-only), so the two are told apart here and in
--   `compare_full_coverage --windows`.
--
-- NULL for `good` and `ineligible` verdicts and for every row written
-- before this migration (all of which were 'rate'). A nullable column with
-- no default is a catalog-only change: no rewrite, no scan. The CHECK is
-- added NOT VALID, so adding it scans nothing under this migration's lock
-- (crates/api/tests/migration_index_locking.rs); it is enforced on every
-- new and updated row, and the existing rows are NULL, which it accepts.
-- -------------------------------------------------------------------------
ALTER TABLE full_coverage_window_verdicts ADD COLUMN basis TEXT;

ALTER TABLE full_coverage_window_verdicts
    ADD CONSTRAINT full_coverage_window_verdicts_basis_check
        CHECK (basis IN ('rate', 'sparse_all_cancelled')) NOT VALID;
