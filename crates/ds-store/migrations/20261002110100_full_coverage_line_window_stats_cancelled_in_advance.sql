SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Of each window's explicit cancellations, how many arrived at least three
-- hours before the train was due (FullCoverageWindowCounts::
-- cancelled_in_advance). It only annotates the sparse all-cancelled rule's
-- reason ("... were cancelled (cancelled in advance).") and changes no
-- verdict. See the "Decisions (2026-10-02, sparse windows)" section of
-- docs/superpowers/specs/2026-09-27-full-coverage-windowed-stats-design.md.
--
-- NOT NULL DEFAULT 0: a constant default is stored in the catalog (no
-- rewrite, no scan; PostgreSQL 11+), and every existing row, like every row
-- from a consumer older than the field, reads 0 ("none known in advance").
-- -------------------------------------------------------------------------
ALTER TABLE full_coverage_line_window_stats
    ADD COLUMN cancelled_in_advance INT NOT NULL DEFAULT 0;
