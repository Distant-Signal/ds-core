-- crates/api/migrations/20260904070000_station_full_coverage_samples.sql
-- -------------------------------------------------------------------------
-- Per-(station, operator) full-coverage producer table. One row per (crs,
-- operator), wholesale-replaced per producer resolution cycle -- same
-- "live snapshot, not history" posture as station_samples (the LDBWS
-- sample-stats sibling this mirrors one level finer) and
-- full_coverage_line_stats (its per-line counterpart, owned by a
-- different chain). No real writer populates this table yet -- see
-- docs/superpowers/specs/2026-09-04-per-station-full-coverage-stats-design.md
-- Decision 2 and its now-resolved Open Question #1: the schema below is
-- adopted verbatim by
-- docs/superpowers/specs/2026-09-04-option-b-live-consumer-design.md
-- Decision 2h/3 as the actual producer contract Option B's future
-- consumer writes to.
-- -------------------------------------------------------------------------

CREATE TABLE station_full_coverage_samples (
    crs         CHAR(3)     NOT NULL,
    operator    TEXT        NOT NULL,
    resolved_at TIMESTAMPTZ NOT NULL,
    stats       JSONB       NOT NULL,
    PRIMARY KEY (crs, operator)
);
