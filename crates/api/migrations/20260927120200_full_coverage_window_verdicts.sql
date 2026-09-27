-- -------------------------------------------------------------------------
-- What aggregator decided about each line's `recent` full-coverage window,
-- one row per (line, 15-minute window bucket), latest evaluation wins.
-- Written only with FULL_COVERAGE_WINDOW_MODE=shadow or enforce (default
-- off). See the "Decisions (2026-09-27)" section of
-- docs/superpowers/specs/2026-09-27-full-coverage-windowed-stats-design.md.
--
-- `would_escalate_to` is the window's tier when it is strictly worse than
-- what the line was already showing (LDBWS and incidents), whether or not
-- it was applied; `below_min_rank` marks the ones held back by
-- FULL_COVERAGE_WINDOW_MIN_ESCALATION_RANK (Minor Delays / Reduced Service
-- at the default Severe-only gate), so widening the enforced tiers later is
-- an evidence-based config change. `enforced` is what actually changed a
-- line's status. Severities are `common::Severity` discriminants.
--
-- Retention: the same FULL_COVERAGE_WINDOW_STATS_RETENTION_DAYS (14) as
-- full_coverage_line_window_stats. A brand-new table: its index is built
-- in this transaction on an empty table, blocking nothing.
-- -------------------------------------------------------------------------
SET LOCAL lock_timeout = '5s';

CREATE TABLE IF NOT EXISTS full_coverage_window_verdicts (
    line_id            TEXT        NOT NULL,
    bucket_start       TIMESTAMPTZ NOT NULL,
    evaluated_at       TIMESTAMPTZ NOT NULL,
    window_computed_at TIMESTAMPTZ NOT NULL,
    mode               TEXT        NOT NULL CHECK (mode IN ('shadow', 'enforce')),
    verdict            TEXT        NOT NULL CHECK (verdict IN ('ineligible', 'good', 'escalate')),
    ineligible_reason  TEXT,
    verdict_severity   SMALLINT,
    current_severity   SMALLINT    NOT NULL,
    would_escalate_to  SMALLINT,
    below_min_rank     BOOLEAN     NOT NULL,
    in_allowlist       BOOLEAN     NOT NULL,
    enforced           BOOLEAN     NOT NULL,
    reason             TEXT,
    PRIMARY KEY (line_id, bucket_start)
);

CREATE INDEX IF NOT EXISTS full_coverage_window_verdicts_bucket
    ON full_coverage_window_verdicts (bucket_start);
