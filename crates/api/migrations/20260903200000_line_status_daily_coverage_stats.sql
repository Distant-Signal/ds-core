-- -------------------------------------------------------------------------
-- Full-coverage sibling of line_status_daily_stats (20260831090001).
-- Column-for-column identical shape -- same accumulate-upsert posture, same
-- rate-derived-at-read-time contract -- except sample_cycles becomes
-- resolved_windows, the full-coverage analog: how many cycles this day saw
-- FullCoverageAvailability::Available (not Pending) data for this line, not
-- how many cycles had any raw LDBWS coverage at all.
--
-- A wholly separate table, NOT a `source` column added to the existing
-- line_status_daily_stats -- see
-- docs/superpowers/specs/2026-09-03-full-coverage-metrics-transition-design.md
-- Decision 4 for the full reasoning: during a gradual per-line rollout, a
-- line can genuinely have both a real sample-derived number and a real
-- full-coverage number for the same overlapping period. These are two
-- different POPULATIONS (a curated sample_stations subset vs. every
-- scheduled service on the line), not two measurements of the same
-- population, so summing or overwriting one with the other in a single row
-- would silently misrepresent both. A source-discriminated composite key
-- would also change an existing table's primary key underneath every
-- consumer that currently assumes one row per (line_id, day)
-- (daily_stats_for_range, TrendsResults.tsx's toChartPoints) -- a new,
-- additive sibling table avoids that entirely.
--
-- No real writer populates this table yet -- crates/aggregator's
-- record_daily_coverage_stats only ever receives a status's
-- full_coverage_stats, which stays None until a per-line materialized
-- signal from a future TRUST-vs-schedule consumer ("Option B") exists.
-- This is deliberate scaffolding, not dead weight: see this repo's
-- 2026-09-03 full-coverage-metrics-scaffolding plan for the full context.
-- -------------------------------------------------------------------------

CREATE TABLE line_status_daily_coverage_stats (
    line_id            TEXT             NOT NULL,
    day                DATE             NOT NULL,  -- Europe/London calendar day, matching line_status_daily_stats

    -- How many cycles this day saw FullCoverageAvailability::Available (not
    -- Pending) data for this line -- the full-coverage analog of
    -- line_status_daily_stats.sample_cycles, and the coverage/gap-rendering
    -- signal a future Trends chart would drive off (see Decision 4).
    resolved_windows   BIGINT           NOT NULL DEFAULT 0,

    total              BIGINT           NOT NULL DEFAULT 0,  -- sum of SampleStats.total
    delayed            BIGINT           NOT NULL DEFAULT 0,
    cancelled          BIGINT           NOT NULL DEFAULT 0,
    skipped            BIGINT           NOT NULL DEFAULT 0,

    -- Sum of "running" (non-cancelled) services per cycle -- the
    -- denominator for avg_delay_minutes, mirroring
    -- line_status_daily_stats.running_count exactly.
    running_count      BIGINT           NOT NULL DEFAULT 0,

    -- Sum of (SampleStats.avg_delay_minutes * that cycle's running count),
    -- so avgDelayMinutes can be recovered at read time as
    -- delay_minutes_sum / running_count, mirroring
    -- line_status_daily_stats.delay_minutes_sum exactly.
    delay_minutes_sum  DOUBLE PRECISION NOT NULL DEFAULT 0,

    PRIMARY KEY (line_id, day)
);

CREATE INDEX line_status_daily_coverage_stats_line_day ON line_status_daily_coverage_stats (line_id, day);
