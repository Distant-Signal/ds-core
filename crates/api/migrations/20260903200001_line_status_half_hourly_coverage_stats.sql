-- -------------------------------------------------------------------------
-- Half-hourly-granularity sibling of line_status_daily_coverage_stats
-- (20260903200000) -- same relationship line_status_half_hourly_stats
-- already has to line_status_daily_stats. Column-for-column identical
-- shape, keyed on half_hour_start (a plain UTC 30-minute boundary from
-- crates/aggregator/src/queries.rs's utc_half_hour_start) instead of a
-- London calendar day. See that table's own migration for the full
-- "separate table, not a source column" reasoning (Decision 4 of
-- docs/superpowers/specs/2026-09-03-full-coverage-metrics-transition-design.md),
-- which applies identically here.
--
-- No real writer populates this table yet -- see the daily migration's own
-- note on that.
-- -------------------------------------------------------------------------

CREATE TABLE line_status_half_hourly_coverage_stats (
    line_id            TEXT             NOT NULL,
    half_hour_start    TIMESTAMPTZ      NOT NULL,

    resolved_windows   BIGINT           NOT NULL DEFAULT 0,

    total              BIGINT           NOT NULL DEFAULT 0,
    delayed            BIGINT           NOT NULL DEFAULT 0,
    cancelled          BIGINT           NOT NULL DEFAULT 0,
    skipped            BIGINT           NOT NULL DEFAULT 0,

    running_count      BIGINT           NOT NULL DEFAULT 0,
    delay_minutes_sum  DOUBLE PRECISION NOT NULL DEFAULT 0,

    PRIMARY KEY (line_id, half_hour_start)
);

CREATE INDEX line_status_half_hourly_coverage_stats_line_half_hour ON line_status_half_hourly_coverage_stats (line_id, half_hour_start);
