-- -------------------------------------------------------------------------
-- Per-line hourly rollup of SampleStats -- the hourly-granularity sibling
-- of line_status_daily_stats (20260831090001), written by the SAME
-- per-cycle deduped SampleStats crates/aggregator/src/main.rs's run_cycle
-- already computes for the daily write (crates/aggregator/src/dedup.rs's
-- dedup_new_sample_stats) -- no second dedup pass. See
-- docs/superpowers/specs/2026-09-02-trend-chart-granularity-design.md,
-- Decisions 1-3.
--
-- `hour_start` is a plain UTC hour boundary (top of the hour), NOT an
-- Europe/London local hour the way line_status_daily_stats.day is a
-- London calendar day -- deliberately different conventions, see
-- Decision 4. A viewer never sees this column directly; it is always
-- rendered through frontend/lib/dateFormat.ts's formatTime (London
-- wall-clock) before display.
--
-- Every numeric column is a running SUM across however many poll cycles
-- contributed to this line in this hour -- rates are derived at READ time
-- (crates/api/src/data/queries.rs's hourly_stats_for_range), never stored
-- pre-divided, identical convention to the daily table.
-- -------------------------------------------------------------------------

CREATE TABLE line_status_hourly_stats (
    line_id           TEXT             NOT NULL,
    hour_start        TIMESTAMPTZ      NOT NULL,  -- plain UTC hour boundary

    sample_cycles     BIGINT           NOT NULL DEFAULT 0,
    total             BIGINT           NOT NULL DEFAULT 0,
    delayed           BIGINT           NOT NULL DEFAULT 0,
    cancelled         BIGINT           NOT NULL DEFAULT 0,
    skipped           BIGINT           NOT NULL DEFAULT 0,
    running_count     BIGINT           NOT NULL DEFAULT 0,
    delay_minutes_sum DOUBLE PRECISION NOT NULL DEFAULT 0,

    PRIMARY KEY (line_id, hour_start)
);

CREATE INDEX line_status_hourly_stats_line_hour ON line_status_hourly_stats (line_id, hour_start);
