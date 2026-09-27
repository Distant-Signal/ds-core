-- -------------------------------------------------------------------------
-- Windowed full-coverage stats: one row per (line, window kind, 15-minute
-- bucket). See
-- docs/superpowers/specs/2026-09-27-full-coverage-windowed-stats-design.md
-- section 7.1.
--
-- full-coverage-consumer POSTs a `recent` (trains due in the last hour,
-- ending a grace period ago) and a `day_to_date` window for every line
-- every minute, but only once FULL_COVERAGE_WINDOWED_STATS=true (off by
-- default). Each write upserts the current bucket (`computed_at` truncated
-- to 15 minutes by api, never taken from the wire), so the newest bucket
-- per line is the live value and the older ones are a 15-minute history
-- for the shadow comparison. Retention: 14 days, pruned by aggregator
-- (FULL_COVERAGE_WINDOW_STATS_RETENTION_DAYS).
--
-- A brand-new table, so its index is built here, in the same transaction,
-- on an empty table no other session can see yet: nothing is blocked.
-- -------------------------------------------------------------------------
SET LOCAL lock_timeout = '5s';

CREATE TABLE IF NOT EXISTS full_coverage_line_window_stats (
    line_id            TEXT             NOT NULL,
    window_kind        TEXT             NOT NULL CHECK (window_kind IN ('recent', 'day_to_date')),
    bucket_start       TIMESTAMPTZ      NOT NULL,
    service_date       DATE             NOT NULL,
    window_start       TIMESTAMPTZ      NOT NULL, -- due-time range covered
    window_end         TIMESTAMPTZ      NOT NULL,
    computed_at        TIMESTAMPTZ      NOT NULL,
    total              INT              NOT NULL, -- evaluable: excludes pending/unobserved
    on_time            INT              NOT NULL,
    delayed            INT              NOT NULL,
    cancelled_explicit INT              NOT NULL,
    cancelled_presumed INT              NOT NULL,
    skipped            INT              NOT NULL,
    pending            INT              NOT NULL,
    unobserved         INT              NOT NULL,
    avg_delay_minutes  DOUBLE PRECISION NOT NULL,
    relevance          TEXT             NOT NULL CHECK (relevance IN ('full', 'stops_only')),
    presumed_enabled   BOOLEAN          NOT NULL,
    partial            BOOLEAN          NOT NULL,
    feed_stale         BOOLEAN          NOT NULL,
    stats_version      SMALLINT         NOT NULL,
    updated_at         TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (line_id, window_kind, bucket_start)
);

CREATE INDEX IF NOT EXISTS full_coverage_line_window_stats_bucket
    ON full_coverage_line_window_stats (bucket_start);
