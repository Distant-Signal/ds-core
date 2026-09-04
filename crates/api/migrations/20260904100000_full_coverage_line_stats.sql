-- ---------------------------------------------------------------------
-- One row per line -- a live snapshot (mirrors LineStatus's own "current
-- state, not history" shape), not an append log; the existing
-- line_status_daily_coverage_stats/line_status_half_hourly_coverage_stats
-- tables remain the historical rollups this table is NOT a substitute
-- for. `service_date` is a real freshness guard: a stale row (a producer
-- outage spanning a rail-day rollover) is detected and treated as Pending
-- on read by Task 14's own service_date == today filter, never served as
-- a silently-aging Available snapshot. See
-- docs/superpowers/specs/2026-09-04-option-b-live-consumer-design.md
-- Decision 3.
-- ---------------------------------------------------------------------

CREATE TABLE full_coverage_line_stats (
    line_id           TEXT             PRIMARY KEY,
    service_date      DATE             NOT NULL,
    availability      TEXT             NOT NULL, -- 'pending' | 'available'
    total             INT              NOT NULL DEFAULT 0,
    delayed           INT              NOT NULL DEFAULT 0,
    cancelled         INT              NOT NULL DEFAULT 0,
    skipped           INT              NOT NULL DEFAULT 0,
    avg_delay_minutes DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at        TIMESTAMPTZ      NOT NULL DEFAULT now()
);
