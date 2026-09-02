-- -------------------------------------------------------------------------
-- Renames line_status_hourly_stats -> line_status_half_hourly_stats and its
-- hour_start column -> half_hour_start, following the switch from 1-hour to
-- 30-minute trend-chart buckets (crates/aggregator/src/queries.rs's
-- utc_half_hour_start, formerly utc_hour_start). The underlying poll cycle
-- (default 60s, crates/aggregator/src/config.rs's poll_interval_secs) is
-- well under 30 minutes, so this is a real granularity increase, not a
-- cosmetic one -- there is genuinely more resolution in the underlying
-- per-cycle SampleStats than the old 1-hour bucket exposed.
--
-- This repo has a demonstrated preference for renaming things when their
-- old name stops matching what they hold (see the INTERNAL_OAUTH_GROUP_*
-- rename this same session), so the table/column/index/constraint are all
-- renamed here rather than kept as "hourly"-named things that actually
-- hold half-hourly data.
--
-- A straightforward rename, NOT a data migration: any existing rows were
-- keyed on the OLD 60-minute boundaries. Left as-is (not reconciled onto
-- the new 30-minute grid), because this table's retention window
-- (`half_hourly_stats_retention_hours`, still 48h by default -- see
-- config.rs's doc comment on why the unit/default don't change with
-- bucket size) means any pre-migration rows age out and get pruned
-- naturally within 48 hours of this migration landing regardless. A
-- rolling-24-hour-window trend chart has no reason to backfill/reconcile
-- rows that will be gone within two days anyway.
-- -------------------------------------------------------------------------

ALTER TABLE line_status_hourly_stats RENAME TO line_status_half_hourly_stats;
ALTER TABLE line_status_half_hourly_stats RENAME COLUMN hour_start TO half_hour_start;
ALTER TABLE line_status_half_hourly_stats RENAME CONSTRAINT line_status_hourly_stats_pkey TO line_status_half_hourly_stats_pkey;
ALTER INDEX line_status_hourly_stats_line_hour RENAME TO line_status_half_hourly_stats_line_half_hour;
