//! `full_coverage_line_window_stats` writes: the windowed full-coverage
//! history `full-coverage-consumer` posts
//! (`POST /private/full-coverage-window-stats`), its validation and its
//! freshness read. The read-only loaders `compare_full_coverage --windows`
//! uses stay in the api (`data::full_coverage_window`).
//! See docs/superpowers/specs/2026-09-27-full-coverage-windowed-stats-design.md
//! section 7.

use anyhow::Result;
use chrono::{DateTime, DurationRound, Utc};
use common::FullCoverageWindowStatsRow;
use sqlx::PgPool;

/// Width of one history bucket: each line keeps one row per window kind per
/// 15 minutes (the latest write in the bucket wins).
pub const BUCKET: chrono::Duration = chrono::Duration::minutes(15);

/// `computed_at` truncated to its 15-minute bucket. Computed here, never
/// taken from the wire.
#[expect(
    clippy::expect_used,
    reason = "a constant or range-checked time is always valid"
)]
pub fn bucket_start(computed_at: DateTime<Utc>) -> DateTime<Utc> {
    computed_at
        .duration_trunc(BUCKET)
        .expect("a 15-minute truncation of a real timestamp cannot overflow")
}

/// Why a posted row was refused -- a 400, not a 500, since retrying the
/// same body can never succeed.
#[derive(Debug)]
pub struct InvalidWindowRow {
    pub line_id: String,
    pub problem: String,
}

impl std::fmt::Display for InvalidWindowRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "invalid full-coverage window row for {}: {}",
            self.line_id, self.problem
        )
    }
}

pub fn validate(rows: &[FullCoverageWindowStatsRow]) -> Result<(), InvalidWindowRow> {
    for row in rows {
        let problem = if row.line_id.trim().is_empty() {
            Some("empty line_id".to_string())
        } else if !matches!(row.relevance.as_str(), "full" | "stops_only") {
            Some(format!("unknown relevance {:?}", row.relevance))
        } else if row.window_end < row.window_start {
            Some("window_end before window_start".to_string())
        } else {
            None
        };
        if let Some(problem) = problem {
            return Err(InvalidWindowRow {
                line_id: row.line_id.clone(),
                problem,
            });
        }
    }
    Ok(())
}

fn int(value: u32) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

/// Upserts one row per `(line_id, window_kind, bucket_start)`. A row whose
/// `computed_at` is OLDER than the one already stored for its bucket is
/// ignored, so a replayed or late POST can never move a bucket backwards.
/// Returns the number of rows written.
#[expect(
    clippy::cast_possible_wrap,
    reason = "stats_version is a small constant"
)]
pub async fn upsert_full_coverage_window_stats(
    pool: &PgPool,
    rows: &[FullCoverageWindowStatsRow],
) -> Result<u64> {
    let mut tx = pool.begin().await?;
    let mut count = 0u64;
    for row in rows {
        let c = &row.counts;
        let result = sqlx::query(
            r"
            INSERT INTO full_coverage_line_window_stats
                (line_id, window_kind, bucket_start, service_date, window_start, window_end,
                 computed_at, total, on_time, delayed, cancelled_explicit, cancelled_presumed,
                 skipped, pending, unobserved, avg_delay_minutes, relevance, presumed_enabled,
                 partial, feed_stale, stats_version, cancelled_in_advance, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
                    $18, $19, $20, $21, $22, now())
            ON CONFLICT (line_id, window_kind, bucket_start) DO UPDATE SET
                service_date       = EXCLUDED.service_date,
                window_start       = EXCLUDED.window_start,
                window_end         = EXCLUDED.window_end,
                computed_at        = EXCLUDED.computed_at,
                total              = EXCLUDED.total,
                on_time            = EXCLUDED.on_time,
                delayed            = EXCLUDED.delayed,
                cancelled_explicit = EXCLUDED.cancelled_explicit,
                cancelled_presumed = EXCLUDED.cancelled_presumed,
                skipped            = EXCLUDED.skipped,
                pending            = EXCLUDED.pending,
                unobserved         = EXCLUDED.unobserved,
                avg_delay_minutes  = EXCLUDED.avg_delay_minutes,
                relevance          = EXCLUDED.relevance,
                presumed_enabled   = EXCLUDED.presumed_enabled,
                partial            = EXCLUDED.partial,
                feed_stale         = EXCLUDED.feed_stale,
                stats_version      = EXCLUDED.stats_version,
                cancelled_in_advance = EXCLUDED.cancelled_in_advance,
                updated_at         = EXCLUDED.updated_at
            WHERE EXCLUDED.computed_at >= full_coverage_line_window_stats.computed_at
            ",
        )
        .bind(&row.line_id)
        .bind(row.window_kind.as_str())
        .bind(bucket_start(row.computed_at))
        .bind(row.service_date)
        .bind(row.window_start)
        .bind(row.window_end)
        .bind(row.computed_at)
        .bind(int(c.total))
        .bind(int(c.on_time))
        .bind(int(c.delayed))
        .bind(int(c.cancelled_explicit))
        .bind(int(c.cancelled_presumed))
        .bind(int(c.skipped))
        .bind(int(c.pending))
        .bind(int(c.unobserved))
        .bind(c.avg_delay_minutes)
        .bind(&row.relevance)
        .bind(row.presumed_enabled)
        .bind(row.partial)
        .bind(row.feed_stale)
        .bind(row.stats_version as i16)
        .bind(int(c.cancelled_in_advance))
        .execute(&mut *tx)
        .await?;
        count += result.rows_affected();
    }
    tx.commit().await?;
    Ok(count)
}

/// The most recent `updated_at` in the table -- the freshness GET.
pub async fn last_full_coverage_window_stats_fetch(pool: &PgPool) -> Result<Option<DateTime<Utc>>> {
    let (fetched_at,): (Option<DateTime<Utc>>,) =
        sqlx::query_as("SELECT MAX(updated_at) FROM full_coverage_line_window_stats")
            .fetch_one(pool)
            .await?;
    Ok(fetched_at)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::full_coverage_window_row as row;

    #[test]
    fn buckets_are_15_minutes_and_computed_from_computed_at() {
        let at = |s: &str| s.parse::<DateTime<Utc>>().unwrap();
        assert_eq!(
            bucket_start(at("2026-09-27T12:14:59Z")),
            at("2026-09-27T12:00:00Z")
        );
        assert_eq!(
            bucket_start(at("2026-09-27T12:15:00Z")),
            at("2026-09-27T12:15:00Z")
        );
    }

    #[test]
    fn validation_rejects_an_unknown_relevance() {
        assert!(validate(&[row("line-a", "2026-09-27T12:00:00Z", 1)]).is_ok());
        let mut bad = row("line-a", "2026-09-27T12:00:00Z", 1);
        bad.relevance = "everything".to_string();
        assert!(validate(&[bad]).is_err());
    }
}
