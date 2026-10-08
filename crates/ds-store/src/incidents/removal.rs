//! "Ended (no longer listed)": inferring that an incident has left RDM's
//! Knowledgebase feed without RDM ever clearing it (user decision,
//! 2026-10-06; docs/superpowers/specs/2026-10-06-incident-source-removal-design.md).
//!
//! The KB feed purges incidents nightly (~22:57-23:00 UTC) whatever their
//! state, and never clears planned ones, so "active" as `NOT is_cleared`
//! alone left rows active forever. `is_cleared` stays RDM's own fact; this
//! module maintains OUR observation that the feed stopped listing a row:
//! `incidents.source_missing_polls` and `incidents.source_removed_at`.
//!
//! # The guard
//!
//! Absence is evidence only when the snapshot is the whole feed, so
//! inference runs once per snapshot, after every chunk of
//! [`super::apply_snapshot`] has committed, and only when ALL of:
//!
//! 1. the poller says the snapshot is complete (`IncidentSnapshot::complete`:
//!    no malformed `<PtIncident>` skipped, document not truncated). An older
//!    poller's bare array is never complete;
//! 2. it lists at least one incident;
//! 3. the previous complete snapshot was at least [`MIN_INFERENCE_GAP_SECS`]
//!    ago: a retried POST whose first attempt already committed (the
//!    response was lost) must not count the same poll twice;
//! 4. a previous complete snapshot exists and this one is not more than 50%
//!    smaller than it.
//!
//! Every complete, non-empty snapshot that is not "too soon" becomes the new
//! baseline for (4) whether or not inference ran, so a real large purge
//! delays inference by one poll instead of blocking it until the feed
//! regrows.
//!
//! When the guard passes, every not-cleared, not-yet-removed row missing
//! from this snapshot gets `source_missing_polls + 1`; a row reaching
//! [`MISSES_TO_REMOVE`] gets `source_removed_at = fetched_at`. Rows the
//! snapshot DOES list were already reset (counter 0, `source_removed_at`
//! NULL) by the chunk upserts, complete snapshot or not: presence is
//! positive evidence on its own. Planned and unplanned rows are treated the
//! same.
//!
//! `source_removed_at` is the row's `fetched_at`, the last time the feed
//! listed it, rather than `now()`: it is the best bound we have on when the
//! row left (some time after that), it does not depend on when the second
//! miss happened to be confirmed, and it is what the one-off backfill
//! (scripts/backfill-2026-10-06-incident-source-removed.sql) writes too, so
//! old and new removals read the same.
//!
//! # With the row heartbeat off (plan 2c.6)
//!
//! A listed row's `fetched_at` no longer advances every poll (see
//! [`super::RowHeartbeat`]), so the first miss (counter 0 to 1) stamps it:
//! `fetched_at = GREATEST(fetched_at, incident_feed_state.previous_snapshot_at)`,
//! the snapshot before this one, which is the last one that listed it. The
//! second miss then copies it into `source_removed_at` exactly as before.
//! `GREATEST` keeps the row's own time when it is later, so a stamp never
//! moves `fetched_at` backwards.
//!
//! The statement is the same in both modes. With the heartbeat on,
//! `previous_snapshot_at` is NULL (`GREATEST` ignores it: today's write,
//! value for value), except on the first snapshot after the heartbeat is
//! turned back on, where it is the last heartbeat-off snapshot: exactly the
//! stamp that rollback needs. A heartbeat-off snapshot right after a
//! heartbeat-on one has a NULL `previous_snapshot_at` too, and the rows its
//! predecessor bumped keep their own (correct) time.

use anyhow::Result;
use sqlx::PgPool;

use super::RowHeartbeat;

/// Consecutive complete snapshots an incident must be missing from before
/// it is marked removed (user decision 2026-10-06: 2).
pub const MISSES_TO_REMOVE: i16 = 2;

/// Inference is skipped for a complete snapshot arriving less than this long
/// after the previous one (guard 3 in the module docs). The poller polls
/// every 300s and gives up retrying a POST after a quarter of that
/// (`common::poller_loop::post_retry_budget`), plus its 30s request timeout,
/// so a retry of an already-committed POST lands well inside 120s while the
/// next real poll does not. A poller configured to poll faster than this
/// only gets every other poll counted: slower, never wrong.
pub const MIN_INFERENCE_GAP_SECS: f64 = 120.0;

/// Suffix of `distant_signal_api_incident_removal_inference_total{outcome}`:
/// one increment per incidents POST, labelled with [`Inference::label`].
pub const INFERENCE_METRIC: &str = "api_incident_removal_inference_total";

/// Suffix of `distant_signal_api_incidents_marked_removed_total`: rows newly
/// given a `source_removed_at` (the per-poll removal count).
pub const MARKED_REMOVED_METRIC: &str = "api_incidents_marked_removed_total";

/// Every [`Inference::label`], for zero-registration at startup.
const OUTCOMES: [&str; 6] = [
    "applied",
    "incomplete",
    "empty",
    "too_soon",
    "no_baseline",
    "shrink",
];

/// Registers both metrics at 0 at startup, so the chart's
/// `DistantSignalIncidentRemovalInferenceStalled` alert sees the first
/// increment of every series.
pub fn register_metrics() {
    for outcome in OUTCOMES {
        metrics::counter!(common::metrics::metric_name(INFERENCE_METRIC), "outcome" => outcome)
            .increment(0);
    }
    metrics::counter!(common::metrics::metric_name(MARKED_REMOVED_METRIC)).increment(0);
}

/// What one snapshot's inference did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Inference {
    /// The guard passed. `missing`: rows whose counter advanced this poll;
    /// `removed`: of those, the ones that reached [`MISSES_TO_REMOVE`].
    Applied { missing: u64, removed: u64 },
    /// The poller did not vouch for the snapshot being the whole feed.
    Incomplete,
    /// A complete snapshot listing nothing: an upstream outage looks
    /// exactly like this, so it is never evidence.
    Empty,
    /// Within [`MIN_INFERENCE_GAP_SECS`] of the previous complete snapshot.
    TooSoon,
    /// No previous complete snapshot to compare sizes with (first poll
    /// after deploy). Recorded as the baseline.
    NoBaseline,
    /// More than 50% smaller than the previous complete snapshot. Recorded
    /// as the new baseline.
    Shrink { previous: i64, current: i64 },
}

impl Inference {
    /// The metric's `outcome` label.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Applied { .. } => "applied",
            Self::Incomplete => "incomplete",
            Self::Empty => "empty",
            Self::TooSoon => "too_soon",
            Self::NoBaseline => "no_baseline",
            Self::Shrink { .. } => "shrink",
        }
    }
}

/// The pure part of the guard (rules 2-4 of the module docs; rule 1 is
/// decided before any database work). `previous` is the stored baseline as
/// `(size, seconds since it was recorded)`. Returns the verdict and whether
/// this snapshot becomes the new baseline.
pub(crate) fn judge(current: i64, previous: Option<(i64, f64)>) -> (Inference, bool) {
    if current == 0 {
        return (Inference::Empty, false);
    }
    let Some((previous_size, age_secs)) = previous else {
        return (Inference::NoBaseline, true);
    };
    if age_secs < MIN_INFERENCE_GAP_SECS {
        return (Inference::TooSoon, false);
    }
    // "More than 50% smaller": current < previous / 2, in integers.
    if current.saturating_mul(2) < previous_size {
        return (
            Inference::Shrink {
                previous: previous_size,
                current,
            },
            true,
        );
    }
    (
        Inference::Applied {
            missing: 0,
            removed: 0,
        },
        true,
    )
}

/// Runs the guard and, if it passes, advances the miss counters, all in one
/// transaction. `present_ids` is every incident id the snapshot listed;
/// the caller has already committed their upserts (and so their resets).
/// `heartbeat` is the one [`super::apply_snapshot`] ran with: with it off,
/// creating the feed-state row also sets `last_snapshot_at`.
///
/// The baseline row is read `FOR UPDATE`, so two overlapping POSTs (a slow
/// request and its retry) run their inference one after the other, and the
/// second sees the first's baseline and is skipped as too soon.
#[expect(
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    reason = "a feed has at most a few thousand incidents; counts are never negative"
)]
pub async fn infer_removals(
    pool: &PgPool,
    present_ids: &[&str],
    complete: bool,
    heartbeat: RowHeartbeat,
) -> Result<Inference> {
    if !complete {
        return Ok(record(Inference::Incomplete));
    }
    let mut distinct: Vec<&str> = present_ids.to_vec();
    distinct.sort_unstable();
    distinct.dedup();
    let current = distinct.len() as i64;

    let mut tx = pool.begin().await?;
    let previous: Option<(i32, f64)> = sqlx::query_as(
        "SELECT last_complete_size, \
                EXTRACT(EPOCH FROM (now() - last_complete_at))::float8 \
           FROM incident_feed_state WHERE singleton FOR UPDATE",
    )
    .fetch_optional(&mut *tx)
    .await?;
    let (verdict, new_baseline) =
        judge(current, previous.map(|(size, age)| (i64::from(size), age)));

    if new_baseline {
        // A new row (the first complete snapshot) also takes this
        // snapshot's time when the heartbeat is off: `apply_snapshot`'s
        // UPDATE of the snapshot times found no row to update.
        sqlx::query(
            "INSERT INTO incident_feed_state \
                 (singleton, last_complete_at, last_complete_size, last_snapshot_at) \
             VALUES (TRUE, now(), $1, CASE WHEN $2 THEN NULL ELSE now() END) \
             ON CONFLICT (singleton) DO UPDATE SET \
                 last_complete_at = EXCLUDED.last_complete_at, \
                 last_complete_size = EXCLUDED.last_complete_size",
        )
        .bind(i32::try_from(current).unwrap_or(i32::MAX))
        .bind(heartbeat.is_on())
        .execute(&mut *tx)
        .await?;
    }

    let verdict = if let Inference::Applied { .. } = verdict {
        // A first miss stamps `fetched_at` (module docs), and
        // `source_removed_at` takes the stamped value (the right-hand sides
        // all read the old row). With the heartbeat on, `previous_snapshot_at`
        // is NULL, so this writes exactly what today's statement did.
        let sql = "WITH missed AS ( \
                 UPDATE incidents SET \
                     source_missing_polls = source_missing_polls + 1, \
                     fetched_at = CASE WHEN source_missing_polls = 0 \
                                       THEN GREATEST(fetched_at, feed.previous_snapshot_at) \
                                       ELSE fetched_at END, \
                     source_removed_at = CASE WHEN source_missing_polls + 1 >= $2 \
                         THEN CASE WHEN source_missing_polls = 0 \
                                   THEN GREATEST(fetched_at, feed.previous_snapshot_at) \
                                   ELSE fetched_at END \
                     END \
                   FROM (SELECT (SELECT previous_snapshot_at FROM incident_feed_state \
                                  WHERE singleton) AS previous_snapshot_at) feed \
                  WHERE NOT is_cleared \
                    AND source_removed_at IS NULL \
                    AND NOT (incident_id = ANY($1)) \
                 RETURNING source_removed_at \
             ) \
             SELECT count(*), count(source_removed_at) FROM missed";
        let (missing, removed): (i64, i64) = sqlx::query_as(sql)
            .bind(&distinct)
            .bind(MISSES_TO_REMOVE)
            .fetch_one(&mut *tx)
            .await?;
        Inference::Applied {
            missing: missing as u64,
            removed: removed as u64,
        }
    } else {
        verdict
    };

    tx.commit().await?;
    Ok(record(verdict))
}

/// Logs and counts one verdict, returning it.
fn record(verdict: Inference) -> Inference {
    metrics::counter!(
        common::metrics::metric_name(INFERENCE_METRIC),
        "outcome" => verdict.label()
    )
    .increment(1);
    match verdict {
        Inference::Applied { missing, removed } => {
            metrics::counter!(common::metrics::metric_name(MARKED_REMOVED_METRIC))
                .increment(removed);
            if missing > 0 {
                tracing::info!(
                    missing,
                    removed,
                    "incidents missing from a complete feed snapshot; those missing twice are now ended (no longer listed)"
                );
            }
        }
        Inference::Incomplete => tracing::warn!(
            "incident snapshot not complete (malformed elements skipped, a truncated body, or an older poller); skipping removal inference"
        ),
        Inference::Empty => tracing::warn!(
            "complete incident snapshot lists no incidents; skipping removal inference"
        ),
        Inference::TooSoon => tracing::info!(
            min_gap_secs = MIN_INFERENCE_GAP_SECS,
            "incident snapshot arrived too soon after the previous complete one (a retried POST?); skipping removal inference"
        ),
        Inference::NoBaseline => tracing::info!(
            "first complete incident snapshot: recorded its size as the baseline; skipping removal inference"
        ),
        Inference::Shrink { previous, current } => tracing::warn!(
            previous,
            current,
            "complete incident snapshot is more than 50% smaller than the previous one; skipping removal inference this poll"
        ),
    }
    verdict
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_snapshot_is_never_evidence_and_never_a_baseline() {
        assert_eq!(judge(0, None), (Inference::Empty, false));
        assert_eq!(judge(0, Some((900, 300.0))), (Inference::Empty, false));
    }

    #[test]
    fn the_first_complete_snapshot_only_records_a_baseline() {
        assert_eq!(judge(900, None), (Inference::NoBaseline, true));
    }

    #[test]
    fn a_retry_inside_the_gap_counts_nothing_and_keeps_the_baseline() {
        assert_eq!(judge(900, Some((900, 30.0))), (Inference::TooSoon, false));
    }

    #[test]
    fn more_than_half_smaller_is_skipped_but_becomes_the_baseline() {
        assert_eq!(
            judge(449, Some((900, 300.0))),
            (
                Inference::Shrink {
                    previous: 900,
                    current: 449
                },
                true
            )
        );
        // Exactly half is not MORE than 50% smaller.
        assert!(matches!(
            judge(450, Some((900, 300.0))),
            (Inference::Applied { .. }, true)
        ));
    }

    #[test]
    fn labels_match_the_registered_outcomes() {
        let verdicts = [
            Inference::Applied {
                missing: 0,
                removed: 0,
            },
            Inference::Incomplete,
            Inference::Empty,
            Inference::TooSoon,
            Inference::NoBaseline,
            Inference::Shrink {
                previous: 2,
                current: 0,
            },
        ];
        let labels: Vec<&str> = verdicts.iter().map(Inference::label).collect();
        assert_eq!(labels, OUTCOMES);
    }

    #[test]
    fn a_normal_poll_applies() {
        assert!(matches!(
            judge(964, Some((965, 300.0))),
            (Inference::Applied { .. }, true)
        ));
        // Growth is never suspicious.
        assert!(matches!(
            judge(2000, Some((965, 300.0))),
            (Inference::Applied { .. }, true)
        ));
    }
}
