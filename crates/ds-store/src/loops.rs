//! The train-domain background loops (ingest architecture spec §12.3, plan
//! 1B.6 and 1B.7), and the advisory-locked [`runner`] both the
//! ingest-writer (`INGEST_WRITER_LOOPS`) and the api
//! (`API_BACKGROUND_LOOPS`) run them with.
//!
//! Each constructor below is the whole loop: its lock
//! ([`common::advisory_locks`]), its interval and its body, which calls the
//! sweep and logs its outcome with the messages the api's own loops have
//! always used. The api and the writer register the same [`LoopSpec`]s, so
//! a sweep logs and counts the same whichever process holds its lock.
//!
//! | Loop | Lock | Body | Interval (env, default) |
//! |---|---|---|---|
//! | [`schedule_match`] | `SCHEDULE_MATCH_SWEEP` | [`run_schedule_match_sweep`] | `SCHEDULE_MATCH_INTERVAL_SECS`, 300 |
//! | [`reconciliation`] | `RECONCILIATION_SWEEP` | [`run_reconciliation_sweep`] | `RECONCILIATION_SWEEP_INTERVAL_SECS`, 300 (grace `SCHEDULE_ENRICHMENT_GRACE_MINUTES`, 30) |
//! | [`backlog_match`] | `BACKLOG_MATCH_SWEEP` | [`run_backlog_match_sweep`] | `BACKLOG_MATCH_SWEEP_INTERVAL_SECS`, 300 |
//! | [`corpus_crosswalk`] | `CORPUS_CROSSWALK` | [`rebuild_if_stale`], then [`refresh_last_delivery_metric`] | writer: `INGEST_WRITER_CORPUS_CROSSWALK_INTERVAL_SECS`, 600; api: once at startup ([`LoopRunner::run_once`]) |
//!
//! [`run_schedule_match_sweep`]: crate::sweeps::schedule_matching::run_schedule_match_sweep
//! [`run_reconciliation_sweep`]: crate::sweeps::reconciliation::run_reconciliation_sweep
//! [`run_backlog_match_sweep`]: crate::backlog::matching::run_backlog_match_sweep
//! [`rebuild_if_stale`]: crate::corpus::crosswalk::rebuild_if_stale
//! [`refresh_last_delivery_metric`]: crate::corpus::refresh_last_delivery_metric

pub mod runner;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common::advisory_locks;

pub use runner::{LockSession, LoopRunner, LoopSpec, RunningLoops, TickOutcome};

/// The schedule-match sweep's CRS-to-line index
/// ([`crate::sweeps::schedule_matching::crs_to_line_ids`] over the line
/// catalogue), shared by the schedule-match and reconciliation loops.
pub type CrsLineIndex = Arc<HashMap<String, Vec<String>>>;

/// The CORPUS crosswalk loop's interval in the ingest-writer (spec §12.3).
pub const CORPUS_CROSSWALK_DEFAULT_INTERVAL: Duration = Duration::from_secs(600);

/// Periodic retry of the schedule-first match against every still-`pending`,
/// never-schedule-matched tracked-train row (the api's former
/// `schedule_match_sweep_loop`).
pub fn schedule_match(interval: Duration, index: CrsLineIndex) -> LoopSpec {
    LoopSpec::new(
        advisory_locks::SCHEDULE_MATCH_SWEEP,
        interval,
        move |pool| {
            let index = Arc::clone(&index);
            async move {
                match crate::sweeps::schedule_matching::run_schedule_match_sweep(&pool, &index)
                    .await
                {
                    Ok(matched) => {
                        if matched > 0 {
                            tracing::info!(matched, "schedule-match sweep resolved pending pins");
                        }
                        Ok(())
                    }
                    Err(err) => {
                        tracing::error!(error = ?err, "schedule-match sweep failed; will retry next interval");
                        Err(err)
                    }
                }
            }
        },
    )
}

/// Periodic retry of the two tracked-train stalls (the api's former
/// `reconciliation_sweep_loop`).
pub fn reconciliation(
    interval: Duration,
    index: CrsLineIndex,
    grace_period: chrono::Duration,
) -> LoopSpec {
    LoopSpec::new(
        advisory_locks::RECONCILIATION_SWEEP,
        interval,
        move |pool| {
            let index = Arc::clone(&index);
            async move {
                match crate::sweeps::reconciliation::run_reconciliation_sweep(
                    &pool,
                    &index,
                    grace_period,
                )
                .await
                {
                    Ok(result) => {
                        if result.resolution_status_reconciled > 0
                            || result.schedule_enrichment_matched > 0
                        {
                            tracing::info!(
                                resolution_status_reconciled = result.resolution_status_reconciled,
                                schedule_enrichment_matched = result.schedule_enrichment_matched,
                                "reconciliation sweep made progress on stuck tracked-train state"
                            );
                        }
                        Ok(())
                    }
                    Err(err) => {
                        tracing::error!(error = ?err, "reconciliation sweep failed; will retry next interval");
                        Err(err)
                    }
                }
            }
        },
    )
}

/// Periodic retry of `attempt_backlog_match` for every still-`pending` pin
/// (the api's former `backlog_match_sweep_loop`).
pub fn backlog_match(interval: Duration) -> LoopSpec {
    LoopSpec::new(
        advisory_locks::BACKLOG_MATCH_SWEEP,
        interval,
        |pool| async move {
            match crate::backlog::matching::run_backlog_match_sweep(&pool).await {
                Ok(matched) => {
                    if matched > 0 {
                        tracing::info!(matched, "backlog-match sweep resolved pending pins");
                    }
                    Ok(())
                }
                Err(err) => {
                    tracing::error!(error = ?err, "backlog-match sweep failed; will retry next interval");
                    Err(err)
                }
            }
        },
    )
}

/// Rebuilds the CORPUS crosswalk if the stored one predates the newest
/// delivery, this build's rules or the current `stations` (one `MAX()` when
/// no CORPUS was ever loaded), then refreshes the CORPUS freshness gauge
/// from the durable marker, so the staleness alert survives restarts
/// between monthly deliveries. Both steps run every tick; either failing
/// fails the tick.
pub fn corpus_crosswalk(interval: Duration) -> LoopSpec {
    LoopSpec::new(
        advisory_locks::CORPUS_CROSSWALK,
        interval,
        |pool| async move {
            let rebuild = crate::corpus::crosswalk::rebuild_if_stale(&pool).await;
            if let Err(err) = &rebuild {
                tracing::error!(error = ?err, "CORPUS crosswalk rebuild failed");
            }
            let gauge = crate::corpus::refresh_last_delivery_metric(&pool).await;
            if let Err(err) = &gauge {
                tracing::error!(error = ?err, "CORPUS freshness gauge read failed");
            }
            rebuild?;
            gauge?;
            Ok(())
        },
    )
}

/// The train-domain loop intervals, as the api's and the writer's settings
/// give them.
#[derive(Debug, Clone, Copy)]
pub struct TrainLoopIntervals {
    pub schedule_match: Duration,
    pub reconciliation: Duration,
    pub schedule_enrichment_grace: chrono::Duration,
    pub backlog_match: Duration,
}

/// The three periodic sweeps both the api and the ingest-writer run:
/// schedule-match, reconciliation and backlog-match. The CORPUS crosswalk
/// loop is separate: periodic in the writer, one-shot in the api.
pub fn periodic_sweeps(intervals: TrainLoopIntervals, index: &CrsLineIndex) -> [LoopSpec; 3] {
    [
        schedule_match(intervals.schedule_match, Arc::clone(index)),
        reconciliation(
            intervals.reconciliation,
            Arc::clone(index),
            intervals.schedule_enrichment_grace,
        ),
        backlog_match(intervals.backlog_match),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_loop_takes_its_own_lock_and_interval() {
        let index: CrsLineIndex = Arc::new(HashMap::new());
        let intervals = TrainLoopIntervals {
            schedule_match: Duration::from_secs(1),
            reconciliation: Duration::from_secs(2),
            schedule_enrichment_grace: chrono::Duration::minutes(30),
            backlog_match: Duration::from_secs(3),
        };
        let specs = periodic_sweeps(intervals, &index);
        let got: Vec<_> = specs
            .iter()
            .map(|spec| (spec.lock, spec.interval))
            .collect();
        assert_eq!(
            got,
            [
                (advisory_locks::SCHEDULE_MATCH_SWEEP, Duration::from_secs(1)),
                (advisory_locks::RECONCILIATION_SWEEP, Duration::from_secs(2)),
                (advisory_locks::BACKLOG_MATCH_SWEEP, Duration::from_secs(3)),
            ]
        );
        let corpus = corpus_crosswalk(CORPUS_CROSSWALK_DEFAULT_INTERVAL);
        assert_eq!(corpus.lock, advisory_locks::CORPUS_CROSSWALK);
        assert_eq!(corpus.interval, Duration::from_secs(600));
    }
}
