//! Liveness-by-progress for a long-running loop. The loop calls
//! [`Progress::beat`] once per completed iteration (and wraps its idle waits
//! in [`Progress::idle`]), and a health endpoint (`health_http`'s `/healthz`
//! and `/livez`) reports unhealthy once no beat has arrived for
//! `stall_after`.
//!
//! Lives in `common` rather than `health-http` so `common::poller_loop` and
//! `common::startup` can beat it too (`health-http` depends on `common`, not
//! the reverse); `health_http::Progress` re-exports this type.
//!
//! A connected-or-not flag alone cannot catch a wedged loop: a loop stuck
//! forever inside an `await` (a half-open HTTP connection with no timeout,
//! the PL-1 incident) leaves the flag at its last value and the liveness
//! probe never restarts the pod.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

// tokio's `Instant` (identical to std's outside tests) so paused-clock tests
// can drive the stall window deterministically.
use tokio::time::Instant;

/// How often [`Progress::idle`] beats while the loop is waiting for its
/// next tick. Far below any sensible `stall_after`.
pub const IDLE_BEAT_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct Progress {
    origin: Instant,
    /// Milliseconds after `origin` of the last beat.
    last_beat_ms: Arc<AtomicU64>,
    stall_after: Duration,
}

impl Progress {
    /// Starts "just beaten", so a fresh process gets a full `stall_after`
    /// of grace before its first iteration has to complete.
    pub fn new(stall_after: Duration) -> Self {
        Self {
            origin: Instant::now(),
            last_beat_ms: Arc::new(AtomicU64::new(0)),
            stall_after,
        }
    }

    fn elapsed_ms(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    /// Records that the loop completed an iteration (or is otherwise
    /// demonstrably alive).
    pub fn beat(&self) {
        self.last_beat_ms
            .store(self.elapsed_ms(), Ordering::Relaxed);
    }

    /// How long since the last beat.
    pub fn since_last_beat(&self) -> Duration {
        Duration::from_millis(
            self.elapsed_ms()
                .saturating_sub(self.last_beat_ms.load(Ordering::Relaxed)),
        )
    }

    pub fn stall_after(&self) -> Duration {
        self.stall_after
    }

    pub fn is_stalled(&self) -> bool {
        self.since_last_beat() > self.stall_after
    }

    /// Awaits `fut` -- an idle wait, typically `interval.tick()` -- beating
    /// every [`IDLE_BEAT_INTERVAL`] meanwhile and once more when it
    /// completes.
    ///
    /// This is what makes "progress" mean "the loop is alive" rather than
    /// "a cycle completed recently" for a loop whose interval is far longer
    /// than any sensible stall window (the 24h RDM pollers): waiting for the
    /// next tick is not a stall, only a single cycle running past
    /// `stall_after` (or a wedged runtime, which stops these beats too) is.
    pub async fn idle<F: Future>(&self, fut: F) -> F::Output {
        let mut beat = tokio::time::interval(IDLE_BEAT_INTERVAL);
        beat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut fut = std::pin::pin!(fut);
        loop {
            tokio::select! {
                out = &mut fut => {
                    self.beat();
                    return out;
                }
                _ = beat.tick() => self.beat(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_fresh_progress_is_not_stalled_and_goes_stalled_without_beats() {
        let progress = Progress::new(Duration::from_millis(50));
        assert!(!progress.is_stalled());
        std::thread::sleep(Duration::from_millis(80));
        assert!(progress.is_stalled());
        progress.beat();
        assert!(!progress.is_stalled());
    }

    /// A long idle wait (the 24h poller interval) keeps beating, so it is
    /// never reported as a stall.
    #[tokio::test(start_paused = true)]
    async fn idle_beats_while_waiting() {
        let progress = Progress::new(Duration::from_secs(60));
        let watcher = progress.clone();
        let wait = progress.idle(tokio::time::sleep(Duration::from_secs(3600)));
        let check = async {
            for _ in 0..30 {
                tokio::time::sleep(Duration::from_secs(100)).await;
                assert!(!watcher.is_stalled());
            }
        };
        tokio::join!(wait, check);
    }

    /// The control for the test above: the same wait NOT wrapped in `idle`
    /// is a stall.
    #[tokio::test(start_paused = true)]
    async fn an_unwrapped_wait_goes_stalled() {
        let progress = Progress::new(Duration::from_secs(60));
        tokio::time::sleep(Duration::from_secs(100)).await;
        assert!(progress.is_stalled());
    }
}
