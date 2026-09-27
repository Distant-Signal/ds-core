//! Retry a service's initial connection to a dependency (Postgres, Redis)
//! instead of exiting (INF-5).
//!
//! After a node reboot every pod starts at once, and Postgres spends tens
//! of seconds (minutes, for a large WAL redo) in crash recovery answering
//! "the database system is starting up". A binary that connects eagerly and
//! exits on failure lands in CrashLoopBackOff, whose exponential restart
//! delay (10s doubling to 5 min) then keeps it down for minutes AFTER the
//! dependency is back. Retrying in-process with a capped backoff instead
//! comes up within seconds of the dependency, logs where it is stuck, and
//! -- because the caller only flips its readiness state once this returns
//! -- stays NotReady until then.
//!
//! Unbounded on purpose: a restart cannot fix an unreachable dependency, it
//! only adds backoff. A persistent misconfiguration (wrong password) is
//! still visible: a warning per attempt, and the pod never goes Ready.

use std::fmt::Debug;
use std::future::Future;
use std::time::Duration;

use crate::backoff::Backoff;
use crate::progress::Progress;

/// 1s, 2s, 4s ... capped at 15s (each jittered into its upper half), so a
/// dependency that comes back is noticed within ~15s.
pub const CONNECT_BACKOFF: Backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(15));

/// Calls `attempt` until it succeeds, sleeping `backoff` between failures
/// and logging each one. `progress`, when given, is beaten on every failed
/// attempt: the process is alive and retrying, so a liveness probe must not
/// restart it (a restart would only add CrashLoopBackOff delay).
pub async fn retry_until_ready<T, E, F, Fut>(
    what: &str,
    backoff: Backoff,
    progress: Option<&Progress>,
    mut attempt: F,
) -> T
where
    E: Debug,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let started = tokio::time::Instant::now();
    let mut failures: u32 = 0;
    loop {
        match attempt().await {
            Ok(value) => {
                if let Some(progress) = progress {
                    progress.beat();
                }
                if failures > 0 {
                    tracing::info!(
                        what,
                        attempts = failures + 1,
                        waited_secs = started.elapsed().as_secs(),
                        "{what} is ready"
                    );
                }
                return value;
            }
            Err(err) => {
                let delay = backoff.delay(failures);
                tracing::warn!(
                    what,
                    attempt = failures + 1,
                    waited_secs = started.elapsed().as_secs(),
                    retry_in_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                    error = ?err,
                    "{what} is not reachable yet; retrying"
                );
                if let Some(progress) = progress {
                    progress.beat();
                }
                tokio::time::sleep(delay).await;
                failures = failures.saturating_add(1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    /// A fake dependency that refuses the first `n` connection attempts.
    struct FlakyDependency {
        refusals_left: Cell<u32>,
        attempts: Cell<u32>,
    }

    impl FlakyDependency {
        fn new(refusals: u32) -> Self {
            Self {
                refusals_left: Cell::new(refusals),
                attempts: Cell::new(0),
            }
        }

        async fn connect(&self) -> Result<&'static str, &'static str> {
            self.attempts.set(self.attempts.get() + 1);
            match self.refusals_left.get() {
                0 => Ok("connection"),
                n => {
                    self.refusals_left.set(n - 1);
                    Err("the database system is starting up")
                }
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn succeeds_immediately_without_sleeping() {
        let dependency = FlakyDependency::new(0);
        let started = tokio::time::Instant::now();
        let conn = retry_until_ready("fake", CONNECT_BACKOFF, None, || dependency.connect()).await;
        assert_eq!(conn, "connection");
        assert_eq!(dependency.attempts.get(), 1);
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    /// INF-5: a dependency that is down at startup is waited for, not
    /// exited on, and the wait between attempts follows the capped backoff.
    #[tokio::test(start_paused = true)]
    async fn retries_with_capped_backoff_until_the_dependency_is_up() {
        let dependency = FlakyDependency::new(8);
        let started = tokio::time::Instant::now();
        let conn = retry_until_ready("fake", CONNECT_BACKOFF, None, || dependency.connect()).await;
        assert_eq!(conn, "connection");
        assert_eq!(dependency.attempts.get(), 9);
        // Ceilings 1+2+4+8+15+15+15+15 = 75s; each jittered into [half, full].
        let waited = started.elapsed();
        assert!(
            waited >= Duration::from_millis(37_500) && waited <= Duration::from_secs(75),
            "waited {waited:?}"
        );
    }

    /// Every failed attempt counts as liveness, so a liveness probe watching
    /// `progress` does not restart a process that is only waiting.
    #[tokio::test(start_paused = true)]
    async fn failed_attempts_beat_progress() {
        let dependency = FlakyDependency::new(20);
        let progress = Progress::new(Duration::from_secs(30));
        let watcher = progress.clone();
        let waiting = retry_until_ready("fake", CONNECT_BACKOFF, Some(&progress), || {
            dependency.connect()
        });
        // 100s: well inside the >= 127s the 20 jittered refusals take.
        let check = async {
            for _ in 0..10 {
                tokio::time::sleep(Duration::from_secs(10)).await;
                assert!(!watcher.is_stalled());
            }
        };
        let (conn, ()) = tokio::join!(waiting, check);
        assert_eq!(conn, "connection");
    }
}
