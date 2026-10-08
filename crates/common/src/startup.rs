//! Retry a service's initial connection to a dependency (Postgres, Redis)
//! instead of exiting (INF-5).
//!
//! After a node reboot every pod starts at once, and Postgres spends tens
//! of seconds (minutes, for a large WAL redo) in crash recovery answering
//! "the database system is starting up". A binary that connects eagerly and
//! exits on failure lands in `CrashLoopBackOff`, whose exponential restart
//! delay (10s doubling to 5 min) then keeps it down for minutes AFTER the
//! dependency is back. Retrying in-process with a capped backoff instead
//! comes up within seconds of the dependency, logs where it is stuck, and
//! -- because the caller only flips its readiness state once this returns
//! -- stays `NotReady` until then.
//!
//! Unbounded on purpose: a restart cannot fix an unreachable dependency, it
//! only adds backoff. A persistent misconfiguration (wrong password) is
//! still visible: a warning per attempt, and the pod never goes Ready.
//!
//! One-shot binaries (the chart's Jobs and `CronJob`s, the operator
//! backfills) have no readiness to hold and must eventually exit, so they
//! use [`retry_until_ready_within`]: the same retry, bounded by a deadline
//! each binary takes from its own `*_CONNECT_DEADLINE_SECS` setting. On
//! 2026-10-08 the api-maintenance `CronJob`'s first pod failed twice with
//! "pool timed out" on a node at load ~100 (one 5 s acquire, then exit),
//! and a fresh pod can also start before kube-router has programmed its
//! `NetworkPolicy`.

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
/// restart it (a restart would only add `CrashLoopBackOff` delay).
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

/// The default for the one-shot binaries' `*_CONNECT_DEADLINE_SECS`:
/// long enough to ride out a saturated node or a late `NetworkPolicy`,
/// short enough that a real outage still fails the Job well inside its
/// `activeDeadlineSeconds`.
pub const DEFAULT_CONNECT_DEADLINE: Duration = Duration::from_secs(120);

/// The api image's operator backfills' connect deadline (`backfill_*`,
/// `replay_uidless_movements`), read with [`connect_deadline_from_env`].
pub const BACKFILL_CONNECT_DEADLINE_ENV: &str = "BACKFILL_CONNECT_DEADLINE_SECS";

/// [`retry_until_ready`], given up after `deadline` (`tokio::time::timeout`)
/// with an error naming `what` and the deadline. Each failed attempt is
/// logged as there, so the cause is in the log above the error.
pub async fn retry_until_ready_within<T, E, F, Fut>(
    what: &str,
    backoff: Backoff,
    deadline: Duration,
    attempt: F,
) -> anyhow::Result<T>
where
    E: Debug,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    tokio::time::timeout(deadline, retry_until_ready(what, backoff, None, attempt))
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "{what} was still not reachable after {}s (the connect deadline); giving up",
                deadline.as_secs()
            )
        })
}

/// A `*_CONNECT_DEADLINE_SECS` variable for a binary without a clap
/// parser: `default` when unset, else a whole number of seconds, at least 1.
pub fn connect_deadline_from_env(var: &str, default: Duration) -> anyhow::Result<Duration> {
    parse_connect_deadline(var, std::env::var(var).ok().as_deref(), default)
}

fn parse_connect_deadline(
    var: &str,
    raw: Option<&str>,
    default: Duration,
) -> anyhow::Result<Duration> {
    let Some(raw) = raw else {
        return Ok(default);
    };
    match raw.trim().parse::<u64>() {
        Ok(secs) if secs > 0 => Ok(Duration::from_secs(secs)),
        _ => anyhow::bail!("{var}={raw:?} is not a whole number of seconds of at least 1"),
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

        fn connect(&self) -> impl Future<Output = Result<&'static str, &'static str>> {
            self.attempts.set(self.attempts.get() + 1);
            std::future::ready(match self.refusals_left.get() {
                0 => Ok("connection"),
                n => {
                    self.refusals_left.set(n - 1);
                    Err("the database system is starting up")
                }
            })
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

    /// A dependency that comes up inside the deadline is waited for.
    #[tokio::test(start_paused = true)]
    async fn within_the_deadline_a_late_dependency_is_waited_for() {
        let dependency = FlakyDependency::new(3);
        let conn =
            retry_until_ready_within("fake", CONNECT_BACKOFF, Duration::from_secs(60), || {
                dependency.connect()
            })
            .await
            .unwrap();
        assert_eq!(conn, "connection");
        assert_eq!(dependency.attempts.get(), 4);
    }

    /// One that does not is given up on at the deadline, with an error that
    /// names it and the deadline.
    #[tokio::test(start_paused = true)]
    async fn past_the_deadline_it_gives_up_with_a_clear_error() {
        let dependency = FlakyDependency::new(u32::MAX);
        let started = tokio::time::Instant::now();
        let err = retry_until_ready_within(
            "postgres",
            CONNECT_BACKOFF,
            Duration::from_secs(120),
            || dependency.connect(),
        )
        .await
        .unwrap_err();
        assert_eq!(started.elapsed(), Duration::from_secs(120));
        assert!(dependency.attempts.get() > 1);
        assert_eq!(
            err.to_string(),
            "postgres was still not reachable after 120s (the connect deadline); giving up"
        );
    }

    /// A refused TCP port that starts listening part-way through is
    /// connected to on a later attempt (real time: about 1 s).
    #[tokio::test]
    async fn a_port_that_comes_up_is_connected_to() {
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let listen = async {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            listener.accept().await.unwrap()
        };
        let attempts = Cell::new(0_u32);
        let connect =
            retry_until_ready_within("fake", CONNECT_BACKOFF, Duration::from_secs(30), || {
                attempts.set(attempts.get() + 1);
                tokio::net::TcpStream::connect(addr)
            });
        let (stream, _accepted) = tokio::join!(connect, listen);
        stream.unwrap();
        assert!(attempts.get() >= 2, "the first attempt must be refused");
    }

    #[test]
    fn connect_deadlines_parse_whole_positive_seconds() {
        let default = DEFAULT_CONNECT_DEADLINE;
        assert_eq!(parse_connect_deadline("X", None, default).unwrap(), default);
        assert_eq!(
            parse_connect_deadline("X", Some(" 30 "), default).unwrap(),
            Duration::from_secs(30)
        );
        for bad in ["0", "", "1.5", "-1", "two"] {
            let err = parse_connect_deadline("X", Some(bad), default).unwrap_err();
            assert!(err.to_string().starts_with("X="), "{bad}: {err}");
        }
    }
}
