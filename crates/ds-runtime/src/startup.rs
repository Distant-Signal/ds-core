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
//!
//! # The startup budget
//!
//! Every new pod's first connects to Postgres and Redis are refused for
//! about 2 to 5 seconds while the k3s network-policy controller adds the
//! pod's IP to the `NetworkPolicy` rules
//! (docs/specs/postgres-connection-pooling.md §1). That is expected, not an
//! outage, so [`retry_until_ready_past_startup_lag`] gives it a budget of
//! its own ([`DEFAULT_STARTUP_CONNECT_BUDGET`], set per dependency by a
//! `*_STARTUP_CONNECT_TIMEOUT_SECS` variable): while the failures are
//! network-level (the caller's `is_lag`) and the budget lasts, it retries
//! every [`STARTUP_LAG_BACKOFF`] (100 ms doubling to 500 ms), logs each at
//! DEBUG and the success at INFO. It is one loop with
//! [`retry_until_ready`], not a retry inside a retry: the first failure
//! that is not network-level, or the first one past the budget, ends the
//! budget for good, and from then on every failure is a WARN on
//! [`CONNECT_BACKOFF`] as before. So a real outage costs at most the budget
//! in quiet retries before the warnings start, and a wrong password warns
//! at once.

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
    attempt: F,
) -> T
where
    E: Debug,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    retry_until_ready_past_startup_lag(
        what,
        Duration::ZERO,
        |_: &E| false,
        backoff,
        progress,
        attempt,
    )
    .await
}

/// The default startup budget: well past the 2 to 5 s a new pod's
/// `NetworkPolicy` takes, and past sqlx's 5 s `acquire_timeout`.
pub const DEFAULT_STARTUP_CONNECT_BUDGET: Duration = Duration::from_secs(15);

/// The retry delays inside the startup budget: short, because the lag is
/// seconds long and the first connect after it should not wait longer than
/// it must, and under a second, so a liveness watchdog with a 1 s stall
/// window still sees a beat between attempts.
pub const STARTUP_LAG_BACKOFF: Backoff =
    Backoff::new(Duration::from_millis(100), Duration::from_millis(500));

/// [`retry_until_ready`] with the startup budget (module docs): failures
/// for which `is_lag` is true are retried quietly on [`STARTUP_LAG_BACKOFF`]
/// until `budget` has passed since the first attempt; after that, or after
/// any failure `is_lag` rejects, it is exactly [`retry_until_ready`]. A zero
/// `budget` turns the budget off. Success after any retry is logged at
/// INFO; only failures outside the budget are logged at WARN.
pub async fn retry_until_ready_past_startup_lag<T, E, F, Fut>(
    what: &str,
    budget: Duration,
    is_lag: impl Fn(&E) -> bool,
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
    let mut state = RetryState::new(budget);
    loop {
        match attempt().await {
            Ok(value) => {
                if let Some(progress) = progress {
                    progress.beat();
                }
                if state.attempts > 0 {
                    let waited = started.elapsed();
                    tracing::info!(
                        what,
                        attempts = state.attempts.saturating_add(1),
                        waited_secs = waited.as_secs(),
                        waited_ms = millis(waited),
                        "{what} is ready"
                    );
                }
                return value;
            }
            Err(err) => {
                let attempt = state.attempts.saturating_add(1);
                let waited = started.elapsed();
                let delay = match state.after_failure(is_lag(&err), waited, backoff) {
                    Next::Quiet(delay) => {
                        tracing::debug!(
                            what,
                            attempt,
                            waited_ms = millis(waited),
                            error = ?err,
                            "{what} is not reachable yet, within the startup budget \
                             (a new pod's network policy); retrying"
                        );
                        delay
                    }
                    Next::Warn(delay) => {
                        tracing::warn!(
                            what,
                            attempt,
                            waited_secs = waited.as_secs(),
                            retry_in_ms = millis(delay),
                            error = ?err,
                            "{what} is not reachable yet; retrying"
                        );
                        delay
                    }
                };
                if let Some(progress) = progress {
                    progress.beat();
                }
                tokio::time::sleep(delay).await;
            }
        }
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// What a failed attempt leads to: a quiet retry within the startup budget
/// (DEBUG), or a warned one on the caller's backoff. Either way, the wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Next {
    Quiet(Duration),
    Warn(Duration),
}

/// The retry loop's state: the startup budget, while it lasts, and the
/// failure counts each backoff is indexed by.
#[derive(Debug)]
struct RetryState {
    budget: Duration,
    in_budget: bool,
    /// Failed attempts so far.
    attempts: u32,
    /// Of those, the warned ones.
    warned: u32,
}

impl RetryState {
    fn new(budget: Duration) -> Self {
        Self {
            budget,
            in_budget: !budget.is_zero(),
            attempts: 0,
            warned: 0,
        }
    }

    /// Records a failed attempt, network-level or not (`lag`), `waited`
    /// after the first attempt began. Within the budget a lag failure waits
    /// [`STARTUP_LAG_BACKOFF`] (never past the budget's end); anything else
    /// ends the budget for good and waits `backoff`.
    fn after_failure(&mut self, lag: bool, waited: Duration, backoff: Backoff) -> Next {
        let quiet = self.attempts;
        self.attempts = self.attempts.saturating_add(1);
        let left = self.budget.saturating_sub(waited);
        if self.in_budget && lag && !left.is_zero() {
            return Next::Quiet(STARTUP_LAG_BACKOFF.delay(quiet).min(left));
        }
        self.in_budget = false;
        let delay = backoff.delay(self.warned);
        self.warned = self.warned.saturating_add(1);
        Next::Warn(delay)
    }
}

/// A `*_STARTUP_CONNECT_TIMEOUT_SECS` variable: the startup budget
/// ([`retry_until_ready_past_startup_lag`]) in whole seconds, `0` to turn
/// it off, [`DEFAULT_STARTUP_CONNECT_BUDGET`] when unset.
pub fn startup_connect_budget_from_env(var: &str) -> anyhow::Result<Duration> {
    parse_startup_connect_budget(var, std::env::var(var).ok().as_deref())
}

/// Parses a `*_STARTUP_CONNECT_TIMEOUT_SECS` value (`raw`, if set) as
/// [`startup_connect_budget_from_env`] does.
pub fn parse_startup_connect_budget(var: &str, raw: Option<&str>) -> anyhow::Result<Duration> {
    let Some(raw) = raw else {
        return Ok(DEFAULT_STARTUP_CONNECT_BUDGET);
    };
    raw.trim()
        .parse::<u64>()
        .map(Duration::from_secs)
        .map_err(|_| anyhow::anyhow!("{var}={raw:?} is not a whole number of seconds"))
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

    /// A fake dependency that fails with each of `errors` in turn, then
    /// connects.
    struct ScriptedDependency {
        errors: std::cell::RefCell<std::collections::VecDeque<&'static str>>,
    }

    impl ScriptedDependency {
        fn new(errors: impl IntoIterator<Item = &'static str>) -> Self {
            Self {
                errors: std::cell::RefCell::new(errors.into_iter().collect()),
            }
        }

        fn connect(&self) -> impl Future<Output = Result<&'static str, &'static str>> {
            std::future::ready(
                self.errors
                    .borrow_mut()
                    .pop_front()
                    .map_or(Ok("connection"), Err),
            )
        }
    }

    const REFUSED: &str = "Connection refused (os error 111)";
    const BAD_PASSWORD: &str = "password authentication failed";

    fn refused(err: &&str) -> bool {
        *err == REFUSED
    }

    /// A new pod's lag: refused for a few seconds, then up. Retried every
    /// 100 to 500 ms, not on the 1 s to 15 s backoff.
    #[tokio::test(start_paused = true)]
    async fn refusals_within_the_startup_budget_are_retried_quickly() {
        let dependency = ScriptedDependency::new([REFUSED; 8]);
        let started = tokio::time::Instant::now();
        let conn = retry_until_ready_past_startup_lag(
            "fake",
            DEFAULT_STARTUP_CONNECT_BUDGET,
            refused,
            CONNECT_BACKOFF,
            None,
            || dependency.connect(),
        )
        .await;
        assert_eq!(conn, "connection");
        // Ceilings 0.1+0.2+0.4+0.5x5 = 3.2 s, each jittered into
        // [half, full]; CONNECT_BACKOFF would take at least 37.5 s.
        let waited = started.elapsed();
        assert!(
            waited >= Duration::from_millis(1600) && waited <= Duration::from_millis(3200),
            "waited {waited:?}"
        );
    }

    fn quiet(next: Next) -> Duration {
        match next {
            Next::Quiet(delay) => delay,
            Next::Warn(_) => panic!("warned: {next:?}"),
        }
    }

    fn warned(next: Next) -> Duration {
        match next {
            Next::Warn(delay) => delay,
            Next::Quiet(_) => panic!("quiet: {next:?}"),
        }
    }

    const SECOND: Duration = Duration::from_secs(1);

    /// Lag failures within the budget are quiet (DEBUG), on
    /// `STARTUP_LAG_BACKOFF`, and never wait past the budget's end.
    #[test]
    fn lag_within_the_startup_budget_is_quiet() {
        let mut state = RetryState::new(Duration::from_secs(15));
        let first = quiet(state.after_failure(true, Duration::ZERO, CONNECT_BACKOFF));
        assert!(first <= Duration::from_millis(100), "{first:?}");
        for _ in 0..5 {
            let delay = quiet(state.after_failure(true, 2 * SECOND, CONNECT_BACKOFF));
            assert!(delay <= Duration::from_millis(500), "{delay:?}");
        }
        let last = quiet(state.after_failure(true, Duration::from_millis(14_900), CONNECT_BACKOFF));
        assert!(
            last <= Duration::from_millis(100),
            "clipped to the budget: {last:?}"
        );
    }

    /// Lag that outlasts the budget is an outage: WARN on the caller's
    /// backoff, counted from its start, and the budget does not come back.
    #[test]
    fn lag_past_the_startup_budget_warns_on_the_normal_backoff() {
        let mut state = RetryState::new(Duration::from_secs(3));
        quiet(state.after_failure(true, Duration::ZERO, CONNECT_BACKOFF));
        let first = warned(state.after_failure(true, 3 * SECOND, CONNECT_BACKOFF));
        assert!(
            first >= Duration::from_millis(500) && first <= SECOND,
            "{first:?}"
        );
        let second = warned(state.after_failure(true, 4 * SECOND, CONNECT_BACKOFF));
        assert!(second >= SECOND && second <= 2 * SECOND, "{second:?}");
    }

    /// A failure that is not the lag (a wrong password) warns at once and
    /// ends the budget: later lag failures warn too.
    #[test]
    fn a_failure_other_than_the_lag_ends_the_startup_budget() {
        let mut state = RetryState::new(Duration::from_secs(15));
        warned(state.after_failure(false, Duration::ZERO, CONNECT_BACKOFF));
        warned(state.after_failure(true, SECOND, CONNECT_BACKOFF));
    }

    /// A zero budget is plain [`retry_until_ready`]: every failure warns.
    #[test]
    fn a_zero_startup_budget_is_off() {
        let mut state = RetryState::new(Duration::ZERO);
        warned(state.after_failure(true, Duration::ZERO, CONNECT_BACKOFF));
    }

    /// The loop end to end: a wrong password, then lag, then success.
    #[tokio::test(start_paused = true)]
    async fn a_mixed_run_still_connects() {
        let dependency = ScriptedDependency::new([REFUSED, BAD_PASSWORD, REFUSED]);
        let conn = retry_until_ready_past_startup_lag(
            "fake",
            DEFAULT_STARTUP_CONNECT_BUDGET,
            refused,
            CONNECT_BACKOFF,
            None,
            || dependency.connect(),
        )
        .await;
        assert_eq!(conn, "connection");
    }

    /// Quiet retries still count as liveness.
    #[tokio::test(start_paused = true)]
    async fn quiet_retries_beat_progress() {
        // At least 0.25 s apart: 100 refusals outlast the 20 s checked.
        let dependency = ScriptedDependency::new([REFUSED; 100]);
        let progress = Progress::new(Duration::from_secs(1));
        let watcher = progress.clone();
        let waiting = retry_until_ready_past_startup_lag(
            "fake",
            Duration::from_secs(60),
            refused,
            CONNECT_BACKOFF,
            Some(&progress),
            || dependency.connect(),
        );
        let check = async {
            for _ in 0..40 {
                tokio::time::sleep(Duration::from_millis(500)).await;
                assert!(!watcher.is_stalled());
            }
        };
        let (conn, ()) = tokio::join!(waiting, check);
        assert_eq!(conn, "connection");
    }

    #[test]
    fn startup_budgets_parse_whole_seconds_and_0_turns_them_off() {
        assert_eq!(
            parse_startup_connect_budget("X", None).unwrap(),
            DEFAULT_STARTUP_CONNECT_BUDGET
        );
        assert_eq!(
            parse_startup_connect_budget("X", Some(" 30 ")).unwrap(),
            Duration::from_secs(30)
        );
        assert_eq!(
            parse_startup_connect_budget("X", Some("0")).unwrap(),
            Duration::ZERO
        );
        for bad in ["", "1.5", "-1", "two"] {
            let err = parse_startup_connect_budget("X", Some(bad)).unwrap_err();
            assert!(err.to_string().starts_with("X="), "{bad}: {err}");
        }
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
