//! Capped exponential backoff with jitter, shared by every retry loop that
//! waits on a dependency that may simply not be ready yet (the OAuth token
//! fetch in [`crate::oauth_client`], `schedule-reference`'s startup seed and
//! its per-product publish retries, full-coverage-consumer's population
//! reload, and trust-consumer's tracked-trains reload through
//! [`RetrySchedule`]).
//!
//! **Why jitter.** Every container in a Pod, and every Pod on a rebooted
//! node, starts at the same instant and hits the same not-yet-ready
//! dependency (the 2026-09-26 node reboot: `sso.cursed.solutions` unresolvable,
//! then 502s, for about a minute). Retrying on a fixed schedule makes them all
//! retry in lockstep; spreading each wait over `[delay/2, delay]` ("equal
//! jitter") breaks that up while keeping a guaranteed minimum wait.
//!
//! Randomness comes from `std`'s randomly-keyed `RandomState` hasher, so this
//! needs no new dependency: jitter does not need to be cryptographic, only
//! different per process and per call.
//!
//! **Retry-After.** Since 2026-10-06 `api` answers 503 + `Retry-After` when
//! its database is unavailable (`api::unavailable`). [`Backoff::delay_honouring`]
//! never waits less than a server's `Retry-After` (capped at
//! [`MAX_HONOURED_RETRY_AFTER`], and spread over `[ra, 1.25 ra]` so clients
//! told the same value don't come back in lockstep). [`FailureStreak`] is the
//! consecutive-failure counter a consume loop keeps around it.

use std::hash::{BuildHasher, Hasher};
use std::time::Duration;

/// A capped exponential schedule: attempt `n` (0-based) waits
/// `min(initial * 2^n, max)`, jittered down into `[half, full]` of that.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backoff {
    pub initial: Duration,
    pub max: Duration,
}

impl Backoff {
    pub const fn new(initial: Duration, max: Duration) -> Self {
        Self { initial, max }
    }

    /// The un-jittered, capped delay before retry number `attempt` (0-based:
    /// `attempt == 0` is the wait after the first failure). Saturates rather
    /// than overflowing however large `attempt` gets.
    pub fn ceiling(&self, attempt: u32) -> Duration {
        let factor = 2u32.checked_pow(attempt.min(31)).unwrap_or(u32::MAX);
        self.initial.saturating_mul(factor).min(self.max)
    }

    /// [`Backoff::ceiling`] with equal jitter applied: uniformly somewhere in
    /// `[ceiling/2, ceiling]`.
    pub fn delay(&self, attempt: u32) -> Duration {
        jittered(self.ceiling(attempt), random_fraction())
    }

    /// [`Backoff::delay`], but never less than `retry_after` (a server's
    /// `Retry-After`, see [`crate::ingest::retry_after`]), which is capped at
    /// [`MAX_HONOURED_RETRY_AFTER`] and jittered up by at most a quarter.
    pub fn delay_honouring(&self, attempt: u32, retry_after: Option<Duration>) -> Duration {
        let delay = self.delay(attempt);
        match retry_after {
            Some(retry_after) => delay.max(jittered_up(
                retry_after.min(MAX_HONOURED_RETRY_AFTER),
                random_fraction(),
            )),
            None => delay,
        }
    }

    /// Sleeps for [`Backoff::delay`]`(attempt)` and returns how long it slept.
    pub async fn sleep(&self, attempt: u32) -> Duration {
        let delay = self.delay(attempt);
        tokio::time::sleep(delay).await;
        delay
    }
}

/// The longest `Retry-After` honoured: a misconfigured or hostile server
/// cannot park a consumer for longer than this per attempt.
pub const MAX_HONOURED_RETRY_AFTER: Duration = Duration::from_secs(300);

/// Consecutive failures of one operation, for a loop that retries it on
/// every pass (a consumer's POST of the batch it keeps un-ACKed): each
/// [`FailureStreak::failed`] returns the next, longer wait, and a success
/// resets it. Replaces the fixed 2s `ERROR_BACKOFF` the consumers used to
/// sleep after every failure, which during the 2026-10-01 outage meant a
/// POST every 2s per consumer for six hours.
#[derive(Debug, Clone, Copy)]
pub struct FailureStreak {
    backoff: Backoff,
    failures: u32,
}

impl FailureStreak {
    pub const fn new(backoff: Backoff) -> Self {
        Self {
            backoff,
            failures: 0,
        }
    }

    /// Records a failure and returns how long to wait before the next
    /// attempt: the backoff for this many failures in a row, and never less
    /// than `retry_after` (see [`Backoff::delay_honouring`]).
    pub fn failed(&mut self, retry_after: Option<Duration>) -> Duration {
        let delay = self.backoff.delay_honouring(self.failures, retry_after);
        self.failures = self.failures.saturating_add(1);
        delay
    }

    /// Records a success: the next failure starts from the shortest wait.
    pub fn succeeded(&mut self) {
        self.failures = 0;
    }

    /// How many attempts have failed in a row.
    pub fn failures(&self) -> u32 {
        self.failures
    }
}

/// When a periodic reload is next due: `interval` after a success, and
/// after a failure the [`Backoff`] delay for however many have failed in a
/// row -- so a reload checked on every pass of a busy loop is not retried on
/// every pass while its dependency is down (trust-consumer's tracked-trains
/// reload, ~23.6k failed GETs in the 2026-10-01 outage).
#[derive(Debug, Clone, Copy)]
pub struct RetrySchedule {
    interval: Duration,
    retry: Backoff,
    due: tokio::time::Instant,
    failures: u32,
}

impl RetrySchedule {
    /// A schedule whose first attempt is due at once.
    pub fn new(interval: Duration, retry: Backoff) -> Self {
        Self {
            interval,
            retry,
            due: tokio::time::Instant::now(),
            failures: 0,
        }
    }

    /// Whether the next attempt is due.
    pub fn is_due(&self) -> bool {
        tokio::time::Instant::now() >= self.due
    }

    /// Records a success: the next attempt is a full `interval` away.
    pub fn succeeded(&mut self) {
        self.failures = 0;
        self.due = tokio::time::Instant::now() + self.interval;
    }

    /// Records a failure and returns how long until the next attempt: the
    /// backoff delay for this many failures in a row, never more than
    /// `interval`.
    pub fn failed(&mut self) -> Duration {
        self.failed_honouring(None)
    }

    /// [`RetrySchedule::failed`], waiting at least a server's `Retry-After`
    /// (still never more than `interval`).
    pub fn failed_honouring(&mut self, retry_after: Option<Duration>) -> Duration {
        let delay = self
            .retry
            .delay_honouring(self.failures, retry_after)
            .min(self.interval);
        self.failures = self.failures.saturating_add(1);
        self.due = tokio::time::Instant::now() + delay;
        delay
    }

    /// How many attempts have failed in a row.
    pub fn failures(&self) -> u32 {
        self.failures
    }
}

/// `ceiling/2 + fraction * ceiling/2`, with `fraction` clamped to `[0, 1]`.
fn jittered(ceiling: Duration, fraction: f64) -> Duration {
    let half = ceiling / 2;
    half + half.mul_f64(fraction.clamp(0.0, 1.0))
}

/// `floor + fraction * floor/4`, with `fraction` clamped to `[0, 1]`: a
/// wait that is at least `floor`.
fn jittered_up(floor: Duration, fraction: f64) -> Duration {
    floor + (floor / 4).mul_f64(fraction.clamp(0.0, 1.0))
}

/// A uniform-ish value in `[0, 1)` from a freshly keyed `RandomState`.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    reason = "jitter only needs the low 64 bits of the clock and 53 random bits"
)]
fn random_fraction() -> f64 {
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u64(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or_default(),
    );
    // Top 53 bits -> an exactly representable f64 in [0, 1).
    (hasher.finish() >> 11) as f64 / (1u64 << 53) as f64
}

#[cfg(test)]
#[expect(
    clippy::float_cmp,
    reason = "test code: exact expected values are the point"
)]
mod tests {
    use super::*;

    const B: Backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(60));

    #[test]
    fn the_ceiling_doubles_per_attempt_and_is_capped() {
        assert_eq!(B.ceiling(0), Duration::from_secs(1));
        assert_eq!(B.ceiling(1), Duration::from_secs(2));
        assert_eq!(B.ceiling(5), Duration::from_secs(32));
        assert_eq!(B.ceiling(6), Duration::from_secs(60));
        assert_eq!(B.ceiling(u32::MAX), Duration::from_secs(60));
    }

    #[test]
    fn jitter_stays_between_half_and_the_full_ceiling() {
        let ceiling = Duration::from_secs(10);
        assert_eq!(jittered(ceiling, 0.0), Duration::from_secs(5));
        assert_eq!(jittered(ceiling, 1.0), Duration::from_secs(10));
        assert_eq!(jittered(ceiling, 7.0), Duration::from_secs(10));
        for attempt in 0..20 {
            let delay = B.delay(attempt);
            assert!(delay >= B.ceiling(attempt) / 2 && delay <= B.ceiling(attempt));
        }
    }

    /// Consecutive failures back off (1s, 2s, 4s, ... jittered, capped at
    /// 60s and at the interval); a success resets to the full interval.
    #[tokio::test(start_paused = true)]
    async fn a_retry_schedule_backs_off_on_consecutive_failures() {
        let mut schedule = RetrySchedule::new(Duration::from_secs(300), B);
        assert!(schedule.is_due(), "the first attempt is due at once");
        let mut previous_ceiling = Duration::ZERO;
        for attempt in 0..10 {
            let delay = schedule.failed();
            let ceiling = B.ceiling(attempt);
            assert!(
                delay >= ceiling / 2 && delay <= ceiling,
                "attempt {attempt}: {delay:?} outside [{:?}, {ceiling:?}]",
                ceiling / 2
            );
            assert!(ceiling >= previous_ceiling, "the ceiling never shrinks");
            previous_ceiling = ceiling;
            assert!(!schedule.is_due(), "not due again straight after a failure");
            tokio::time::advance(delay).await;
            assert!(schedule.is_due(), "due once the delay has passed");
        }
        assert_eq!(schedule.failures(), 10);
        assert_eq!(previous_ceiling, Duration::from_secs(60), "capped at 60s");

        schedule.succeeded();
        assert_eq!(schedule.failures(), 0);
        tokio::time::advance(Duration::from_secs(299)).await;
        assert!(!schedule.is_due());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(schedule.is_due());
        assert!(
            schedule.failed() <= Duration::from_secs(1),
            "the backoff restarts"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_retry_never_waits_longer_than_the_interval() {
        let mut schedule = RetrySchedule::new(Duration::from_secs(5), B);
        for _ in 0..10 {
            assert!(schedule.failed() <= Duration::from_secs(5));
        }
    }

    #[test]
    fn retry_after_is_a_floor_jittered_up_and_capped() {
        let thirty = Duration::from_secs(30);
        for attempt in 0..3 {
            let delay = B.delay_honouring(attempt, Some(thirty));
            assert!(delay >= thirty && delay <= thirty + thirty / 4, "{delay:?}");
        }
        // A longer backoff wins over a shorter Retry-After.
        let delay = B.delay_honouring(10, Some(Duration::from_secs(1)));
        assert!(delay >= Duration::from_secs(30), "{delay:?}");
        // No hint: the plain backoff.
        assert!(B.delay_honouring(0, None) <= Duration::from_secs(1));
        // A huge Retry-After is capped.
        let delay = B.delay_honouring(0, Some(Duration::from_secs(86_400)));
        assert!(delay <= MAX_HONOURED_RETRY_AFTER + MAX_HONOURED_RETRY_AFTER / 4);
        assert_eq!(jittered_up(thirty, 0.0), thirty);
        assert_eq!(jittered_up(thirty, 9.0), thirty + thirty / 4);
    }

    /// The consumers' failure streak: doubling (jittered, capped), honouring
    /// Retry-After, reset by a success.
    #[test]
    fn a_failure_streak_backs_off_and_resets() {
        let mut streak = FailureStreak::new(B);
        let mut previous_ceiling = Duration::ZERO;
        for attempt in 0..10 {
            let delay = streak.failed(None);
            let ceiling = B.ceiling(attempt);
            assert!(
                delay >= ceiling / 2 && delay <= ceiling,
                "{attempt}: {delay:?}"
            );
            assert!(ceiling >= previous_ceiling);
            previous_ceiling = ceiling;
        }
        assert_eq!(streak.failures(), 10);
        assert_eq!(previous_ceiling, Duration::from_secs(60));
        streak.succeeded();
        assert_eq!(streak.failures(), 0);
        assert!(streak.failed(None) <= Duration::from_secs(1), "reset");
        assert!(streak.failed(Some(Duration::from_secs(30))) >= Duration::from_secs(30));
    }

    #[tokio::test(start_paused = true)]
    async fn a_retry_schedule_honours_retry_after_up_to_its_interval() {
        let mut schedule = RetrySchedule::new(Duration::from_secs(60), B);
        let delay = schedule.failed_honouring(Some(Duration::from_secs(30)));
        assert!(delay >= Duration::from_secs(30) && delay <= Duration::from_secs(60));
        let delay = schedule.failed_honouring(Some(Duration::from_secs(200)));
        assert_eq!(delay, Duration::from_secs(60), "never past the interval");
    }

    #[test]
    fn random_fraction_is_in_the_unit_interval_and_varies() {
        let samples: Vec<f64> = (0..64).map(|_| random_fraction()).collect();
        assert!(samples.iter().all(|f| (0.0..1.0).contains(f)));
        assert!(
            samples.windows(2).any(|w| w[0] != w[1]),
            "jitter must not be a constant"
        );
    }
}
