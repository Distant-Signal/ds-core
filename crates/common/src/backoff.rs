//! Capped exponential backoff with jitter, shared by every retry loop that
//! waits on a dependency that may simply not be ready yet (the OAuth token
//! fetch in [`crate::oauth_client`], `schedule-reference`'s startup seed and
//! its per-product publish retries).
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

    /// Sleeps for [`Backoff::delay`]`(attempt)` and returns how long it slept.
    pub async fn sleep(&self, attempt: u32) -> Duration {
        let delay = self.delay(attempt);
        tokio::time::sleep(delay).await;
        delay
    }
}

/// `ceiling/2 + fraction * ceiling/2`, with `fraction` clamped to `[0, 1]`.
fn jittered(ceiling: Duration, fraction: f64) -> Duration {
    let half = ceiling / 2;
    half + half.mul_f64(fraction.clamp(0.0, 1.0))
}

/// A uniform-ish value in `[0, 1)` from a freshly keyed `RandomState`.
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
