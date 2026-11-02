//! A process-lifetime, in-memory "log this key at least once, never flood
//! on repeats" dedup helper.
//!
//! Exists for the recurring shape a continuously-polling app like this one
//! keeps running into: a genuinely rare, real data-quality gap (e.g. a CIF
//! TIPLOC that never resolved to a CRS at all -- see
//! `crates/schedule-query/src/resolve.rs::unresolved_booked_tiplocs` and
//! `crates/api/src/data/journey.rs`'s own call site, both added for the
//! 2026-09-23 Northampton/`NMPTN` production incident) is worth a log line
//! SOMEONE will eventually read and go fix, but the same gap is also seen
//! again on every single poll cycle for as long as it remains unfixed --
//! this app polls its CIF/TRUST feeds continuously, so logging
//! unconditionally on every occurrence would flood the logs for a station
//! whose gap is already known and simply hasn't been fixed yet, while
//! logging nothing at all is exactly how the Northampton gap went
//! undiscovered until a live forensic investigation was needed.
//!
//! **Deliberately NOT persisted across process restarts.** Two real options
//! were weighed here:
//!
//! - A dedicated Postgres table (`logged_data_quality_gaps` or similar),
//!   keyed by whatever identifier this call site cares about, checked and
//!   inserted on every miss. This would survive a restart, at the cost of a
//!   migration, a write on a path that previously had none, and a new
//!   failure mode (what does a write failure here do to the caller's own
//!   result?) -- all to solve a problem that barely exists: this codebase's
//!   long-running services (`schedule-reference`, `api`) restart rarely
//!   (deploys, crashes), and CIF full-timetable deliveries land roughly
//!   once a day, so even a same-day restart re-logs at most a handful of
//!   lines, not a flood.
//! - An in-memory, process-lifetime [`HashSet`] (this module). Simpler,
//!   zero I/O, zero new failure mode. The accepted tradeoff: a process
//!   restart re-logs every gap that TIPLOC-shaped call site has already
//!   seen, once each, before falling silent again for the rest of that
//!   process's life. That is a strictly better failure mode than either
//!   alternative (a flood, or total silence), and "one extra log line per
//!   known gap after a restart" is a cost this app's own operators can
//!   easily read past.
//!
//! This module went with the second option. If a future caller's access
//! pattern genuinely needs cross-restart distinctness (e.g. a
//! once-a-year-restart service logging thousands of distinct keys, where
//! even "once per restart" would be a real flood), reconsider the table --
//! nothing here forecloses that.

use std::collections::HashSet;
use std::sync::Mutex;

/// See this module's own doc comment for the full reasoning; in short, a
/// thread-safe, process-lifetime "have I already logged this key" set.
#[derive(Debug, Default)]
pub struct LogOnceSet {
    seen: Mutex<HashSet<String>>,
}

impl LogOnceSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` the FIRST time `key` is passed to this instance
    /// (across its whole process lifetime), and `false` on every
    /// subsequent call with the same `key` -- callers log only when this
    /// returns `true`, which is what makes "at least once, never a flood"
    /// hold.
    ///
    /// A poisoned lock (another thread panicked while holding it) still
    /// recovers the set rather than propagating the panic -- this is a
    /// best-effort observability aid, not load-bearing application state,
    /// so losing a poisoned mutex's guarantees here is an acceptable
    /// tradeoff against a caller's unrelated request panicking merely
    /// because it happened to log a gap after some other unrelated task
    /// already poisoned this lock.
    pub fn should_log(&self, key: &str) -> bool {
        let mut seen = match self.seen.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        seen.insert(key.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_call_for_a_key_returns_true() {
        let set = LogOnceSet::new();
        assert!(set.should_log("NMPTN"));
    }

    #[test]
    fn a_repeated_call_for_the_same_key_returns_false() {
        let set = LogOnceSet::new();
        assert!(set.should_log("NMPTN"));
        assert!(!set.should_log("NMPTN"));
        assert!(!set.should_log("NMPTN"), "still false on a third repeat");
    }

    #[test]
    fn distinct_keys_are_tracked_independently() {
        let set = LogOnceSet::new();
        assert!(set.should_log("NMPTN"));
        assert!(
            set.should_log("HANSLPJ"),
            "a different key must still return true on its own first call"
        );
        assert!(!set.should_log("NMPTN"), "NMPTN itself is still deduped");
    }
}
