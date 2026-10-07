//! Session advisory-lock keys for the train-domain background loops
//! (ingest architecture, spec §12.3 and plan 1B.6/1B.7).
//!
//! Each loop runs only while its process holds `pg_try_advisory_lock(key)`
//! on a session it keeps open. A runner that does not hold the lock skips
//! that tick. The keys live here, not in the `ingest-writer` or `api`,
//! because both take them: the writer from 1B.6, and the api's own loops
//! from 1B.7, so during the cutover (writer on, `API_BACKGROUND_LOOPS`
//! still true) each sweep runs in one place at a time.
//!
//! Plain constants, no sqlx: `common` builds them without its `postgres`
//! feature.
//!
//! # Keys already in use elsewhere (not moved here)
//!
//! - `ds_store::migrate::MIGRATION_LOCK_KEY`, ASCII `"dsmigrat"`
//!   (`0x6473_6d69_6772_6174`), a session lock held while migrating;
//! - `api::data::corpus::CORPUS_LOAD_LOCK_KEY` (`0x0C0B_9053`), a
//!   transaction lock around a CORPUS load and the crosswalk rebuild;
//! - the schedule publish locks in `api::data::queries` (per product,
//!   transaction-scoped);
//! - `common::pg::lock_user_train_subscription`
//!   (`hashtextextended('train_subscription:…')`, transaction-scoped).
//!
//! Every key below is eight ASCII bytes read big-endian, like the
//! migration key, so it reads back in `pg_locks` (`classid`/`objid` are
//! the high and low halves) and cannot collide with the small CORPUS key.

/// One loop's lock: the loop's name (its log and metric label) and its key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoopLock {
    pub name: &'static str,
    pub key: i64,
}

/// Eight ASCII bytes as a big-endian `i64`.
const fn ascii_key(bytes: [u8; 8]) -> i64 {
    i64::from_be_bytes(bytes)
}

/// `schedule_matching::run_schedule_match_sweep`.
pub const SCHEDULE_MATCH_SWEEP: LoopLock = LoopLock {
    name: "schedule_match",
    key: ascii_key(*b"dslpschm"),
};

/// `reconciliation::run_reconciliation_sweep`.
pub const RECONCILIATION_SWEEP: LoopLock = LoopLock {
    name: "reconciliation",
    key: ascii_key(*b"dslprecn"),
};

/// `trust_event_backlog_match::run_backlog_match_sweep`.
pub const BACKLOG_MATCH_SWEEP: LoopLock = LoopLock {
    name: "backlog_match",
    key: ascii_key(*b"dslpbklg"),
};

/// `corpus_crosswalk::rebuild_if_stale` plus the CORPUS freshness gauge.
/// The rebuild also takes the CORPUS load lock inside its transaction; this
/// key only stops two runners from both checking staleness each tick.
pub const CORPUS_CROSSWALK: LoopLock = LoopLock {
    name: "corpus_crosswalk",
    key: ascii_key(*b"dslpcorp"),
};

/// The writer's no-op canary loop (`SELECT 1`): proves the runner, its lock
/// and its metrics work in production before the real sweeps move over.
pub const WRITER_CANARY: LoopLock = LoopLock {
    name: "canary",
    key: ascii_key(*b"dslpcnry"),
};

/// Every loop lock, for uniqueness checks.
pub const ALL: [LoopLock; 5] = [
    SCHEDULE_MATCH_SWEEP,
    RECONCILIATION_SWEEP,
    BACKLOG_MATCH_SWEEP,
    CORPUS_CROSSWALK,
    WRITER_CANARY,
];

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn names_and_keys_are_unique() {
        let names: HashSet<_> = ALL.iter().map(|lock| lock.name).collect();
        let keys: HashSet<_> = ALL.iter().map(|lock| lock.key).collect();
        assert_eq!(names.len(), ALL.len());
        assert_eq!(keys.len(), ALL.len());
    }

    /// The keys taken elsewhere (see the module docs), as literals: `common`
    /// cannot depend on `api`.
    #[test]
    fn no_key_collides_with_the_existing_locks() {
        let migration = ascii_key(*b"dsmigrat");
        assert_eq!(migration, 0x6473_6d69_6772_6174);
        for lock in ALL {
            assert_ne!(lock.key, migration, "{}", lock.name);
            assert_ne!(lock.key, 0x0C0B_9053, "{}", lock.name);
        }
    }

    #[test]
    fn keys_read_back_as_their_ascii_name() {
        assert_eq!(
            SCHEDULE_MATCH_SWEEP.key.to_be_bytes(),
            *b"dslpschm",
            "pg_locks shows classid/objid as the two halves of this"
        );
    }
}
