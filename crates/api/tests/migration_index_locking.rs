//! Guards the one migration hazard this crate cannot express in SQL: an index
//! built inside sqlx's wrapping transaction takes an `ACCESS EXCLUSIVE`-grade
//! lock (a plain `SHARE` lock on the table, which blocks every `INSERT`,
//! `UPDATE` and `DELETE`) for the whole duration of the build.
//!
//! That matters here more than in most codebases, because of WHERE migrations
//! run. `crates/api/src/main.rs` calls `sqlx::migrate!().run(...)` BEFORE it
//! binds its listener, so:
//!
//! 1. writes to the table are blocked for the length of the index build; and
//! 2. `/public/health` does not answer for the length of the index build
//!    either, so the startup probe's total budget
//!    (`api.probes.startup` in `charts/distant-signal/values.yaml`) is a hard
//!    ceiling on it. Exceed that and the kubelet SIGKILLs the container, the
//!    DDL transaction rolls back, and the next start repeats it from scratch
//!    -- a crash loop that never converges.
//!
//! Postgres's answer is `CREATE INDEX CONCURRENTLY`, which takes only a lock
//! that permits writes. It cannot run inside a transaction block, which is
//! exactly why sqlx offers a per-file opt-out: a migration file whose FIRST
//! line is `-- no-transaction` is executed without the wrapping transaction
//! (`sqlx-core-0.8.6/src/migrate/source.rs:127` --
//! `let no_tx = sql.starts_with("-- no-transaction");`).
//!
//! Both halves of that were verified for real against Postgres 18 with
//! sqlx-cli 0.8.6 on 2026-09-25:
//!
//! * `CREATE INDEX CONCURRENTLY` with no opt-out ->
//!   `error: while executing migration ...: CREATE INDEX CONCURRENTLY cannot
//!   run inside a transaction block`
//! * the same DDL with `-- no-transaction` as line 1 -> applied cleanly, and
//!   `pg_index.indisvalid` = true.
//!
//! # Why the existing offenders are grandfathered rather than fixed
//!
//! Every file in `GRANDFATHERED` below builds a non-concurrent index on a
//! table that already existed before that migration ran, and every one of
//! them has ALREADY BEEN APPLIED in production. sqlx validates the checksum of
//! every applied migration on connect, so editing one of those files does not
//! improve anything -- it breaks startup on precisely the databases the change
//! was meant to protect. This repo already documents that constraint, for the
//! same reason, in `docs/shared-train-identity-backfill.md`:
//!
//! > the migration has already been applied in every existing environment:
//! > sqlx validates the checksum of every applied migration on connect, so
//! > editing that file would break `sqlx migrate run` on exactly the databases
//! > the check was meant to protect -- and would never re-run there anyway.
//!
//! Also verified for real on 2026-09-25 rather than assumed: applying the
//! concurrent rewrite to
//! `20260906111500_trust_event_backlog_train_uid_index.sql` in place, against
//! a database that already had it, produced
//! `error: migration 20260906111500 was previously applied but has been
//! modified` from `sqlx migrate run`, and `sqlx migrate info` reported
//! `20260906111500/installed (different checksum)`. In production that is
//! `sqlx::migrate!()` returning `Err` before `api` ever binds -- i.e. a
//! permanent CrashLoopBackOff, made worse by the api Deployment's
//! `strategy: Recreate`.
//!
//! A new migration cannot retroactively help either: the old file still runs
//! first on any database that has not seen it, and on a database that has,
//! there is nothing left to build. So the honest scope is forward-looking,
//! which is what this test enforces: the NEXT index migration must use the
//! concurrent pattern, and this list must not grow.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use regex::Regex;

/// Migration files that build a non-concurrent index on a table which already
/// existed when they ran, and which are already applied in production and so
/// can never be edited (see this file's module docs).
///
/// **This list must only ever shrink** -- and it can only shrink by a file
/// being deleted, never by being rewritten. Adding an entry means shipping the
/// exact write-blocking startup hazard this test exists to prevent; use
/// `-- no-transaction` plus `CREATE INDEX CONCURRENTLY` instead.
const GRANDFATHERED: &[&str] = &[
    // `incidents` -- small, but the oldest instance of the pattern.
    "20260706004003_reference_data.sql",
    "20260828100000_add_ownership.sql",
    "20260906100000_trains.sql",
    // Four indexes on `train_movement_events` / `train_current_state`, the
    // two highest-write tables fed by the live TRUST movement feed. This one
    // also mixes DDL with a data backfill, so it genuinely wants its
    // transaction -- splitting it would have needed more than a CONCURRENTLY
    // rewrite even if it had not already been applied.
    "20260906110000_train_movement_trains_id.sql",
    // `trust_event_backlog` -- named by the 2026-09-25 review.
    "20260906111500_trust_event_backlog_train_uid_index.sql",
    // Three indexes on `schedule_destination_departures` (~377k rows per rail
    // day) -- the `calling_point_search` and `train_uid_idx` files named by
    // the 2026-09-25 review.
    "20260908120000_schedule_destination_departures_calling_point_search.sql",
    "20260908140000_schedule_destination_departures_train_uid_idx.sql",
    "20260908150000_schedule_destination_departures_train_uid_idx.sql",
    "20260912090000_incidents_first_seen_at_id.sql",
    // `incidents_affected_lines` -- named by the 2026-09-25 review. A GIN
    // build, which is several times slower than an equivalent btree.
    "20260917090000_incidents_affected_lines.sql",
    "20260922140000_journey_templates.sql",
];

fn migrations_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("migrations")
}

fn migration_files() -> Vec<PathBuf> {
    let dir = migrations_dir();
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|err| panic!("read_dir {}: {err}", dir.display()))
        .map(|entry| entry.expect("read migrations dir entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "sql"))
        .collect();
    files.sort();
    assert!(
        files.len() > 50,
        "sanity check: this crate has many migrations; found {}",
        files.len()
    );
    files
}

/// `--` comments stripped and whitespace collapsed, so the multi-line
/// `CREATE INDEX x\n    ON tbl (...)` form this codebase uses throughout
/// matches the same pattern as the single-line form.
fn normalized_sql(raw: &str) -> String {
    let comment = Regex::new(r"--[^\n]*").unwrap();
    let ws = Regex::new(r"\s+").unwrap();
    ws.replace_all(&comment.replace_all(raw, " "), " ")
        .to_string()
}

/// Every non-concurrent index in `sql` whose table is NOT also created by the
/// same file, as `(index_name, table_name)`. An index created alongside its own
/// brand-new table is harmless: the table is empty and no other session can
/// see it yet, so the lock has nothing to block.
fn blocking_index_builds(sql: &str) -> Vec<(String, String)> {
    let created_tables: BTreeSet<String> = Regex::new(
        r"(?i)\bCREATE\s+(?:UNLOGGED\s+|TEMP(?:ORARY)?\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z_][A-Za-z0-9_]*)",
    )
    .unwrap()
    .captures_iter(sql)
    .map(|caps| caps[1].to_lowercase())
    .collect();

    Regex::new(
        r"(?i)\bCREATE\s+(?:UNIQUE\s+)?INDEX\s+(CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z_][A-Za-z0-9_]*)\s+ON\s+([A-Za-z_][A-Za-z0-9_]*)",
    )
    .unwrap()
    .captures_iter(sql)
    .filter(|caps| caps.get(1).is_none())
    .filter(|caps| !created_tables.contains(&caps[3].to_lowercase()))
    .map(|caps| (caps[2].to_string(), caps[3].to_string()))
    .collect()
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .expect("migration path has a file name")
        .to_string_lossy()
        .to_string()
}

#[test]
fn no_new_migration_builds_a_blocking_index_on_an_existing_table() {
    let grandfathered: BTreeSet<&str> = GRANDFATHERED.iter().copied().collect();
    let mut offenders = Vec::new();

    for path in migration_files() {
        let name = file_name(&path);
        if grandfathered.contains(name.as_str()) {
            continue;
        }
        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
        for (index, table) in blocking_index_builds(&normalized_sql(&raw)) {
            offenders.push(format!("{name}: CREATE INDEX {index} ON {table}"));
        }
    }

    assert!(
        offenders.is_empty(),
        "these migrations build a NON-CONCURRENT index on a table that already existed before \
         they ran:\n  {}\n\nsqlx wraps each migration file in one transaction, so such a build \
         blocks every write to that table for its whole duration -- and because \
         crates/api/src/main.rs runs sqlx::migrate!() BEFORE binding its listener, it also holds \
         /public/health down for that long, against the finite budget in \
         charts/distant-signal/values.yaml's api.probes.startup. Use the concurrent pattern \
         instead: make `-- no-transaction` the FIRST line of the file (sqlx only recognises it \
         there) and write `CREATE INDEX CONCURRENTLY`.\n\nTRADEOFF to accept deliberately: a \
         CONCURRENTLY build that fails leaves an INVALID index behind, which Postgres will not \
         use and will not clean up -- recovery is a manual `DROP INDEX` followed by a re-run, \
         not an automatic rollback. That is the price of not locking the table, and it is the \
         right trade for any table with production-scale rows.",
        offenders.join("\n  ")
    );
}

#[test]
fn a_concurrent_index_build_opts_out_of_the_wrapping_transaction() {
    let concurrently = Regex::new(r"(?i)\bCREATE\s+(?:UNIQUE\s+)?INDEX\s+CONCURRENTLY\b").unwrap();
    let mut broken = Vec::new();

    for path in migration_files() {
        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
        if concurrently.is_match(&normalized_sql(&raw)) && !raw.starts_with("-- no-transaction") {
            broken.push(file_name(&path));
        }
    }

    assert!(
        broken.is_empty(),
        "these migrations use CREATE INDEX CONCURRENTLY but do NOT begin with the exact line \
         `-- no-transaction`, so sqlx still wraps them in a transaction and Postgres rejects them \
         outright at apply time with \"CREATE INDEX CONCURRENTLY cannot run inside a transaction \
         block\" (verified against Postgres 18 / sqlx-cli 0.8.6). sqlx matches the marker with \
         `sql.starts_with(..)`, so it must be the very first line -- before any other comment: {}",
        broken.join(", ")
    );
}

/// Keeps `GRANDFATHERED` honest. A stale entry would silently exempt a file
/// that no longer needs exempting, or name a file that no longer exists.
#[test]
fn the_grandfathered_list_has_no_stale_entries() {
    let dir = migrations_dir();
    let mut missing = Vec::new();
    let mut no_longer_offending = Vec::new();

    for name in GRANDFATHERED {
        let path = dir.join(name);
        let Ok(raw) = std::fs::read_to_string(&path) else {
            missing.push((*name).to_string());
            continue;
        };
        if blocking_index_builds(&normalized_sql(&raw)).is_empty() {
            no_longer_offending.push((*name).to_string());
        }
    }

    assert!(
        missing.is_empty(),
        "GRANDFATHERED names migration files that do not exist -- delete these entries: {}",
        missing.join(", ")
    );
    assert!(
        no_longer_offending.is_empty(),
        "GRANDFATHERED names migration files that no longer build a blocking index, so the \
         exemption is dead weight hiding future regressions -- delete these entries: {}",
        no_longer_offending.join(", ")
    );

    let mut sorted = GRANDFATHERED.to_vec();
    sorted.sort_unstable();
    assert_eq!(
        GRANDFATHERED,
        &sorted[..],
        "keep GRANDFATHERED in version order so it reads as a timeline and duplicate entries are \
         obvious"
    );
}
