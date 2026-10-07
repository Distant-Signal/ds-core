//! Guards against editing a migration after it has merged (Repeater Signal
//! review, finding M16).
//!
//! sqlx stores the SHA-384 of every applied migration file in
//! `_sqlx_migrations.checksum` and compares it on every
//! `sqlx::migrate!().run(...)`. `crates/api/src/main.rs` runs migrations
//! (`ds_store::migrate`, over `crates/ds-store/migrations`; the chart's
//! `ds-migrate` Job runs the same code) before it binds, so a merged
//! migration edited in place makes `api` fail at startup on every database
//! that already applied it
//! ("migration ... was previously applied but has been modified"), a
//! `CrashLoopBackOff` under the Deployment's `strategy: Recreate`. See also
//! `migration_index_locking.rs`'s module docs.
//!
//! M16 found two files edited after merge (`20260831090001`,
//! `20260908150000`). On 2026-09-27 production's `_sqlx_migrations` held 98
//! rows whose checksums all equal the SHA-384 of the current files, those two
//! included, so nothing has drifted. `tests/migration_checksums.lock` records
//! those checksums; this test fails if any recorded file changes or
//! disappears.
//!
//! A migration newer than every locked one is allowed to be unlisted (it has
//! not merged yet); append its line to the lock when it does. An unlisted file
//! older than the newest locked one fails: it was either renamed or inserted
//! out of order.
//!
//! "Append it when it merges" was not enough on its own: the lock lagged
//! four deployed migrations (`20260928100000` to `20260928180000`) behind
//! main, and an unlocked migration is unprotected. So an unlisted migration
//! whose timestamp is more than `UNLOCKED_GRACE_DAYS` old also fails
//! (`unlocked_migrations_do_not_linger`). That test depends on the date: it
//! can start failing on an unchanged tree, which is the point. The fix is
//! always the line the failure message prints; add it in the PR that adds the
//! migration, or in the next PR after it merges.

#![expect(
    clippy::expect_used,
    clippy::format_collect,
    clippy::unwrap_used,
    reason = "test code: a panic is the right failure in a test; test string building is not hot"
)]

use std::collections::BTreeMap;
use std::path::PathBuf;

use sha2::{Digest, Sha384};

/// Resolved at run time from the `CARGO_MANIFEST_DIR` cargo sets for the
/// test process, not baked in with `env!` (Train Register verification
/// 2026-10-01, N6): with a target dir shared between worktrees, a reused
/// binary would otherwise read another worktree's migrations. Not embedded
/// with `sqlx::migrate!` either: that cannot see a NEW file until something
/// else forces a rebuild.
fn api_dir() -> PathBuf {
    common::manifest_dir!()
}

fn locked_checksums() -> BTreeMap<String, String> {
    let path = api_dir().join("tests/migration_checksums.lock");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            let (file, checksum) = line
                .split_once(' ')
                .unwrap_or_else(|| panic!("malformed lock line: {line:?}"));
            (file.to_string(), checksum.trim().to_string())
        })
        .collect()
}

fn current_checksums() -> BTreeMap<String, String> {
    // Plan task 1B.1 moved the directory into ds-store; the lock stays here.
    let dir = api_dir().join("../ds-store/migrations");
    std::fs::read_dir(&dir)
        .unwrap_or_else(|err| panic!("read_dir {}: {err}", dir.display()))
        .map(|entry| entry.expect("read migrations dir entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "sql"))
        .map(|path| {
            let bytes =
                std::fs::read(&path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
            let checksum: String = Sha384::digest(&bytes)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect();
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            (name, checksum)
        })
        .collect()
}

#[test]
fn merged_migrations_are_never_edited_renamed_or_removed() {
    let locked = locked_checksums();
    assert!(
        locked.len() > 50,
        "sanity check: the lock should list many migrations; found {}",
        locked.len()
    );
    let current = current_checksums();

    let mut problems = Vec::new();
    for (file, checksum) in &locked {
        match current.get(file) {
            None => problems.push(format!(
                "{file}: merged migration is missing (renamed or deleted)"
            )),
            Some(actual) if actual != checksum => problems.push(format!(
                "{file}: contents changed after merge (lock {checksum}, file {actual}); \
                 add a new migration instead"
            )),
            Some(_) => {}
        }
    }

    let newest_locked = locked.keys().next_back().expect("non-empty lock");
    for (file, checksum) in &current {
        if !locked.contains_key(file) && file < newest_locked {
            problems.push(format!(
                "{file}: not in tests/migration_checksums.lock but older than the newest \
                 locked migration ({newest_locked}); if it is new, give it a later timestamp, \
                 otherwise add `{file} {checksum}`"
            ));
        }
    }

    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

#[test]
fn the_checksum_matches_what_sqlx_records() {
    // sqlx 0.8 records `Sha384::digest(file contents)`, the whole file,
    // `-- no-transaction` header included. Spot-check one lock entry against
    // sqlx's own resolver so a change in how sqlx checksums would show up
    // here rather than as a surprise in production.
    let locked = locked_checksums();
    let migrator = sqlx::migrate!("../ds-store/migrations");
    let migration = migrator
        .iter()
        .find(|m| m.version == 20_260_831_090_001)
        .expect("migration 20260831090001 exists");
    let sqlx_hex: String = migration
        .checksum
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let (_, locked_hex) = locked
        .iter()
        .find(|(file, _)| file.starts_with("20260831090001_"))
        .expect("20260831090001 is locked");
    assert_eq!(&sqlx_hex, locked_hex);
}

/// How long a migration may stay out of the lock, counted from the timestamp
/// in its file name. Long enough for an ordinary PR to merge first; short
/// enough that the lock cannot silently lag main for weeks.
const UNLOCKED_GRACE_DAYS: i64 = 14;

#[test]
fn unlocked_migrations_do_not_linger() {
    let locked = locked_checksums();
    let now = chrono::Utc::now().naive_utc();

    let mut problems = Vec::new();
    for (file, checksum) in current_checksums() {
        if locked.contains_key(&file) {
            continue;
        }
        let stamp = file.split('_').next().unwrap_or_default();
        let created = chrono::NaiveDateTime::parse_from_str(stamp, "%Y%m%d%H%M%S")
            .unwrap_or_else(|err| panic!("{file}: timestamp prefix {stamp:?}: {err}"));
        let age_days = (now - created).num_days();
        if age_days > UNLOCKED_GRACE_DAYS {
            problems.push(format!(
                "{file}: {age_days} days old and still not in tests/migration_checksums.lock; \
                 once it is on main, add `{file} {checksum}`"
            ));
        }
    }

    assert!(problems.is_empty(), "{}", problems.join("\n"));
}
