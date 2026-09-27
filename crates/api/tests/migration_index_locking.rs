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
    // `unlisted_links` -- merged the same day as this guard test itself,
    // slightly ahead of it landing on `main`, and already pushed (likely
    // already applied in production given today's deploy cadence) before
    // the collision was caught here. Same non-negotiable constraint as
    // every other entry above: sqlx checksums an applied migration on
    // connect, so rewriting this file now risks a production
    // CrashLoopBackOff on exactly the databases this guard exists to
    // protect, for a table (`unlisted_links`) that is small today but
    // would need the same CONCURRENTLY treatment if it ever needed a new
    // index again.
    "20260925091000_unlisted_links_one_active_per_resource.sql",
];

/// Already-applied migrations that run one of the [`TableLockHazard`]s on an
/// existing table. Found when the guard was widened on 2026-09-27 (A2 /
/// DB2-33); same rules as `GRANDFATHERED`: they can never be edited, and
/// this list must only ever shrink.
const GRANDFATHERED_TABLE_LOCK_HAZARDS: &[(&str, TableLockHazard)] = &[
    // `custom_lines` -- small.
    (
        "20260901120000_custom_lines_owner_not_null.sql",
        TableLockHazard::SetNotNull,
    ),
    // `tracked_trains` -- since replaced by `trains`.
    (
        "20260905150000_schedule_matched_resolution.sql",
        TableLockHazard::ValidatingConstraint,
    ),
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

/// Comments (`--` and `/* */`) stripped and whitespace collapsed, so the
/// multi-line `CREATE INDEX x\n    ON tbl (...)` form this codebase uses
/// throughout matches the same pattern as the single-line form, and a
/// commented-out statement never counts. String literals, quoted identifiers
/// and dollar-quoted bodies are kept verbatim (a `--` inside one is not a
/// comment).
fn normalized_sql(raw: &str) -> String {
    let ws = Regex::new(r"\s+").unwrap();
    ws.replace_all(&lex(raw).text, " ").trim().to_string()
}

/// The result of one pass over a migration file.
struct Lexed {
    /// The file with comments replaced by a space.
    text: String,
    /// Top-level statements (split on `;` outside quotes, dollar quotes and
    /// comments), comments removed, trimmed, empty ones dropped.
    statements: Vec<String>,
}

/// A small SQL lexer: just enough to tell comments, `'strings'`,
/// `"identifiers"` and `$tag$ bodies $tag$` apart from code, so comment
/// stripping and statement splitting are not fooled by a `;` or `--` inside
/// a `DO $$ ... $$` block or a string.
fn lex(raw: &str) -> Lexed {
    let bytes = raw.as_bytes();
    let mut text = String::with_capacity(raw.len());
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut i = 0;
    let push = |text: &mut String, current: &mut String, s: &str| {
        text.push_str(s);
        current.push_str(s);
    };
    while i < bytes.len() {
        let rest = &raw[i..];
        if rest.starts_with("--") {
            let end = rest.find('\n').map_or(raw.len(), |n| i + n);
            push(&mut text, &mut current, " ");
            i = end;
        } else if rest.starts_with("/*") {
            // Postgres block comments nest.
            let mut depth = 0usize;
            let mut j = i;
            while j < bytes.len() {
                if bytes[j..].starts_with(b"/*") {
                    depth += 1;
                    j += 2;
                } else if bytes[j..].starts_with(b"*/") {
                    depth -= 1;
                    j += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    j += 1;
                }
            }
            push(&mut text, &mut current, " ");
            i = j.min(raw.len());
        } else if bytes[i] == b'\'' || bytes[i] == b'"' {
            let quote = bytes[i];
            let mut j = i + 1;
            while j < bytes.len() {
                if bytes[j] == quote {
                    if bytes.get(j + 1) == Some(&quote) {
                        j += 2;
                        continue;
                    }
                    break;
                }
                j += 1;
            }
            let end = (j + 1).min(raw.len());
            push(&mut text, &mut current, &raw[i..end]);
            i = end;
        } else if let Some(tag) = dollar_quote_tag(rest) {
            let body_start = i + tag.len();
            let end = raw[body_start..]
                .find(tag)
                .map_or(raw.len(), |n| body_start + n + tag.len());
            push(&mut text, &mut current, &raw[i..end]);
            i = end;
        } else if bytes[i] == b';' {
            text.push(';');
            let statement = current.trim().to_string();
            if !statement.is_empty() {
                statements.push(statement);
            }
            current.clear();
            i += 1;
        } else {
            let ch = rest.chars().next().unwrap();
            let mut buf = [0u8; 4];
            push(&mut text, &mut current, ch.encode_utf8(&mut buf));
            i += ch.len_utf8();
        }
    }
    let statement = current.trim().to_string();
    if !statement.is_empty() {
        statements.push(statement);
    }
    Lexed { text, statements }
}

/// The opening `$tag$` / `$$` of a dollar-quoted string at the start of
/// `rest`, if there is one. `$1` (a parameter) is not one: a tag cannot start
/// with a digit.
fn dollar_quote_tag(rest: &str) -> Option<&str> {
    let bytes = rest.as_bytes();
    if bytes.first() != Some(&b'$') {
        return None;
    }
    let mut j = 1;
    while j < bytes.len()
        && (bytes[j].is_ascii_alphabetic()
            || bytes[j] == b'_'
            || (j > 1 && bytes[j].is_ascii_digit()))
    {
        j += 1;
    }
    (bytes.get(j) == Some(&b'$')).then(|| &rest[..=j])
}

/// An identifier, optionally schema-qualified, bare or double-quoted.
const IDENT: &str = r#"(?:"(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_$]*)(?:\.(?:"(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_$]*))?"#;

/// Lower-cased, unquoted, schema-stripped: `public."Foo"` -> `foo`, so a
/// table is recognised however the file spells it.
fn canonical_ident(ident: &str) -> String {
    let last = ident.rsplit('.').next().unwrap_or(ident);
    last.trim_matches('"').replace("\"\"", "\"").to_lowercase()
}

/// Tables `sql` itself creates. Anything done to such a table in the same
/// file is harmless: it is empty and no other session can see it yet.
fn created_tables(sql: &str) -> BTreeSet<String> {
    Regex::new(&format!(
        r"(?i)\bCREATE\s+(?:UNLOGGED\s+|TEMP(?:ORARY)?\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?({IDENT})"
    ))
    .unwrap()
    .captures_iter(sql)
    .map(|caps| canonical_ident(&caps[1]))
    .collect()
}

/// Every non-concurrent index in `sql` whose table is NOT also created by the
/// same file, as `(index_name, table_name)`. An index created alongside its own
/// brand-new table is harmless: the table is empty and no other session can
/// see it yet, so the lock has nothing to block.
///
/// Also catches an unnamed index (`CREATE INDEX ON t (...)`, reported as
/// `<unnamed>`), quoted and schema-qualified names, `ON ONLY`, and the
/// index a `PRIMARY KEY` / `UNIQUE` / `EXCLUDE` constraint added by
/// `ALTER TABLE` builds under that statement's ACCESS EXCLUSIVE lock (unless
/// it adopts an existing index with `USING INDEX`).
fn blocking_index_builds(sql: &str) -> Vec<(String, String)> {
    let created = created_tables(sql);
    let mut found: Vec<(String, String)> = Regex::new(&format!(
        r"(?i)\bCREATE\s+(?:UNIQUE\s+)?INDEX\s+(CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?(?:({IDENT})\s+)??ON\s+(?:ONLY\s+)?({IDENT})"
    ))
    .unwrap()
    .captures_iter(sql)
    .filter(|caps| caps.get(1).is_none())
    .filter(|caps| !created.contains(&canonical_ident(&caps[3])))
    .map(|caps| {
        let name = caps
            .get(2)
            .map_or_else(|| "<unnamed>".to_string(), |m| m.as_str().to_string());
        (name, caps[3].to_string())
    })
    .collect();

    let constraint_index = Regex::new(&format!(
        r"(?i)^ADD\s+(?:CONSTRAINT\s+({IDENT})\s+)?(PRIMARY\s+KEY|UNIQUE|EXCLUDE)\b"
    ))
    .unwrap();
    let using_index = Regex::new(r"(?i)\bUSING\s+INDEX\s+[A-Za-z_\x22]").unwrap();
    for (table, action) in alter_table_actions(sql) {
        if created.contains(&canonical_ident(&table)) {
            continue;
        }
        if let Some(caps) = constraint_index.captures(&action)
            && !using_index.is_match(&action)
        {
            let name = caps.get(1).map_or_else(
                || format!("<unnamed {}>", caps[2].to_uppercase()),
                |m| m.as_str().to_string(),
            );
            found.push((name, table));
        }
    }
    found
}

/// Every `ALTER TABLE` action in `sql`, as `(table, action)`, with the
/// action list split on top-level commas: `ALTER TABLE t ADD a int, ALTER b
/// SET NOT NULL` gives two entries.
fn alter_table_actions(sql: &str) -> Vec<(String, String)> {
    let alter = Regex::new(&format!(
        r"(?is)^ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?({IDENT})\s*\*?\s+(.*)$"
    ))
    .unwrap();
    let ws = Regex::new(r"\s+").unwrap();
    let mut out = Vec::new();
    for statement in lex(sql).statements {
        let statement = ws.replace_all(&statement, " ").to_string();
        let Some(caps) = alter.captures(&statement) else {
            continue;
        };
        let table = caps[1].to_string();
        let mut depth = 0i32;
        let mut action = String::new();
        for ch in caps[2].chars() {
            match ch {
                '(' => depth += 1,
                ')' => depth -= 1,
                ',' if depth == 0 => {
                    out.push((table.clone(), action.trim().to_string()));
                    action.clear();
                    continue;
                }
                _ => {}
            }
            action.push(ch);
        }
        if !action.trim().is_empty() {
            out.push((table.clone(), action.trim().to_string()));
        }
    }
    out
}

/// A table-wide scan or rewrite that an `ALTER TABLE` runs while holding
/// ACCESS EXCLUSIVE on a table that already has rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum TableLockHazard {
    /// `ALTER COLUMN ... SET NOT NULL` scans the whole table (unless a valid
    /// `CHECK (col IS NOT NULL)` exists; add one `NOT VALID`, `VALIDATE` it,
    /// then `SET NOT NULL`).
    SetNotNull,
    /// `ADD [CONSTRAINT] FOREIGN KEY` / `CHECK` without `NOT VALID` checks
    /// every row under the lock; add it `NOT VALID` and `VALIDATE
    /// CONSTRAINT` in a later migration (a much weaker lock).
    ValidatingConstraint,
    /// `ALTER COLUMN ... [SET DATA] TYPE` usually rewrites the table and
    /// every index on it.
    AlterType,
}

fn table_lock_hazards(sql: &str) -> Vec<(TableLockHazard, String)> {
    let created = created_tables(sql);
    let set_not_null = Regex::new(&format!(
        r"(?i)^ALTER\s+(?:COLUMN\s+)?{IDENT}\s+SET\s+NOT\s+NULL\b"
    ))
    .unwrap();
    let validating = Regex::new(&format!(
        r"(?i)^ADD\s+(?:CONSTRAINT\s+{IDENT}\s+)?(?:FOREIGN\s+KEY|CHECK)\b"
    ))
    .unwrap();
    let not_valid = Regex::new(r"(?i)\bNOT\s+VALID\s*$").unwrap();
    let alter_type = Regex::new(&format!(
        r"(?i)^ALTER\s+(?:COLUMN\s+)?{IDENT}\s+(?:SET\s+DATA\s+)?TYPE\b"
    ))
    .unwrap();

    let mut out = Vec::new();
    for (table, action) in alter_table_actions(sql) {
        if created.contains(&canonical_ident(&table)) {
            continue;
        }
        let hazard = if set_not_null.is_match(&action) {
            Some(TableLockHazard::SetNotNull)
        } else if validating.is_match(&action) && !not_valid.is_match(&action) {
            Some(TableLockHazard::ValidatingConstraint)
        } else if alter_type.is_match(&action) {
            Some(TableLockHazard::AlterType)
        } else {
            None
        };
        if let Some(hazard) = hazard {
            out.push((hazard, format!("ALTER TABLE {table} {action}")));
        }
    }
    out
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
         use and will not clean up itself. api's startup (`api::migrate`) drops INVALID \
         indexes before migrating, so the retry rebuilds it; keep the file to exactly one \
         statement so that retry is clean. That is the price of not locking the table, and it \
         is the right trade for any table with production-scale rows.",
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

    // A hard cap, so growing the list is a visible, deliberate edit of this
    // number rather than a quiet extra line (DB2-33). Lower it when an entry
    // is removed; never raise it.
    assert_eq!(
        GRANDFATHERED.len(),
        12,
        "GRANDFATHERED must only ever shrink; lower this cap when removing an entry"
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

#[test]
fn no_new_migration_scans_or_rewrites_an_existing_table_under_its_lock() {
    let grandfathered: BTreeSet<(&str, TableLockHazard)> =
        GRANDFATHERED_TABLE_LOCK_HAZARDS.iter().copied().collect();
    let mut offenders = Vec::new();
    for path in migration_files() {
        let name = file_name(&path);
        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
        for (hazard, statement) in table_lock_hazards(&normalized_sql(&raw)) {
            if !grandfathered.contains(&(name.as_str(), hazard)) {
                offenders.push(format!("{name}: {hazard:?}: {statement}"));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "these migrations scan or rewrite a table that already existed while holding ACCESS \
         EXCLUSIVE on it, blocking every read and write of that table -- and, because api \
         migrates before it binds, holding /public/health down -- for the whole scan:\n  {}\n\n\
         Instead: SET NOT NULL -> add `CHECK (col IS NOT NULL) NOT VALID`, `VALIDATE CONSTRAINT` \
         it in a later migration, then SET NOT NULL (no scan once a valid check exists). \
         FOREIGN KEY / CHECK -> add it `NOT VALID`, then `VALIDATE CONSTRAINT` in a later \
         migration (takes only SHARE UPDATE EXCLUSIVE). ALTER COLUMN TYPE -> add a new column, \
         backfill in batches, swap.",
        offenders.join("\n  ")
    );
}

#[test]
fn the_table_lock_hazard_grandfather_list_has_no_stale_entries() {
    assert_eq!(
        GRANDFATHERED_TABLE_LOCK_HAZARDS.len(),
        2,
        "GRANDFATHERED_TABLE_LOCK_HAZARDS must only ever shrink; lower this cap when removing an \
         entry"
    );
    let dir = migrations_dir();
    for (name, hazard) in GRANDFATHERED_TABLE_LOCK_HAZARDS {
        let raw = std::fs::read_to_string(dir.join(name))
            .unwrap_or_else(|err| panic!("grandfathered {name} is missing: {err}"));
        assert!(
            table_lock_hazards(&normalized_sql(&raw))
                .iter()
                .any(|(found, _)| found == hazard),
            "{name} no longer has {hazard:?}; delete its entry"
        );
    }
}

/// Postgres runs a multi-statement simple query as ONE implicit transaction
/// block, so a `-- no-transaction` file with a second statement next to its
/// `CREATE INDEX CONCURRENTLY` (a `DROP INDEX`, a `SET lock_timeout`) fails
/// at deploy with "cannot run inside a transaction block" -- even though
/// both tests above pass.
#[test]
fn a_no_transaction_migration_holds_exactly_one_statement() {
    let mut broken = Vec::new();
    for path in migration_files() {
        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
        if !raw.starts_with("-- no-transaction") {
            continue;
        }
        let statements = lex(&raw).statements;
        if statements.len() != 1 {
            broken.push(format!(
                "{} ({} statements)",
                file_name(&path),
                statements.len()
            ));
        }
    }
    assert!(
        broken.is_empty(),
        "these `-- no-transaction` migrations must hold exactly one statement; split them into \
         one file per statement: {}",
        broken.join(", ")
    );
}

// ---- The guard's own patterns, against sample SQL ----

fn index_hits(sql: &str) -> Vec<(String, String)> {
    blocking_index_builds(&normalized_sql(sql))
}

fn hazard_kinds(sql: &str) -> Vec<TableLockHazard> {
    table_lock_hazards(&normalized_sql(sql))
        .into_iter()
        .map(|(hazard, _)| hazard)
        .collect()
}

#[test]
fn guard_catches_named_unnamed_quoted_and_qualified_indexes() {
    for (sql, name, table) in [
        ("CREATE INDEX t_a ON t (a);", "t_a", "t"),
        (
            "CREATE UNIQUE INDEX IF NOT EXISTS t_a ON t (a);",
            "t_a",
            "t",
        ),
        ("CREATE INDEX ON t (a);", "<unnamed>", "t"),
        ("create index on only t using gin (a);", "<unnamed>", "t"),
        ("CREATE INDEX \"T a\" ON \"T\" (a);", "\"T a\"", "\"T\""),
        (
            "CREATE INDEX public.t_a ON public.t (a);",
            "public.t_a",
            "public.t",
        ),
        ("CREATE INDEX\n    t_a\n    ON t (a);", "t_a", "t"),
    ] {
        assert_eq!(
            index_hits(sql),
            vec![(name.to_string(), table.to_string())],
            "{sql}"
        );
    }
}

#[test]
fn guard_allows_concurrent_builds_and_indexes_on_new_tables() {
    for sql in [
        "CREATE INDEX CONCURRENTLY t_a ON t (a);",
        "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ON t (a);",
        "CREATE TABLE t (a int); CREATE INDEX t_a ON t (a);",
        "CREATE TABLE \"T\" (a int); CREATE INDEX ON public.t (a);",
        "CREATE TABLE t (a int); ALTER TABLE t ADD PRIMARY KEY (a);",
        "CREATE TABLE t (a int); ALTER TABLE t ALTER COLUMN a SET NOT NULL;",
    ] {
        assert!(index_hits(sql).is_empty(), "{sql}");
        assert!(hazard_kinds(sql).is_empty(), "{sql}");
    }
}

#[test]
fn guard_catches_constraints_that_build_an_index() {
    for (sql, name) in [
        (
            "ALTER TABLE t ADD PRIMARY KEY (a);",
            "<unnamed PRIMARY KEY>",
        ),
        ("ALTER TABLE t ADD CONSTRAINT t_pk PRIMARY KEY (a);", "t_pk"),
        ("ALTER TABLE t ADD CONSTRAINT t_u UNIQUE (a, b);", "t_u"),
        ("ALTER TABLE ONLY t ADD UNIQUE (a);", "<unnamed UNIQUE>"),
        (
            "ALTER TABLE t ADD CONSTRAINT t_x EXCLUDE USING gist (r WITH &&);",
            "t_x",
        ),
        (
            "ALTER TABLE t ADD COLUMN b int, ADD CONSTRAINT t_u UNIQUE (b);",
            "t_u",
        ),
    ] {
        assert_eq!(
            index_hits(sql),
            vec![(name.to_string(), "t".to_string())],
            "{sql}"
        );
    }
}

#[test]
fn guard_allows_adopting_an_existing_index() {
    for sql in [
        "ALTER TABLE t ADD CONSTRAINT t_pkey PRIMARY KEY USING INDEX t_new_key;",
        "ALTER TABLE t DROP CONSTRAINT t_pkey, ADD CONSTRAINT t_pkey PRIMARY KEY USING INDEX k;",
        "ALTER TABLE t ADD CONSTRAINT t_u UNIQUE USING INDEX t_u_idx;",
    ] {
        assert!(index_hits(sql).is_empty(), "{sql}");
    }
}

#[test]
fn guard_catches_scans_and_rewrites_under_access_exclusive() {
    use TableLockHazard::*;
    for (sql, expected) in [
        (
            "ALTER TABLE t ALTER COLUMN a SET NOT NULL;",
            vec![SetNotNull],
        ),
        ("ALTER TABLE t ALTER a SET NOT NULL;", vec![SetNotNull]),
        (
            "ALTER TABLE t ADD CONSTRAINT t_fk FOREIGN KEY (a) REFERENCES u (id);",
            vec![ValidatingConstraint],
        ),
        (
            "ALTER TABLE t ADD FOREIGN KEY (a) REFERENCES u (id) ON DELETE CASCADE;",
            vec![ValidatingConstraint],
        ),
        (
            "ALTER TABLE t ADD CONSTRAINT t_c CHECK (a > 0);",
            vec![ValidatingConstraint],
        ),
        ("ALTER TABLE t ALTER COLUMN a TYPE bigint;", vec![AlterType]),
        (
            "ALTER TABLE t ALTER COLUMN a SET DATA TYPE text USING a::text;",
            vec![AlterType],
        ),
        (
            "ALTER TABLE t ALTER a SET NOT NULL, ALTER b TYPE bigint;",
            vec![SetNotNull, AlterType],
        ),
    ] {
        assert_eq!(hazard_kinds(sql), expected, "{sql}");
    }
}

#[test]
fn guard_allows_the_safe_forms() {
    for sql in [
        "ALTER TABLE t ADD CONSTRAINT t_fk FOREIGN KEY (a) REFERENCES u (id) NOT VALID;",
        "ALTER TABLE t ADD CONSTRAINT t_c CHECK (a IS NOT NULL) NOT VALID;",
        "ALTER TABLE t VALIDATE CONSTRAINT t_c;",
        "ALTER TABLE t ALTER COLUMN a DROP NOT NULL;",
        "ALTER TABLE t ALTER COLUMN a SET DEFAULT 0;",
        "ALTER TABLE t ADD COLUMN b int NOT NULL DEFAULT 0;",
        "ALTER TABLE t SET (autovacuum_vacuum_scale_factor = 0.01);",
    ] {
        assert!(hazard_kinds(sql).is_empty(), "{sql}");
        assert!(index_hits(sql).is_empty(), "{sql}");
    }
}

#[test]
fn comments_never_count_and_quotes_are_not_comments() {
    for sql in [
        "/* CREATE INDEX t_a ON t (a); */ SELECT 1;",
        "/* outer /* nested */ CREATE INDEX t_a ON t (a); */ SELECT 1;",
        "-- CREATE INDEX t_a ON t (a);\nSELECT 1;",
        "/*\nALTER TABLE t ALTER a SET NOT NULL;\n*/",
    ] {
        assert!(index_hits(sql).is_empty(), "{sql}");
        assert!(hazard_kinds(sql).is_empty(), "{sql}");
    }
    // Code after a block comment on the same line still counts.
    assert_eq!(index_hits("/* why */ CREATE INDEX t_a ON t (a);").len(), 1);
    // `--` inside a string is not a comment, so the index after it counts.
    assert_eq!(
        index_hits("SELECT '--'; CREATE INDEX t_a ON t (a);").len(),
        1
    );
}

#[test]
fn statement_splitting_respects_quotes_dollar_quotes_and_comments() {
    let count = |sql: &str| lex(sql).statements.len();
    assert_eq!(
        count("-- no-transaction\nCREATE INDEX CONCURRENTLY a ON t (a);\n"),
        1
    );
    assert_eq!(
        count(
            "-- no-transaction\n-- header; with a semicolon\n/* and; here */\nCREATE INDEX CONCURRENTLY a ON t (a)"
        ),
        1
    );
    assert_eq!(
        count("-- no-transaction\nDROP INDEX a;\nCREATE INDEX CONCURRENTLY a ON t (a);"),
        2
    );
    assert_eq!(
        count("-- no-transaction\nSET lock_timeout = '5s'; CREATE INDEX CONCURRENTLY a ON t (a);"),
        2
    );
    assert_eq!(count("DO $$ BEGIN PERFORM 1; PERFORM 2; END $$;"), 1);
    assert_eq!(
        count("DO $body$ BEGIN RAISE NOTICE '$$;'; END $body$; SELECT 1;"),
        2
    );
    assert_eq!(count("SELECT 'a;b', \"c;d\", 'it''s;';"), 1);
    assert_eq!(count("SELECT $1; SELECT 2;"), 2);
}

/// The first migration that follows the convention; everything before it is
/// applied in production and can never be edited.
const LOCK_TIMEOUT_CONVENTION_FROM: &str = "20260927050000";

/// A3 / DB2-34: a transactional migration's first statement is
/// `SET LOCAL lock_timeout = ...`, so DDL that queues behind a long
/// transaction fails fast (and the crash-loop retry converges) instead of
/// waiting -- and blocking every other query on the table behind its lock
/// request -- until the startup probe kills the pod. api's migration
/// connection also sets a session `lock_timeout` (`api::migrate`), but a
/// hand-run `sqlx migrate run` does not.
#[test]
fn a_new_transactional_migration_starts_with_set_local_lock_timeout() {
    let first = Regex::new(r"(?i)^SET\s+LOCAL\s+lock_timeout\s*(?:=|TO)\s*").unwrap();
    let mut broken = Vec::new();
    for path in migration_files() {
        let name = file_name(&path);
        if name.as_str() < LOCK_TIMEOUT_CONVENTION_FROM {
            continue;
        }
        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
        if raw.starts_with("-- no-transaction") {
            continue;
        }
        let statements = lex(&raw).statements;
        if !statements.first().is_some_and(|s| first.is_match(s)) {
            broken.push(name);
        }
    }
    assert!(
        broken.is_empty(),
        "these transactional migrations must start with `SET LOCAL lock_timeout = '5s';`: {}",
        broken.join(", ")
    );
}
