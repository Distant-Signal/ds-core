//! Runs the embedded migrations (`crates/ds-store/migrations`) on a
//! dedicated connection rather than a request pool (DB review 2026-09-27:
//! A1/DB2-32, A3/DB2-34). Moved from `api::migrate` with the directory (plan
//! task 1B.1, decision D9); the api re-exports it as `api::migrate`.
//!
//! Two callers run [`run`], after [`ensure_ready_for_contract_migration`]:
//! the api at startup (while `api.migrateOnStartup` is on), and
//! `ds-migrate run`, the chart's migrate hook Job (`migrate.job`, whose
//! `activeDeadlineSeconds` is the same 900s budget as the probe below).
//!
//! # Budget: the startup probe
//!
//! The api's `main.rs` migrates BEFORE it binds, so `api.probes.startup` in
//! the chart (450 x 2s = 900s) is a hard ceiling on the whole migration run:
//! past it the kubelet SIGKILLs the pod mid-DDL. The dedicated connection
//! therefore carries:
//!
//! * `lock_timeout` (default 10s, `MIGRATION_LOCK_TIMEOUT_SECS`): DDL that
//!   cannot get its lock -- an `ALTER TABLE` queued behind a long reader, or
//!   a `CREATE INDEX CONCURRENTLY` waiting out an old transaction -- fails in
//!   10s with a clear error instead of queueing (and blocking every later
//!   reader/writer of the table behind its lock request) until the probe
//!   kills it. The pod restarts and retries; once the blocker is gone the
//!   retry converges. A transactional migration's own
//!   `SET LOCAL lock_timeout` still wins inside that migration.
//! * `statement_timeout` (default 240s, `MIGRATION_STATEMENT_TIMEOUT_SECS`):
//!   bounds each statement well below the 900s probe budget so a runaway
//!   statement fails with SQLSTATE 57014 in the log rather than a silent
//!   SIGKILL. 240s was first picked to fit under the old 300s budget; it
//!   stayed when the budget rose to 900s (2026-09-27) because one statement
//!   running 4+ minutes is itself worth failing loudly, and three of them
//!   still fit inside one probe budget. It is per statement, not per run:
//!   several long statements can still add up past 900s. A migration that genuinely needs longer must raise both this
//!   and the startup probe (or be run by hand with `sqlx migrate run`).
//!   NOT the request pool's 60s default -- that is why this is its own
//!   connection.
//! * `application_name` `distant-signal-api-migrations`, so the migration
//!   is identifiable in `pg_stat_activity`.
//!
//! # Healing a failed `CREATE INDEX CONCURRENTLY`
//!
//! A CIC that is killed or times out leaves an INVALID index behind, and
//! sqlx 0.8.6 records a migration only after it succeeds, so the retry runs
//! the same `CREATE INDEX CONCURRENTLY` again and fails with "relation
//! already exists" -- a permanent crash loop until someone drops the index by
//! hand. (`IF NOT EXISTS` would not help: it would silently keep the INVALID
//! index.) Before migrating, [`heal_invalid_indexes`] drops every INVALID
//! index in the app's own schema with `DROP INDEX CONCURRENTLY`, so the retry
//! rebuilds it cleanly.
//!
//! An index that is INVALID because a `CREATE INDEX [CONCURRENTLY]` or
//! `REINDEX` is building it RIGHT NOW (it has a `pg_stat_progress_create_index`
//! row) is skipped. Every migrator (each api replica, `ds-migrate`) takes
//! [`MIGRATION_LOCK_KEY`] first, so another run's in-flight migration is
//! never one of those; a build a human started in psql is left alone.
//!
//! # Which role migrates
//!
//! The migrations connect with `MIGRATION_DATABASE_URL` when it is set and
//! non-empty, else with `DATABASE_URL` ([`migration_url`]). With the role
//! split (docs/postgres-app-role.md) that is the schema owner role, which
//! may run DDL, while `DATABASE_URL` -- every pool -- is the non-superuser
//! app role with DML only. The owner needs `pg_read_all_stats` so the heal
//! above can see `pg_stat_progress_create_index` rows of other roles'
//! builds; without it such a row hides its `index_relid`/`relid` and a
//! human's in-flight build would look abandoned.

pub mod contract;
pub mod legacy_backfill;

use std::time::Duration;

use anyhow::{Context, Result};
use sqlx::postgres::PgConnectOptions;
use sqlx::{ConnectOptions, Connection, PgConnection};

pub use contract::ensure_ready_for_contract_migration;

/// `pg_stat_activity.application_name` of the migration connection.
pub const MIGRATION_APPLICATION_NAME: &str = "distant-signal-api-migrations";

/// Session advisory lock every migrator (each api replica, `ds-migrate`)
/// holds while it heals and migrates, so one run's heal can never drop the
/// index another run's in-flight `CREATE INDEX CONCURRENTLY` is building.
/// ASCII "dsmigrat"; distinct from sqlx's own migration lock and from the
/// publish locks in `ds_store::schedule`.
pub const MIGRATION_LOCK_KEY: i64 = 0x6473_6d69_6772_6174;

pub const DEFAULT_MIGRATION_LOCK_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_MIGRATION_STATEMENT_TIMEOUT: Duration = Duration::from_secs(240);

pub const MIGRATION_LOCK_TIMEOUT_ENV: &str = "MIGRATION_LOCK_TIMEOUT_SECS";
pub const MIGRATION_STATEMENT_TIMEOUT_ENV: &str = "MIGRATION_STATEMENT_TIMEOUT_SECS";

/// Environment variable naming the migration connection's URL.
pub const MIGRATION_DATABASE_URL_ENV: &str = "MIGRATION_DATABASE_URL";

/// Every migration in `crates/ds-store/migrations`, embedded. [`run`]
/// applies it; other crates' tests use it instead of the directory, which
/// stays in `ds-core` when they leave it (repo split spec §9, phase 4):
/// `#[sqlx::test(migrator = "ds_store::migrate::MIGRATOR")]`, and
/// [`migration_sql`] for one migration's text.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

/// The SQL of the embedded migration `version` (its file name's numeric
/// prefix), for tests that replay or inspect one migration.
///
/// # Panics
///
/// When no embedded migration has that version: a test's typo.
pub fn migration_sql(version: i64) -> &'static str {
    let migrations: &'static [sqlx::migrate::Migration] = &MIGRATOR.migrations;
    migrations
        .iter()
        .find(|m| m.version == version)
        .map_or_else(|| panic!("no embedded migration {version}"), |m| &*m.sql)
}

/// The URL the migrations connect with, and the variable it came from (for
/// error messages): `migration_database_url` when set and not blank, else
/// `database_url`. See the module docs, "Which role migrates".
pub fn migration_url<'a>(
    database_url: &'a str,
    migration_database_url: Option<&'a str>,
) -> (&'a str, &'static str) {
    match migration_database_url {
        Some(url) if !url.trim().is_empty() => (url, MIGRATION_DATABASE_URL_ENV),
        _ => (database_url, "DATABASE_URL"),
    }
}

/// Timeouts for the migration connection. See the module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MigrationSettings {
    pub lock_timeout: Duration,
    pub statement_timeout: Duration,
}

impl Default for MigrationSettings {
    fn default() -> Self {
        Self {
            lock_timeout: DEFAULT_MIGRATION_LOCK_TIMEOUT,
            statement_timeout: DEFAULT_MIGRATION_STATEMENT_TIMEOUT,
        }
    }
}

impl MigrationSettings {
    /// The defaults, overridden by `MIGRATION_LOCK_TIMEOUT_SECS` /
    /// `MIGRATION_STATEMENT_TIMEOUT_SECS` when set (whole seconds; 0
    /// disables). An unparsable value is an error.
    pub fn from_env() -> Result<Self> {
        let mut settings = Self::default();
        let secs = |name: &str| -> Result<Option<Duration>> {
            std::env::var(name)
                .ok()
                .map(|raw| {
                    raw.trim()
                        .parse::<u64>()
                        .map(Duration::from_secs)
                        .with_context(|| format!("{name}={raw:?} is not a whole number of seconds"))
                })
                .transpose()
        };
        if let Some(value) = secs(MIGRATION_LOCK_TIMEOUT_ENV)? {
            settings.lock_timeout = value;
        }
        if let Some(value) = secs(MIGRATION_STATEMENT_TIMEOUT_ENV)? {
            settings.statement_timeout = value;
        }
        Ok(settings)
    }
}

/// Opens the dedicated migration connection, takes [`MIGRATION_LOCK_KEY`],
/// heals INVALID indexes, runs every pending embedded migration, and closes.
pub async fn run(base: PgConnectOptions, settings: MigrationSettings) -> Result<()> {
    let mut conn = base
        .application_name(MIGRATION_APPLICATION_NAME)
        .connect()
        .await
        .context("could not open the migration connection")?;

    // Waiting for another replica's migration run is expected and bounded by
    // the startup probe, so the lock wait itself is exempt from lock_timeout.
    sqlx::query("SET lock_timeout = 0")
        .execute(&mut conn)
        .await?;
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(MIGRATION_LOCK_KEY)
        .execute(&mut conn)
        .await
        .context("could not take the migration advisory lock")?;
    // `SET` takes no bind parameters; these are our own integers (ms).
    sqlx::query(&format!(
        "SET lock_timeout = {}",
        settings.lock_timeout.as_millis()
    ))
    .execute(&mut conn)
    .await?;
    sqlx::query(&format!(
        "SET statement_timeout = {}",
        settings.statement_timeout.as_millis()
    ))
    .execute(&mut conn)
    .await?;

    let result = migrate_locked(&mut conn).await;

    // Closing the session releases the advisory lock anyway; unlock
    // explicitly so a failure to close cleanly doesn't matter.
    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(MIGRATION_LOCK_KEY)
        .execute(&mut conn)
        .await;
    let _ = conn.close().await;
    result
}

async fn migrate_locked(conn: &mut PgConnection) -> Result<()> {
    match heal_invalid_indexes(conn).await {
        Ok(dropped) if !dropped.is_empty() => tracing::warn!(
            ?dropped,
            "dropped INVALID indexes left by a failed CREATE INDEX CONCURRENTLY before \
             migrating; the migration that builds them will now run again"
        ),
        Ok(_) => {}
        // Not fatal on its own: if a pending migration needs the name it
        // fails next with its own, clearer error; if not, nothing is lost.
        Err(err) => tracing::error!(
            error = ?err,
            "could not drop INVALID indexes before migrating; continuing"
        ),
    }
    MIGRATOR
        .run(&mut *conn)
        .await
        .context("running database migrations")?;
    Ok(())
}

/// Drops, with `DROP INDEX CONCURRENTLY`, every INVALID plain index in the
/// schemas on the connection's `search_path` that is not being built right
/// now. Returns the dropped indexes' qualified names. Must run outside a
/// transaction (as `DROP INDEX CONCURRENTLY` itself must).
pub async fn heal_invalid_indexes(conn: &mut PgConnection) -> Result<Vec<String>> {
    // relkind 'i' only: a partitioned index ('I') can be INVALID merely
    // because a partition lacks its index, and cannot be dropped
    // CONCURRENTLY anyway.
    let invalid: Vec<(String, bool)> = sqlx::query_as(
        "SELECT format('%I.%I', n.nspname, c.relname), \
                EXISTS (SELECT 1 FROM pg_stat_progress_create_index p \
                        WHERE p.index_relid = c.oid OR (p.index_relid = 0 AND p.relid = i.indrelid)) \
         FROM pg_index i \
         JOIN pg_class c ON c.oid = i.indexrelid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE NOT i.indisvalid \
           AND c.relkind = 'i' \
           AND n.nspname = ANY (current_schemas(false)) \
         ORDER BY 1",
    )
    .fetch_all(&mut *conn)
    .await
    .context("listing INVALID indexes")?;

    let mut dropped = Vec::new();
    for (name, in_progress) in invalid {
        if in_progress {
            tracing::warn!(
                index = %name,
                "INVALID index is being built right now by another session; leaving it alone"
            );
            continue;
        }
        tracing::warn!(index = %name, "dropping INVALID index");
        // `name` is `format('%I.%I')`-quoted by Postgres itself.
        sqlx::query(&format!("DROP INDEX CONCURRENTLY IF EXISTS {name}"))
            .execute(&mut *conn)
            .await
            .with_context(|| format!("DROP INDEX CONCURRENTLY {name}"))?;
        dropped.push(name);
    }
    Ok(dropped)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_url_falls_back_to_database_url() {
        let app = "postgres://app@db/ds";
        let owner = "postgres://owner@db/ds";
        assert_eq!(migration_url(app, None), (app, "DATABASE_URL"));
        assert_eq!(migration_url(app, Some("")), (app, "DATABASE_URL"));
        assert_eq!(migration_url(app, Some("  ")), (app, "DATABASE_URL"));
        assert_eq!(
            migration_url(app, Some(owner)),
            (owner, MIGRATION_DATABASE_URL_ENV)
        );
    }

    #[test]
    fn defaults_fit_inside_the_startup_probe_budget() {
        // api.probes.startup: 450 x 2s = 900s.
        let settings = MigrationSettings::default();
        assert!(settings.statement_timeout < Duration::from_secs(900));
        // Kept at 240s (below the old 300s budget) by decision.
        assert_eq!(settings.statement_timeout, Duration::from_secs(240));
        assert!(settings.lock_timeout < settings.statement_timeout);
    }

    /// `MIGRATION_DATABASE_URL` when set and not blank, else
    /// `DATABASE_URL`: the schema owner with the role split.
    fn owner_database_url() -> String {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let migration_database_url = std::env::var(MIGRATION_DATABASE_URL_ENV).ok();
        migration_url(&database_url, migration_database_url.as_deref())
            .0
            .to_owned()
    }

    /// As the schema owner (`MIGRATION_DATABASE_URL`, else `DATABASE_URL`):
    /// these tests create tables, drop indexes and run the migrator.
    async fn connect() -> PgConnection {
        let url = owner_database_url();
        PgConnection::connect(&url).await.expect("connect")
    }

    /// A deliberately INVALID index (a unique CIC that fails on duplicate
    /// rows -- exactly what a killed/failed migration leaves) is dropped, a
    /// valid one on the same table is kept, and re-running the CIC then
    /// succeeds.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                migrate::tests -- --ignored --test-threads=1`"]
    async fn heal_drops_an_invalid_index_and_keeps_valid_ones() {
        let mut conn = connect().await;
        for sql in [
            "DROP TABLE IF EXISTS heal_invalid_index_test",
            "CREATE TABLE heal_invalid_index_test (id int, v int)",
            "INSERT INTO heal_invalid_index_test VALUES (1, 1), (2, 1)",
            "CREATE INDEX heal_invalid_index_test_ok ON heal_invalid_index_test (id)",
        ] {
            sqlx::query(sql).execute(&mut conn).await.expect(sql);
        }
        sqlx::query(
            "CREATE UNIQUE INDEX CONCURRENTLY heal_invalid_index_test_v \
             ON heal_invalid_index_test (v)",
        )
        .execute(&mut conn)
        .await
        .expect_err("duplicate values must fail the unique build");

        let valid = |name: &'static str| {
            sqlx::query_scalar::<_, bool>(
                "SELECT i.indisvalid FROM pg_index i \
                 JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = $1",
            )
            .bind(name)
        };
        assert_eq!(
            valid("heal_invalid_index_test_v")
                .fetch_optional(&mut conn)
                .await
                .unwrap(),
            Some(false),
            "the failed CIC must have left an INVALID index behind"
        );

        let dropped = heal_invalid_indexes(&mut conn).await.expect("heal");
        assert!(
            dropped
                .iter()
                .any(|name| name.ends_with(".heal_invalid_index_test_v")),
            "{dropped:?}"
        );
        assert!(
            valid("heal_invalid_index_test_v")
                .fetch_optional(&mut conn)
                .await
                .unwrap()
                .is_none(),
            "the INVALID index must be gone"
        );
        assert_eq!(
            valid("heal_invalid_index_test_ok")
                .fetch_optional(&mut conn)
                .await
                .unwrap(),
            Some(true),
            "a valid index must be kept"
        );

        // The "retry" now succeeds instead of "relation already exists".
        sqlx::query("DELETE FROM heal_invalid_index_test WHERE id = 2")
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query(
            "CREATE UNIQUE INDEX CONCURRENTLY heal_invalid_index_test_v \
             ON heal_invalid_index_test (v)",
        )
        .execute(&mut conn)
        .await
        .expect("the retried CIC must succeed");
        assert!(
            heal_invalid_indexes(&mut conn)
                .await
                .unwrap()
                .iter()
                .all(|name| !name.contains("heal_invalid_index_test")),
            "nothing left to heal"
        );

        sqlx::query("DROP TABLE heal_invalid_index_test")
            .execute(&mut conn)
            .await
            .unwrap();
    }

    /// Account continuity (20261104090200/090300): every foreign key to
    /// `users(id)` is validated and ON UPDATE CASCADE, so re-keying a user
    /// (`UPDATE users SET id = <new sub>`, the api's login callback) carries
    /// their rows along. A new foreign key to `users(id)` without
    /// `ON UPDATE CASCADE` fails here: with NO ACTION the re-key would fail
    /// for every user with a row in that table.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                migrate::tests -- --ignored --test-threads=1`"]
    async fn every_users_foreign_key_cascades_updates() {
        let mut conn = connect().await;
        let rows: Vec<(String, String, bool)> = sqlx::query_as(
            "SELECT conrelid::regclass::text || '.' || conname, confupdtype::text, convalidated \
             FROM pg_constraint \
             WHERE contype = 'f' AND confrelid = 'public.users'::regclass \
             ORDER BY 1",
        )
        .fetch_all(&mut conn)
        .await
        .unwrap();
        assert!(rows.len() >= 29, "found only {} foreign keys", rows.len());
        let wrong: Vec<&String> = rows
            .iter()
            .filter(|(_, on_update, validated)| on_update != "c" || !validated)
            .map(|(name, _, _)| name)
            .collect();
        assert!(
            wrong.is_empty(),
            "these foreign keys to users(id) must be validated and ON UPDATE CASCADE: {wrong:?}"
        );

        // And the cascade really runs, and the partial unique index holds
        // (in a transaction that is rolled back).
        let mut tx = conn.begin().await.unwrap();
        for sql in [
            "INSERT INTO users (id, discord_id) VALUES ('u-rekey-old', 'd-rekey')",
            "INSERT INTO sessions (id, user_id, expires_at) \
             VALUES ('s-rekey', 'u-rekey-old', now() + interval '1 day')",
            "INSERT INTO pinned_lines (user_id, line_id) VALUES ('u-rekey-old', 'wcml')",
            "UPDATE users SET id = 'u-rekey-new' WHERE id = 'u-rekey-old'",
        ] {
            sqlx::query(sql).execute(&mut *tx).await.unwrap();
        }
        let moved: (String, String) = sqlx::query_as(
            "SELECT (SELECT user_id FROM sessions WHERE id = 's-rekey'), \
                    (SELECT user_id FROM pinned_lines WHERE line_id = 'wcml' \
                       AND user_id LIKE 'u-rekey-%')",
        )
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        assert_eq!(moved, ("u-rekey-new".to_owned(), "u-rekey-new".to_owned()));
        let duplicate =
            sqlx::query("INSERT INTO users (id, discord_id) VALUES ('u-rekey-2', 'd-rekey')")
                .execute(&mut *tx)
                .await
                .expect_err("two users with one discord_id");
        assert_eq!(
            duplicate
                .as_database_error()
                .and_then(sqlx::error::DatabaseError::constraint),
            Some("users_discord_id_key")
        );
        tx.rollback().await.unwrap();
    }

    /// One `journey_templates` row's target columns: origin, destination,
    /// kind, time, day offset, plan options, usual itinerary.
    type TargetRow = (
        Option<&'static str>,
        Option<&'static str>,
        Option<&'static str>,
        Option<&'static str>,
        i16,
        Option<&'static str>,
        Option<&'static str>,
    );

    async fn insert_target(conn: &mut PgConnection, row: TargetRow) -> sqlx::Result<i64> {
        let (origin, destination, kind, time, offset, options, usual) = row;
        let (id,): (i64,) = sqlx::query_as(
            "INSERT INTO journey_templates \
             (user_id, days_of_week, origin_crs, destination_crs, target_kind, target_time, \
              target_day_offset, plan_options, usual_itinerary) \
             VALUES ('u-regular', 31, $1, $2, $3, $4::time, $5, $6::jsonb, $7::jsonb) \
             RETURNING id",
        )
        .bind(origin)
        .bind(destination)
        .bind(kind)
        .bind(time)
        .bind(offset)
        .bind(options)
        .bind(usual)
        .fetch_one(conn)
        .await?;
        Ok(id)
    }

    async fn add_regular_user(conn: &mut PgConnection) {
        sqlx::query("INSERT INTO users (id) VALUES ('u-regular')")
            .execute(conn)
            .await
            .unwrap();
    }

    fn constraint_of(error: &sqlx::Error) -> Option<&str> {
        error
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::constraint)
    }

    /// Regular journeys (20261106130000): the three constraints added NOT
    /// VALID are validated by the next migration.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                migrate::tests -- --ignored --test-threads=1`"]
    async fn regular_journey_constraints_are_validated() {
        let mut conn = connect().await;
        let validated: Vec<(String, bool)> = sqlx::query_as(
            "SELECT conname::text, convalidated FROM pg_constraint \
             WHERE conrelid = 'public.journey_templates'::regclass \
               AND conname IN ('journey_templates_target_check', \
                               'journey_templates_paired_fkey', \
                               'journey_templates_paired_not_self') \
             ORDER BY 1",
        )
        .fetch_all(&mut conn)
        .await
        .unwrap();
        assert_eq!(
            validated,
            vec![
                ("journey_templates_paired_fkey".to_owned(), true),
                ("journey_templates_paired_not_self".to_owned(), true),
                ("journey_templates_target_check".to_owned(), true),
            ]
        );
    }

    /// The target check accepts a full target and an old-style template
    /// without one, and rejects a partial target, a bad station code, a day
    /// offset of 2 or a non-object itinerary. In a transaction that is
    /// rolled back.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                migrate::tests -- --ignored --test-threads=1`"]
    async fn regular_journey_targets_are_checked() {
        let mut conn = connect().await;
        let mut tx = conn.begin().await.unwrap();
        add_regular_user(&mut tx).await;
        let accepted: [TargetRow; 3] = [
            (None, None, None, None, 0, None, None),
            (
                Some("WOK"),
                Some("WAT"),
                Some("depart_after"),
                Some("07:45"),
                0,
                Some(r#"{"maxChanges": 1}"#),
                Some(r#"{"legs": []}"#),
            ),
            (
                Some("WAT"),
                Some("WOK"),
                Some("arrive_by"),
                Some("00:30"),
                1,
                None,
                None,
            ),
        ];
        for row in accepted {
            insert_target(&mut tx, row)
                .await
                .unwrap_or_else(|e| panic!("{row:?}: {e}"));
        }
        let rejected: [TargetRow; 8] = [
            // A target with no time.
            (
                Some("WOK"),
                Some("WAT"),
                Some("depart_after"),
                None,
                0,
                None,
                None,
            ),
            // A time with no target.
            (None, None, None, Some("07:45"), 0, None, None),
            // A target with no origin or destination.
            (
                None,
                Some("WAT"),
                Some("depart_after"),
                Some("07:45"),
                0,
                None,
                None,
            ),
            (
                Some("WOK"),
                None,
                Some("arrive_by"),
                Some("07:45"),
                0,
                None,
                None,
            ),
            // A usual itinerary with no target.
            (None, None, None, None, 0, None, Some("{}")),
            (
                Some("wok"),
                Some("WAT"),
                Some("arrive_by"),
                Some("07:45"),
                0,
                None,
                None,
            ),
            (
                Some("WOK"),
                Some("WAT"),
                Some("arrive_by"),
                Some("07:45"),
                2,
                None,
                None,
            ),
            (
                Some("WOK"),
                Some("WAT"),
                Some("arrive_by"),
                Some("07:45"),
                0,
                None,
                Some("[]"),
            ),
        ];
        for row in rejected {
            let mut savepoint = tx.begin().await.unwrap();
            let error = insert_target(&mut savepoint, row)
                .await
                .expect_err("rejected");
            assert_eq!(
                constraint_of(&error),
                Some("journey_templates_target_check"),
                "{row:?}"
            );
            savepoint.rollback().await.unwrap();
        }
        tx.rollback().await.unwrap();
    }

    /// A pair can't point at itself, and deleting one side unlinks the
    /// other (20261106130000). In a transaction that is rolled back.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                migrate::tests -- --ignored --test-threads=1`"]
    async fn regular_journey_pairs_unlink_on_delete() {
        let mut conn = connect().await;
        let mut tx = conn.begin().await.unwrap();
        add_regular_user(&mut tx).await;
        let outbound = (
            Some("WOK"),
            Some("WAT"),
            Some("depart_after"),
            Some("07:45"),
            0,
            None,
            None,
        );
        let back = (
            Some("WAT"),
            Some("WOK"),
            Some("depart_after"),
            Some("17:30"),
            0,
            None,
            None,
        );
        let outbound = insert_target(&mut tx, outbound).await.unwrap();
        let back = insert_target(&mut tx, back).await.unwrap();

        let mut savepoint = tx.begin().await.unwrap();
        let error =
            sqlx::query("UPDATE journey_templates SET paired_template_id = id WHERE id = $1")
                .bind(outbound)
                .execute(&mut *savepoint)
                .await
                .expect_err("paired with itself");
        assert_eq!(
            constraint_of(&error),
            Some("journey_templates_paired_not_self")
        );
        savepoint.rollback().await.unwrap();

        for (id, other) in [(outbound, back), (back, outbound)] {
            sqlx::query("UPDATE journey_templates SET paired_template_id = $2 WHERE id = $1")
                .bind(id)
                .bind(other)
                .execute(&mut *tx)
                .await
                .unwrap();
        }
        sqlx::query("DELETE FROM journey_templates WHERE id = $1")
            .bind(back)
            .execute(&mut *tx)
            .await
            .unwrap();
        let (paired,): (Option<i64>,) =
            sqlx::query_as("SELECT paired_template_id FROM journey_templates WHERE id = $1")
                .bind(outbound)
                .fetch_one(&mut *tx)
                .await
                .unwrap();
        assert_eq!(paired, None);
        tx.rollback().await.unwrap();
    }

    /// Regular plan (20261106184000): the status and plan check is
    /// validated, accepts each status and an object plan, and rejects an
    /// unknown status or a non-object plan. In a transaction that is rolled
    /// back.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                migrate::tests -- --ignored --test-threads=1`"]
    async fn regular_plan_is_checked() {
        let mut conn = connect().await;
        let (validated,): (bool,) = sqlx::query_as(
            "SELECT convalidated FROM pg_constraint \
             WHERE conrelid = 'public.journeys'::regclass \
               AND conname = 'journeys_regular_plan_check'",
        )
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert!(validated);
        let mut tx = conn.begin().await.unwrap();
        add_regular_user(&mut tx).await;
        let (journey,): (i64,) =
            sqlx::query_as("INSERT INTO journeys (user_id) VALUES ('u-regular') RETURNING id")
                .fetch_one(&mut *tx)
                .await
                .unwrap();
        let set = "UPDATE journeys SET regular_plan_status = $2, regular_plan = $3::jsonb, \
                   regular_plan_marker = 'm1', regular_planned_at = now() WHERE id = $1";
        for (status, plan) in [
            (None, None),
            (Some("pending"), None),
            (
                Some("suggested"),
                Some(r#"{"choice": null, "alternatives": []}"#),
            ),
            (Some("committed"), Some(r#"{"sweepLegIds": [1]}"#)),
            (Some("no_service"), None),
        ] {
            sqlx::query(set)
                .bind(journey)
                .bind(status)
                .bind(plan)
                .execute(&mut *tx)
                .await
                .unwrap_or_else(|e| panic!("{status:?} {plan:?}: {e}"));
        }
        for (status, plan) in [(Some("chosen"), None), (Some("suggested"), Some("[]"))] {
            let mut savepoint = tx.begin().await.unwrap();
            let error = sqlx::query(set)
                .bind(journey)
                .bind(status)
                .bind(plan)
                .execute(&mut *savepoint)
                .await
                .expect_err("rejected");
            assert_eq!(
                constraint_of(&error),
                Some("journeys_regular_plan_check"),
                "{status:?} {plan:?}"
            );
            savepoint.rollback().await.unwrap();
        }
        tx.rollback().await.unwrap();
    }

    /// Leg outcomes (20261106200000): the check is validated, accepts a
    /// recorded outcome (known or not) and nothing at all, and rejects an
    /// outcome with no `final_recorded_at`. In a transaction that is rolled
    /// back.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                migrate::tests -- --ignored --test-threads=1`"]
    async fn leg_final_arrival_is_checked() {
        let mut conn = connect().await;
        let (validated,): (bool,) = sqlx::query_as(
            "SELECT convalidated FROM pg_constraint \
             WHERE conrelid = 'public.journey_legs'::regclass \
               AND conname = 'journey_legs_final_arrival_check'",
        )
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert!(validated);
        let mut tx = conn.begin().await.unwrap();
        add_regular_user(&mut tx).await;
        let (leg,): (i64,) = sqlx::query_as(
            "WITH j AS (INSERT INTO journeys (user_id) VALUES ('u-regular') RETURNING id) \
             INSERT INTO journey_legs (journey_id, leg_order, service_date) \
             SELECT id, 1, CURRENT_DATE FROM j RETURNING id",
        )
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        let set = "UPDATE journey_legs SET final_arrival_delay_minutes = $2, \
                   final_train_cancelled = $3, \
                   final_recorded_at = CASE WHEN $4 THEN now() END WHERE id = $1";
        for (delay, cancelled, recorded) in [
            (None, None, false),
            (Some(16), Some(false), true),
            (None, Some(true), true),
            (None, None, true),
        ] {
            sqlx::query(set)
                .bind(leg)
                .bind(delay)
                .bind(cancelled)
                .bind(recorded)
                .execute(&mut *tx)
                .await
                .unwrap_or_else(|e| panic!("{delay:?} {cancelled:?} {recorded}: {e}"));
        }
        for (delay, cancelled) in [(Some(3), None), (None, Some(false))] {
            let mut savepoint = tx.begin().await.unwrap();
            let error = sqlx::query(set)
                .bind(leg)
                .bind(delay)
                .bind(cancelled)
                .bind(false)
                .execute(&mut *savepoint)
                .await
                .expect_err("rejected");
            assert_eq!(
                constraint_of(&error),
                Some("journey_legs_final_arrival_check"),
                "{delay:?} {cancelled:?}"
            );
            savepoint.rollback().await.unwrap();
        }
        tx.rollback().await.unwrap();
    }

    /// The whole startup path against an already-migrated database: takes the
    /// lock, heals, finds nothing pending, releases the lock.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                migrate::tests -- --ignored --test-threads=1`"]
    async fn run_is_a_no_op_on_a_migrated_database_and_releases_its_lock() {
        let url = owner_database_url();
        let base: PgConnectOptions = url.parse().unwrap();
        run(base, MigrationSettings::default())
            .await
            .expect("migrations already applied");
        let mut conn = connect().await;
        let free: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(MIGRATION_LOCK_KEY)
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert!(free, "run() must release the migration lock");
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(MIGRATION_LOCK_KEY)
            .execute(&mut conn)
            .await
            .unwrap();
    }

    /// With the role split (docs/postgres-app-role.md; CI's per-service run,
    /// `scripts/test-postgres-roles.py --mode per-service`), proves the
    /// suite really ran as a service's own DML-only role and the migrations
    /// as a separate owner: the `DATABASE_URL` role is no superuser and may
    /// not create, alter or truncate anything, while the
    /// `MIGRATION_DATABASE_URL` role owns schema public and every table in
    /// it. Passes trivially (checks nothing) when `DATABASE_URL` is a
    /// superuser (the ordinary single-role run) or is the owner itself (the
    /// role-split passes that run the suites as the owner, since the shared
    /// app role was dropped in release E).
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=<service role> \
                MIGRATION_DATABASE_URL=<owner> cargo test -p ds-store migrate::tests -- --ignored \
                --test-threads=1`"]
    async fn a_service_role_has_dml_only_and_the_owner_owns_the_schema() {
        let app_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let mut app = PgConnection::connect(&app_url)
            .await
            .expect("connect as the service role");
        let app_is_superuser: bool =
            sqlx::query_scalar("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")
                .fetch_one(&mut app)
                .await
                .unwrap();
        if app_is_superuser {
            eprintln!("DATABASE_URL is a superuser: not a role-split run, nothing to check");
            return;
        }
        let owner_name: String = {
            let mut owner = connect().await;
            sqlx::query_scalar("SELECT current_user::text")
                .fetch_one(&mut owner)
                .await
                .unwrap()
        };
        let app_name: String = sqlx::query_scalar("SELECT current_user::text")
            .fetch_one(&mut app)
            .await
            .unwrap();
        if owner_name == app_name {
            eprintln!("DATABASE_URL is the owner: not a service-role run, nothing to check");
            return;
        }

        for ddl in [
            "CREATE TABLE app_role_must_not_create (id int)",
            "CREATE UNLOGGED TABLE app_role_must_not_create (id int)",
            "ALTER TABLE users ADD COLUMN app_role_must_not_add int",
            "TRUNCATE corpus_deliveries",
            "DROP INDEX IF EXISTS schedule_destination_departures_publish_keys_probe",
            "CREATE INDEX app_role_must_not_index ON users (id)",
            "INSERT INTO _sqlx_migrations (version, description, success, checksum, \
             execution_time) VALUES (-1, 'app', true, '\\x00', 0)",
        ] {
            let err = sqlx::query(ddl)
                .execute(&mut app)
                .await
                .expect_err(&format!("the service role must not be able to run: {ddl}"));
            assert_eq!(
                err.as_database_error()
                    .and_then(sqlx::error::DatabaseError::code)
                    .as_deref(),
                Some("42501"),
                "{ddl}: expected insufficient_privilege, got {err}"
            );
        }
        let (schema_owner, foreign_tables): (String, Vec<String>) = sqlx::query_as(
            "SELECT (SELECT nspowner::regrole::text FROM pg_namespace WHERE nspname = 'public'), \
                    COALESCE((SELECT array_agg(c.relname::text ORDER BY c.relname) \
                              FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                              WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'S') \
                                AND pg_get_userbyid(c.relowner) <> $1 \
                                AND NOT EXISTS (SELECT 1 FROM pg_depend d \
                                                WHERE d.objid = c.oid AND d.deptype = 'e')), \
                             '{}')",
        )
        .bind(&owner_name)
        .fetch_one(&mut app)
        .await
        .unwrap();
        assert_eq!(schema_owner, owner_name, "schema public's owner");
        assert!(
            foreign_tables.is_empty(),
            "tables/sequences in public not owned by {owner_name}: {foreign_tables:?}"
        );
    }
}
