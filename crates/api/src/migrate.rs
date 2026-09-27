//! Runs the embedded migrations at `api` startup, on a dedicated connection
//! rather than the request pool (DB review 2026-09-27: A1/DB2-32, A3/DB2-34).
//!
//! # Budget: the startup probe
//!
//! `main.rs` migrates BEFORE it binds, so `api.probes.startup` in the chart
//! (150 x 2s = 300s) is a hard ceiling on the whole migration run: past it
//! the kubelet SIGKILLs the pod mid-DDL. The dedicated connection therefore
//! carries:
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
//!   bounds each statement below the 300s probe budget so a runaway statement
//!   fails with SQLSTATE 57014 in the log rather than a silent SIGKILL. It is
//!   per statement, not per run: several long statements can still add up
//!   past 300s. A migration that genuinely needs longer must raise both this
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
//! row) is skipped. Every api replica takes [`MIGRATION_LOCK_KEY`] first, so
//! another replica's in-flight migration is never one of those; a build a
//! human started in psql is left alone.

use std::time::Duration;

use anyhow::{Context, Result};
use sqlx::postgres::PgConnectOptions;
use sqlx::{ConnectOptions, Connection, PgConnection};

/// `pg_stat_activity.application_name` of the migration connection.
pub const MIGRATION_APPLICATION_NAME: &str = "distant-signal-api-migrations";

/// Session advisory lock every api replica holds while it heals and
/// migrates, so one replica's heal can never drop the index another replica's
/// in-flight `CREATE INDEX CONCURRENTLY` is building. ASCII "dsmigrat";
/// distinct from sqlx's own migration lock and from the publish locks in
/// `data::queries`.
pub const MIGRATION_LOCK_KEY: i64 = 0x6473_6d69_6772_6174;

pub const DEFAULT_MIGRATION_LOCK_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_MIGRATION_STATEMENT_TIMEOUT: Duration = Duration::from_secs(240);

pub const MIGRATION_LOCK_TIMEOUT_ENV: &str = "MIGRATION_LOCK_TIMEOUT_SECS";
pub const MIGRATION_STATEMENT_TIMEOUT_ENV: &str = "MIGRATION_STATEMENT_TIMEOUT_SECS";

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
    sqlx::migrate!()
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
    fn defaults_fit_inside_the_startup_probe_budget() {
        // api.probes.startup: 150 x 2s = 300s.
        let settings = MigrationSettings::default();
        assert!(settings.statement_timeout < Duration::from_secs(300));
        assert!(settings.lock_timeout < settings.statement_timeout);
    }

    async fn connect() -> PgConnection {
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        PgConnection::connect(&url).await.expect("connect")
    }

    /// A deliberately INVALID index (a unique CIC that fails on duplicate
    /// rows -- exactly what a killed/failed migration leaves) is dropped, a
    /// valid one on the same table is kept, and re-running the CIC then
    /// succeeds.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
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

    /// The whole startup path against an already-migrated database: takes the
    /// lock, heals, finds nothing pending, releases the lock.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                migrate::tests -- --ignored --test-threads=1`"]
    async fn run_is_a_no_op_on_a_migrated_database_and_releases_its_lock() {
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
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
}
