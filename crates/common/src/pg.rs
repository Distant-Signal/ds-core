//! Postgres pool settings shared by every crate that holds a pool (`api`,
//! `aggregator`, `notifier`, `enricher`). Behind the `postgres` feature so
//! the pollers and consumers, which never talk to Postgres, don't build sqlx.
//!
//! # Why (DB review 2026-09-27: F6, F7, DB2-1, INF-7)
//!
//! Production Postgres runs with `statement_timeout`,
//! `idle_in_transaction_session_timeout` and `lock_timeout` all at 0, and
//! every pool used sqlx's 30s `acquire_timeout` with a blank
//! `application_name`. On 2026-09-27 that let 8 orphaned 19-minute DELETEs
//! from a dead api pod pin ~5 cores, and nothing in `pg_stat_activity` said
//! which service a backend belonged to.
//!
//! Every pooled connection now carries, in the startup packet (so they apply
//! from the first statement, even before any `after_connect` could run):
//!
//! * `application_name` -- one per crate (`distant-signal-api`, ...).
//! * `statement_timeout` -- default 60s. A statement that is MEANT to run
//!   longer raises it for its own transaction with
//!   [`set_local_statement_timeout`] (`SET LOCAL`, gone at commit/rollback):
//!   the schedule publish chunks, the retention prunes and the archive
//!   batches do.
//! * `idle_in_transaction_session_timeout` -- default 30s. Ends a session
//!   that opened a transaction and then stopped talking (a panicked task, a
//!   hung await), which would otherwise hold its locks and pin the xmin
//!   horizon indefinitely.
//!
//! and the pool fails an `acquire` after 5s (was 30s), so an overloaded pool
//! surfaces as a fast error rather than a queue of requests each waiting half
//! a minute.
//!
//! # Configuration
//!
//! Environment variables, read by [`PoolSettings::from_env`]; the chart sets
//! them from `databasePool` in `values.yaml`. `0` disables a timeout.
//!
//! | variable | default |
//! |---|---|
//! | `DATABASE_STATEMENT_TIMEOUT_SECS` | 60 |
//! | `DATABASE_IDLE_IN_TRANSACTION_TIMEOUT_SECS` | 30 |
//! | `DATABASE_ACQUIRE_TIMEOUT_SECS` | 5 |
//! | `DATABASE_MAX_CONNECTIONS` | per crate (api 50, aggregator 10, notifier 5, enricher 5) |

use std::time::Duration;

use anyhow::{Context, Result};
use sqlx::PgConnection;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};

/// Default server-side `statement_timeout` for pooled connections.
pub const DEFAULT_STATEMENT_TIMEOUT: Duration = Duration::from_secs(60);
/// Default `idle_in_transaction_session_timeout` for pooled connections.
pub const DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(30);
/// Default pool `acquire_timeout`.
pub const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);

pub const STATEMENT_TIMEOUT_ENV: &str = "DATABASE_STATEMENT_TIMEOUT_SECS";
pub const IDLE_IN_TRANSACTION_TIMEOUT_ENV: &str = "DATABASE_IDLE_IN_TRANSACTION_TIMEOUT_SECS";
pub const ACQUIRE_TIMEOUT_ENV: &str = "DATABASE_ACQUIRE_TIMEOUT_SECS";
pub const MAX_CONNECTIONS_ENV: &str = "DATABASE_MAX_CONNECTIONS";

/// Everything a crate's pool is built from. See the module docs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolSettings {
    /// Reported in `pg_stat_activity.application_name`.
    pub application_name: String,
    pub max_connections: u32,
    /// `Duration::ZERO` leaves the server's own setting alone.
    pub statement_timeout: Duration,
    /// `Duration::ZERO` leaves the server's own setting alone.
    pub idle_in_transaction_timeout: Duration,
    pub acquire_timeout: Duration,
}

impl PoolSettings {
    /// The defaults, without reading the environment.
    pub fn new(application_name: &str, max_connections: u32) -> Self {
        Self {
            application_name: application_name.to_owned(),
            max_connections,
            statement_timeout: DEFAULT_STATEMENT_TIMEOUT,
            idle_in_transaction_timeout: DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT,
            acquire_timeout: DEFAULT_ACQUIRE_TIMEOUT,
        }
    }

    /// The defaults, overridden by any of the `DATABASE_*` variables in the
    /// module docs that are set. A set but unparsable value is an error, so
    /// a typo fails the deploy instead of silently running with the default.
    pub fn from_env(application_name: &str, default_max_connections: u32) -> Result<Self> {
        Self::from_lookup(application_name, default_max_connections, |name| {
            std::env::var(name).ok()
        })
    }

    fn from_lookup(
        application_name: &str,
        default_max_connections: u32,
        lookup: impl Fn(&str) -> Option<String>,
    ) -> Result<Self> {
        let mut settings = Self::new(application_name, default_max_connections);
        let secs = |name: &str| -> Result<Option<Duration>> {
            lookup(name)
                .map(|raw| {
                    raw.trim()
                        .parse::<u64>()
                        .map(Duration::from_secs)
                        .with_context(|| format!("{name}={raw:?} is not a whole number of seconds"))
                })
                .transpose()
        };
        if let Some(value) = secs(STATEMENT_TIMEOUT_ENV)? {
            settings.statement_timeout = value;
        }
        if let Some(value) = secs(IDLE_IN_TRANSACTION_TIMEOUT_ENV)? {
            settings.idle_in_transaction_timeout = value;
        }
        if let Some(value) = secs(ACQUIRE_TIMEOUT_ENV)? {
            anyhow::ensure!(
                !value.is_zero(),
                "{ACQUIRE_TIMEOUT_ENV} must be at least 1 second"
            );
            settings.acquire_timeout = value;
        }
        if let Some(raw) = lookup(MAX_CONNECTIONS_ENV) {
            let value: u32 = raw.trim().parse().with_context(|| {
                format!("{MAX_CONNECTIONS_ENV}={raw:?} is not a positive integer")
            })?;
            anyhow::ensure!(value > 0, "{MAX_CONNECTIONS_ENV} must be at least 1");
            settings.max_connections = value;
        }
        Ok(settings)
    }

    /// The server settings sent as `-c name=value` in the startup packet,
    /// in milliseconds (a bare integer is read in the setting's base unit,
    /// ms for both). A zero timeout is omitted.
    pub fn session_options(&self) -> Vec<(&'static str, String)> {
        let mut options = Vec::with_capacity(2);
        if !self.statement_timeout.is_zero() {
            options.push((
                "statement_timeout",
                self.statement_timeout.as_millis().to_string(),
            ));
        }
        if !self.idle_in_transaction_timeout.is_zero() {
            options.push((
                "idle_in_transaction_session_timeout",
                self.idle_in_transaction_timeout.as_millis().to_string(),
            ));
        }
        options
    }

    /// `base` plus `application_name` and [`Self::session_options`].
    /// sqlx appends to any `options` already present (e.g. from the URL or
    /// api's dead-client detection), so those keep working.
    pub fn connect_options(&self, base: PgConnectOptions) -> PgConnectOptions {
        let base = base.application_name(&self.application_name);
        let options = self.session_options();
        if options.is_empty() {
            base
        } else {
            base.options(options)
        }
    }

    /// Pool sizing and `acquire_timeout`.
    pub fn pool_options(&self) -> PgPoolOptions {
        PgPoolOptions::new()
            .max_connections(self.max_connections)
            .acquire_timeout(self.acquire_timeout)
    }

    /// Parses `database_url` and connects a pool with every setting above.
    pub async fn connect(&self, database_url: &str) -> Result<PgPool> {
        let base: PgConnectOptions = database_url
            .parse()
            .context("could not parse DATABASE_URL")?;
        self.pool_options()
            .connect_with(self.connect_options(base))
            .await
            .context("could not connect to the database")
    }
}

/// `SET LOCAL statement_timeout` for the rest of the current transaction:
/// how a statement that legitimately runs past the pool default gets its
/// longer budget. Outside a transaction block `SET LOCAL` only warns and
/// does nothing, so always call it on a transaction.
pub async fn set_local_statement_timeout(
    conn: &mut PgConnection,
    timeout: Duration,
) -> sqlx::Result<()> {
    // `SET` cannot take a bind parameter; this is our own integer (ms).
    sqlx::query(&format!(
        "SET LOCAL statement_timeout = {}",
        timeout.as_millis()
    ))
    .execute(conn)
    .await
    .map(drop)
}

/// `SET LOCAL idle_in_transaction_session_timeout` for the rest of the
/// current transaction, for a transaction that deliberately waits on
/// something else (the archive's object-storage uploads) between statements.
pub async fn set_local_idle_in_transaction_timeout(
    conn: &mut PgConnection,
    timeout: Duration,
) -> sqlx::Result<()> {
    sqlx::query(&format!(
        "SET LOCAL idle_in_transaction_session_timeout = {}",
        timeout.as_millis()
    ))
    .execute(conn)
    .await
    .map(drop)
}

/// Whether `err` (anywhere in its chain) is Postgres cancelling a statement,
/// SQLSTATE 57014 `query_canceled` -- `statement_timeout` expiring.
pub fn is_query_canceled(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<sqlx::Error>()
            .and_then(sqlx::Error::as_database_error)
            .and_then(|db| db.code())
            .is_some_and(|code| code == "57014")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn lookup(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |name| map.get(name).cloned()
    }

    #[test]
    fn defaults_apply_when_nothing_is_set() {
        let settings = PoolSettings::from_lookup("distant-signal-test", 7, lookup(&[])).unwrap();
        assert_eq!(settings, PoolSettings::new("distant-signal-test", 7));
        assert_eq!(settings.statement_timeout, Duration::from_secs(60));
        assert_eq!(
            settings.idle_in_transaction_timeout,
            Duration::from_secs(30)
        );
        assert_eq!(settings.acquire_timeout, Duration::from_secs(5));
        assert_eq!(
            settings.session_options(),
            vec![
                ("statement_timeout", "60000".to_owned()),
                ("idle_in_transaction_session_timeout", "30000".to_owned()),
            ]
        );
    }

    #[test]
    fn env_overrides_each_setting() {
        let settings = PoolSettings::from_lookup(
            "x",
            7,
            lookup(&[
                (STATEMENT_TIMEOUT_ENV, "90"),
                (IDLE_IN_TRANSACTION_TIMEOUT_ENV, " 45 "),
                (ACQUIRE_TIMEOUT_ENV, "2"),
                (MAX_CONNECTIONS_ENV, "12"),
            ]),
        )
        .unwrap();
        assert_eq!(settings.statement_timeout, Duration::from_secs(90));
        assert_eq!(
            settings.idle_in_transaction_timeout,
            Duration::from_secs(45)
        );
        assert_eq!(settings.acquire_timeout, Duration::from_secs(2));
        assert_eq!(settings.max_connections, 12);
    }

    #[test]
    fn zero_disables_a_timeout() {
        let settings = PoolSettings::from_lookup(
            "x",
            1,
            lookup(&[
                (STATEMENT_TIMEOUT_ENV, "0"),
                (IDLE_IN_TRANSACTION_TIMEOUT_ENV, "0"),
            ]),
        )
        .unwrap();
        assert!(settings.session_options().is_empty());
    }

    #[test]
    fn bad_values_fail_loudly() {
        for vars in [
            [(STATEMENT_TIMEOUT_ENV, "60s")],
            [(IDLE_IN_TRANSACTION_TIMEOUT_ENV, "-1")],
            [(ACQUIRE_TIMEOUT_ENV, "0")],
            [(MAX_CONNECTIONS_ENV, "0")],
            [(MAX_CONNECTIONS_ENV, "many")],
        ] {
            let err = PoolSettings::from_lookup("x", 1, lookup(&vars)).unwrap_err();
            assert!(err.to_string().contains(vars[0].0), "{err:#}");
        }
    }

    #[test]
    fn connect_options_keep_existing_options() {
        let base: PgConnectOptions = "postgres://u@localhost/db?options=-c%20work_mem%3D8MB"
            .parse()
            .unwrap();
        let options = PoolSettings::new("distant-signal-test", 1).connect_options(base);
        let debug = format!("{options:?}");
        assert!(debug.contains("work_mem=8MB"), "{debug}");
        assert!(debug.contains("statement_timeout=60000"), "{debug}");
        assert!(
            debug.contains("idle_in_transaction_session_timeout=30000"),
            "{debug}"
        );
        assert!(debug.contains("distant-signal-test"), "{debug}");
    }

    async fn pool(settings: &PoolSettings) -> PgPool {
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        settings.connect(&url).await.expect("connect")
    }

    /// The settings reach the session: `SHOW` on a pooled connection.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p common \
                --features postgres pg:: -- --ignored`"]
    async fn pooled_connections_carry_the_settings() {
        let mut settings = PoolSettings::new("distant-signal-pg-test", 2);
        settings.statement_timeout = Duration::from_secs(42);
        settings.idle_in_transaction_timeout = Duration::from_secs(17);
        let pool = pool(&settings).await;
        for (name, expected) in [
            ("statement_timeout", "42s"),
            ("idle_in_transaction_session_timeout", "17s"),
            ("application_name", "distant-signal-pg-test"),
        ] {
            let value: String = sqlx::query_scalar(&format!("SHOW {name}"))
                .fetch_one(&pool)
                .await
                .expect("SHOW");
            assert_eq!(value, expected, "{name}");
        }

        // SET LOCAL raises it for one transaction only.
        let mut tx = pool.begin().await.unwrap();
        set_local_statement_timeout(&mut tx, Duration::from_secs(120))
            .await
            .unwrap();
        let inside: String = sqlx::query_scalar("SHOW statement_timeout")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert_eq!(inside, "2min");
        tx.commit().await.unwrap();
        let after: String = sqlx::query_scalar("SHOW statement_timeout")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(after, "42s");
    }

    /// A statement over the timeout is cancelled (57014), and the connection
    /// goes back to the pool usable, with nothing left running server-side.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p common \
                --features postgres pg:: -- --ignored`"]
    async fn a_statement_over_the_timeout_is_cancelled_and_does_not_leak() {
        let app = "distant-signal-pg-timeout-test";
        let mut settings = PoolSettings::new(app, 1);
        settings.statement_timeout = Duration::from_secs(1);
        let pool = pool(&settings).await;

        let started = std::time::Instant::now();
        let err = sqlx::query("SELECT pg_sleep(30)")
            .execute(&pool)
            .await
            .map_err(anyhow::Error::from)
            .expect_err("pg_sleep(30) must hit the 1s statement_timeout");
        assert!(is_query_canceled(&err), "expected 57014, got {err:#}");
        assert!(started.elapsed() < Duration::from_secs(10));

        // The pool's only connection is healthy and idle again.
        let one: i32 = sqlx::query_scalar("SELECT 1")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(one, 1);
        let active: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity \
             WHERE application_name = $1 AND query LIKE '%pg_sleep%' AND state = 'active' \
               AND pid <> pg_backend_pid()",
        )
        .bind(app)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            active, 0,
            "the cancelled pg_sleep must not still be running"
        );
    }

    /// A transaction left idle is ended by the server.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p common \
                --features postgres pg:: -- --ignored`"]
    async fn an_idle_transaction_is_terminated() {
        let mut settings = PoolSettings::new("distant-signal-pg-idle-test", 1);
        settings.idle_in_transaction_timeout = Duration::from_secs(1);
        let pool = pool(&settings).await;
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("SELECT 1").execute(&mut *tx).await.unwrap();
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(
            sqlx::query("SELECT 1").execute(&mut *tx).await.is_err(),
            "the server must have terminated the idle-in-transaction session"
        );
        drop(tx);
        // The pool replaces the dead connection.
        let one: i32 = sqlx::query_scalar("SELECT 1")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(one, 1);
    }

    /// An exhausted pool fails an acquire after `acquire_timeout`, not 30s.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p common \
                --features postgres pg:: -- --ignored`"]
    async fn an_exhausted_pool_fails_fast() {
        let mut settings = PoolSettings::new("distant-signal-pg-acquire-test", 1);
        settings.acquire_timeout = Duration::from_secs(1);
        let pool = pool(&settings).await;
        let held = pool.acquire().await.unwrap();
        let started = std::time::Instant::now();
        let err = pool
            .acquire()
            .await
            .expect_err("the only connection is held");
        assert!(matches!(err, sqlx::Error::PoolTimedOut), "{err:?}");
        assert!(started.elapsed() < Duration::from_secs(5));
        drop(held);
    }
}
