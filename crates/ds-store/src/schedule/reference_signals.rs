//! schedule-reference's run signals and run lock (`CronJob` phase 4, DS-241;
//! docs/scheduled-jobs.md, "schedule-reference").
//!
//! - [`record`] and [`settled_delivery`]: `schedule_reference_signals`,
//!   one row per (kind, product), overwritten each time ([`Signal`]).
//!   schedule-reference writes them; the always-on ingest-writer reads
//!   them ([`all`]) and exports them as gauges, since a `CronJob` run exits
//!   before Prometheus could scrape it.
//! - [`RunLock`]: a session advisory lock held for a whole run (or, as a
//!   Deployment, a whole cycle), so two runs never publish at once: a
//!   doubled Job, a manual run, or the Deployment and the `CronJob` both
//!   alive while the mode switches. The server frees it when its
//!   connection goes (a killed backend, a failover, a dropped keepalive)
//!   without the run noticing, so a run calls [`RunLock::ensure_held`]
//!   between products or dates and stops once it is lost.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sqlx::{ConnectOptions, Connection, PgConnection, PgPool};

/// One kind of signal (the `kind` column).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    /// A run (or the Deployment, at start) read its completion marker.
    /// Product `""`.
    Seeded,
    /// A product's publish was refused as bad data.
    Rejected,
    /// A final chunk's staged key count did not match the publisher's
    /// total, so its delete was skipped (SCHED-2).
    StagedMismatch,
    /// A `CronJob` run finished a delivery with a failure the same delivery
    /// can't fix; later runs skip that delivery. Product `""`.
    Settled,
}

impl Signal {
    /// Every kind, as the `kind` column's CHECK lists them.
    pub const ALL: [Self; 4] = [
        Self::Seeded,
        Self::Rejected,
        Self::StagedMismatch,
        Self::Settled,
    ];

    /// The `kind` column's value.
    pub const fn kind(self) -> &'static str {
        match self {
            Self::Seeded => "seeded",
            Self::Rejected => "rejected",
            Self::StagedMismatch => "staged_mismatch",
            Self::Settled => "settled",
        }
    }
}

/// Records `signal` for `product` (`""` for a run-wide one) now, with the
/// delivery it happened for, replacing the previous row of that pair.
pub async fn record(
    executor: impl sqlx::PgExecutor<'_>,
    signal: Signal,
    product: &str,
    delivery: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO schedule_reference_signals (kind, product, recorded_at, delivery) \
         VALUES ($1, $2, now(), $3) \
         ON CONFLICT (kind, product) \
         DO UPDATE SET recorded_at = EXCLUDED.recorded_at, delivery = EXCLUDED.delivery",
    )
    .bind(signal.kind())
    .bind(product)
    .bind(delivery)
    .execute(executor)
    .await
    .with_context(|| format!("recording the schedule-reference {} signal", signal.kind()))?;
    Ok(())
}

/// The delivery the last [`Signal::Settled`] named, if any.
pub async fn settled_delivery(executor: impl sqlx::PgExecutor<'_>) -> Result<Option<String>> {
    let row: Option<(Option<String>,)> = sqlx::query_as(
        "SELECT delivery FROM schedule_reference_signals WHERE kind = 'settled' AND product = ''",
    )
    .fetch_optional(executor)
    .await?;
    Ok(row.and_then(|(delivery,)| delivery))
}

/// One `schedule_reference_signals` row, as the exporter reads it.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct SignalRow {
    pub kind: String,
    pub product: String,
    pub recorded_at: DateTime<Utc>,
    pub delivery: Option<String>,
}

/// Every signal row (a few dozen at most).
pub async fn all(executor: impl sqlx::PgExecutor<'_>) -> Result<Vec<SignalRow>> {
    Ok(sqlx::query_as(
        "SELECT kind, product, recorded_at, delivery FROM schedule_reference_signals \
         ORDER BY kind, product",
    )
    .fetch_all(executor)
    .await?)
}

/// A delivery directory name (`YYYYMMDDTHHMMSSZ`, schedule-ingest's
/// `delivery_dir_name`) as Unix seconds.
pub fn delivery_timestamp(dir_name: &str) -> Option<i64> {
    chrono::NaiveDateTime::parse_from_str(dir_name, "%Y%m%dT%H%M%SZ")
        .ok()
        .map(|at| at.and_utc().timestamp())
}

/// [`RunLock`]'s session advisory-lock key
/// (`ds_runtime::advisory_locks::SCHEDULE_REFERENCE_RUN_KEY`).
pub const RUN_LOCK_KEY: i64 = ds_runtime::advisory_locks::SCHEDULE_REFERENCE_RUN_KEY;

/// `pg_try_advisory_lock(RUN_LOCK_KEY)` on a connection of its own, held
/// until [`RunLock::release`] (or the process exits: the server frees a
/// session lock when its connection goes). Its own connection, opened with
/// the pool's options, so the publish keeps the whole pool; the chart
/// counts it in the role's connection limit.
///
/// Nothing tells the run when the server frees the lock under it (its
/// backend terminated, a failover, a keepalive drop): another run can then
/// take it and publish alongside. So a run calls [`Self::ensure_held`]
/// (or [`Self::still_held`]) between units of work and stops when it fails.
#[derive(Debug)]
pub struct RunLock {
    conn: PgConnection,
}

/// How long [`RunLock::still_held`] waits for its answer: a connection that
/// hangs (a half-open TCP session) counts as lost.
pub const STILL_HELD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// `pg_locks`' view of [`RUN_LOCK_KEY`]: a bigint advisory key is
/// (classid, objid) = (high, low) 32 bits, objsubid 1.
fn run_lock_halves() -> (i64, i64) {
    const LOW_32: i64 = 0xffff_ffff;
    ((RUN_LOCK_KEY >> 32) & LOW_32, RUN_LOCK_KEY & LOW_32)
}

impl RunLock {
    /// Takes the lock, or `None` when another session holds it.
    ///
    /// # Errors
    ///
    /// The connection or the lock query failed.
    pub async fn try_acquire(pool: &PgPool, application_name: &str) -> Result<Option<Self>> {
        let options = (*pool.connect_options())
            .clone()
            .application_name(application_name);
        let mut conn = options
            .connect()
            .await
            .context("could not open the run-lock connection")?;
        let taken: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(RUN_LOCK_KEY)
            .fetch_one(&mut conn)
            .await
            .context("pg_try_advisory_lock failed")?;
        if taken {
            Ok(Some(Self { conn }))
        } else {
            if let Err(err) = conn.close().await {
                tracing::debug!(error = ?err, "closing the unused run-lock connection failed");
            }
            Ok(None)
        }
    }

    /// Whether this session still holds the lock: one cheap query on the
    /// lock's own connection, `false` when it fails or takes longer than
    /// [`STILL_HELD_TIMEOUT`] (the connection, and so the lock, is gone).
    pub async fn still_held(&mut self) -> bool {
        let (classid, objid) = run_lock_halves();
        let query = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM pg_locks \
             WHERE locktype = 'advisory' AND pid = pg_backend_pid() AND granted \
               AND classid::bigint = $1 AND objid::bigint = $2 AND objsubid = 1)",
        )
        .bind(classid)
        .bind(objid)
        .fetch_one(&mut self.conn);
        match tokio::time::timeout(STILL_HELD_TIMEOUT, query).await {
            Ok(Ok(held)) => held,
            Ok(Err(err)) => {
                tracing::warn!(error = ?err, "checking the schedule-reference run lock failed");
                false
            }
            Err(_) => {
                tracing::warn!(
                    timeout = ?STILL_HELD_TIMEOUT,
                    "checking the schedule-reference run lock timed out"
                );
                false
            }
        }
    }

    /// [`Self::still_held`], as an error once the lock is lost, for a run to
    /// stop on (`?`) between products or dates rather than publish beside
    /// another run that has since taken it.
    ///
    /// # Errors
    ///
    /// The lock is no longer held.
    pub async fn ensure_held(&mut self) -> Result<()> {
        if self.still_held().await {
            Ok(())
        } else {
            anyhow::bail!(
                "lost the schedule-reference run lock (its connection went); stopping \
                 this run so it can't publish alongside another"
            )
        }
    }

    /// Releases the lock and closes its connection. A failed unlock still
    /// closes the connection, which releases the lock.
    pub async fn release(mut self) {
        if let Err(err) = sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(RUN_LOCK_KEY)
            .execute(&mut self.conn)
            .await
        {
            tracing::debug!(error = ?err, "pg_advisory_unlock failed; closing the connection");
        }
        if let Err(err) = self.conn.close().await {
            tracing::debug!(error = ?err, "closing the run-lock connection failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delivery_names_parse_as_utc() {
        assert_eq!(
            delivery_timestamp("20261031T210342Z"),
            Some(1_793_480_622),
            "2026-10-31 21:03:42 UTC"
        );
        assert_eq!(delivery_timestamp("latest"), None);
    }

    #[test]
    fn kinds_match_the_column_check() {
        let migration =
            include_str!("../../migrations/20261101140000_schedule_reference_signals.sql");
        for signal in Signal::ALL {
            assert!(
                migration.contains(&format!("'{}'", signal.kind())),
                "{} missing from the CHECK",
                signal.kind()
            );
        }
    }

    async fn pool() -> PgPool {
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        PgPool::connect(&url).await.expect("connect to postgres")
    }

    /// One run holds the lock; a second can't take it until the first
    /// releases it, or until the first's connection is gone.
    #[tokio::test]
    #[ignore = "requires a live database (DATABASE_URL)"]
    async fn the_run_lock_admits_one_run_at_a_time() {
        let pool = pool().await;
        let first = RunLock::try_acquire(&pool, "ds-store-run-lock-test-a")
            .await
            .unwrap()
            .expect("free at first");
        assert!(
            RunLock::try_acquire(&pool, "ds-store-run-lock-test-b")
                .await
                .unwrap()
                .is_none(),
            "a second run must not take a held lock"
        );
        first.release().await;
        let second = RunLock::try_acquire(&pool, "ds-store-run-lock-test-b")
            .await
            .unwrap()
            .expect("free again once released");
        // Dropped without release (a killed run): the server frees it when
        // the connection goes, which a dropped PgConnection closes.
        drop(second);
        let mut freed = None;
        for _ in 0..50 {
            freed = RunLock::try_acquire(&pool, "ds-store-run-lock-test-c")
                .await
                .unwrap();
            if freed.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        freed
            .expect("a dropped run's lock is freed")
            .release()
            .await;
    }

    /// A held lock reports so; once its backend is terminated (as a
    /// failover or a dropped connection would), it reports lost and
    /// another run can take it.
    #[tokio::test]
    #[ignore = "requires a live database (DATABASE_URL)"]
    async fn a_run_notices_when_its_lock_is_lost() {
        let pool = pool().await;
        let application_name = format!("ds-store-run-lock-lost-{}", std::process::id());
        let mut lock = RunLock::try_acquire(&pool, &application_name)
            .await
            .unwrap()
            .expect("free at first");
        assert!(lock.still_held().await);
        lock.ensure_held().await.expect("held");

        let terminated: bool = sqlx::query_scalar(
            "SELECT bool_and(pg_terminate_backend(pid)) FROM pg_stat_activity \
             WHERE application_name = $1",
        )
        .bind(&application_name)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(terminated, "the lock's backend must be terminated");
        assert!(
            !lock.still_held().await,
            "a terminated session holds nothing"
        );
        let err = lock.ensure_held().await.expect_err("lost");
        assert!(
            err.to_string()
                .contains("lost the schedule-reference run lock")
        );

        let mut other = None;
        for _ in 0..50 {
            other = RunLock::try_acquire(&pool, "ds-store-run-lock-lost-b")
                .await
                .unwrap();
            if other.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        other
            .expect("another run takes the freed lock")
            .release()
            .await;
        lock.release().await;
    }

    #[test]
    fn the_lock_key_splits_as_pg_locks_shows_it() {
        let (classid, objid) = run_lock_halves();
        assert_eq!((classid << 32) | objid, RUN_LOCK_KEY);
        assert!((0..=0xffff_ffff).contains(&classid) && (0..=0xffff_ffff).contains(&objid));
    }

    /// A signal row is overwritten per (kind, product), and the settled
    /// delivery reads back.
    #[tokio::test]
    #[ignore = "requires a live database (DATABASE_URL), migrated"]
    async fn signals_overwrite_their_row_and_read_back() {
        let pool = pool().await;
        let product = format!("test-{}", std::process::id());
        record(&pool, Signal::Rejected, &product, Some("20260101T000000Z"))
            .await
            .unwrap();
        record(&pool, Signal::Rejected, &product, Some("20260102T000000Z"))
            .await
            .unwrap();
        let rows: Vec<SignalRow> = all(&pool)
            .await
            .unwrap()
            .into_iter()
            .filter(|row| row.product == product)
            .collect();
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].kind, "rejected");
        assert_eq!(rows[0].delivery.as_deref(), Some("20260102T000000Z"));

        let before = settled_delivery(&pool).await.unwrap();
        record(&pool, Signal::Settled, "", Some("20260103T000000Z"))
            .await
            .unwrap();
        assert_eq!(
            settled_delivery(&pool).await.unwrap().as_deref(),
            Some("20260103T000000Z")
        );
        // Put the shared rows back (a broader role; the narrow one can't
        // delete).
        let _ = sqlx::query("DELETE FROM schedule_reference_signals WHERE product = $1")
            .bind(&product)
            .execute(&pool)
            .await;
        match before {
            Some(delivery) => record(&pool, Signal::Settled, "", Some(&delivery))
                .await
                .unwrap(),
            None => {
                let _ =
                    sqlx::query("DELETE FROM schedule_reference_signals WHERE kind = 'settled'")
                        .execute(&pool)
                        .await;
            }
        }
    }
}
