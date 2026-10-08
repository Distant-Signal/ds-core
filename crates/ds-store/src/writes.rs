//! Direct-write telemetry and failure classes (spec §9.1, §14.1; plan
//! 2a.5).
//!
//! A producer that writes Postgres directly (schedule-reference with
//! `INGEST_SINK=db` first, then the other phase 2 producers) records each
//! write as
//!
//! - `distant_signal_db_writes_total{operation, outcome}`, and
//! - `distant_signal_db_write_seconds{operation}` (a histogram, the
//!   default buckets of `common::metrics::install`),
//!
//! where `outcome` is `ok` or a [`WriteFailure`]. [`classify`] is the one
//! place a write error becomes a [`WriteFailure`], so every direct writer
//! answers the same thing for the same database error.

use std::time::Duration;

use common::metrics::metric_name;

/// `db_writes_total{operation, outcome}`.
pub const DB_WRITES_METRIC: &str = "db_writes_total";
/// `db_write_seconds{operation}`.
pub const DB_WRITE_SECONDS_METRIC: &str = "db_write_seconds";

/// The `outcome` label of a successful write.
pub const OK: &str = "ok";

/// Why a direct write failed: the `outcome` label, and what the producer's
/// retry logic does with it (spec §9.1's `SinkError`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteFailure {
    /// Another writer holds the work this one needs
    /// ([`crate::schedule::SchedulePublishBusy`]): nothing was written.
    Busy,
    /// A statement hit its `statement_timeout` (SQLSTATE 57014) and the
    /// transaction rolled back.
    Timeout,
    /// The data itself was refused: SQLSTATE class 22 (data exception) or
    /// 23 (integrity constraint violation). Retrying the same data fails
    /// the same way.
    Rejected,
    /// Anything else: a lost connection, a pool timeout, a deadlock, a
    /// serialization failure.
    Transient,
}

impl WriteFailure {
    /// Every failure, for [`register`].
    pub const ALL: [Self; 4] = [Self::Busy, Self::Timeout, Self::Rejected, Self::Transient];

    /// The `outcome` label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Busy => "busy",
            Self::Timeout => "timeout",
            Self::Rejected => "rejected",
            Self::Transient => "transient",
        }
    }
}

/// Classifies a write error: [`crate::schedule::SchedulePublishBusy`]
/// anywhere in the chain is [`WriteFailure::Busy`], SQLSTATE 57014
/// [`WriteFailure::Timeout`], class 22 or 23 [`WriteFailure::Rejected`], and
/// everything else [`WriteFailure::Transient`].
pub fn classify(err: &anyhow::Error) -> WriteFailure {
    if err
        .chain()
        .any(<dyn std::error::Error>::is::<crate::schedule::SchedulePublishBusy>)
    {
        return WriteFailure::Busy;
    }
    if crate::schedule::is_statement_timeout(err) {
        return WriteFailure::Timeout;
    }
    let code = err.chain().find_map(|cause| {
        cause
            .downcast_ref::<sqlx::Error>()
            .and_then(sqlx::Error::as_database_error)
            .and_then(sqlx::error::DatabaseError::code)
    });
    match code.as_deref().map(|code| code.get(..2)) {
        Some(Some("22" | "23")) => WriteFailure::Rejected,
        _ => WriteFailure::Transient,
    }
}

/// Registers every `operation` at 0 for each outcome, so a dashboard or
/// `increase()` sees the first write.
pub fn register(operations: &[&'static str]) {
    for &operation in operations {
        for outcome in std::iter::once(OK).chain(WriteFailure::ALL.map(WriteFailure::as_str)) {
            metrics::counter!(
                metric_name(DB_WRITES_METRIC),
                "operation" => operation,
                "outcome" => outcome
            )
            .increment(0);
        }
    }
}

/// Records one write of `operation` that took `elapsed` and ended with
/// `failure` (`None`: it succeeded).
pub fn record(operation: &'static str, failure: Option<WriteFailure>, elapsed: Duration) {
    let outcome = failure.map_or(OK, WriteFailure::as_str);
    metrics::counter!(
        metric_name(DB_WRITES_METRIC),
        "operation" => operation,
        "outcome" => outcome
    )
    .increment(1);
    metrics::histogram!(metric_name(DB_WRITE_SECONDS_METRIC), "operation" => operation)
        .record(elapsed.as_secs_f64());
}

#[cfg(test)]
mod tests {
    use metrics_exporter_prometheus::PrometheusBuilder;

    use super::*;

    #[test]
    fn busy_and_other_errors_classify() {
        let busy = anyhow::Error::new(crate::schedule::SchedulePublishBusy {
            product: "schedule_calling_points_full",
        })
        .context("final chunk");
        assert_eq!(classify(&busy), WriteFailure::Busy);
        assert_eq!(
            classify(&anyhow::anyhow!("connection reset")),
            WriteFailure::Transient
        );
        assert_eq!(
            classify(&anyhow::Error::new(sqlx::Error::PoolTimedOut)),
            WriteFailure::Transient
        );
    }

    #[test]
    fn outcomes_are_the_documented_labels() {
        let labels: Vec<&str> = WriteFailure::ALL.map(WriteFailure::as_str).to_vec();
        assert_eq!(labels, ["busy", "timeout", "rejected", "transient"]);
    }

    #[test]
    fn record_counts_the_write_and_its_duration() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            register(&["publish_part"]);
            record("publish_part", None, Duration::from_millis(20));
            record(
                "publish_part",
                Some(WriteFailure::Timeout),
                Duration::from_secs(120),
            );
        });
        let rendered = handle.render();
        for line in [
            r#"distant_signal_db_writes_total{operation="publish_part",outcome="ok"} 1"#,
            r#"distant_signal_db_writes_total{operation="publish_part",outcome="timeout"} 1"#,
            r#"distant_signal_db_writes_total{operation="publish_part",outcome="busy"} 0"#,
            r#"distant_signal_db_write_seconds_count{operation="publish_part"} 2"#,
        ] {
            assert!(rendered.contains(line), "{line}\n{rendered}");
        }
    }
}

/// [`classify`] against real database errors.
#[cfg(test)]
mod db_tests {
    use super::*;

    async fn pool() -> sqlx::PgPool {
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        sqlx::PgPool::connect(&url).await.expect("connect")
    }

    async fn error_of(pool: &sqlx::PgPool, sql: &str) -> anyhow::Error {
        anyhow::Error::new(
            sqlx::query(sql)
                .execute(pool)
                .await
                .expect_err("the statement fails"),
        )
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
    async fn sqlstate_classes_map_to_failures() {
        let pool = pool().await;
        // 22P02 invalid_text_representation (class 22).
        assert_eq!(
            classify(&error_of(&pool, "SELECT 'x'::int").await),
            WriteFailure::Rejected
        );
        // 57014 query_canceled.
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("SET LOCAL statement_timeout = '10ms'")
            .execute(&mut *tx)
            .await
            .unwrap();
        let timeout = sqlx::query("SELECT pg_sleep(1)")
            .execute(&mut *tx)
            .await
            .expect_err("cancelled");
        assert_eq!(
            classify(&anyhow::Error::new(timeout)),
            WriteFailure::Timeout
        );
        drop(tx);
        // 42P01 undefined_table: not the data's fault.
        assert_eq!(
            classify(&error_of(&pool, "SELECT * FROM no_such_table_p2a").await),
            WriteFailure::Transient
        );
    }
}
