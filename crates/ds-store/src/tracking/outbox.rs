//! The train-event outbox (ingest architecture plan 3b.3, decided
//! 2026-10-08; migration `20261009130000_train_event_outbox.sql`).
//!
//! trust-consumer's direct DB sink connects as `distant_signal_trust_consumer`,
//! which may only read `train_subscriptions`. [`upsert_train_event_on`] (what
//! the api's `POST /private/train-events` runs per event) updates a
//! subscription in exactly three cases ([`needs_subscription_write`]):
//!
//! | Event | Statement | Condition |
//! |---|---|---|
//! | a resolution (`resolved_train_id` set) | `flip_legacy_resolution`: `SET resolution_status = 'resolved', unresolved_from = NULL`, and `SET trains_id` when none was linked (with `find_or_create_train`/`mark_train_resolved` on `trains`) | the subscription exists |
//! | a cancellation (`status = 'cancelled'`) | `mark_subscription_unresolved_on_cancellation`: `SET resolution_status = 'unresolved', unresolved_from = resolution_status` | it is `pending` or `schedule_matched` |
//! | a reinstatement (`msg_type = '0005'`) | `reopen_subscriptions_after_reinstatement`: `SET resolution_status = unresolved_from` | it, or another subscription of the same train, was moved to `unresolved` by a cancellation |
//!
//! Every other event only reads the subscription's `trains_id` and writes
//! the shared movement and current state.
//!
//! [`write_train_events_deferring`] (the sink, in its transaction) writes
//! those other events directly and puts the three kinds in
//! `train_event_outbox`, with every later event of the same subscription
//! while one of its rows is pending: a resolution links the `trains_id`
//! the subscription's later movements are written against, so they must
//! not overtake it. [`apply_train_event_outbox`] (the ingest-writer's
//! `train_event_outbox` loop, as the writer role) applies the rows in id
//! order with [`upsert_train_event_on`] and deletes them, queueing each
//! row's forward signal once its event landed.
//!
//! What waits for the loop (at most one tick, 5 s by default): the
//! subscription's `resolution_status` (the user's tracked-train state, and
//! `list_active_tracked_trains`, which trust-consumer reloads every 60 s);
//! the `trains_id` link and the resolving departure's movement and current
//! state, which the notifier reads; and the forward signal of a deferred
//! event. The schedule-match and backlog-match sweeps (300 s) select
//! `pending` rows with no `trains_id`; one that matches a pin whose
//! resolution is still queued links it first, and the queued resolution
//! then meets an already-linked subscription: the same race the api's
//! inline path has with those sweeps, with the same uid guard.

use std::collections::HashSet;

use common::{RejectedTrustBacklogRow, TrainForwardSignalMessage, TrainMovementEventMessage};
use sqlx::{Connection, PgConnection, PgPool};

use super::{TrainEventsBatchOutcome, upsert_train_event_on, upsert_train_events_batch_in};

/// `store_train_event_outbox_total{outcome}`: rows the loop applied
/// (`applied`) or refused for a data error (`rejected`, left in the table).
pub const OUTBOX_METRIC: &str = "store_train_event_outbox_total";

/// `train_event_outbox_oldest_pending_timestamp_seconds`: the
/// `created_at` (Unix seconds) of the oldest row not yet applied or
/// rejected, 0 when there is none; set after each successful
/// [`apply_train_event_outbox`]. A timestamp rather than an age, so a loop
/// that stops ticking (the value is not refreshed) still shows the row
/// getting older: `DistantSignalTrainEventOutboxStuck` alerts on
/// `time() - x` where `x > 0`.
pub const OLDEST_PENDING_METRIC: &str = "train_event_outbox_oldest_pending_timestamp_seconds";

/// Rows one [`apply_train_event_outbox`] call applies at most.
pub const APPLY_BATCH: i64 = 500;

/// Whether [`upsert_train_event_on`] would update `train_subscriptions`
/// for this event (see the module docs' table).
pub fn needs_subscription_write(event: &TrainMovementEventMessage) -> bool {
    event.resolved_train_id.is_some() || event.status == "cancelled" || event.msg_type == "0005"
}

/// What [`write_train_events_deferring`] did with one batch.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct DeferringOutcome {
    /// The direct writes' outcome, with `rejected` indices into the whole
    /// batch.
    pub written: TrainEventsBatchOutcome,
    /// Batch indices put in the outbox (a redelivered one already there
    /// counts too).
    pub deferred: Vec<usize>,
    /// Forward signals queued now (those of deferred events wait in their
    /// outbox rows).
    pub signals_queued: u64,
}

/// trust-consumer's DB-sink write (see the module docs), inside the
/// caller's transaction `tx`: the direct events through
/// [`upsert_train_events_batch_in`], the rest into the outbox, and the
/// forward signals `signals` builds from every event not rejected (in
/// batch order, as the HTTP sink builds them from what the api wrote).
/// A signal raised by a deferred event (its `dedup_key` is
/// `<trains_id>:<event dedup_key>`) goes into that event's outbox row.
pub async fn write_train_events_deferring<F>(
    tx: &mut PgConnection,
    events: &[TrainMovementEventMessage],
    signals: F,
) -> anyhow::Result<DeferringOutcome>
where
    F: FnOnce(&[&TrainMovementEventMessage]) -> Vec<TrainForwardSignalMessage>,
{
    let mut outcome = DeferringOutcome::default();
    if events.is_empty() {
        return Ok(outcome);
    }
    let ids: Vec<i64> = events.iter().map(|event| event.tracked_train_id).collect();
    let pending: Vec<i64> = sqlx::query_scalar(
        "SELECT DISTINCT tracked_train_id FROM train_event_outbox \
         WHERE rejected_at IS NULL AND tracked_train_id = ANY($1)",
    )
    .bind(&ids)
    .fetch_all(&mut *tx)
    .await?;
    let mut deferred_subscriptions: HashSet<i64> = pending.into_iter().collect();
    let mut direct = Vec::new();
    for (index, event) in events.iter().enumerate() {
        if needs_subscription_write(event)
            || deferred_subscriptions.contains(&event.tracked_train_id)
        {
            deferred_subscriptions.insert(event.tracked_train_id);
            outcome.deferred.push(index);
        } else {
            direct.push(index);
        }
    }

    let direct_events: Vec<TrainMovementEventMessage> =
        direct.iter().map(|&index| events[index].clone()).collect();
    let mut written = upsert_train_events_batch_in(tx, &direct_events).await?;
    for row in &mut written.rejected {
        row.index = direct[row.index];
    }
    let rejected: HashSet<usize> = written.rejected.iter().map(|row| row.index).collect();
    outcome.written = written;

    let kept: Vec<&TrainMovementEventMessage> = events
        .iter()
        .enumerate()
        .filter(|(index, _)| !rejected.contains(index))
        .map(|(_, event)| event)
        .collect();
    let mut signals = signals(&kept);
    for &index in &outcome.deferred {
        let event = &events[index];
        let signal_at = signals.iter().position(|signal| {
            signal
                .dedup_key
                .as_deref()
                .and_then(|key| key.split_once(':'))
                .is_some_and(|(_, key)| key == event.dedup_key)
        });
        let signal = signal_at.map(|at| signals.remove(at));
        sqlx::query(
            "INSERT INTO train_event_outbox (tracked_train_id, dedup_key, event, forward_signal) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (tracked_train_id, dedup_key) DO NOTHING",
        )
        .bind(event.tracked_train_id)
        .bind(&event.dedup_key)
        .bind(sqlx::types::Json(event))
        .bind(signal.as_ref().map(sqlx::types::Json))
        .execute(&mut *tx)
        .await?;
    }
    outcome.signals_queued = super::insert_forward_signals_on(&mut *tx, &signals).await?;
    Ok(outcome)
}

/// How [`apply_train_event_outbox_with`] treats rows it cannot apply
/// (security review L1, 2026-10-08).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OutboxPolicy {
    /// A row whose apply fails with an error that is not a data error (a
    /// lock or statement timeout, a serialization failure, ...) on this
    /// many ticks is marked rejected, so it cannot hold up the rows behind
    /// it forever (`INGEST_WRITER_OUTBOX_MAX_ATTEMPTS`). At least 1.
    pub max_attempts: u32,
    /// Rejected rows are deleted this long after they were rejected
    /// (`INGEST_WRITER_OUTBOX_REJECTED_RETENTION_DAYS`).
    pub rejected_retention: std::time::Duration,
}

impl Default for OutboxPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            rejected_retention: std::time::Duration::from_secs(14 * 24 * 3600),
        }
    }
}

/// What one [`apply_train_event_outbox`] call did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct OutboxTick {
    /// Rows applied and deleted.
    pub applied: u64,
    /// Rows refused for a data error, left in the table with
    /// `rejected_at` and `rejection`.
    pub rejected: Vec<RejectedTrustBacklogRow>,
    /// Rows rejected without being applied: their `tracked_train_id`
    /// column is not their event's (the `trust_consumer` role wrote a row
    /// for another subscription), or they failed `max_attempts` times.
    pub rejected_other: u64,
    /// Rejected rows deleted after `rejected_retention`.
    pub pruned: u64,
}

impl OutboxTick {
    /// Every row this tick rejected.
    pub fn rejected_count(&self) -> u64 {
        u64::try_from(self.rejected.len())
            .unwrap_or(u64::MAX)
            .saturating_add(self.rejected_other)
    }
}

/// Registers `store_train_event_outbox_total` for both outcomes at 0, so
/// `increase()` sees the first rejection (the writer calls it at startup).
pub fn register_metrics() {
    for outcome in ["applied", "rejected"] {
        metrics::counter!(common::metrics::metric_name(OUTBOX_METRIC), "outcome" => outcome)
            .increment(0);
    }
}

#[derive(sqlx::FromRow)]
struct OutboxRow {
    id: i64,
    tracked_train_id: i64,
    attempts: i32,
    event: sqlx::types::Json<TrainMovementEventMessage>,
    forward_signal: Option<sqlx::types::Json<TrainForwardSignalMessage>>,
}

/// Marks outbox row `id` rejected with `rejection`.
async fn reject_row(conn: &mut PgConnection, id: i64, rejection: &str) -> sqlx::Result<()> {
    sqlx::query("UPDATE train_event_outbox SET rejected_at = now(), rejection = $2 WHERE id = $1")
        .bind(id)
        .bind(rejection)
        .execute(conn)
        .await
        .map(|_| ())
}

/// What [`row_failed`] did with a row whose apply failed.
enum RowFailure {
    /// A data error: rejected.
    DataError(RejectedTrustBacklogRow),
    /// Another error, on the row's last allowed attempt: rejected.
    GaveUp,
    /// Another error, attempt counted: the tick stops here.
    Retry(anyhow::Error),
}

/// Handles outbox row `id`'s failed apply (its savepoint already rolled
/// back): a data error rejects it; any other error counts an attempt and,
/// at `policy.max_attempts`, rejects it too.
async fn row_failed(
    tx: &mut PgConnection,
    id: i64,
    attempts: i32,
    event: &TrainMovementEventMessage,
    err: anyhow::Error,
    policy: &OutboxPolicy,
) -> anyhow::Result<RowFailure> {
    if let Some(data_error) = crate::backlog::classify_anyhow_data_error(&err) {
        let rejected = data_error
            .into_rejected_row(usize::try_from(id).unwrap_or(usize::MAX), &event.dedup_key);
        tracing::error!(
            outbox_id = id,
            tracked_train_id = event.tracked_train_id,
            dedup_key = %event.dedup_key,
            sqlstate = %rejected.sqlstate,
            message = %rejected.message,
            event = ?event,
            "train-event outbox row refused for a data error; left in the table"
        );
        let rejection = format!(
            "{} {} (constraint {}): {}",
            rejected.sqlstate,
            rejected.reason,
            rejected.constraint.as_deref().unwrap_or("-"),
            rejected.message
        );
        reject_row(tx, id, &rejection).await?;
        return Ok(RowFailure::DataError(rejected));
    }
    let attempts = attempts.saturating_add(1);
    sqlx::query("UPDATE train_event_outbox SET attempts = $2 WHERE id = $1")
        .bind(id)
        .bind(attempts)
        .execute(&mut *tx)
        .await?;
    if u32::try_from(attempts).unwrap_or(u32::MAX) >= policy.max_attempts.max(1) {
        tracing::error!(
            outbox_id = id,
            tracked_train_id = event.tracked_train_id,
            dedup_key = %event.dedup_key,
            attempts,
            error = ?err,
            "train-event outbox row failed on every attempt; rejected"
        );
        reject_row(tx, id, &format!("failed {attempts} times: {err:#}")).await?;
        return Ok(RowFailure::GaveUp);
    }
    Ok(RowFailure::Retry(err.context(format!(
        "train-event outbox row {id} failed (attempt {attempts} of {})",
        policy.max_attempts
    ))))
}

/// [`apply_train_event_outbox_with`] with the default [`OutboxPolicy`].
pub async fn apply_train_event_outbox(pool: &PgPool) -> anyhow::Result<OutboxTick> {
    apply_train_event_outbox_with(pool, &OutboxPolicy::default()).await
}

/// The ingest-writer's `train_event_outbox` loop body: up to [`APPLY_BATCH`]
/// pending rows, oldest first, in one transaction. Each row runs
/// [`upsert_train_event_on`] (and queues its forward signal) behind its own
/// savepoint, then is deleted. Re-applying an event is harmless (every write
/// is idempotent), so a redelivered entry that lands here again after its
/// first row was applied changes nothing.
///
/// A row is marked rejected (`rejected_at`, `rejection`; left in the
/// table, `store_train_event_outbox_total{outcome="rejected"}`) when:
///
/// - its `tracked_train_id` column differs from its event's: the row would
///   apply to a subscription other than the one it is filed under (and
///   ordered behind), so it is refused unapplied;
/// - its apply fails with a data error (SQLSTATE class 22/23): exactly that
///   row's writes roll back;
/// - its apply fails with any other error for the `max_attempts`-th time.
///   Before that, the failure is counted on the row (`attempts`), the rows
///   applied before it commit, and the call returns the error: the rest
///   wait for the next tick.
///
/// Rejected rows older than `rejected_retention` are then deleted.
pub async fn apply_train_event_outbox_with(
    pool: &PgPool,
    policy: &OutboxPolicy,
) -> anyhow::Result<OutboxTick> {
    let mut tick = OutboxTick::default();
    let mut tx = pool.begin().await?;
    let rows: Vec<OutboxRow> = sqlx::query_as(
        "SELECT id, tracked_train_id, attempts, event, forward_signal FROM train_event_outbox \
         WHERE rejected_at IS NULL ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED",
    )
    .bind(APPLY_BATCH)
    .fetch_all(&mut *tx)
    .await?;
    let mut failed = None;
    for row in rows {
        let event = row.event.0;
        if row.tracked_train_id != event.tracked_train_id {
            let rejection = format!(
                "tracked_train_id mismatch: the row's {} but its event's {}",
                row.tracked_train_id, event.tracked_train_id
            );
            tracing::error!(
                outbox_id = row.id,
                row_tracked_train_id = row.tracked_train_id,
                event_tracked_train_id = event.tracked_train_id,
                dedup_key = %event.dedup_key,
                "train-event outbox row's subscription is not its event's; rejected unapplied"
            );
            reject_row(&mut tx, row.id, &rejection).await?;
            tick.rejected_other += 1;
            continue;
        }
        let mut savepoint = Connection::begin(&mut *tx).await?;
        let applied = async {
            upsert_train_event_on(&mut savepoint, &event).await?;
            if let Some(signal) = &row.forward_signal {
                super::insert_forward_signals_on(&mut *savepoint, std::slice::from_ref(&signal.0))
                    .await?;
            }
            anyhow::Ok(())
        }
        .await;
        match applied {
            Ok(()) => {
                savepoint.commit().await?;
                sqlx::query("DELETE FROM train_event_outbox WHERE id = $1")
                    .bind(row.id)
                    .execute(&mut *tx)
                    .await?;
                tick.applied += 1;
            }
            Err(err) => {
                // If the savepoint cannot even roll back, the connection is
                // gone: nothing row-specific, so the whole tick is retried.
                savepoint.rollback().await?;
                match row_failed(&mut tx, row.id, row.attempts, &event, err, policy).await? {
                    RowFailure::DataError(rejected) => tick.rejected.push(rejected),
                    RowFailure::GaveUp => tick.rejected_other += 1,
                    RowFailure::Retry(err) => {
                        failed = Some(err);
                        break;
                    }
                }
            }
        }
    }
    tx.commit().await?;
    metrics::counter!(common::metrics::metric_name(OUTBOX_METRIC), "outcome" => "applied")
        .increment(tick.applied);
    metrics::counter!(common::metrics::metric_name(OUTBOX_METRIC), "outcome" => "rejected")
        .increment(tick.rejected_count());
    if let Some(err) = failed {
        return Err(err);
    }
    let retention_secs = policy.rejected_retention.as_secs_f64();
    tick.pruned = sqlx::query(
        "DELETE FROM train_event_outbox \
         WHERE rejected_at < now() - make_interval(secs => $1)",
    )
    .bind(retention_secs)
    .execute(pool)
    .await?
    .rows_affected();
    let oldest: Option<f64> = sqlx::query_scalar(
        "SELECT EXTRACT(EPOCH FROM min(created_at))::float8 FROM train_event_outbox \
         WHERE rejected_at IS NULL",
    )
    .fetch_one(pool)
    .await?;
    metrics::gauge!(common::metrics::metric_name(OLDEST_PENDING_METRIC)).set(oldest.unwrap_or(0.0));
    Ok(tick)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_resolutions_cancellations_and_reinstatements_need_a_subscription_write() {
        let event = crate::test_support::fixture_event(1, "k");
        assert!(!needs_subscription_write(&event));
        assert!(needs_subscription_write(&TrainMovementEventMessage {
            resolved_train_id: Some("221832406".to_string()),
            ..event.clone()
        }));
        assert!(needs_subscription_write(&TrainMovementEventMessage {
            status: "cancelled".to_string(),
            ..event.clone()
        }));
        assert!(needs_subscription_write(&TrainMovementEventMessage {
            msg_type: "0005".to_string(),
            ..event
        }));
    }

    /// Security review L1: both outcomes exist at 0 from startup.
    #[test]
    fn register_metrics_exports_both_outcomes_at_zero() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, register_metrics);
        let text = handle.render();
        for outcome in ["applied", "rejected"] {
            let line = format!(
                "{}{{outcome=\"{outcome}\"}} 0",
                common::metrics::metric_name(OUTBOX_METRIC)
            );
            assert!(text.contains(&line), "{line} not in {text}");
        }
    }
}

#[cfg(test)]
mod db_tests {
    //! Security review L1 (2026-10-08), against a live database
    //! (`DATABASE_URL`, `--ignored`). Each test files rows under its own
    //! fixture user's subscriptions and deletes them.
    use super::*;
    use crate::test_support::{
        cleanup_user, connect, fixture_event, seed_tracked_train, seed_user,
    };

    fn user() -> String {
        format!("outbox-l1-{}", uuid_like())
    }

    fn uuid_like() -> String {
        use std::hash::{BuildHasher, Hasher};
        let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
        hasher.write_u128(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        );
        format!("{:012x}", hasher.finish() & 0xffff_ffff_ffff)
    }

    async fn file(
        pool: &PgPool,
        row_tracked_train_id: i64,
        event: &TrainMovementEventMessage,
    ) -> i64 {
        sqlx::query_scalar(
            "INSERT INTO train_event_outbox (tracked_train_id, dedup_key, event) \
             VALUES ($1, $2, $3) RETURNING id",
        )
        .bind(row_tracked_train_id)
        .bind(&event.dedup_key)
        .bind(sqlx::types::Json(event))
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn state(pool: &PgPool, id: i64) -> Option<(i32, Option<String>)> {
        sqlx::query_as("SELECT attempts, rejection FROM train_event_outbox WHERE id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await
            .unwrap()
    }

    async fn delete_rows(pool: &PgPool, ids: &[i64]) {
        sqlx::query("DELETE FROM train_event_outbox WHERE id = ANY($1)")
            .bind(ids)
            .execute(pool)
            .await
            .unwrap();
    }

    /// A row filed under one subscription whose event names another is
    /// rejected unapplied; the rows behind it still apply.
    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
    async fn a_row_whose_event_names_another_subscription_is_rejected() {
        let pool = connect().await;
        let user = user();
        seed_user(&pool, &user).await;
        let mine = seed_tracked_train(&pool, &user).await;
        let other = seed_tracked_train(&pool, &user).await;
        let forged = file(&pool, mine, &fixture_event(other, &format!("{user}-a"))).await;
        let honest = file(&pool, mine, &fixture_event(mine, &format!("{user}-b"))).await;

        let tick = apply_train_event_outbox(&pool).await.unwrap();
        assert!(tick.rejected_other >= 1, "{tick:?}");
        let (_, rejection) = state(&pool, forged).await.expect("left in the table");
        assert!(
            rejection
                .as_deref()
                .is_some_and(|r| r.contains("tracked_train_id mismatch")),
            "{rejection:?}"
        );
        assert_eq!(state(&pool, honest).await, None, "applied and deleted");

        delete_rows(&pool, &[forged]).await;
        cleanup_user(&pool, &user).await;
    }

    /// A row that fails with an error that is not a data error (here a lock
    /// timeout on its subscription) counts an attempt and holds the rows
    /// behind it for that tick only; at `max_attempts` it is rejected and
    /// they apply. Rejected rows past the retention are pruned.
    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL set and --ignored"]
    async fn a_row_failing_every_tick_is_rejected_after_max_attempts() {
        let pool = connect().await;
        let user = user();
        seed_user(&pool, &user).await;
        let stuck_sub = seed_tracked_train(&pool, &user).await;
        let next_sub = seed_tracked_train(&pool, &user).await;
        let resolution = TrainMovementEventMessage {
            resolved_train_id: Some("1A23".to_string()),
            ..fixture_event(stuck_sub, &format!("{user}-stuck"))
        };
        let stuck = file(&pool, stuck_sub, &resolution).await;
        let next = file(
            &pool,
            next_sub,
            &fixture_event(next_sub, &format!("{user}-next")),
        )
        .await;

        // Hold the subscription's row lock; the applier gives up after 200 ms.
        let mut holder = pool.begin().await.unwrap();
        sqlx::query("SELECT 1 FROM train_subscriptions WHERE id = $1 FOR UPDATE")
            .bind(stuck_sub)
            .execute(&mut *holder)
            .await
            .unwrap();
        let impatient = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .after_connect(|conn, _| {
                Box::pin(async move {
                    sqlx::query("SET lock_timeout = '200ms'")
                        .execute(conn)
                        .await?;
                    Ok(())
                })
            })
            .connect(&std::env::var("DATABASE_URL").unwrap())
            .await
            .unwrap();
        let policy = OutboxPolicy {
            max_attempts: 2,
            ..OutboxPolicy::default()
        };

        let first = apply_train_event_outbox_with(&impatient, &policy).await;
        assert!(first.is_err(), "{first:?}");
        assert_eq!(state(&pool, stuck).await, Some((1, None)));
        assert!(
            state(&pool, next).await.is_some(),
            "held behind it this tick"
        );

        let second = apply_train_event_outbox_with(&impatient, &policy)
            .await
            .unwrap();
        assert!(second.rejected_other >= 1, "{second:?}");
        let (attempts, rejection) = state(&pool, stuck).await.expect("left in the table");
        assert_eq!(attempts, 2);
        assert!(
            rejection
                .as_deref()
                .is_some_and(|r| r.starts_with("failed 2 times")),
            "{rejection:?}"
        );
        assert_eq!(
            state(&pool, next).await,
            None,
            "applied once the stuck row was rejected"
        );
        holder.rollback().await.unwrap();

        // Pruned once older than the retention.
        sqlx::query(
            "UPDATE train_event_outbox SET rejected_at = now() - interval '15 days' WHERE id = $1",
        )
        .bind(stuck)
        .execute(&pool)
        .await
        .unwrap();
        let third = apply_train_event_outbox(&pool).await.unwrap();
        assert!(third.pruned >= 1, "{third:?}");
        assert_eq!(state(&pool, stuck).await, None);

        cleanup_user(&pool, &user).await;
    }
}
