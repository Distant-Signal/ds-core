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

/// What one [`apply_train_event_outbox`] call did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct OutboxTick {
    /// Rows applied and deleted.
    pub applied: u64,
    /// Rows refused for a data error, left in the table with
    /// `rejected_at` and `rejection`.
    pub rejected: Vec<RejectedTrustBacklogRow>,
}

#[derive(sqlx::FromRow)]
struct OutboxRow {
    id: i64,
    event: sqlx::types::Json<TrainMovementEventMessage>,
    forward_signal: Option<sqlx::types::Json<TrainForwardSignalMessage>>,
}

/// The ingest-writer's `train_event_outbox` loop body: up to [`APPLY_BATCH`]
/// pending rows, oldest first, in one transaction. Each row runs
/// [`upsert_train_event_on`] (and queues its forward signal) behind its own
/// savepoint, then is deleted. A data error (SQLSTATE class 22/23) rolls
/// back exactly that row's writes and marks it rejected; any other error
/// rolls the whole call back, to be retried next tick. Re-applying an event
/// is harmless (every write is idempotent), so a redelivered entry that
/// lands here again after its first row was applied changes nothing.
pub async fn apply_train_event_outbox(pool: &PgPool) -> anyhow::Result<OutboxTick> {
    let mut tick = OutboxTick::default();
    let mut tx = pool.begin().await?;
    let rows: Vec<OutboxRow> = sqlx::query_as(
        "SELECT id, event, forward_signal FROM train_event_outbox \
         WHERE rejected_at IS NULL ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED",
    )
    .bind(APPLY_BATCH)
    .fetch_all(&mut *tx)
    .await?;
    for row in rows {
        let event = row.event.0;
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
                let Some(data_error) = crate::backlog::classify_anyhow_data_error(&err) else {
                    return Err(err);
                };
                savepoint.rollback().await?;
                let rejected = data_error.into_rejected_row(
                    usize::try_from(row.id).unwrap_or(usize::MAX),
                    &event.dedup_key,
                );
                tracing::error!(
                    outbox_id = row.id,
                    tracked_train_id = event.tracked_train_id,
                    dedup_key = %event.dedup_key,
                    sqlstate = %rejected.sqlstate,
                    message = %rejected.message,
                    event = ?event,
                    "train-event outbox row refused for a data error; left in the table"
                );
                sqlx::query(
                    "UPDATE train_event_outbox SET rejected_at = now(), rejection = $2 WHERE id = $1",
                )
                .bind(row.id)
                .bind(format!(
                    "{} {} (constraint {}): {}",
                    rejected.sqlstate,
                    rejected.reason,
                    rejected.constraint.as_deref().unwrap_or("-"),
                    rejected.message
                ))
                .execute(&mut *tx)
                .await?;
                tick.rejected.push(rejected);
            }
        }
    }
    tx.commit().await?;
    metrics::counter!(common::metrics::metric_name(OUTBOX_METRIC), "outcome" => "applied")
        .increment(tick.applied);
    metrics::counter!(common::metrics::metric_name(OUTBOX_METRIC), "outcome" => "rejected")
        .increment(tick.rejected.len() as u64);
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
}
