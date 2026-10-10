//! The TRUST event backlog: `trust_event_backlog.rs` (this module) and
//! `trust_event_backlog_match.rs` ([`matching`]), whole, and the
//! `train_reasons` writers ([`reasons`]: `upsert_reasons`, `reason_text`,
//! `reason_fields`). The api's modules of the same names re-export them.
//!
//! Storage for `trust_event_backlog`
//! (docs/superpowers/plans/2026-09-05-trust-event-backlog-plan.md). Write
//! side only -- Task 5 (`schedule_matching.rs` or a new sibling module)
//! owns the read/consumption side.

pub mod matching;
pub mod reasons;

use std::collections::{HashMap, HashSet};

use anyhow::Context;
use chrono::NaiveDate;
use common::TrustBacklogEventMessage;
use sqlx::PgPool;
use trust_schema::journey::{self, DerivedState};
use trust_schema::schema::Movement;

/// What [`upsert_trust_event_backlog_batch`] did with one batch.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct BacklogBatchOutcome {
    /// Rows this call actually inserted -- not the batch length: a
    /// redelivered batch legitimately inserts 0.
    pub inserted: u64,
    /// Rows refused because of a data error. Empty on the fast path.
    pub rejected: Vec<common::RejectedTrustBacklogRow>,
}

/// Blind, at-least-once-safe batch insert -- `ON CONFLICT DO NOTHING` on
/// `dedup_key` (the same posture `train_movement_events` already uses for
/// the same reason: Redis Streams' own at-least-once delivery means a
/// redelivered batch after a crash-before-XACK is expected, not
/// exceptional).
///
/// **One bad row no longer fails the batch.** The whole batch is first
/// inserted in one UNNEST statement (DB2-4). If that fails with a
/// *data* error (see [`classify_data_error`]: a constraint violation or
/// invalid input, which no retry can ever fix), the batch is inserted again
/// row by row, each row behind its own savepoint, so every valid row lands
/// and only the offending rows come back in
/// [`BacklogBatchOutcome::rejected`]. Before this, a batch holding one row
/// the table's `msg_type` CHECK refused (TRUST `0005`, before migration
/// 20260926210000) failed as a whole; trust-backlog-consumer never `XACKed`
/// it, and XAUTOCLAIM replayed the same failing batch every 30 seconds,
/// holding more than a thousand valid rows hostage until the capped stream
/// trimmed them.
///
/// Any other error -- a dropped connection, a pool timeout, a serialization
/// failure, a lock or statement timeout, or an unexpected SQLSTATE -- still
/// fails the whole call (with nothing committed), so the caller answers 500
/// and the consumer retries the batch later.
pub async fn upsert_trust_event_backlog_batch(
    pool: &PgPool,
    events: &[TrustBacklogEventMessage],
) -> anyhow::Result<BacklogBatchOutcome> {
    match insert_batch_in_one_statement(pool, events).await {
        Ok(inserted) => Ok(BacklogBatchOutcome {
            inserted,
            rejected: Vec::new(),
        }),
        Err(err) if classify_data_error(&err).is_some() => {
            tracing::warn!(
                error = %err,
                batch_len = events.len(),
                "trust-event-backlog batch insert hit a data error; retrying row by row so the valid rows land"
            );
            insert_rows_individually(pool, events).await
        }
        Err(err) => Err(err.into()),
    }
}

/// The whole write of one `trust-backlog-consumer` batch, as
/// `POST /private/trust-event-backlog` does it and as the consumer's
/// direct DB sink does it (ingest architecture plan 3b.1; moved here from
/// the api's handler so both are this one function):
///
/// 1. [`upsert_trust_event_backlog_batch`]. Each row it refuses for a data
///    error is logged with its event and counted in
///    `api_trust_event_backlog_rejected_rows_total{reason}`.
/// 2. [`ingest_shared_movements_batch`] for the rows step 1 accepted only
///    (API-4): a refused row must not go on to create `trains` rows or
///    movements.
/// 3. PL-7: a data error on one event there is reported in `rejected` too
///    (its message prefixed `shared movement write: `), while a transient
///    error fails the whole call, so the caller retries the batch. The
///    retry is safe: committed backlog rows conflict harmlessly on
///    `dedup_key`, and every shared write is idempotent too. Both count in
///    `api_trust_event_backlog_shared_movement_errors_total{class}`.
///
/// `rejected` is sorted by index. `inserted` is step 1's insert count.
pub async fn ingest_trust_event_backlog(
    pool: &PgPool,
    events: &[TrustBacklogEventMessage],
) -> anyhow::Result<BacklogBatchOutcome> {
    let outcome = upsert_trust_event_backlog_batch(pool, events).await?;
    for rejected in &outcome.rejected {
        tracing::warn!(
            index = rejected.index,
            dedup_key = %rejected.dedup_key,
            sqlstate = %rejected.sqlstate,
            reason = %rejected.reason,
            constraint = ?rejected.constraint,
            message = %rejected.message,
            event = ?events.get(rejected.index),
            "rejected trust-event-backlog row; inserted the rest of its batch"
        );
        metrics::counter!(
            common::metrics::metric_name("api_trust_event_backlog_rejected_rows_total"),
            "reason" => rejected.reason.clone()
        )
        .increment(1);
    }

    let already_rejected: HashSet<usize> = outcome.rejected.iter().map(|r| r.index).collect();
    let accepted: Vec<usize> = (0..events.len())
        .filter(|index| !already_rejected.contains(index))
        .collect();
    let accepted_events: Vec<TrustBacklogEventMessage> = accepted
        .iter()
        .map(|&index| events[index].clone())
        .collect();
    let shared_movement_results = ingest_shared_movements_batch(pool, &accepted_events).await;
    let mut rejected = outcome.rejected;
    let mut transient: Option<anyhow::Error> = None;
    for (&index, result) in accepted.iter().zip(shared_movement_results) {
        let Err(err) = result else { continue };
        let event = &events[index];
        if let Some(data_error) = classify_anyhow_data_error(&err) {
            tracing::warn!(
                index,
                error = ?err,
                train_id = %event.train_id,
                event = ?event,
                "shared movement write rejected this backlog event for a data error"
            );
            metrics::counter!(
                common::metrics::metric_name("api_trust_event_backlog_shared_movement_errors_total"),
                "class" => "data"
            )
            .increment(1);
            let mut row = data_error.into_rejected_row(index, &event.dedup_key);
            row.message = format!("shared movement write: {}", row.message);
            rejected.push(row);
        } else {
            tracing::error!(
                index,
                error = ?err,
                train_id = %event.train_id,
                "shared movement write failed transiently; failing the batch so it is retried"
            );
            metrics::counter!(
                common::metrics::metric_name("api_trust_event_backlog_shared_movement_errors_total"),
                "class" => "transient"
            )
            .increment(1);
            transient.get_or_insert(err);
        }
    }
    if let Some(err) = transient {
        return Err(err);
    }
    rejected.sort_by_key(|row| row.index);
    Ok(BacklogBatchOutcome {
        inserted: outcome.inserted,
        rejected,
    })
}

fn insert_backlog_row(
    event: &TrustBacklogEventMessage,
) -> sqlx::query::Query<'_, sqlx::Postgres, sqlx::postgres::PgArguments> {
    sqlx::query(
        "INSERT INTO trust_event_backlog \
            (crs, train_uid, train_id, service_date, msg_type, event_type, \
             planned_timestamp, actual_timestamp, variation_status, delay_minutes, dedup_key, \
             gbtt_timestamp) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) \
         ON CONFLICT (dedup_key) DO NOTHING",
    )
    .bind(&event.crs)
    .bind(&event.train_uid)
    .bind(&event.train_id)
    .bind(event.service_date)
    .bind(&event.msg_type)
    .bind(&event.event_type)
    .bind(event.planned_timestamp)
    .bind(event.actual_timestamp)
    .bind(&event.variation_status)
    .bind(event.delay_minutes)
    .bind(&event.dedup_key)
    .bind(event.gbtt_timestamp)
}

/// The fast path: the whole batch in one `INSERT ... SELECT FROM UNNEST`
/// (DB2-4), so a 100-1000 row batch is one round trip rather than one per
/// row. One statement is also all-or-nothing, as the old single
/// transaction was. `ON CONFLICT (dedup_key) DO NOTHING` also skips a
/// duplicate key repeated within the same batch.
#[expect(
    clippy::similar_names,
    reason = "the similar names are distinct domain terms"
)]
async fn insert_batch_in_one_statement(
    pool: &PgPool,
    events: &[TrustBacklogEventMessage],
) -> Result<u64, sqlx::Error> {
    if events.is_empty() {
        return Ok(0);
    }
    let crs: Vec<Option<&str>> = events.iter().map(|e| e.crs.as_deref()).collect();
    let train_uid: Vec<Option<&str>> = events.iter().map(|e| e.train_uid.as_deref()).collect();
    let train_id: Vec<&str> = events.iter().map(|e| e.train_id.as_str()).collect();
    let service_date: Vec<NaiveDate> = events.iter().map(|e| e.service_date).collect();
    let msg_type: Vec<&str> = events.iter().map(|e| e.msg_type.as_str()).collect();
    let event_type: Vec<Option<&str>> = events.iter().map(|e| e.event_type.as_deref()).collect();
    let planned: Vec<Option<chrono::DateTime<chrono::Utc>>> =
        events.iter().map(|e| e.planned_timestamp).collect();
    let actual: Vec<Option<chrono::DateTime<chrono::Utc>>> =
        events.iter().map(|e| e.actual_timestamp).collect();
    let variation: Vec<Option<&str>> = events
        .iter()
        .map(|e| e.variation_status.as_deref())
        .collect();
    let delay: Vec<Option<i32>> = events.iter().map(|e| e.delay_minutes).collect();
    let dedup_key: Vec<&str> = events.iter().map(|e| e.dedup_key.as_str()).collect();
    let gbtt: Vec<Option<chrono::DateTime<chrono::Utc>>> =
        events.iter().map(|e| e.gbtt_timestamp).collect();
    let result = sqlx::query(
        "INSERT INTO trust_event_backlog \
            (crs, train_uid, train_id, service_date, msg_type, event_type, \
             planned_timestamp, actual_timestamp, variation_status, delay_minutes, dedup_key, \
             gbtt_timestamp) \
         SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::date[], $5::text[], \
                              $6::text[], $7::timestamptz[], $8::timestamptz[], $9::text[], \
                              $10::int4[], $11::text[], $12::timestamptz[]) \
         ON CONFLICT (dedup_key) DO NOTHING",
    )
    .bind(&crs)
    .bind(&train_uid)
    .bind(&train_id)
    .bind(&service_date)
    .bind(&msg_type)
    .bind(&event_type)
    .bind(&planned)
    .bind(&actual)
    .bind(&variation)
    .bind(&delay)
    .bind(&dedup_key)
    .bind(&gbtt)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// The fallback path: still one transaction, but each row behind its own
/// savepoint, so a data error rolls back only that row. A non-data error
/// returns straight away, dropping (and so rolling back) the transaction:
/// the caller retries the whole batch, and `ON CONFLICT (dedup_key) DO
/// NOTHING` makes that retry safe either way.
async fn insert_rows_individually(
    pool: &PgPool,
    events: &[TrustBacklogEventMessage],
) -> anyhow::Result<BacklogBatchOutcome> {
    let mut outcome = BacklogBatchOutcome::default();
    let mut tx = pool.begin().await?;
    for (index, event) in events.iter().enumerate() {
        sqlx::query("SAVEPOINT backlog_row")
            .execute(&mut *tx)
            .await?;
        match insert_backlog_row(event).execute(&mut *tx).await {
            Ok(result) => {
                sqlx::query("RELEASE SAVEPOINT backlog_row")
                    .execute(&mut *tx)
                    .await?;
                outcome.inserted += result.rows_affected();
            }
            Err(err) => {
                let Some(data_error) = classify_data_error(&err) else {
                    return Err(err.into());
                };
                sqlx::query("ROLLBACK TO SAVEPOINT backlog_row")
                    .execute(&mut *tx)
                    .await?;
                outcome
                    .rejected
                    .push(data_error.into_rejected_row(index, &event.dedup_key));
            }
        }
    }
    tx.commit().await?;
    Ok(outcome)
}

/// A Postgres error caused by the row itself rather than by the database
/// or the connection -- see [`classify_data_error`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataError {
    pub sqlstate: String,
    pub reason: &'static str,
    pub constraint: Option<String>,
    pub message: String,
}

impl DataError {
    /// The wire shape a route reports this row's rejection in.
    pub fn into_rejected_row(
        self,
        index: usize,
        dedup_key: &str,
    ) -> common::RejectedTrustBacklogRow {
        common::RejectedTrustBacklogRow {
            index,
            dedup_key: dedup_key.to_string(),
            sqlstate: self.sqlstate,
            reason: self.reason.to_string(),
            constraint: self.constraint,
            message: self.message,
        }
    }
}

/// [`classify_data_error`] for an `anyhow::Error` from the data layer: walks
/// the error chain for the underlying `sqlx::Error` (or a
/// [`SharedMovementError`], which carries its classification with it).
/// `None` -- "transient, fail the request so the caller retries" -- for
/// anything else, including an error with no database cause at all.
pub fn classify_anyhow_data_error(err: &anyhow::Error) -> Option<DataError> {
    err.chain().find_map(|cause| {
        if let Some(sqlx_err) = cause.downcast_ref::<sqlx::Error>() {
            return classify_data_error(sqlx_err);
        }
        cause
            .downcast_ref::<SharedMovementError>()
            .and_then(|shared| shared.data_error.clone())
    })
}

/// One failure of a batched shared-movement step, reported against every
/// event that step covered. An `anyhow::Error` cannot be cloned, so the
/// fan-out keeps the message and -- what the route actually needs -- the
/// data-vs-transient classification of the original error (PL-7).
#[derive(Debug)]
pub struct SharedMovementError {
    message: String,
    data_error: Option<DataError>,
}

impl SharedMovementError {
    fn new(step: &str, err: &anyhow::Error) -> Self {
        Self {
            message: format!("{step} failed: {err:#}"),
            data_error: classify_anyhow_data_error(err),
        }
    }
}

impl std::fmt::Display for SharedMovementError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SharedMovementError {}

/// `Some` only for a data error: SQLSTATE class 23 (integrity constraint
/// violation: check, not-null, unique, foreign-key, exclusion) or class 22
/// (data exception: invalid text representation, out-of-range value, a NUL
/// byte in text, and so on). Resending such a row can never succeed.
///
/// Everything else is `None` and must fail the request so the consumer
/// retries: connection and I/O errors, pool timeouts, class 40
/// (serialization failure, deadlock), class 57 (query canceled / statement
/// timeout, admin shutdown), 55P03 (lock timeout), class 53 (out of
/// memory/disk), and any SQLSTATE this function does not expect, such as a
/// class 42 schema mismatch during a rolling deploy.
pub fn classify_data_error(err: &sqlx::Error) -> Option<DataError> {
    let sqlx::Error::Database(db) = err else {
        return None;
    };
    let sqlstate = db.code()?.into_owned();
    let reason = data_error_reason(&sqlstate)?;
    Some(DataError {
        reason,
        constraint: db.constraint().map(str::to_string),
        message: db.message().to_string(),
        sqlstate,
    })
}

/// Maps a class 22/23 SQLSTATE to its Postgres condition name (from the
/// Postgres "Error Codes" appendix); `None` for any other class. Unlisted
/// codes in either class fall back to the class name, so the result is
/// always one of a fixed set -- it is used as a metric label.
fn data_error_reason(sqlstate: &str) -> Option<&'static str> {
    let reason = match sqlstate {
        "23000" => "integrity_constraint_violation",
        "23001" => "restrict_violation",
        "23502" => "not_null_violation",
        "23503" => "foreign_key_violation",
        "23505" => "unique_violation",
        "23514" => "check_violation",
        "23P01" => "exclusion_violation",
        "22001" => "string_data_right_truncation",
        "22003" => "numeric_value_out_of_range",
        "22007" => "invalid_datetime_format",
        "22008" => "datetime_field_overflow",
        "22021" => "character_not_in_repertoire",
        "22P02" => "invalid_text_representation",
        "22P05" => "untranslatable_character",
        code if code.starts_with("23") => "integrity_constraint_violation",
        code if code.starts_with("22") => "data_exception",
        _ => return None,
    };
    Some(reason)
}

/// Mirrors one `TrustBacklogEventMessage` onto the shared `trains`/
/// `train_movement_events`/`train_current_state` tables -- the concrete
/// wiring behind "trust-backlog-consumer becomes the primary movement-event
/// writer" (docs/superpowers/specs/2026-09-06-shared-train-identity-design.md
/// §3). A deliberate no-op, not an error, whenever `event.train_uid` is
/// `None` -- this process never saw the Activation for this `train_id`
/// (Task 13's own named, accepted gap), so there is no natural key to
/// create or find a `trains` row by. Independent of, and additional to,
/// this same batch's existing `upsert_trust_event_backlog_batch` write --
/// `trust_event_backlog` itself is a separate, parallel system (this
/// plan's Global Constraints).
pub async fn ingest_shared_movement(
    pool: &PgPool,
    event: &TrustBacklogEventMessage,
) -> anyhow::Result<()> {
    // Thin wrapper over the batch-shaped implementation, called with a
    // batch of one -- see `ingest_shared_movements_batch`'s own doc
    // comment for why that function exists and how it preserves this
    // function's exact single-event behaviour. `results` always has
    // exactly one entry: `ingest_shared_movements_batch` never shrinks or
    // grows its output relative to its input.
    let mut results = ingest_shared_movements_batch(pool, std::slice::from_ref(event)).await;
    results.remove(0)
}

/// Batch-shaped sibling of [`ingest_shared_movement`] -- same per-event
/// behaviour (including the `event.train_uid == None` no-op documented on
/// that function), but collapses the once-per-event
/// `find_or_create_train`/`mark_train_resolved`/`fetch_previous_derived_state`
/// round trips into ONE multi-row query each for the WHOLE incoming
/// `events` slice, rather than one query per event. For `N` events over
/// `M` distinct `(train_uid, service_date)` pairs, the old per-event loop
/// cost `5*N` sequential round trips (`find_or_create_train`,
/// `mark_train_resolved`, `fetch_previous_derived_state`, and
/// `upsert_train_movement`'s own two writes -- all five, every event, even
/// when M events share an identity and each hits the SAME upsert target
/// again); this costs at most `3 + 2*N` in the common, fully-successful
/// case: 3 batched round trips total for identity resolution and the
/// previous-state fetch (regardless of N or M), plus the two final writes
/// (`train_movement_events`/`train_current_state`) still done once per
/// event, deliberately -- see below.
///
/// Returns one `Result` per input event, same length and order as
/// `events`. This is what lets `post_trust_event_backlog` keep its
/// "one bad event doesn't kill the whole batch" contract: it classifies
/// each `Err` with [`classify_anyhow_data_error`] (PL-7) -- a data error is
/// reported as that row's rejection, anything else fails the request so
/// the consumer retries. Errors fanned out from one batched step to several
/// events are [`SharedMovementError`]s, which keep that classification.
///
/// The one place collapsing these round trips changes that isolation
/// story: the two batched *write* queries
/// (`trains::find_or_create_trains_batch`, `trains::mark_trains_resolved_batch`)
/// are each ONE SQL statement covering every event's identity in this
/// batch, so a single malformed row (a `train_uid` violating some column
/// constraint, say) can fail that whole statement, where the old per-event
/// `INSERT`/`UPDATE` would only ever have failed for that one event. Both
/// calls are wrapped in a fallback here: on error, this function retries
/// that step the OLD way, one row at a time, so a genuinely bad row still
/// only fails the event(s) that reference it -- full isolation is
/// preserved, just at the cost of the old per-event round trips again, and
/// only on this already-abnormal path. The final two writes are left
/// per-event exactly as before (via [`crate::tracking::upsert_train_movement`]),
/// since isolation was never at risk there to begin with.
///
/// Intra-batch causality for `fetch_previous_derived_state` is preserved
/// too, not merely approximated: the one batched SELECT only seeds an
/// in-memory map with each distinct `trains_id`'s state as it exists in
/// the database BEFORE this call; as events are then processed one at a
/// time in their original order, each event's derived state is both read
/// from AND written back into that same in-memory map before the next
/// event is considered -- so a second event in this batch for the same
/// `trains_id` sees the FIRST event's just-computed state, exactly as it
/// would if `fetch_previous_derived_state` had been re-run against the
/// database after the first event's write (which is what the old
/// sequential loop actually did).
#[expect(
    clippy::expect_used,
    clippy::too_many_lines,
    reason = "the invariant is established just above; the expect message names it; long but linear; splitting it would scatter its shared state across helpers"
)]
pub async fn ingest_shared_movements_batch(
    pool: &PgPool,
    events: &[TrustBacklogEventMessage],
) -> Vec<anyhow::Result<()>> {
    let mut results: Vec<anyhow::Result<()>> = (0..events.len()).map(|_| Ok(())).collect();

    // Step 0: give each uid-less event the identity of its train's
    // Activation, when exactly one is on record (see `infer_train_identities`).
    // A failed lookup fails only those events, as a transient error, so the
    // consumer retries the batch rather than dropping them.
    let enriched: Vec<TrustBacklogEventMessage>;
    let events = match infer_train_identities(pool, events).await {
        Ok(inferred) if inferred.is_empty() => events,
        Ok(inferred) => {
            enriched = events
                .iter()
                .enumerate()
                .map(|(i, event)| match inferred.get(&i) {
                    Some((train_uid, service_date)) => TrustBacklogEventMessage {
                        train_uid: Some(train_uid.clone()),
                        service_date: *service_date,
                        ..event.clone()
                    },
                    None => event.clone(),
                })
                .collect();
            &enriched
        }
        Err(err) => {
            for (i, event) in events.iter().enumerate() {
                if needs_identity(event) {
                    results[i] =
                        Err(SharedMovementError::new("infer_train_identities", &err).into());
                }
            }
            events
        }
    };

    let known_indices: Vec<usize> = events
        .iter()
        .enumerate()
        .filter(|(i, _)| results[*i].is_ok())
        .filter_map(|(i, e)| e.train_uid.as_ref().map(|_| i))
        .collect();
    // A Reinstatement whose Activation this consumer never parked (no
    // `train_uid`) can still name the shared row by the TRUST `train_id`
    // a live resolution already wrote onto it. Without this, a train
    // cancelled and then reinstated after trust-backlog-consumer lost its
    // parked Activation stayed "cancelled" in `train_current_state`, and
    // its subscriptions stayed closed (H4 residual, 2026-10-01).
    let reinstated_by_train_id = match trains_for_uidless_reinstatements(pool, events).await {
        Ok(found) => found,
        Err(err) => {
            tracing::warn!(
                error = ?err,
                "train_id lookup for uid-less reinstatements failed; they stay unapplied"
            );
            Vec::new()
        }
    };
    if known_indices.is_empty() && reinstated_by_train_id.is_empty() {
        return results;
    }

    // Step 1: find_or_create_train, batched across every DISTINCT
    // (train_uid, service_date) pair this POST's batch actually needs.
    let mut pair_order: Vec<(String, NaiveDate)> = Vec::new();
    let mut seen_pairs: HashSet<(String, NaiveDate)> = HashSet::new();
    for &i in &known_indices {
        let key = (
            events[i]
                .train_uid
                .clone()
                .expect("known_indices only contains Some(train_uid) events"),
            events[i].service_date,
        );
        if seen_pairs.insert(key.clone()) {
            pair_order.push(key);
        }
    }

    let id_map = if let Ok(map) =
        crate::trains::find_or_create_trains_batch(pool, &pair_order).await
    {
        map
    } else {
        // Fallback: the batched INSERT failed outright (e.g. one bad
        // row in an otherwise-fine batch) -- retry one pair at a time,
        // the old way, so only the pair(s) that actually fail take
        // down the event(s) that reference them.
        let mut map = HashMap::new();
        for pair in &pair_order {
            match crate::trains::find_or_create_train(pool, &pair.0, pair.1).await {
                Ok(id) => {
                    map.insert(pair.clone(), id);
                }
                Err(err) => {
                    for &i in &known_indices {
                        if events[i].train_uid.as_deref() == Some(pair.0.as_str())
                            && events[i].service_date == pair.1
                        {
                            results[i] =
                                Err(SharedMovementError::new("find_or_create_train", &err).into());
                        }
                    }
                }
            }
        }
        map
    };

    // Every known-train_uid event whose identity resolved to a trains_id.
    let mut resolved: Vec<(usize, i64)> = Vec::new();
    for &i in &known_indices {
        if results[i].is_err() {
            continue;
        }
        let key = (
            events[i]
                .train_uid
                .clone()
                .expect("known_indices only contains Some(train_uid) events"),
            events[i].service_date,
        );
        match id_map.get(&key) {
            Some(&id) => resolved.push((i, id)),
            None => {
                results[i] = Err(anyhow::anyhow!(
                    "find_or_create_train did not resolve an id for this event's identity"
                ));
            }
        }
    }

    // Step 2: mark_train_resolved, batched -- deduped to one row per
    // trains_id, keeping the LAST event's train_id in batch order (the
    // same final value the old sequential-overwrite loop would leave,
    // since each call plainly overwrites the column).
    let mut resolve_map: HashMap<i64, String> = HashMap::new();
    for &(i, trains_id) in &resolved {
        resolve_map.insert(trains_id, events[i].train_id.clone());
    }
    let resolve_pairs: Vec<(i64, String)> = resolve_map.into_iter().collect();
    if crate::trains::mark_trains_resolved_batch(pool, &resolve_pairs)
        .await
        .is_err()
    {
        // Fallback: same reasoning as Step 1's -- retry one pair at a
        // time so a single bad row doesn't fail every event in the batch.
        for (trains_id, train_id) in &resolve_pairs {
            if let Err(err) = crate::trains::mark_train_resolved(pool, *trains_id, train_id).await {
                for &(i, tid) in &resolved {
                    if tid == *trains_id {
                        results[i] =
                            Err(SharedMovementError::new("mark_train_resolved", &err).into());
                    }
                }
            }
        }
    }

    // Only events whose identity resolution AND resolution-mark both
    // succeeded continue past this point -- matching the original
    // `find_or_create_train(...).await?; mark_train_resolved(...).await?;`
    // early-return-on-error shape.
    let mut active: Vec<(usize, i64)> = resolved
        .into_iter()
        .filter(|&(i, _)| results[i].is_ok())
        .collect();
    // Already-identified rows: nothing to create or mark, so they join
    // here, in batch order with the rest.
    active.extend(reinstated_by_train_id);
    active.sort_by_key(|&(i, _)| i);
    if active.is_empty() {
        return results;
    }

    // Step 3: fetch_previous_derived_state, batched into one SELECT
    // covering every DISTINCT trains_id this batch will touch.
    let mut distinct_trains_ids: Vec<i64> = Vec::new();
    let mut seen_ids: HashSet<i64> = HashSet::new();
    for &(_, trains_id) in &active {
        if seen_ids.insert(trains_id) {
            distinct_trains_ids.push(trains_id);
        }
    }
    let mut state_map =
        if let Ok(map) = fetch_previous_derived_states_batch(pool, &distinct_trains_ids).await {
            map
        } else {
            // Fallback: same reasoning as Steps 1-2's -- this is a read,
            // not a write, so there's no data-shape reason it should ever
            // fail differently per row, but the fallback costs nothing to
            // keep for symmetry and defense-in-depth.
            let mut map = HashMap::new();
            for &id in &distinct_trains_ids {
                match fetch_previous_derived_state(pool, id).await {
                    Ok(state) => {
                        map.insert(id, state);
                    }
                    Err(err) => {
                        for &(i, tid) in &active {
                            if tid == id {
                                results[i] = Err(SharedMovementError::new(
                                    "fetch_previous_derived_state",
                                    &err,
                                )
                                .into());
                            }
                        }
                    }
                }
            }
            map
        };

    // Step 3.5: destination_crs, batched the same way as Step 3 -- feeds
    // confirmed-terminus-ARRIVAL detection below
    // (`trust_schema::journey::apply_movement`'s `destination_crs` param).
    // A read failure here is non-fatal and NOT threaded into `results`:
    // this data only refines status detection, it never gates whether the
    // event itself is written, so a lookup miss degrades gracefully to
    // "destination unknown" (the same as if no schedule had ever matched)
    // rather than failing the whole batch. It is logged, though (DB2-5): a
    // connection error used to look exactly like "no destination known".
    let destination_map =
        match crate::trains::destination_crs_for_trains_batch(pool, &distinct_trains_ids).await {
            Ok(map) => map,
            Err(err) => {
                tracing::warn!(
                    error = ?err,
                    trains = distinct_trains_ids.len(),
                    "destination_crs lookup failed; terminus ARRIVAL detection is degraded \
                     for this batch"
                );
                HashMap::new()
            }
        };

    // Step 4: derive + write, one event at a time, in original batch
    // order -- preserving both intra-batch causality (see this function's
    // own doc comment) and the existing per-event isolation for the two
    // final writes.
    for &(i, trains_id) in &active {
        if results[i].is_err() {
            continue;
        }
        let event = &events[i];
        let previous = state_map
            .get(&trains_id)
            .cloned()
            .unwrap_or_else(DerivedState::awaiting_activation);

        let derived = match event.msg_type.as_str() {
            "0003" => {
                let movement = Movement {
                    train_id: event.train_id.clone(),
                    event_type: event.event_type.clone().unwrap_or_default(),
                    gbtt_timestamp: event
                        .gbtt_timestamp
                        .map(|t| t.timestamp_millis().to_string()),
                    planned_timestamp: event
                        .planned_timestamp
                        .map(|t| t.timestamp_millis().to_string()),
                    actual_timestamp: event
                        .actual_timestamp
                        .map(|t| t.timestamp_millis().to_string()),
                    reporting_stanox: None,
                    loc_stanox: None,
                    toc_id: None,
                    variation_status: event.variation_status.clone(),
                    timetable_variation: None,
                };
                let destination_crs = destination_map.get(&trains_id).map(String::as_str);
                let mut derived = journey::apply_movement(
                    &previous,
                    &movement,
                    event.crs.as_deref(),
                    destination_crs,
                );
                if let (Some(p), Some(a), Some("LATE")) = (
                    event.planned_timestamp,
                    event.actual_timestamp,
                    event.variation_status.as_deref(),
                ) {
                    // Finding #4 (2026-09-25 review): guarded against a
                    // corrupt `actual_timestamp` producing an implausible
                    // delay -- see `common::trust_timestamp::plausible_delay_minutes`'s
                    // own doc comment for why `None` (keep `apply_movement`'s
                    // coarser, already-computed estimate) rather than
                    // clamping to a fabricated-but-bounded number.
                    if let Some(delay) = common::trust_timestamp::plausible_delay_minutes(a, p) {
                        derived.delay_minutes = Some(delay);
                    }
                }
                derived
            }
            "0002" => journey::apply_cancellation(&previous),
            // "0005" (Reinstatement, confirmed by the H4 fix of the
            // 2026-09-26 review): un-sticks a "cancelled" journey back to
            // "en_route" -- see `journey::apply_reinstatement`'s own doc
            // comment. Without this arm, a Reinstatement flowing through
            // this shared write path would silently fall into the `_ =>
            // continue` no-op below and never un-stick anything.
            "0005" => journey::apply_reinstatement(&previous),
            // "0001" (Activation) carries no derivable state of its own --
            // find_or_create_train/mark_train_resolved above already did
            // everything an Activation contributes to the shared row.
            _ => continue,
        };
        state_map.insert(trains_id, derived.clone());

        let movement_event = common::TrainMovementEventMessage {
            tracked_train_id: 0, // unused by upsert_train_movement -- see its own doc comment
            resolved_train_uid: None,
            resolved_train_id: None,
            identity_date: None,
            dedup_key: event.dedup_key.clone(),
            msg_type: event.msg_type.clone(),
            event_type: event.event_type.clone(),
            loc_stanox: None,
            loc_crs: event.crs.clone(),
            planned_timestamp: event.planned_timestamp,
            gbtt_timestamp: event.gbtt_timestamp,
            actual_timestamp: event.actual_timestamp,
            variation_status: event.variation_status.clone(),
            raw_body: serde_json::json!({}),
            status: derived.status,
            last_reported_location: derived.last_reported_location,
            last_event_type: derived.last_event_type,
            delay_minutes: derived.delay_minutes,
            next_calling_point: derived.next_calling_point,
            eta_next: None,
            eta_source: None,
        };
        if let Err(err) = write_movement_and_subscriptions(pool, trains_id, &movement_event).await {
            results[i] = Err(err);
        }
    }

    results
}

/// One backlog event's shared-table write and the subscription changes it
/// implies, in one transaction.
///
/// This path sees every Cancellation and Reinstatement, unlike
/// trust-consumer, which forwards one only for a train it still holds in
/// memory -- so this is what reopens a cancelled subscription after a
/// trust-consumer restart, and what closes the subscriptions of a train
/// whose Cancellation it stored first (trust-consumer's copy of that event
/// carries the same `dedup_key`, finds the row already stored and leaves
/// the subscriptions to this write; see `tracking::upsert_train_event_on`).
///
/// Both changes run only when the movement row is new. A replayed range
/// (a consumer-group rewind, a redelivered batch) re-sends events already
/// stored; re-running a replayed Reinstatement would reopen a subscription
/// that a later Cancellation of the same train had closed (2026-10-09
/// redelivery review). The movement row and the subscription changes
/// commit together, so a failure after the insert cannot leave the row
/// stored with its one-shot change skipped for good on the retry.
async fn write_movement_and_subscriptions(
    pool: &PgPool,
    trains_id: i64,
    movement_event: &common::TrainMovementEventMessage,
) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;
    let fresh =
        crate::tracking::upsert_train_movement_on(&mut tx, trains_id, movement_event).await?;
    if fresh {
        match movement_event.msg_type.as_str() {
            "0002" => {
                crate::tracking::mark_subscriptions_unresolved_on_cancellation(
                    &mut tx,
                    None,
                    Some(trains_id),
                )
                .await?;
            }
            "0005" => {
                crate::tracking::reopen_subscriptions_after_reinstatement(
                    &mut tx,
                    None,
                    Some(trains_id),
                )
                .await?;
            }
            _ => {}
        }
    }
    tx.commit().await?;
    Ok(())
}

/// A uid-less event the shared tables can only take once its train's
/// identity is known: anything but an Activation (which always carries its
/// own `train_uid`).
fn needs_identity(event: &TrustBacklogEventMessage) -> bool {
    event.train_uid.is_none() && event.msg_type != "0001"
}

/// The metric counting how each uid-less event's identity was found.
pub const UID_INFERRED_METRIC: &str = "api_trust_event_backlog_uid_inferred_total";

/// Registers [`UID_INFERRED_METRIC`] at 0 for every `source`.
pub fn register_uid_inference_metrics() {
    for source in ["activation", "none"] {
        metrics::counter!(common::metrics::metric_name(UID_INFERRED_METRIC), "source" => source)
            .increment(0);
    }
}

/// `event index -> (train_uid, service_date)` for each uid-less event (see
/// [`needs_identity`]) whose `train_id` has exactly one Activation (`0001`)
/// in `trust_event_backlog` with a `train_uid`, dated within a day of the
/// event's `service_date`; the Activation's own `service_date` replaces the
/// event's, as the consumer itself would have filed it.
///
/// **Why** (2026-10-01): trust-backlog-consumer keeps the `train_id ->
/// train_uid` map from each Activation in memory only, so after every
/// restart the Movements of every train activated before it arrived
/// uid-less, and this function's caller used to drop them without an
/// error. TRUST activates a train about an hour before it departs, so each
/// restart (08:50 that day) left `train_movement_events` with ~55 rows for
/// the next hour against the backlog's usual ~5.4k per 10 minutes. The
/// Activations themselves were in `trust_event_backlog` all along (it keeps
/// a day), and this batch's own rows are inserted before this runs, so an
/// Activation earlier in the same POST counts too.
///
/// TRUST recycles a `train_id` roughly monthly, so the date window is what
/// keeps an old Activation from matching; two candidates are ambiguous and
/// the event is left uid-less (and, as before, not written to the shared
/// tables). One batched query, served by `trust_event_backlog_train
/// (train_id, service_date)`.
async fn infer_train_identities(
    pool: &PgPool,
    events: &[TrustBacklogEventMessage],
) -> anyhow::Result<HashMap<usize, (String, NaiveDate)>> {
    let mut indices: Vec<i32> = Vec::new();
    let mut train_ids: Vec<&str> = Vec::new();
    let mut dates: Vec<NaiveDate> = Vec::new();
    for (i, event) in events.iter().enumerate() {
        if needs_identity(event) {
            indices.push(i32::try_from(i).context("batch index out of range")?);
            train_ids.push(&event.train_id);
            dates.push(event.service_date);
        }
    }
    if indices.is_empty() {
        return Ok(HashMap::new());
    }
    let rows: Vec<(i32, String, NaiveDate)> = sqlx::query_as(
        "SELECT u.idx, a.train_uid, a.service_date \
         FROM unnest($1::int4[], $2::text[], $3::date[]) AS u(idx, train_id, service_date) \
         CROSS JOIN LATERAL ( \
             SELECT DISTINCT b.train_uid, b.service_date \
             FROM trust_event_backlog b \
             WHERE b.msg_type = '0001' \
               AND b.train_id = u.train_id \
               AND b.service_date BETWEEN u.service_date - 1 AND u.service_date + 1 \
               AND b.train_uid IS NOT NULL \
             LIMIT 2 \
         ) a",
    )
    .bind(&indices)
    .bind(&train_ids)
    .bind(&dates)
    .fetch_all(pool)
    .await
    .context("looking up uid-less events' Activations")?;

    let mut candidates: HashMap<usize, Vec<(String, NaiveDate)>> = HashMap::new();
    for (idx, train_uid, service_date) in rows {
        let idx = usize::try_from(idx).context("negative batch index")?;
        candidates
            .entry(idx)
            .or_default()
            .push((train_uid, service_date));
    }
    let inferred: HashMap<usize, (String, NaiveDate)> = candidates
        .into_iter()
        .filter_map(|(idx, mut found)| (found.len() == 1).then(|| (idx, found.remove(0))))
        .collect();
    let unresolved = indices.len() - inferred.len();
    metrics::counter!(common::metrics::metric_name(UID_INFERRED_METRIC), "source" => "activation")
        .increment(inferred.len() as u64);
    metrics::counter!(common::metrics::metric_name(UID_INFERRED_METRIC), "source" => "none")
        .increment(unresolved as u64);
    Ok(inferred)
}

/// What [`replay_uidless_backlog`] did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct UidlessReplayReport {
    /// uid-less non-Activation backlog rows read.
    pub rows: u64,
    /// Of those, rows whose shared write failed (logged; re-run to retry).
    pub failed: u64,
}

/// Re-runs the shared-table write ([`ingest_shared_movements_batch`]) for
/// every uid-less non-Activation row in `trust_event_backlog` received at
/// or after `since`, oldest first, `chunk` rows at a time: the rows a
/// trust-backlog-consumer restart left out of `train_movement_events`
/// before api inferred their identity (see [`infer_train_identities`]).
/// Every shared write is idempotent (dedup keys, the event-time guard), so
/// re-running it is safe. The backlog keeps a day, so this only reaches
/// that far back.
pub async fn replay_uidless_backlog(
    pool: &PgPool,
    since: chrono::DateTime<chrono::Utc>,
    chunk: i64,
) -> anyhow::Result<UidlessReplayReport> {
    type Row = (
        i64,
        Option<String>,
        String,
        NaiveDate,
        String,
        Option<String>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<String>,
        Option<i32>,
        String,
        Option<chrono::DateTime<chrono::Utc>>,
    );
    let mut report = UidlessReplayReport::default();
    let mut after_id = 0i64;
    loop {
        let rows: Vec<Row> = sqlx::query_as(
            "SELECT id, crs, train_id, service_date, msg_type, event_type, planned_timestamp, \
                    actual_timestamp, variation_status, delay_minutes, dedup_key, gbtt_timestamp \
             FROM trust_event_backlog \
             WHERE train_uid IS NULL AND msg_type <> '0001' AND received_at >= $1 AND id > $2 \
             ORDER BY id LIMIT $3",
        )
        .bind(since)
        .bind(after_id)
        .bind(chunk)
        .fetch_all(pool)
        .await
        .context("reading uid-less backlog rows")?;
        let Some(last) = rows.last() else {
            return Ok(report);
        };
        after_id = last.0;
        let events: Vec<TrustBacklogEventMessage> = rows
            .into_iter()
            .map(|r| TrustBacklogEventMessage {
                crs: r.1,
                train_uid: None,
                train_id: r.2,
                service_date: r.3,
                msg_type: r.4,
                event_type: r.5,
                planned_timestamp: r.6,
                actual_timestamp: r.7,
                variation_status: r.8,
                delay_minutes: r.9,
                dedup_key: r.10,
                gbtt_timestamp: r.11,
            })
            .collect();
        let results = ingest_shared_movements_batch(pool, &events).await;
        report.rows += events.len() as u64;
        for (event, result) in events.iter().zip(results) {
            if let Err(err) = result {
                report.failed += 1;
                tracing::warn!(error = ?err, dedup_key = %event.dedup_key, "replaying a uid-less backlog row failed");
            }
        }
        tracing::info!(
            rows = report.rows,
            failed = report.failed,
            after_id,
            "replayed a chunk of uid-less backlog rows"
        );
    }
}

/// `(event index, trains_id)` for each Reinstatement in `events` that has
/// no `train_uid` but whose `train_id` names exactly one shared `trains`
/// row dated within a day of the event's `service_date` (an overnight
/// train's rows can sit either side of midnight). TRUST recycles a
/// `train_id` roughly monthly, so the date window is what keeps an old
/// row from matching; more than one candidate is ambiguous and skipped.
async fn trains_for_uidless_reinstatements(
    pool: &PgPool,
    events: &[TrustBacklogEventMessage],
) -> anyhow::Result<Vec<(usize, i64)>> {
    let mut found = Vec::new();
    for (i, event) in events.iter().enumerate() {
        if event.msg_type != "0005" || event.train_uid.is_some() {
            continue;
        }
        let ids: Vec<i64> = sqlx::query_scalar(
            "SELECT id FROM trains \
             WHERE train_id = $1 \
               AND service_date BETWEEN $2::date - 1 AND $2::date + 1 \
             LIMIT 2",
        )
        .bind(&event.train_id)
        .bind(event.service_date)
        .fetch_all(pool)
        .await?;
        if let [id] = ids.as_slice() {
            found.push((i, *id));
        }
    }
    Ok(found)
}

async fn fetch_previous_derived_state(
    pool: &PgPool,
    trains_id: i64,
) -> anyhow::Result<DerivedState> {
    // status, last_reported_location, last_event_type, delay_minutes,
    // next_calling_point.
    type DerivedStateRow = (
        String,
        Option<String>,
        Option<String>,
        Option<i32>,
        Option<String>,
    );
    let row: Option<DerivedStateRow> = sqlx::query_as(
        "SELECT status, last_reported_location, last_event_type, delay_minutes, next_calling_point \
             FROM train_current_state WHERE trains_id = $1",
    )
    .bind(trains_id)
    .fetch_optional(pool)
    .await?;
    Ok(match row {
        Some((
            status,
            last_reported_location,
            last_event_type,
            delay_minutes,
            next_calling_point,
        )) => DerivedState {
            status,
            last_reported_location,
            last_event_type,
            delay_minutes,
            next_calling_point,
        },
        None => DerivedState::awaiting_activation(),
    })
}

/// Test-only, one-shot fault injector for [`fetch_previous_derived_states_batch`]'s
/// batched SELECT, compiled out of every non-test build. It exists because,
/// unlike `find_or_create_trains_batch`/`mark_trains_resolved_batch` (whose
/// batched fallback IS reachable via a real, per-row-attributable DB failure
/// -- a value violating a genuine column constraint), this function's only
/// bound parameter is `trains_ids: &[i64]` -- internally generated
/// surrogate keys from a PRIOR successful step, never raw/untrusted event
/// data -- so there is no "bad row" shape that can make this particular
/// SELECT fail for one id but not another. `ingest_shared_movements_batch`'s
/// own fallback here is reachable in production, though: real-world
/// failures like a dropped connection or statement timeout hit the whole
/// query regardless of row content. This flag lets a test force exactly
/// that kind of failure once, deterministically, so the REAL fallback loop
/// (this function's caller retrying one `trains_id` at a time via
/// [`fetch_previous_derived_state`]) can be exercised against a real
/// database rather than mocked out.
#[cfg(test)]
static FORCE_FETCH_PREVIOUS_DERIVED_STATES_BATCH_FAILURE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Batch-shaped sibling of [`fetch_previous_derived_state`] -- one SELECT
/// covering every DISTINCT `trains_id` in `trains_ids`, rather than one
/// SELECT per id. A `trains_id` with no `train_current_state` row yet
/// (never seen a Movement/Cancellation before) is simply absent from the
/// returned map -- callers must apply the same
/// `DerivedState::awaiting_activation()` fallback
/// [`fetch_previous_derived_state`] applies for that case.
#[expect(
    clippy::items_after_statements,
    reason = "a local type or import sits next to its only use"
)]
async fn fetch_previous_derived_states_batch(
    pool: &PgPool,
    trains_ids: &[i64],
) -> anyhow::Result<HashMap<i64, DerivedState>> {
    if trains_ids.is_empty() {
        return Ok(HashMap::new());
    }
    #[cfg(test)]
    if FORCE_FETCH_PREVIOUS_DERIVED_STATES_BATCH_FAILURE
        .swap(false, std::sync::atomic::Ordering::SeqCst)
    {
        return Err(anyhow::anyhow!(
            "test-injected fetch_previous_derived_states_batch failure"
        ));
    }
    // trains_id, status, last_reported_location, last_event_type,
    // delay_minutes, next_calling_point.
    type DerivedStateBatchRow = (
        i64,
        String,
        Option<String>,
        Option<String>,
        Option<i32>,
        Option<String>,
    );
    let rows: Vec<DerivedStateBatchRow> = sqlx::query_as(
        "SELECT trains_id, status, last_reported_location, last_event_type, delay_minutes, \
                next_calling_point \
         FROM train_current_state WHERE trains_id = ANY($1)",
    )
    .bind(trains_ids)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(
                trains_id,
                status,
                last_reported_location,
                last_event_type,
                delay_minutes,
                next_calling_point,
            )| {
                (
                    trains_id,
                    DerivedState {
                        status,
                        last_reported_location,
                        last_event_type,
                        delay_minutes,
                        next_calling_point,
                    },
                )
            },
        )
        .collect())
}

#[cfg(test)]
#[expect(
    clippy::similar_names,
    clippy::too_many_lines,
    reason = "test code: paired test values share names; scenario tests read top to bottom"
)]
mod db_tests {
    use super::*;
    use crate::test_support::connect;
    use sqlx::postgres::PgPoolOptions;

    fn fixture_event(train_id: &str, dedup_key: &str) -> TrustBacklogEventMessage {
        TrustBacklogEventMessage {
            crs: Some("EUS".to_string()),
            train_uid: Some("C11052".to_string()),
            train_id: train_id.to_string(),
            service_date: "2026-09-05".parse().unwrap(),
            msg_type: "0003".to_string(),
            event_type: Some("DEPARTURE".to_string()),
            planned_timestamp: Some("2026-09-05T19:15:00Z".parse().unwrap()),
            actual_timestamp: Some("2026-09-05T19:16:00Z".parse().unwrap()),
            variation_status: Some("LATE".to_string()),
            delay_minutes: Some(1),
            dedup_key: dedup_key.to_string(),
            gbtt_timestamp: None,
        }
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                a_fresh_batch_inserts_every_row -- --ignored --test-threads=1`"]
    async fn a_fresh_batch_inserts_every_row() {
        let pool = connect().await;
        let events = vec![
            fixture_event("TEST-TRUST-BACKLOG-1", "test-dedup-key-1"),
            fixture_event("TEST-TRUST-BACKLOG-2", "test-dedup-key-2"),
        ];

        let inserted = upsert_trust_event_backlog_batch(&pool, &events)
            .await
            .expect("insert");
        assert_eq!(inserted.inserted, 2);
        assert!(inserted.rejected.is_empty());

        sqlx::query("DELETE FROM trust_event_backlog WHERE dedup_key LIKE 'test-dedup-key-%'")
            .execute(&pool)
            .await
            .expect("cleanup");
    }

    /// Regression test: trust-backlog-consumer forwards `0005`
    /// (Reinstatement) since the H4 fix, but the table's `msg_type` CHECK
    /// still only allowed `0001`/`0002`/`0003`, so any batch containing
    /// one failed as a whole -- dropping the valid Movements alongside it
    /// -- until migration 20260926210000 widened the constraint.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                a_batch_with_a_reinstatement_inserts_every_row -- --ignored --test-threads=1`"]
    async fn a_batch_with_a_reinstatement_inserts_every_row() {
        let pool = connect().await;
        let reinstatement = TrustBacklogEventMessage {
            crs: None,
            event_type: None,
            planned_timestamp: None,
            actual_timestamp: None,
            variation_status: None,
            delay_minutes: None,
            msg_type: "0005".to_string(),
            ..fixture_event("TEST-TRUST-BACKLOG-0005", "test-dedup-key-0005-reinstate")
        };
        let events = vec![
            fixture_event("TEST-TRUST-BACKLOG-0005", "test-dedup-key-0005-movement"),
            reinstatement,
        ];

        let inserted = upsert_trust_event_backlog_batch(&pool, &events).await;

        sqlx::query("DELETE FROM trust_event_backlog WHERE dedup_key LIKE 'test-dedup-key-0005-%'")
            .execute(&pool)
            .await
            .expect("cleanup");
        let outcome = inserted.expect("insert");
        assert_eq!(outcome.inserted, 2);
        assert!(outcome.rejected.is_empty(), "{:?}", outcome.rejected);
    }

    /// The widened CHECK still rejects the message types neither backlog
    /// replay path handles (`0006`/`0007` carry no location or timing).
    /// The row is now reported as rejected instead of failing the call.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                the_msg_type_check_still_rejects_change_of_origin -- --ignored --test-threads=1`"]
    async fn the_msg_type_check_still_rejects_change_of_origin() {
        let pool = connect().await;
        let change_of_origin = TrustBacklogEventMessage {
            msg_type: "0006".to_string(),
            ..fixture_event("TEST-TRUST-BACKLOG-0006", "test-dedup-key-0006")
        };

        let result = upsert_trust_event_backlog_batch(&pool, &[change_of_origin]).await;

        sqlx::query("DELETE FROM trust_event_backlog WHERE dedup_key = 'test-dedup-key-0006'")
            .execute(&pool)
            .await
            .expect("cleanup");
        let outcome = result.expect("a data error is reported per row, not as an Err");
        assert_eq!(outcome.inserted, 0);
        assert_eq!(outcome.rejected.len(), 1);
        assert_eq!(
            outcome.rejected[0].constraint.as_deref(),
            Some("trust_event_backlog_msg_type_check"),
            "unexpected rejection: {:?}",
            outcome.rejected[0]
        );
    }

    /// The production incident: one row the `msg_type` CHECK refuses used to
    /// fail the whole batch, and every valid row beside it with it. Now the
    /// valid rows land and only the bad one is reported. `0009` is not a
    /// TRUST message type at all, so it stays invalid however the CHECK is
    /// widened in future.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                a_batch_with_one_bad_row_inserts_the_rest -- --ignored --test-threads=1`"]
    async fn a_batch_with_one_bad_row_inserts_the_rest_and_reports_the_bad_one() {
        let pool = connect().await;
        sqlx::query(
            "DELETE FROM trust_event_backlog WHERE dedup_key LIKE 'test-dedup-key-poison-%'",
        )
        .execute(&pool)
        .await
        .expect("pre-clean");
        let bad = TrustBacklogEventMessage {
            msg_type: "0009".to_string(),
            ..fixture_event("TEST-TRUST-BACKLOG-POISON", "test-dedup-key-poison-bad")
        };
        let events = vec![
            fixture_event("TEST-TRUST-BACKLOG-POISON", "test-dedup-key-poison-1"),
            bad,
            fixture_event("TEST-TRUST-BACKLOG-POISON", "test-dedup-key-poison-2"),
        ];

        let result = upsert_trust_event_backlog_batch(&pool, &events).await;
        let landed: Vec<String> = sqlx::query_scalar(
            "SELECT dedup_key FROM trust_event_backlog \
             WHERE dedup_key LIKE 'test-dedup-key-poison-%' ORDER BY dedup_key",
        )
        .fetch_all(&pool)
        .await
        .expect("read back");
        // A redelivery of the same batch: the good rows now conflict (not
        // rejected, not inserted twice) and the bad row is rejected again.
        let redelivered = upsert_trust_event_backlog_batch(&pool, &events).await;
        sqlx::query(
            "DELETE FROM trust_event_backlog WHERE dedup_key LIKE 'test-dedup-key-poison-%'",
        )
        .execute(&pool)
        .await
        .expect("cleanup");

        let outcome = result.expect("a data error must not fail the batch");
        assert_eq!(outcome.inserted, 2, "both valid rows land");
        assert_eq!(
            landed,
            vec!["test-dedup-key-poison-1", "test-dedup-key-poison-2"]
        );
        assert_eq!(outcome.rejected.len(), 1);
        let rejected = &outcome.rejected[0];
        assert_eq!(rejected.index, 1);
        assert_eq!(rejected.dedup_key, "test-dedup-key-poison-bad");
        assert_eq!(rejected.sqlstate, "23514");
        assert_eq!(rejected.reason, "check_violation");
        assert_eq!(
            rejected.constraint.as_deref(),
            Some("trust_event_backlog_msg_type_check")
        );

        let redelivered = redelivered.expect("redelivery");
        assert_eq!(
            redelivered.inserted, 0,
            "ON CONFLICT DO NOTHING still holds"
        );
        assert_eq!(redelivered.rejected.len(), 1);
        assert_eq!(redelivered.rejected[0].index, 1);
    }

    /// A transient failure is not a data error: it must still fail the
    /// whole call, so the route answers 500 and the consumer retries, and
    /// it must commit nothing. Simulated with a lock timeout (SQLSTATE
    /// 55P03): another transaction holds the table exclusively while this
    /// pool's connections give up after 200ms.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                a_transient_failure_still_fails_the_whole_batch -- --ignored --test-threads=1`"]
    async fn a_transient_failure_still_fails_the_whole_batch() {
        let pool = connect().await;
        let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
        let impatient = PgPoolOptions::new()
            .max_connections(1)
            .after_connect(|conn, _| {
                Box::pin(async move {
                    sqlx::query("SET lock_timeout = '200ms'")
                        .execute(conn)
                        .await?;
                    Ok(())
                })
            })
            .connect(&database_url)
            .await
            .expect("connect impatient pool");

        let mut blocker = pool.begin().await.expect("begin blocker");
        sqlx::query("LOCK TABLE trust_event_backlog IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *blocker)
            .await
            .expect("lock table");

        let events = vec![fixture_event(
            "TEST-TRUST-BACKLOG-TRANSIENT",
            "test-dedup-key-transient",
        )];
        let result = upsert_trust_event_backlog_batch(&impatient, &events).await;
        blocker.rollback().await.expect("release lock");

        let err = result.expect_err("a lock timeout must fail the batch, not reject rows");
        let sqlstate = err
            .downcast_ref::<sqlx::Error>()
            .and_then(|e| e.as_database_error())
            .and_then(sqlx::error::DatabaseError::code)
            .map(std::borrow::Cow::into_owned);
        assert_eq!(
            sqlstate.as_deref(),
            Some("55P03"),
            "unexpected error: {err:#}"
        );
        let landed: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM trust_event_backlog WHERE dedup_key = 'test-dedup-key-transient'",
        )
        .fetch_one(&pool)
        .await
        .expect("count");
        assert_eq!(landed, 0);
    }

    /// A connection that cannot be established at all is the plainest
    /// transient failure.
    #[tokio::test]
    async fn an_unreachable_database_fails_the_whole_batch() {
        let unreachable = PgPoolOptions::new()
            .acquire_timeout(std::time::Duration::from_secs(2))
            .connect_lazy("postgres://nobody:nothing@127.0.0.1:1/none")
            .expect("lazy pool");
        let events = vec![fixture_event(
            "TEST-TRUST-BACKLOG-DOWN",
            "test-dedup-key-down",
        )];

        let result = upsert_trust_event_backlog_batch(&unreachable, &events).await;

        assert!(result.is_err(), "got {result:?}");
    }

    /// DB2-4: the single UNNEST insert keeps every column (NULLs included),
    /// and skips a `dedup_key` repeated inside the same batch as well as one
    /// already stored.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                a_batch_repeating_a_dedup_key -- --ignored --test-threads=1`"]
    async fn a_batch_repeating_a_dedup_key_inserts_it_once_and_keeps_every_column() {
        let pool = connect().await;
        let cleanup = || async {
            sqlx::query("DELETE FROM trust_event_backlog WHERE dedup_key LIKE 'test-db2-4-%'")
                .execute(&pool)
                .await
                .expect("cleanup");
        };
        cleanup().await;
        let stored = fixture_event("TEST-DB2-4-A", "test-db2-4-stored");
        upsert_trust_event_backlog_batch(&pool, std::slice::from_ref(&stored))
            .await
            .expect("seed");

        let activation = TrustBacklogEventMessage {
            crs: None,
            msg_type: "0001".to_string(),
            event_type: None,
            planned_timestamp: None,
            actual_timestamp: None,
            variation_status: None,
            delay_minutes: None,
            ..fixture_event("TEST-DB2-4-B", "test-db2-4-activation")
        };
        let batch = vec![
            stored,
            fixture_event("TEST-DB2-4-C", "test-db2-4-twice"),
            activation,
            fixture_event("TEST-DB2-4-C", "test-db2-4-twice"),
        ];
        let outcome = upsert_trust_event_backlog_batch(&pool, &batch)
            .await
            .expect("batch insert");
        assert_eq!(outcome.inserted, 2);
        assert!(outcome.rejected.is_empty());

        let row: (Option<String>, String, Option<String>, bool, Option<i32>) = sqlx::query_as(
            "SELECT crs, msg_type, event_type, planned_timestamp IS NULL, delay_minutes \
             FROM trust_event_backlog WHERE dedup_key = 'test-db2-4-activation'",
        )
        .fetch_one(&pool)
        .await
        .expect("read activation");
        assert_eq!(row, (None, "0001".to_string(), None, true, None));
        let (planned, delay): (Option<chrono::DateTime<chrono::Utc>>, Option<i32>) =
            sqlx::query_as(
                "SELECT planned_timestamp, delay_minutes FROM trust_event_backlog \
                 WHERE dedup_key = 'test-db2-4-twice'",
            )
            .fetch_one(&pool)
            .await
            .expect("read movement");
        assert_eq!(planned, Some("2026-09-05T19:15:00Z".parse().unwrap()));
        assert_eq!(delay, Some(1));

        cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                a_redelivered_batch_inserts_nothing_twice -- --ignored --test-threads=1`"]

    async fn a_redelivered_batch_inserts_nothing_twice() {
        let pool = connect().await;
        let event = fixture_event("TEST-TRUST-BACKLOG-3", "test-dedup-key-3");

        let first = upsert_trust_event_backlog_batch(&pool, std::slice::from_ref(&event))
            .await
            .expect("first insert");
        assert_eq!(first.inserted, 1);

        let redelivered = upsert_trust_event_backlog_batch(&pool, &[event])
            .await
            .expect("redelivered insert");
        assert_eq!(
            redelivered,
            BacklogBatchOutcome::default(),
            "same dedup_key must not insert twice, and a conflict is not a rejection"
        );

        sqlx::query("DELETE FROM trust_event_backlog WHERE dedup_key = 'test-dedup-key-3'")
            .execute(&pool)
            .await
            .expect("cleanup");
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                ingest_shared_movement_writes_the_shared_tables_when_train_uid_is_known \
                -- --ignored --test-threads=1`"]
    async fn ingest_shared_movement_writes_the_shared_tables_when_train_uid_is_known() {
        let pool = connect().await;
        let event = TrustBacklogEventMessage {
            crs: Some("WAT".to_string()),
            train_uid: Some("TEST-INGEST-UID".to_string()),
            train_id: "TEST-INGEST-TRAIN-ID".to_string(),
            service_date: "2026-09-06".parse().unwrap(),
            msg_type: "0003".to_string(),
            event_type: Some("DEPARTURE".to_string()),
            planned_timestamp: Some("2026-09-06T19:15:00Z".parse().unwrap()),
            actual_timestamp: Some("2026-09-06T19:16:00Z".parse().unwrap()),
            variation_status: Some("LATE".to_string()),
            delay_minutes: Some(1),
            dedup_key: "test-ingest-shared-dedup".to_string(),
            gbtt_timestamp: None,
        };

        ingest_shared_movement(&pool, &event)
            .await
            .expect("ingest_shared_movement");

        let (trains_id,): (i64,) =
            sqlx::query_as("SELECT id FROM trains WHERE train_uid = 'TEST-INGEST-UID'")
                .fetch_one(&pool)
                .await
                .expect("a shared trains row must have been created");

        let (status, delay_minutes): (String, Option<i32>) = sqlx::query_as(
            "SELECT status, delay_minutes FROM train_current_state WHERE trains_id = $1",
        )
        .bind(trains_id)
        .fetch_one(&pool)
        .await
        .expect("a current-state row must exist");
        assert_eq!(status, "en_route");
        assert_eq!(delay_minutes, Some(1));

        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
    }

    fn activation(
        train_id: &str,
        train_uid: &str,
        service_date: &str,
        dedup: &str,
    ) -> TrustBacklogEventMessage {
        TrustBacklogEventMessage {
            crs: None,
            train_uid: Some(train_uid.to_string()),
            train_id: train_id.to_string(),
            service_date: service_date.parse().unwrap(),
            msg_type: "0001".to_string(),
            event_type: None,
            planned_timestamp: None,
            actual_timestamp: None,
            variation_status: None,
            delay_minutes: None,
            dedup_key: dedup.to_string(),
            gbtt_timestamp: None,
        }
    }

    fn uidless_movement(
        train_id: &str,
        service_date: &str,
        dedup: &str,
    ) -> TrustBacklogEventMessage {
        TrustBacklogEventMessage {
            crs: Some("WAT".to_string()),
            train_uid: None,
            train_id: train_id.to_string(),
            service_date: service_date.parse().unwrap(),
            msg_type: "0003".to_string(),
            event_type: Some("DEPARTURE".to_string()),
            planned_timestamp: Some("2026-10-01T08:55:00Z".parse().unwrap()),
            actual_timestamp: Some("2026-10-01T08:56:00Z".parse().unwrap()),
            variation_status: Some("LATE".to_string()),
            delay_minutes: Some(1),
            dedup_key: dedup.to_string(),
            gbtt_timestamp: None,
        }
    }

    async fn cleanup_uid_inference(pool: &PgPool, train_id: &str) {
        sqlx::query("DELETE FROM trains WHERE train_id = $1 OR train_uid LIKE 'TEST-INFER-%'")
            .bind(train_id)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trust_event_backlog WHERE train_id = $1")
            .bind(train_id)
            .execute(pool)
            .await
            .ok();
    }

    /// 2026-10-01: after trust-backlog-consumer restarted at 08:50, the
    /// Movements of every train activated before then arrived without a
    /// `train_uid` and never reached `train_movement_events` (~55 rows in
    /// the next hour). Their Activations were in `trust_event_backlog`:
    /// the Movement now takes its `train_uid` AND `service_date` from there
    /// (here the Activation's D, not the Movement's fallback D+1).
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                a_uidless_movement_takes_its_activations_identity -- --ignored --test-threads=1`"]
    async fn a_uidless_movement_takes_its_activations_identity() {
        let pool = connect().await;
        let train_id = "TEST-INFER-TID-1";
        cleanup_uid_inference(&pool, train_id).await;
        upsert_trust_event_backlog_batch(
            &pool,
            &[activation(
                train_id,
                "TEST-INFER-UID-1",
                "2026-09-30",
                "test-infer-act-1",
            )],
        )
        .await
        .expect("seed the Activation");

        let movement = uidless_movement(train_id, "2026-10-01", "test-infer-mov-1");
        upsert_trust_event_backlog_batch(&pool, std::slice::from_ref(&movement))
            .await
            .expect("backlog row");
        let results = ingest_shared_movements_batch(&pool, &[movement]).await;
        assert!(results.iter().all(Result::is_ok), "{results:?}");

        let (trains_id, service_date): (i64, NaiveDate) = sqlx::query_as(
            "SELECT id, service_date FROM trains WHERE train_uid = 'TEST-INFER-UID-1'",
        )
        .fetch_one(&pool)
        .await
        .expect("the Movement created the Activation's trains row");
        assert_eq!(service_date, "2026-09-30".parse::<NaiveDate>().unwrap());
        let movements: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM train_movement_events WHERE trains_id = $1 AND dedup_key = 'test-infer-mov-1'",
        )
        .bind(trains_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(movements, 1);
        cleanup_uid_inference(&pool, train_id).await;
    }

    /// The Activation may come earlier in the same POST: its backlog row is
    /// inserted before the shared write runs.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                an_activation_in_the_same_batch_identifies_a_uidless_movement -- --ignored --test-threads=1`"]
    async fn an_activation_in_the_same_batch_identifies_a_uidless_movement() {
        let pool = connect().await;
        let train_id = "TEST-INFER-TID-2";
        cleanup_uid_inference(&pool, train_id).await;
        let batch = [
            activation(
                train_id,
                "TEST-INFER-UID-2",
                "2026-10-01",
                "test-infer-act-2",
            ),
            uidless_movement(train_id, "2026-10-01", "test-infer-mov-2"),
        ];
        upsert_trust_event_backlog_batch(&pool, &batch)
            .await
            .expect("backlog rows");
        let results = ingest_shared_movements_batch(&pool, &batch).await;
        assert!(results.iter().all(Result::is_ok), "{results:?}");
        let movements: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM train_movement_events m JOIN trains t ON t.id = m.trains_id \
             WHERE t.train_uid = 'TEST-INFER-UID-2' AND m.dedup_key = 'test-infer-mov-2'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(movements, 1);
        cleanup_uid_inference(&pool, train_id).await;
    }

    /// Two Activations with different uids for one `train_id` within the
    /// window (a recycled `train_id`) are ambiguous; no Activation at all is
    /// the old accepted gap. Either way: no shared write, and no error.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                an_ambiguous_or_missing_activation_leaves_the_movement_unwritten -- --ignored --test-threads=1`"]
    async fn an_ambiguous_or_missing_activation_leaves_the_movement_unwritten() {
        let pool = connect().await;
        let ambiguous = "TEST-INFER-TID-3";
        let missing = "TEST-INFER-TID-4";
        cleanup_uid_inference(&pool, ambiguous).await;
        cleanup_uid_inference(&pool, missing).await;
        upsert_trust_event_backlog_batch(
            &pool,
            &[
                activation(
                    ambiguous,
                    "TEST-INFER-UID-3A",
                    "2026-10-01",
                    "test-infer-act-3a",
                ),
                activation(
                    ambiguous,
                    "TEST-INFER-UID-3B",
                    "2026-09-30",
                    "test-infer-act-3b",
                ),
                // Outside the one-day window: an old use of the same train_id.
                activation(
                    missing,
                    "TEST-INFER-UID-4",
                    "2026-09-25",
                    "test-infer-act-4",
                ),
            ],
        )
        .await
        .expect("seed the Activations");

        let batch = [
            uidless_movement(ambiguous, "2026-10-01", "test-infer-mov-3"),
            uidless_movement(missing, "2026-10-01", "test-infer-mov-4"),
        ];
        let results = ingest_shared_movements_batch(&pool, &batch).await;
        assert!(results.iter().all(Result::is_ok), "{results:?}");
        let trains: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM trains WHERE train_id IN ($1, $2) OR train_uid LIKE 'TEST-INFER-UID-%'",
        )
        .bind(ambiguous)
        .bind(missing)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            trains, 0,
            "no trains row from an ambiguous or missing Activation"
        );
        cleanup_uid_inference(&pool, ambiguous).await;
        cleanup_uid_inference(&pool, missing).await;
    }

    /// The replay writes what a restart left out, once: a second run
    /// changes nothing.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                replaying_uidless_backlog_rows_writes_the_missing_movements_once -- --ignored --test-threads=1`"]
    async fn replaying_uidless_backlog_rows_writes_the_missing_movements_once() {
        let pool = connect().await;
        let train_id = "TEST-INFER-TID-5";
        cleanup_uid_inference(&pool, train_id).await;
        let since = chrono::Utc::now() - chrono::Duration::seconds(5);
        upsert_trust_event_backlog_batch(
            &pool,
            &[
                activation(
                    train_id,
                    "TEST-INFER-UID-5",
                    "2026-10-01",
                    "test-infer-act-5",
                ),
                TrustBacklogEventMessage {
                    gbtt_timestamp: Some("2026-10-01T08:54:00Z".parse().unwrap()),
                    ..uidless_movement(train_id, "2026-10-01", "test-infer-mov-5")
                },
            ],
        )
        .await
        .expect("backlog rows, as written before the fix (no shared write)");
        let count = || async {
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM train_movement_events m JOIN trains t ON t.id = m.trains_id \
                 WHERE t.train_uid = 'TEST-INFER-UID-5'",
            )
            .fetch_one(&pool)
            .await
            .unwrap()
        };
        assert_eq!(count().await, 0);

        let first = replay_uidless_backlog(&pool, since, 1)
            .await
            .expect("replay");
        assert!(first.rows >= 1 && first.failed == 0, "{first:?}");
        assert_eq!(count().await, 1);
        replay_uidless_backlog(&pool, since, 1)
            .await
            .expect("replay again");
        assert_eq!(count().await, 1, "idempotent");
        let gbtt: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
            "SELECT m.gbtt_timestamp FROM train_movement_events m JOIN trains t ON t.id = m.trains_id \
             WHERE t.train_uid = 'TEST-INFER-UID-5'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            gbtt,
            Some("2026-10-01T08:54:00Z".parse().unwrap()),
            "the replay carries the stored public time"
        );
        cleanup_uid_inference(&pool, train_id).await;
    }

    /// Both backlog insert paths -- the one-statement UNNEST and the
    /// row-by-row fallback a bad batch-mate forces -- store
    /// `gbtt_timestamp` (2026-10-07: neither did, and nothing downstream
    /// could replay what was never stored).
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                both_backlog_insert_paths_store_the_gbtt_timestamp -- --ignored --test-threads=1`"]
    async fn both_backlog_insert_paths_store_the_gbtt_timestamp() {
        let pool = connect().await;
        let cleanup =
            "DELETE FROM trust_event_backlog WHERE dedup_key LIKE 'test-dedup-key-gbtt-%'";
        sqlx::query(cleanup)
            .execute(&pool)
            .await
            .expect("pre-clean");
        let gbtt: chrono::DateTime<chrono::Utc> = "2026-09-05T19:14:00Z".parse().unwrap();
        let with_gbtt = |dedup_key: &str| TrustBacklogEventMessage {
            gbtt_timestamp: Some(gbtt),
            ..fixture_event("TEST-TRUST-BACKLOG-GBTT", dedup_key)
        };

        let fast =
            upsert_trust_event_backlog_batch(&pool, &[with_gbtt("test-dedup-key-gbtt-fast")])
                .await
                .expect("fast path");
        assert_eq!(fast.inserted, 1);
        let bad = TrustBacklogEventMessage {
            msg_type: "0009".to_string(),
            ..fixture_event("TEST-TRUST-BACKLOG-GBTT", "test-dedup-key-gbtt-bad")
        };
        let slow =
            upsert_trust_event_backlog_batch(&pool, &[with_gbtt("test-dedup-key-gbtt-slow"), bad])
                .await
                .expect("row-by-row path");
        assert_eq!((slow.inserted, slow.rejected.len()), (1, 1));

        let stored: Vec<(String, Option<chrono::DateTime<chrono::Utc>>)> = sqlx::query_as(
            "SELECT dedup_key, gbtt_timestamp FROM trust_event_backlog \
             WHERE dedup_key LIKE 'test-dedup-key-gbtt-%' ORDER BY dedup_key",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        sqlx::query(cleanup).execute(&pool).await.expect("cleanup");
        assert_eq!(
            stored,
            vec![
                ("test-dedup-key-gbtt-fast".to_string(), Some(gbtt)),
                ("test-dedup-key-gbtt-slow".to_string(), Some(gbtt)),
            ]
        );
    }

    /// The shared write (`ingest_shared_movements_batch`, the path
    /// `POST /private/trust-event-backlog` drives) stores the event's
    /// `gbtt_timestamp` on `train_movement_events`.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                the_shared_movement_write_stores_the_gbtt_timestamp -- --ignored --test-threads=1`"]
    async fn the_shared_movement_write_stores_the_gbtt_timestamp() {
        let pool = connect().await;
        let uid = "TEST-INGEST-GBTT-UID";
        sqlx::query("DELETE FROM trains WHERE train_uid = $1")
            .bind(uid)
            .execute(&pool)
            .await
            .ok();
        let gbtt: chrono::DateTime<chrono::Utc> = "2026-09-06T19:14:00Z".parse().unwrap();
        let event = TrustBacklogEventMessage {
            train_uid: Some(uid.to_string()),
            service_date: "2026-09-06".parse().unwrap(),
            gbtt_timestamp: Some(gbtt),
            ..fixture_event("TEST-INGEST-GBTT-TID", "test-ingest-gbtt-dedup")
        };
        let results = ingest_shared_movements_batch(&pool, std::slice::from_ref(&event)).await;
        assert!(results.iter().all(Result::is_ok), "{results:?}");

        let stored: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
            "SELECT m.gbtt_timestamp FROM train_movement_events m JOIN trains t ON t.id = m.trains_id \
             WHERE t.train_uid = $1",
        )
        .bind(uid)
        .fetch_one(&pool)
        .await
        .expect("one shared movement row");
        sqlx::query("DELETE FROM trains WHERE train_uid = $1")
            .bind(uid)
            .execute(&pool)
            .await
            .ok();
        assert_eq!(stored, Some(gbtt));
    }

    /// Corroborating proof, for THIS call path specifically, of the guard
    /// documented on `upsert_train_movement`'s own doc comment (see
    /// `train_tracking.rs`'s `an_out_of_order_event_does_not_regress_current_state`
    /// for the direct, same-technique proof against that function itself).
    /// `ingest_shared_movement` requires no production code change of its
    /// own -- it already funnels every write through `upsert_train_movement`
    /// (`ingest_shared_movements_batch`'s final write, above), so the guard
    /// applies automatically; this test exists only to confirm that's
    /// actually true end-to-end through THIS module's own public entry
    /// point, not merely inferred from reading the call graph.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                an_out_of_order_ingest_shared_movement_call_does_not_regress_current_state \
                -- --ignored --test-threads=1`"]
    async fn an_out_of_order_ingest_shared_movement_call_does_not_regress_current_state() {
        let pool = connect().await;
        let service_date: NaiveDate = "2026-09-06".parse().unwrap();

        let newer_movement = TrustBacklogEventMessage {
            crs: Some("MKC".to_string()),
            train_uid: Some("TEST-BACKLOG-OUT-OF-ORDER-UID".to_string()),
            train_id: "TEST-BACKLOG-OUT-OF-ORDER-TID".to_string(),
            service_date,
            msg_type: "0003".to_string(),
            event_type: Some("ARRIVAL".to_string()),
            planned_timestamp: Some("2026-09-06T19:45:00Z".parse().unwrap()),
            actual_timestamp: Some("2026-09-06T19:45:00Z".parse().unwrap()),
            variation_status: Some("ON TIME".to_string()),
            delay_minutes: Some(0),
            dedup_key: "test-backlog-out-of-order-newer-dedup".to_string(),
            gbtt_timestamp: None,
        };
        ingest_shared_movement(&pool, &newer_movement)
            .await
            .expect("the newer movement's own ingest must succeed");

        let (trains_id,): (i64,) = sqlx::query_as(
            "SELECT id FROM trains WHERE train_uid = 'TEST-BACKLOG-OUT-OF-ORDER-UID'",
        )
        .fetch_one(&pool)
        .await
        .expect("a shared trains row must have been created");

        // A STALE Cancellation, arriving SECOND, timestamped BEFORE the
        // movement above -- exactly the design doc's §1.3 scenario: this
        // consumer's own batch redelivering an old message out of
        // real-world order, or racing the live trust-consumer path for the
        // same trains_id. Without the guard, `apply_cancellation` would
        // flip a train that has already progressed (and, in reality, may
        // never have been cancelled at all) to `status = 'cancelled'`.
        let stale_cancellation = TrustBacklogEventMessage {
            crs: None,
            train_uid: Some("TEST-BACKLOG-OUT-OF-ORDER-UID".to_string()),
            train_id: "TEST-BACKLOG-OUT-OF-ORDER-TID".to_string(),
            service_date,
            msg_type: "0002".to_string(),
            event_type: None,
            planned_timestamp: None,
            actual_timestamp: Some("2026-09-06T19:15:00Z".parse().unwrap()),
            variation_status: None,
            delay_minutes: None,
            dedup_key: "test-backlog-out-of-order-stale-cancel-dedup".to_string(),
            gbtt_timestamp: None,
        };
        ingest_shared_movement(&pool, &stale_cancellation)
            .await
            .expect("the stale cancellation's call must succeed (a guarded no-op is not an error)");

        let (status, last_reported_location, delay_minutes): (String, Option<String>, Option<i32>) =
            sqlx::query_as(
                "SELECT status, last_reported_location, delay_minutes \
                 FROM train_current_state WHERE trains_id = $1",
            )
            .bind(trains_id)
            .fetch_one(&pool)
            .await
            .expect("a current-state row must exist");
        assert_eq!(
            status, "en_route",
            "the stale cancellation must not have regressed status to 'cancelled'"
        );
        assert_eq!(
            last_reported_location,
            Some("MKC".to_string()),
            "the stale cancellation must not have touched the newer movement's location"
        );
        assert_eq!(
            delay_minutes,
            Some(0),
            "the stale cancellation must not have regressed delay_minutes"
        );

        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                ingest_shared_movement_is_a_no_op_with_no_known_train_uid -- --ignored --test-threads=1`"]
    async fn ingest_shared_movement_is_a_no_op_with_no_known_train_uid() {
        let pool = connect().await;
        let event = TrustBacklogEventMessage {
            crs: Some("WAT".to_string()),
            train_uid: None, // the accepted gap -- this process never saw the Activation
            train_id: "TEST-INGEST-NO-UID-TRAIN-ID".to_string(),
            service_date: "2026-09-06".parse().unwrap(),
            msg_type: "0003".to_string(),
            event_type: Some("DEPARTURE".to_string()),
            planned_timestamp: None,
            actual_timestamp: None,
            variation_status: None,
            delay_minutes: None,
            dedup_key: "test-ingest-shared-no-uid-dedup".to_string(),
            gbtt_timestamp: None,
        };
        ingest_shared_movement(&pool, &event)
            .await
            .expect("must not error, just no-op");
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM trains WHERE train_id = 'TEST-INGEST-NO-UID-TRAIN-ID'",
        )
        .fetch_one(&pool)
        .await
        .expect("count");
        assert_eq!(
            count, 0,
            "no trains row should be created with no known train_uid"
        );
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                ingest_shared_movement_twice_with_the_same_event_writes_exactly_one_row_each \
                -- --ignored --test-threads=1`"]
    async fn ingest_shared_movement_twice_with_the_same_event_writes_exactly_one_row_each() {
        // Same real code path (ingest_shared_movement) called twice against
        // non-reset state -- not a duplicated inline copy, not a reset in
        // between -- proving the composed find_or_create_train (ON CONFLICT
        // DO UPDATE ... RETURNING id), mark_train_resolved (plain overwrite
        // UPDATE), and upsert_train_movement (ON CONFLICT DO NOTHING /
        // DO UPDATE) are genuinely safe against Redis Streams' at-least-once
        // redelivery of the same trust-backlog batch. Tasks 7 and 10 both had
        // to be fixed in review because their "idempotency" tests didn't
        // actually re-invoke the same code path against non-reset state --
        // this test exists specifically so that mistake can't recur silently
        // here.
        let pool = connect().await;
        let event = TrustBacklogEventMessage {
            crs: Some("WAT".to_string()),
            train_uid: Some("TEST-INGEST-TWICE-UID".to_string()),
            train_id: "TEST-INGEST-TWICE-TRAIN-ID".to_string(),
            service_date: "2026-09-06".parse().unwrap(),
            msg_type: "0003".to_string(),
            event_type: Some("DEPARTURE".to_string()),
            planned_timestamp: Some("2026-09-06T19:15:00Z".parse().unwrap()),
            actual_timestamp: Some("2026-09-06T19:16:00Z".parse().unwrap()),
            variation_status: Some("LATE".to_string()),
            delay_minutes: Some(1),
            dedup_key: "test-ingest-shared-twice-dedup".to_string(),
            gbtt_timestamp: None,
        };

        ingest_shared_movement(&pool, &event)
            .await
            .expect("first ingest_shared_movement");
        ingest_shared_movement(&pool, &event)
            .await
            .expect("second ingest_shared_movement, same event, non-reset state");

        let trains_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM trains WHERE train_uid = 'TEST-INGEST-TWICE-UID'",
        )
        .fetch_one(&pool)
        .await
        .expect("count trains");
        assert_eq!(
            trains_count, 1,
            "exactly one trains row after two identical calls"
        );

        let (trains_id,): (i64,) =
            sqlx::query_as("SELECT id FROM trains WHERE train_uid = 'TEST-INGEST-TWICE-UID'")
                .fetch_one(&pool)
                .await
                .expect("trains id");

        let movement_events_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM train_movement_events WHERE trains_id = $1")
                .bind(trains_id)
                .fetch_one(&pool)
                .await
                .expect("count train_movement_events");
        assert_eq!(
            movement_events_count, 1,
            "exactly one train_movement_events row after two identical calls"
        );

        let current_state_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM train_current_state WHERE trains_id = $1")
                .bind(trains_id)
                .fetch_one(&pool)
                .await
                .expect("count train_current_state");
        assert_eq!(
            current_state_count, 1,
            "exactly one train_current_state row after two identical calls"
        );

        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                ingest_shared_movements_batch_collapses_round_trips_across_a_real_batch \
                -- --ignored --test-threads=1`"]
    async fn ingest_shared_movements_batch_collapses_round_trips_across_a_real_batch() {
        // A real, non-trivial batch: two distinct trains (A, B), with A
        // seeing TWO events (proving the batched find_or_create_train/
        // mark_train_resolved calls dedup a repeated identity rather than
        // writing it twice), plus a no-known-train_uid event (proving the
        // existing no-op path still no-ops from inside a batch, not just
        // when called standalone).
        //
        // NOTE: this test does NOT prove intra-batch causality for
        // fetch_previous_derived_state, despite an earlier version of this
        // comment claiming it did -- `journey::apply_movement` derives
        // every field this test asserts on (status, last_event_type,
        // delay_minutes) from the CURRENT movement alone, never from
        // `previous`, so those assertions would pass identically whether or
        // not `previous` state were threaded correctly between A's two
        // events. See
        // `ingest_shared_movements_batch_preserves_intra_batch_causality_for_a_movement_then_cancellation_pair`
        // below for a test that actually discriminates on this (using a
        // Movement -> Cancellation pair, since `apply_cancellation` DOES
        // copy fields straight from `previous`).
        //
        // Before this task's batching change, this
        // would have cost 5 sequential round trips per event (15 total);
        // after, it costs 3 batched round trips for identity resolution/
        // previous-state-fetch plus 2 per event that actually needs a
        // shared-table write (the no-uid event needs none) -- 3 + 2*3 = 9,
        // and NEITHER count grows with how many events happen to share an
        // identity, unlike the old per-event loop.
        let pool = connect().await;
        let service_date: NaiveDate = "2026-09-06".parse().unwrap();

        let event_a1 = TrustBacklogEventMessage {
            crs: Some("EUS".to_string()),
            train_uid: Some("TEST-BATCH-UID-A".to_string()),
            train_id: "TEST-BATCH-TRAIN-ID-A1".to_string(),
            service_date,
            msg_type: "0003".to_string(),
            event_type: Some("DEPARTURE".to_string()),
            planned_timestamp: Some("2026-09-06T19:15:00Z".parse().unwrap()),
            actual_timestamp: Some("2026-09-06T19:16:00Z".parse().unwrap()),
            variation_status: Some("LATE".to_string()),
            delay_minutes: Some(1),
            dedup_key: "test-batch-dedup-a1".to_string(),
            gbtt_timestamp: None,
        };
        let event_a2 = TrustBacklogEventMessage {
            crs: Some("MKC".to_string()),
            train_uid: Some("TEST-BATCH-UID-A".to_string()),
            // A later Movement re-supplying a DIFFERENT train_id than
            // event_a1's -- proves mark_trains_resolved_batch keeps the
            // LAST event's value, same as the old sequential-overwrite
            // loop would.
            train_id: "TEST-BATCH-TRAIN-ID-A2".to_string(),
            service_date,
            msg_type: "0003".to_string(),
            event_type: Some("ARRIVAL".to_string()),
            planned_timestamp: Some("2026-09-06T19:45:00Z".parse().unwrap()),
            actual_timestamp: Some("2026-09-06T19:45:00Z".parse().unwrap()),
            variation_status: Some("ON TIME".to_string()),
            delay_minutes: Some(0),
            dedup_key: "test-batch-dedup-a2".to_string(),
            gbtt_timestamp: None,
        };
        let event_b = TrustBacklogEventMessage {
            crs: Some("WAT".to_string()),
            train_uid: Some("TEST-BATCH-UID-B".to_string()),
            train_id: "TEST-BATCH-TRAIN-ID-B".to_string(),
            service_date,
            msg_type: "0003".to_string(),
            event_type: Some("DEPARTURE".to_string()),
            planned_timestamp: Some("2026-09-06T20:00:00Z".parse().unwrap()),
            actual_timestamp: Some("2026-09-06T20:00:00Z".parse().unwrap()),
            variation_status: Some("ON TIME".to_string()),
            delay_minutes: Some(0),
            dedup_key: "test-batch-dedup-b".to_string(),
            gbtt_timestamp: None,
        };
        let event_no_uid = TrustBacklogEventMessage {
            crs: Some("WAT".to_string()),
            train_uid: None, // the accepted gap -- see ingest_shared_movement's doc comment
            train_id: "TEST-BATCH-NO-UID-TRAIN-ID".to_string(),
            service_date,
            msg_type: "0003".to_string(),
            event_type: Some("DEPARTURE".to_string()),
            planned_timestamp: None,
            actual_timestamp: None,
            variation_status: None,
            delay_minutes: None,
            dedup_key: "test-batch-dedup-no-uid".to_string(),
            gbtt_timestamp: None,
        };

        let events = vec![event_a1, event_a2, event_b, event_no_uid];
        let results = ingest_shared_movements_batch(&pool, &events).await;
        assert_eq!(results.len(), 4, "one result per input event");
        for (i, result) in results.iter().enumerate() {
            assert!(
                result.is_ok(),
                "event {i} should have succeeded: {result:?}"
            );
        }

        // Exactly one `trains` row per distinct train_uid -- the whole
        // point of deduping (train_uid, service_date) pairs before the
        // batched find_or_create_train call.
        let trains_a_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM trains WHERE train_uid = 'TEST-BATCH-UID-A'")
                .fetch_one(&pool)
                .await
                .expect("count trains for A");
        assert_eq!(trains_a_count, 1);

        let (trains_a_id, train_id_a): (i64, Option<String>) =
            sqlx::query_as("SELECT id, train_id FROM trains WHERE train_uid = 'TEST-BATCH-UID-A'")
                .fetch_one(&pool)
                .await
                .expect("trains row for A");
        assert_eq!(
            train_id_a,
            Some("TEST-BATCH-TRAIN-ID-A2".to_string()),
            "mark_train_resolved must keep the LAST event's train_id, same as the old \
             sequential-overwrite loop"
        );

        let (trains_b_id,): (i64,) =
            sqlx::query_as("SELECT id FROM trains WHERE train_uid = 'TEST-BATCH-UID-B'")
                .fetch_one(&pool)
                .await
                .expect("trains row for B");

        // Both of A's movement events were written -- the batched identity
        // writes did not swallow the second event for the same train.
        let movement_events_a: i64 =
            sqlx::query_scalar("SELECT count(*) FROM train_movement_events WHERE trains_id = $1")
                .bind(trains_a_id)
                .fetch_one(&pool)
                .await
                .expect("count train_movement_events for A");
        assert_eq!(movement_events_a, 2);

        // A's current_state reflects the SECOND event, not the first --
        // proving intra-batch causality (event_a2's derived state was
        // computed from event_a1's just-written state, not a stale
        // pre-batch DB read) survived collapsing fetch_previous_derived_state
        // into one SELECT.
        let (status_a, last_event_type_a, delay_a): (String, Option<String>, Option<i32>) =
            sqlx::query_as(
                "SELECT status, last_event_type, delay_minutes FROM train_current_state \
                 WHERE trains_id = $1",
            )
            .bind(trains_a_id)
            .fetch_one(&pool)
            .await
            .expect("current state for A");
        assert_eq!(status_a, "en_route");
        assert_eq!(last_event_type_a, Some("ARRIVAL".to_string()));
        assert_eq!(delay_a, Some(0));

        let (status_b,): (String,) =
            sqlx::query_as("SELECT status FROM train_current_state WHERE trains_id = $1")
                .bind(trains_b_id)
                .fetch_one(&pool)
                .await
                .expect("current state for B");
        assert_eq!(status_b, "en_route");

        let no_uid_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM trains WHERE train_id = 'TEST-BATCH-NO-UID-TRAIN-ID'",
        )
        .fetch_one(&pool)
        .await
        .expect("count no-uid trains");
        assert_eq!(
            no_uid_count, 0,
            "the no-known-train_uid event must still no-op inside a batch"
        );

        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_a_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_b_id)
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                ingest_shared_movements_batch_falls_back_to_per_pair_find_or_create_when_the_batched_insert_fails \
                -- --ignored --test-threads=1`"]
    async fn ingest_shared_movements_batch_falls_back_to_per_pair_find_or_create_when_the_batched_insert_fails()
     {
        // Forces `find_or_create_trains_batch`'s batched `INSERT ...
        // UNNEST(...)` to fail for the WHOLE batch by giving one event's
        // train_uid an embedded NUL byte -- Postgres genuinely rejects any
        // TEXT value containing one (`invalid byte sequence for encoding
        // "UTF8": 0x00`, confirmed against this same live database before
        // writing this test), so this isn't a contrived/mocked failure.
        // Asserts the fallback then retries one pair at a time, so the
        // GOOD event still succeeds and the BAD event's failure is
        // reported against only itself -- not silently swallowed, and not
        // misattributed to the good event.
        let pool = connect().await;
        let service_date: NaiveDate = "2026-09-06".parse().unwrap();

        let good = TrustBacklogEventMessage {
            crs: Some("EUS".to_string()),
            train_uid: Some("TEST-FALLBACK-STEP1-GOOD".to_string()),
            train_id: "TEST-FALLBACK-STEP1-GOOD-TID".to_string(),
            service_date,
            msg_type: "0003".to_string(),
            event_type: Some("DEPARTURE".to_string()),
            planned_timestamp: Some("2026-09-06T19:15:00Z".parse().unwrap()),
            actual_timestamp: Some("2026-09-06T19:16:00Z".parse().unwrap()),
            variation_status: Some("ON TIME".to_string()),
            delay_minutes: Some(0),
            dedup_key: "test-fallback-step1-good-dedup".to_string(),
            gbtt_timestamp: None,
        };
        let bad = TrustBacklogEventMessage {
            crs: Some("WAT".to_string()),
            // The embedded NUL byte is what trips the batched INSERT.
            train_uid: Some("TEST-FALLBACK-STEP1-BAD\u{0}".to_string()),
            train_id: "TEST-FALLBACK-STEP1-BAD-TID".to_string(),
            service_date,
            msg_type: "0003".to_string(),
            event_type: Some("DEPARTURE".to_string()),
            planned_timestamp: None,
            actual_timestamp: None,
            variation_status: None,
            delay_minutes: None,
            dedup_key: "test-fallback-step1-bad-dedup".to_string(),
            gbtt_timestamp: None,
        };

        let results = ingest_shared_movements_batch(&pool, &[good, bad]).await;
        assert_eq!(results.len(), 2);
        assert!(
            results[0].is_ok(),
            "the good event must still succeed: {:?}",
            results[0]
        );
        let bad_err = results[1]
            .as_ref()
            .expect_err("the bad event must fail, not be silently swallowed");
        assert!(
            bad_err.to_string().contains("find_or_create_train failed"),
            "the bad event's error must be attributed to the find_or_create_train fallback step, \
             got: {bad_err}"
        );

        let (trains_id, status): (i64, String) = sqlx::query_as(
            "SELECT tr.id, cs.status FROM trains tr \
             JOIN train_current_state cs ON cs.trains_id = tr.id \
             WHERE tr.train_uid = 'TEST-FALLBACK-STEP1-GOOD'",
        )
        .fetch_one(&pool)
        .await
        .expect("the good event's trains/current_state rows must exist");
        assert_eq!(status, "en_route");

        let bad_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM trains WHERE train_id = 'TEST-FALLBACK-STEP1-BAD-TID'",
        )
        .fetch_one(&pool)
        .await
        .expect("count");
        assert_eq!(
            bad_count, 0,
            "the bad event's identity must never have been created"
        );

        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                ingest_shared_movements_batch_falls_back_to_per_pair_mark_resolved_when_the_batched_update_fails \
                -- --ignored --test-threads=1`"]
    async fn ingest_shared_movements_batch_falls_back_to_per_pair_mark_resolved_when_the_batched_update_fails()
     {
        // Same technique as the find_or_create_trains_batch fallback test
        // above, but the NUL byte is on `train_id` instead of `train_uid`,
        // so BOTH events resolve a real trains_id via Step 1 (proving this
        // failure is genuinely isolated to Step 2, `mark_trains_resolved_batch`'s
        // batched `UPDATE ... FROM UNNEST(...)`), which then fails only for
        // the bad event's pair. Asserts the good event still gets its
        // train_id resolved and its current_state written, while the bad
        // event's failure is attributed to only itself and its identity
        // row is left un-resolved (train_id still NULL) rather than ending
        // up with a wrong/partial value.
        let pool = connect().await;
        let service_date: NaiveDate = "2026-09-06".parse().unwrap();

        let good = TrustBacklogEventMessage {
            crs: Some("EUS".to_string()),
            train_uid: Some("TEST-FALLBACK-STEP2-GOOD".to_string()),
            train_id: "TEST-FALLBACK-STEP2-GOOD-TID".to_string(),
            service_date,
            msg_type: "0003".to_string(),
            event_type: Some("DEPARTURE".to_string()),
            planned_timestamp: Some("2026-09-06T19:15:00Z".parse().unwrap()),
            actual_timestamp: Some("2026-09-06T19:16:00Z".parse().unwrap()),
            variation_status: Some("ON TIME".to_string()),
            delay_minutes: Some(0),
            dedup_key: "test-fallback-step2-good-dedup".to_string(),
            gbtt_timestamp: None,
        };
        let bad = TrustBacklogEventMessage {
            crs: Some("WAT".to_string()),
            train_uid: Some("TEST-FALLBACK-STEP2-BAD".to_string()),
            // The embedded NUL byte is what trips the batched UPDATE, not
            // the (perfectly valid) train_uid this time.
            train_id: "TEST-FALLBACK-STEP2-BAD-TID\u{0}".to_string(),
            service_date,
            msg_type: "0003".to_string(),
            event_type: Some("DEPARTURE".to_string()),
            planned_timestamp: None,
            actual_timestamp: None,
            variation_status: None,
            delay_minutes: None,
            dedup_key: "test-fallback-step2-bad-dedup".to_string(),
            gbtt_timestamp: None,
        };

        let results = ingest_shared_movements_batch(&pool, &[good, bad]).await;
        assert_eq!(results.len(), 2);
        assert!(
            results[0].is_ok(),
            "the good event must still succeed: {:?}",
            results[0]
        );
        let bad_err = results[1]
            .as_ref()
            .expect_err("the bad event must fail, not be silently swallowed");
        assert!(
            bad_err.to_string().contains("mark_train_resolved failed"),
            "the bad event's error must be attributed to the mark_train_resolved fallback step, \
             got: {bad_err}"
        );

        let (good_trains_id, good_train_id, good_status): (i64, Option<String>, String) =
            sqlx::query_as(
                "SELECT tr.id, tr.train_id, cs.status FROM trains tr \
                 JOIN train_current_state cs ON cs.trains_id = tr.id \
                 WHERE tr.train_uid = 'TEST-FALLBACK-STEP2-GOOD'",
            )
            .fetch_one(&pool)
            .await
            .expect("the good event's trains/current_state rows must exist");
        assert_eq!(
            good_train_id,
            Some("TEST-FALLBACK-STEP2-GOOD-TID".to_string())
        );
        assert_eq!(good_status, "en_route");

        // Step 1 (identity resolution) succeeded for the bad event too --
        // only Step 2 (resolving train_id) failed for it -- so a trains
        // row exists, but must have been left un-resolved rather than
        // ending up with a truncated/garbage train_id.
        let (bad_trains_id, bad_train_id): (i64, Option<String>) = sqlx::query_as(
            "SELECT id, train_id FROM trains WHERE train_uid = 'TEST-FALLBACK-STEP2-BAD'",
        )
        .fetch_one(&pool)
        .await
        .expect("the bad event's trains row must still have been created by Step 1");
        assert_eq!(
            bad_train_id, None,
            "a failed mark_train_resolved must leave train_id un-resolved, not partially written"
        );
        let bad_current_state_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM train_current_state WHERE trains_id = $1")
                .bind(bad_trains_id)
                .fetch_one(&pool)
                .await
                .expect("count");
        assert_eq!(
            bad_current_state_count, 0,
            "an event whose resolution failed must not reach the derive/write step"
        );

        sqlx::query("DELETE FROM trains WHERE id IN ($1, $2)")
            .bind(good_trains_id)
            .bind(bad_trains_id)
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                ingest_shared_movements_batch_falls_back_to_per_id_fetch_when_the_batched_previous_state_select_fails \
                -- --ignored --test-threads=1`"]
    async fn ingest_shared_movements_batch_falls_back_to_per_id_fetch_when_the_batched_previous_state_select_fails()
     {
        // Unlike Steps 1/2, `fetch_previous_derived_states_batch`'s only
        // bound parameter is a `Vec<i64>` of ALREADY-resolved surrogate
        // trains_ids, not raw/untrusted event data -- there is no "bad
        // row" shape a caller can supply that makes this SELECT fail for
        // one id but not another. What IS real and reachable in
        // production is the batched SELECT failing outright for reasons
        // unrelated to row content (a dropped connection, a statement
        // timeout) while the DATABASE ITSELF stays healthy -- so this test
        // uses `FORCE_FETCH_PREVIOUS_DERIVED_STATES_BATCH_FAILURE`, a
        // one-shot, #[cfg(test)]-only fault injector (compiled out of
        // every non-test build -- see its own doc comment), to force
        // exactly that once, and then exercises the REAL per-row fallback
        // (`fetch_previous_derived_state`) against real data.
        //
        // Two trains each get a real Movement first (establishing REAL,
        // non-default previous state), then, with the fault armed, both
        // get a Cancellation in the SAME batch call. `apply_cancellation`
        // copies `last_reported_location`/`last_event_type`/`delay_minutes`
        // straight from `previous` -- so this only passes if the fallback
        // actually fetched each train's REAL prior state from the
        // database, not a default/lost one.
        let pool = connect().await;
        let service_date: NaiveDate = "2026-09-06".parse().unwrap();

        let movement_a = TrustBacklogEventMessage {
            crs: Some("EUS".to_string()),
            train_uid: Some("TEST-FALLBACK-STEP3-UID-A".to_string()),
            train_id: "TEST-FALLBACK-STEP3-TID-A".to_string(),
            service_date,
            msg_type: "0003".to_string(),
            event_type: Some("DEPARTURE".to_string()),
            planned_timestamp: Some("2026-09-06T19:15:00Z".parse().unwrap()),
            actual_timestamp: Some("2026-09-06T19:20:00Z".parse().unwrap()),
            variation_status: Some("LATE".to_string()),
            delay_minutes: Some(5),
            dedup_key: "test-fallback-step3-movement-a-dedup".to_string(),
            gbtt_timestamp: None,
        };
        let movement_b = TrustBacklogEventMessage {
            crs: Some("WAT".to_string()),
            train_uid: Some("TEST-FALLBACK-STEP3-UID-B".to_string()),
            train_id: "TEST-FALLBACK-STEP3-TID-B".to_string(),
            service_date,
            msg_type: "0003".to_string(),
            event_type: Some("ARRIVAL".to_string()),
            planned_timestamp: Some("2026-09-06T20:00:00Z".parse().unwrap()),
            actual_timestamp: Some("2026-09-06T20:03:00Z".parse().unwrap()),
            variation_status: Some("LATE".to_string()),
            delay_minutes: Some(3),
            dedup_key: "test-fallback-step3-movement-b-dedup".to_string(),
            gbtt_timestamp: None,
        };
        let seed_results = ingest_shared_movements_batch(&pool, &[movement_a, movement_b]).await;
        assert!(
            seed_results.iter().all(Result::is_ok),
            "seeding movements must succeed: {seed_results:?}"
        );

        let (trains_a_id,): (i64,) =
            sqlx::query_as("SELECT id FROM trains WHERE train_uid = 'TEST-FALLBACK-STEP3-UID-A'")
                .fetch_one(&pool)
                .await
                .expect("trains row for A");
        let (trains_b_id,): (i64,) =
            sqlx::query_as("SELECT id FROM trains WHERE train_uid = 'TEST-FALLBACK-STEP3-UID-B'")
                .fetch_one(&pool)
                .await
                .expect("trains row for B");

        // Arm the one-shot fault injector, then send both trains a
        // Cancellation in the SAME batch call.
        FORCE_FETCH_PREVIOUS_DERIVED_STATES_BATCH_FAILURE
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let cancel_a = TrustBacklogEventMessage {
            crs: None,
            train_uid: Some("TEST-FALLBACK-STEP3-UID-A".to_string()),
            train_id: "TEST-FALLBACK-STEP3-TID-A".to_string(),
            service_date,
            msg_type: "0002".to_string(),
            event_type: None,
            planned_timestamp: None,
            // A real Cancellation always carries TRUST's own `canx_timestamp`
            // in this field (see `upsert_train_movement`'s own doc comment,
            // and `crates/trust-backlog-consumer/src/process.rs`'s
            // Cancellation construction) -- `None` here would be an
            // unrealistic fixture as of the event-time monotonicity guard
            // (Option C of the write-race design doc): this event's
            // `event_time` must be at or after movement_a's (19:20) for its
            // write to apply at all.
            actual_timestamp: Some("2026-09-06T19:25:00Z".parse().unwrap()),
            variation_status: None,
            delay_minutes: None,
            dedup_key: "test-fallback-step3-cancel-a-dedup".to_string(),
            gbtt_timestamp: None,
        };
        let cancel_b = TrustBacklogEventMessage {
            crs: None,
            train_uid: Some("TEST-FALLBACK-STEP3-UID-B".to_string()),
            train_id: "TEST-FALLBACK-STEP3-TID-B".to_string(),
            service_date,
            msg_type: "0002".to_string(),
            event_type: None,
            planned_timestamp: None,
            // Same reasoning as cancel_a's own comment -- at or after
            // movement_b's actual_timestamp (20:03).
            actual_timestamp: Some("2026-09-06T20:05:00Z".parse().unwrap()),
            variation_status: None,
            delay_minutes: None,
            dedup_key: "test-fallback-step3-cancel-b-dedup".to_string(),
            gbtt_timestamp: None,
        };
        let results = ingest_shared_movements_batch(&pool, &[cancel_a, cancel_b]).await;

        assert!(
            !FORCE_FETCH_PREVIOUS_DERIVED_STATES_BATCH_FAILURE
                .load(std::sync::atomic::Ordering::SeqCst),
            "the fault must actually have fired during this call (one-shot flag consumed), \
             otherwise this test isn't exercising the fallback at all"
        );
        assert!(
            results.iter().all(Result::is_ok),
            "both cancellations must still succeed via the per-id fallback: {results:?}"
        );

        let (status_a, last_loc_a, last_event_a, delay_a): (
            String,
            Option<String>,
            Option<String>,
            Option<i32>,
        ) = sqlx::query_as(
            "SELECT status, last_reported_location, last_event_type, delay_minutes \
             FROM train_current_state WHERE trains_id = $1",
        )
        .bind(trains_a_id)
        .fetch_one(&pool)
        .await
        .expect("current state for A");
        assert_eq!(status_a, "cancelled");
        assert_eq!(
            last_loc_a,
            Some("EUS".to_string()),
            "must carry over A's REAL previous location, fetched via the per-id fallback"
        );
        assert_eq!(last_event_a, Some("DEPARTURE".to_string()));
        assert_eq!(delay_a, Some(5));

        let (status_b, last_loc_b, last_event_b, delay_b): (
            String,
            Option<String>,
            Option<String>,
            Option<i32>,
        ) = sqlx::query_as(
            "SELECT status, last_reported_location, last_event_type, delay_minutes \
             FROM train_current_state WHERE trains_id = $1",
        )
        .bind(trains_b_id)
        .fetch_one(&pool)
        .await
        .expect("current state for B");
        assert_eq!(status_b, "cancelled");
        assert_eq!(
            last_loc_b,
            Some("WAT".to_string()),
            "must carry over B's REAL previous location, fetched via the per-id fallback"
        );
        assert_eq!(last_event_b, Some("ARRIVAL".to_string()));
        assert_eq!(delay_b, Some(3));

        sqlx::query("DELETE FROM trains WHERE id IN ($1, $2)")
            .bind(trains_a_id)
            .bind(trains_b_id)
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                ingest_shared_movements_batch_preserves_intra_batch_causality_for_a_movement_then_cancellation_pair \
                -- --ignored --test-threads=1`"]
    async fn ingest_shared_movements_batch_preserves_intra_batch_causality_for_a_movement_then_cancellation_pair()
     {
        // The discriminating test the flagship
        // `ingest_shared_movements_batch_collapses_round_trips_across_a_real_batch`
        // test above claimed to be, but isn't: `journey::apply_movement`
        // derives every field a same-train Movement-then-Movement pair
        // could assert on purely from the CURRENT movement, never from
        // `previous` -- so that test would pass identically even if a
        // broken implementation read every event in a batch from the SAME
        // stale pre-batch snapshot instead of the causally-updated
        // in-memory state map.
        //
        // `journey::apply_cancellation`, by contrast, copies
        // `last_reported_location`/`last_event_type`/`delay_minutes`
        // straight from `previous` -- so a same-train Movement followed by
        // a Cancellation, in ONE batch call, for a train with NO prior
        // database row (previous == DerivedState::awaiting_activation()
        // before this call), actually discriminates: a correct
        // implementation has the Cancellation observe the Movement's
        // JUST-COMPUTED state (real location/event_type/delay); a broken
        // one that re-reads a pre-batch snapshot would have it observe the
        // pre-batch default instead (None/None/None), since there was no
        // database row before this call started.
        let pool = connect().await;
        let service_date: NaiveDate = "2026-09-06".parse().unwrap();

        let movement = TrustBacklogEventMessage {
            crs: Some("EUS".to_string()),
            train_uid: Some("TEST-CAUSALITY-UID".to_string()),
            train_id: "TEST-CAUSALITY-TID".to_string(),
            service_date,
            msg_type: "0003".to_string(),
            event_type: Some("DEPARTURE".to_string()),
            planned_timestamp: Some("2026-09-06T19:15:00Z".parse().unwrap()),
            actual_timestamp: Some("2026-09-06T19:20:00Z".parse().unwrap()),
            variation_status: Some("LATE".to_string()),
            delay_minutes: Some(5),
            dedup_key: "test-causality-movement-dedup".to_string(),
            gbtt_timestamp: None,
        };
        let cancellation = TrustBacklogEventMessage {
            crs: None,
            train_uid: Some("TEST-CAUSALITY-UID".to_string()),
            train_id: "TEST-CAUSALITY-TID".to_string(),
            service_date,
            msg_type: "0002".to_string(),
            event_type: None,
            planned_timestamp: None,
            // A real Cancellation always carries TRUST's own `canx_timestamp`
            // here (see `upsert_train_movement`'s own doc comment) -- `None`
            // would be unrealistic as of the event-time monotonicity guard
            // (Option C of the write-race design doc): this event's
            // `event_time` must be at or after the movement's (19:20) for
            // its write to apply at all.
            actual_timestamp: Some("2026-09-06T19:25:00Z".parse().unwrap()),
            variation_status: None,
            delay_minutes: None,
            dedup_key: "test-causality-cancellation-dedup".to_string(),
            gbtt_timestamp: None,
        };

        let results = ingest_shared_movements_batch(&pool, &[movement, cancellation]).await;
        assert!(
            results.iter().all(Result::is_ok),
            "both events should succeed: {results:?}"
        );

        let (trains_id,): (i64,) =
            sqlx::query_as("SELECT id FROM trains WHERE train_uid = 'TEST-CAUSALITY-UID'")
                .fetch_one(&pool)
                .await
                .expect("trains row");

        let (status, last_reported_location, last_event_type, delay_minutes): (
            String,
            Option<String>,
            Option<String>,
            Option<i32>,
        ) = sqlx::query_as(
            "SELECT status, last_reported_location, last_event_type, delay_minutes \
             FROM train_current_state WHERE trains_id = $1",
        )
        .bind(trains_id)
        .fetch_one(&pool)
        .await
        .expect("current state");

        assert_eq!(status, "cancelled");
        assert_eq!(
            last_reported_location,
            Some("EUS".to_string()),
            "the Cancellation must have observed the Movement's just-computed location, not a \
             stale pre-batch default -- this is what proves intra-batch causality survived \
             batching"
        );
        assert_eq!(
            last_event_type,
            Some("DEPARTURE".to_string()),
            "same causality proof, for last_event_type"
        );
        assert_eq!(
            delay_minutes,
            Some(5),
            "same causality proof, for delay_minutes"
        );

        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
    }
}

#[cfg(test)]
mod classify_tests {
    use super::*;

    #[test]
    fn constraint_violations_and_invalid_input_are_data_errors() {
        assert_eq!(data_error_reason("23514"), Some("check_violation"));
        assert_eq!(data_error_reason("23502"), Some("not_null_violation"));
        assert_eq!(
            data_error_reason("22P02"),
            Some("invalid_text_representation")
        );
        assert_eq!(
            data_error_reason("22021"),
            Some("character_not_in_repertoire")
        );
        assert_eq!(
            data_error_reason("23999"),
            Some("integrity_constraint_violation")
        );
        assert_eq!(data_error_reason("22999"), Some("data_exception"));
    }

    #[test]
    fn transient_and_unexpected_sqlstates_are_not_data_errors() {
        for code in [
            "40001", // serialization_failure
            "40P01", // deadlock_detected
            "57014", // query_canceled (statement_timeout)
            "55P03", // lock_not_available (lock_timeout)
            "08006", // connection_failure
            "53300", // too_many_connections
            "42703", // undefined_column, e.g. mid rolling deploy
        ] {
            assert_eq!(data_error_reason(code), None, "{code}");
        }
    }

    #[test]
    fn non_database_errors_are_not_data_errors() {
        assert!(classify_data_error(&sqlx::Error::PoolTimedOut).is_none());
        assert!(classify_data_error(&sqlx::Error::Io(std::io::Error::other("reset"))).is_none());
    }

    /// The anyhow-level classifier the two ingest routes use: a pool
    /// timeout under any amount of context is transient, a plain error with
    /// no database cause is transient, and a fanned-out
    /// [`SharedMovementError`] keeps the classification it was built with.
    #[test]
    fn anyhow_errors_are_classified_through_their_chain() {
        let pool_timeout = anyhow::Error::from(sqlx::Error::PoolTimedOut).context("while writing");
        assert!(classify_anyhow_data_error(&pool_timeout).is_none());
        assert!(classify_anyhow_data_error(&anyhow::anyhow!("no database cause")).is_none());

        let transient = anyhow::Error::from(SharedMovementError::new(
            "find_or_create_train",
            &pool_timeout,
        ));
        assert!(classify_anyhow_data_error(&transient).is_none());

        let data = DataError {
            sqlstate: "23514".to_string(),
            reason: "check_violation",
            constraint: None,
            message: "bad".to_string(),
        };
        let fanned_out = anyhow::Error::from(SharedMovementError {
            message: "mark_train_resolved failed".to_string(),
            data_error: Some(data.clone()),
        });
        assert_eq!(classify_anyhow_data_error(&fanned_out), Some(data));
    }
}
