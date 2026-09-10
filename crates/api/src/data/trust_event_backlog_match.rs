// crates/api/src/data/trust_event_backlog_match.rs
//! Backlog-consumption side of `trust_event_backlog`
//! (docs/superpowers/specs/2026-09-05-trust-event-backlog-design.md
//! Decision 3, docs/superpowers/plans/2026-09-05-trust-event-backlog-plan.md
//! Task 5). Walks Decision 3 steps 2-4 exactly:
//!
//! 1. CRS+time lookup against `trust_event_backlog` to discover a
//!    `train_id` (TRUST's own daily identifier) for a pin whose live
//!    TRUST window has already closed, plus a `train_uid` (CIF's own
//!    identifier) if an Activation for that `train_id` is also in the
//!    backlog.
//! 2. Full backfill: every backlog row for that `train_id`+`service_date`,
//!    in `received_at` order. Keyed on `train_id`, NOT `train_uid` --
//!    see `fetch_backlog_history`'s own doc comment for why (a real bug
//!    caught in this plan's second review pass: `train_uid` is only ever
//!    non-NULL on an Activation row in this table, never on a Movement/
//!    Cancellation row, so a `train_uid`-keyed backfill query would only
//!    ever retrieve the Activation row itself and silently miss every
//!    Movement/Cancellation event this feature exists to replay).
//! 3. Replay each row through the SAME `train_tracking::upsert_train_event`
//!    path a live event would have taken, so `train_movement_events`/
//!    `train_current_state`/`resolution_status` end up exactly where a
//!    live-watching trust-consumer would have left them.
//!
//! **Deviation from the plan's own text, confirmed directly against this
//! codebase rather than assumed**: the plan's own Task 5 sketch defines a
//! *local* `MATCH_TOLERANCE` constant, reasoning that `common::MATCH_TOLERANCE`
//! "only exists once `worktree-schedule-first-plan`'s own Task 3 lands."
//! That plan has since landed on `main` (confirmed:
//! `grep -n "MATCH_TOLERANCE" crates/common/src/lib.rs` finds a real, public
//! `pub const MATCH_TOLERANCE: chrono::Duration = chrono::Duration::minutes(20);`,
//! and `crates/api/src/data/schedule_matching.rs` already imports and uses
//! it). This module uses `common::MATCH_TOLERANCE` directly instead of
//! duplicating it locally -- exactly the collapse the plan's own text asked
//! "whoever lands second" to perform, and duplicating it here today would
//! just be two names for the same already-shared constant.
//!
//! **A second deviation, also confirmed directly**: this module's docs
//! (and the plan's own "Dependency on the schedule-first plan" section)
//! describe `upsert_train_event`'s guard as requiring BOTH
//! `resolved_train_uid` and `resolved_train_id` before advancing
//! `resolution_status` to `'resolved'`, and therefore describe an
//! Activation-less backfill as stuck at `'schedule_matched'`/`'pending'`
//! forever. That was accurate against the version of `train_tracking.rs`
//! this plan was originally written against, but the schedule-first
//! design's own Task 9 (its Decision 5, the guard relaxation this plan
//! explicitly named as "not this plan's job") has ALSO since landed on
//! `main` (confirmed: `crates/api/src/data/train_tracking.rs`'s real
//! `upsert_train_event` now fires its `UPDATE ... resolution_status =
//! 'resolved'` on `event.resolved_train_id.is_some()` alone, using
//! `COALESCE($2, train_uid)` for `train_uid` so an already-known value is
//! never clobbered). This module's own code needs no change for that --
//! `replay_backlog_history` already just supplies whatever
//! `resolved_train_uid`/`resolved_train_id` it has, same as before -- but
//! the practical effect is now BETTER than the plan's own worst case: a
//! Movement/Cancellation-only backfill with no Activation in the retention
//! window now still reaches `resolution_status = 'resolved'` (just with
//! `train_uid` left `NULL` if nothing else ever supplied one), not stuck
//! one step short of it. Named here so a future reader comparing this
//! module against the plan's own prose isn't confused by the mismatch.

use chrono::{DateTime, NaiveDate, Utc};
use common::MATCH_TOLERANCE;
use sqlx::PgPool;
use trust_schema::journey::{self, DerivedState};
use trust_schema::schema::Movement;

use crate::data::train_tracking;

#[derive(Debug, Clone, sqlx::FromRow)]
struct BacklogRow {
    train_id: String,
    msg_type: String,
    event_type: Option<String>,
    // The already-translated CRS a Movement row was observed at (`None`
    // for Activation/Cancellation, which carry no location at all -- see
    // Task 1's migration). MUST be threaded through to `apply_movement`'s
    // `loc_crs` param and the replayed event's own `loc_crs` field below --
    // an earlier draft of this function didn't select this column at all
    // and passed `None` unconditionally, silently discarding a value the
    // table actually stores. That would have left every backfilled pin's
    // `train_current_state.last_reported_location` permanently `NULL`
    // even though the real CRS was sitting right there in
    // `trust_event_backlog.crs` -- caught during this plan's second
    // review pass.
    crs: Option<String>,
    planned_timestamp: Option<DateTime<Utc>>,
    actual_timestamp: Option<DateTime<Utc>>,
    variation_status: Option<String>,
}

/// Decision 3 step 2: does any backlog row at `pin_origin_crs`, within
/// `MATCH_TOLERANCE` of `pin_scheduled_departure`, exist? Returns that
/// row's `train_id` (TRUST's own daily identifier -- present on every row
/// this table ever stores, per Task 9) plus, opportunistically, a
/// `train_uid` (CIF's own identifier) if an Activation row for that same
/// `train_id` is also present somewhere in the backlog (it may not be --
/// see this module's own doc comment and this plan's "Dependency on the
/// schedule-first plan" section on why that's an accepted, named gap, not
/// a bug). Arbitrary among ties -- this table has no equivalent of
/// `resolve_origin_departure`'s own "only a DEPARTURE may claim"
/// refinement, since by construction this table already excludes PASS and
/// only Activation/Cancellation/Movement rows exist here at all.
///
/// Deliberately does NOT look at this matching row's own `train_uid`
/// column: a Movement/Cancellation row's `train_uid` is always NULL as
/// written by Task 9's own consumer (only an Activation row ever carries
/// one), so the matching row found here is realistically always a
/// Movement (the only kept type that carries a `crs`) and its `train_uid`
/// column is realistically always NULL. The real train_uid lookup is the
/// second, explicit query below, by `train_id`.
async fn find_backlog_match(
    pool: &PgPool,
    pin_origin_crs: &str,
    pin_scheduled_departure: DateTime<Utc>,
) -> anyhow::Result<Option<(String, Option<String>)>> {
    let window_start = pin_scheduled_departure - MATCH_TOLERANCE;
    let window_end = pin_scheduled_departure + MATCH_TOLERANCE;

    let row: Option<(String,)> = sqlx::query_as(
        "SELECT train_id FROM trust_event_backlog \
         WHERE UPPER(crs) = UPPER($1) AND planned_timestamp BETWEEN $2 AND $3 \
         ORDER BY planned_timestamp LIMIT 1",
    )
    .bind(pin_origin_crs)
    .bind(window_start)
    .bind(window_end)
    .fetch_optional(pool)
    .await?;

    let Some((train_id,)) = row else {
        return Ok(None);
    };

    // Look for an Activation row for the SAME train_id anywhere in the
    // backlog, unscoped by CRS (an Activation carries no location at
    // all) -- the only row type in this table that ever carries a
    // train_uid.
    let activation_uid: Option<(String,)> = sqlx::query_as(
        "SELECT train_uid FROM trust_event_backlog \
         WHERE train_id = $1 AND msg_type = '0001' AND train_uid IS NOT NULL \
         LIMIT 1",
    )
    .bind(&train_id)
    .fetch_optional(pool)
    .await?;
    Ok(Some((train_id, activation_uid.map(|(uid,)| uid))))
}

/// Resolves a bare `(train_uid, service_date)` to TRUST's own `train_id`,
/// via the one row type in this table that ever carries a `train_uid` at
/// all -- an Activation (`msg_type = '0001'`). Unlike `find_backlog_match`
/// above (a CRS+time lookup that discovers an unknown `train_id`), this is
/// the inverse direction: identity is already known, and the caller wants
/// TRUST's own daily identifier to key a `fetch_backlog_history`-style
/// lookup by. `None` covers both "no Activation for this identity is in
/// the backlog's retention window" and "never existed" uniformly -- this
/// table has no way to distinguish them, same posture as every other
/// lookup in this module.
pub async fn find_train_id_by_uid(
    pool: &PgPool,
    train_uid: &str,
    service_date: NaiveDate,
) -> anyhow::Result<Option<String>> {
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT train_id FROM trust_event_backlog \
         WHERE train_uid = $1 AND service_date = $2 AND msg_type = '0001' \
         LIMIT 1",
    )
    .bind(train_uid)
    .bind(service_date)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(train_id,)| train_id))
}

/// Decision 3 step 3: every backlog row for `train_id`/`service_date`, in
/// `received_at` order -- the entire observed history for this train.
///
/// Keyed on `train_id`, NOT `train_uid`. This is deliberate, not a typo:
/// Task 9's own consumer writes `train_uid: None` on every Movement and
/// Cancellation row (only an Activation row ever carries a real
/// `train_uid` -- see that task's own "this consumer doesn't correlate
/// Activation->Movement in-process" comment), so a query filtering on
/// `train_uid = $1` would only ever match the Activation row itself and
/// would silently return zero Movement/Cancellation rows -- exactly the
/// data this whole function exists to retrieve. `train_id`, by contrast,
/// is `NOT NULL` on all three kept message types (the migration's own
/// schema, Task 1) and is the column that actually ties one train's
/// Activation/Movement/Cancellation rows together in this table.
async fn fetch_backlog_history(
    pool: &PgPool,
    train_id: &str,
    service_date: NaiveDate,
) -> anyhow::Result<Vec<BacklogRow>> {
    let rows = sqlx::query_as::<_, BacklogRow>(
        "SELECT train_id, msg_type, event_type, crs, planned_timestamp, \
                actual_timestamp, variation_status \
         FROM trust_event_backlog \
         WHERE train_id = $1 AND service_date = $2 \
         ORDER BY received_at",
    )
    .bind(train_id)
    .bind(service_date)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Decision 3 step 4: replays `history` through the SAME
/// `train_tracking::upsert_train_event` path a live event would have
/// taken. `resolved_train_id` is set on the FIRST replayed row only,
/// mirroring `trust-consumer::process.rs`'s own "only the resolving
/// message carries these" convention -- every subsequent row passes
/// `None`, since `upsert_train_event`'s guard only needs to fire once per
/// pin. `resolved_train_uid` is set alongside it on that same first row
/// **only if `train_uid` is `Some`** -- i.e. only if `find_backlog_match`
/// found an Activation for this `train_id` somewhere in the backlog. If it
/// didn't (the Activation fell outside the retention window, predates
/// this consumer's own deployment, or was simply never emitted on the
/// slice of the feed this consumer saw), `resolved_train_uid` stays `None`
/// on every row -- but `upsert_train_event`'s own guard on `main` today
/// fires on `resolved_train_id.is_some()` alone (see this module's own
/// top-level doc comment on this plan-vs-`main` deviation), so
/// `resolution_status` still advances to `'resolved'` from this replay;
/// only `train_uid` itself is left `NULL` in that case, not the status.
async fn replay_backlog_history(
    pool: &PgPool,
    tracked_train_id: i64,
    train_uid: Option<&str>,
    history: Vec<BacklogRow>,
) -> anyhow::Result<()> {
    let mut previous = DerivedState::awaiting_activation();
    let mut resolution_claimed = false;

    for row in history {
        let (derived, event_type, planned, actual, variation_status) = match row.msg_type.as_str() {
            "0003" => {
                let movement = Movement {
                    train_id: row.train_id.clone(),
                    event_type: row.event_type.clone().unwrap_or_default(),
                    gbtt_timestamp: None,
                    planned_timestamp: row
                        .planned_timestamp
                        .map(|t| t.timestamp_millis().to_string()),
                    actual_timestamp: row
                        .actual_timestamp
                        .map(|t| t.timestamp_millis().to_string()),
                    reporting_stanox: None,
                    loc_stanox: None,
                    toc_id: None,
                    variation_status: row.variation_status.clone(),
                };
                let mut derived = journey::apply_movement(&previous, &movement, row.crs.as_deref());
                // Mirrors trust-consumer::process.rs's own post-apply_movement
                // override exactly: apply_movement's own delay_minutes is a
                // coarse variation_status-only estimate; a real timestamp
                // delta is used when both timestamps and a "LATE" variation
                // are present, same as a live event.
                if let (Some(p), Some(a), Some("LATE")) = (
                    row.planned_timestamp,
                    row.actual_timestamp,
                    row.variation_status.as_deref(),
                ) {
                    derived.delay_minutes = Some((a - p).num_minutes() as i32);
                }
                (
                    derived,
                    row.event_type.clone(),
                    row.planned_timestamp,
                    row.actual_timestamp,
                    row.variation_status.clone(),
                )
            }
            "0002" => (
                journey::apply_cancellation(&previous),
                None,
                None,
                row.actual_timestamp, // canx_timestamp lands in actual_timestamp, mirrors process.rs
                None,
            ),
            // "0001" (Activation) carries no derivable state change of its
            // own in trust_schema::journey -- it only supplies train_uid,
            // already known by the time this function is called. Skipped
            // as a no-op replay step, same as trust-consumer's own
            // process_message treating Activation as producing no posted
            // event.
            _ => continue,
        };

        // `loc_stanox` is always `None` here -- `trust_event_backlog`
        // never persists it (only the already-translated `crs`, see
        // Task 1's migration), so this dedup_key can differ from what a
        // live trust-consumer would have computed for the exact same
        // real-world event (which passes the real `loc_stanox`). Named,
        // accepted limitation, same posture as the plan's own raw_body
        // gap: `ON CONFLICT (tracked_train_id, dedup_key) DO NOTHING`
        // still makes this replay idempotent against ITSELF (a retried
        // `attempt_backlog_match` call, or a redelivered ingest batch
        // upstream of it), which is all this table's own writes ever
        // need -- a live trust-consumer event for the same tracked_train_id
        // arriving *after* a full backfill of an already-departed train is
        // not a realistic scenario this design needs to guard against (by
        // the time a backlog match runs, that train's live TRUST window
        // has already closed, which is the entire reason this feature
        // exists).
        let dedup = trust_schema::dedup::dedup_key(
            &row.train_id,
            &row.msg_type,
            event_type.as_deref(),
            None,
            planned.map(|t| t.timestamp_millis().to_string()).as_deref(),
        );

        let (resolved_train_uid, resolved_train_id) = if !resolution_claimed {
            resolution_claimed = true;
            (train_uid.map(str::to_string), Some(row.train_id.clone()))
        } else {
            (None, None)
        };

        let event = common::TrainMovementEventMessage {
            tracked_train_id,
            resolved_train_uid,
            resolved_train_id,
            dedup_key: dedup,
            msg_type: row.msg_type.clone(),
            event_type,
            loc_stanox: None, // never persisted by trust_event_backlog -- see the dedup_key note above
            loc_crs: row.crs.clone(),
            planned_timestamp: planned,
            actual_timestamp: actual,
            variation_status,
            raw_body: serde_json::json!({}),
            status: derived.status.clone(),
            last_reported_location: derived.last_reported_location.clone(),
            last_event_type: derived.last_event_type.clone(),
            delay_minutes: derived.delay_minutes,
            next_calling_point: derived.next_calling_point.clone(),
            eta_next: None,
            eta_source: None,
        };
        train_tracking::upsert_train_event(pool, &event).await?;
        previous = derived;
    }
    Ok(())
}

/// Entry point: attempts a full backlog match+replay for one pin.
/// Returns `Ok(true)` only if a matching `train_id` was found AND at
/// least one history row was replayed. `Ok(false)` covers every honest
/// "nothing in the backlog for this pin" outcome (no CRS+time match, or
/// the backlog's retention window has already rolled past this
/// service_date) -- exactly Decision 3 step 8's "no regression, no new
/// failure mode" posture: a pin left `Ok(false)` here is exactly as it
/// would have been without this feature at all.
///
/// `Ok(true)` does NOT by itself mean `resolution_status` reached
/// `'resolved'` in every historical version of `upsert_train_event`, but
/// on this codebase's real, current `main` (see this module's own
/// top-level doc comment) it does: `upsert_train_event`'s guard fires on
/// `resolved_train_id.is_some()` alone, and `replay_backlog_history`
/// always supplies one on its first replayed row whenever a match was
/// found at all.
pub async fn attempt_backlog_match(
    pool: &PgPool,
    tracked_train_id: i64,
    pin_origin_crs: &str,
    pin_scheduled_departure: DateTime<Utc>,
    service_date: NaiveDate,
) -> anyhow::Result<bool> {
    let Some((train_id, train_uid)) =
        find_backlog_match(pool, pin_origin_crs, pin_scheduled_departure).await?
    else {
        return Ok(false);
    };

    let history = fetch_backlog_history(pool, &train_id, service_date).await?;
    if history.is_empty() {
        return Ok(false);
    }

    replay_backlog_history(pool, tracked_train_id, train_uid.as_deref(), history).await?;

    // Step A dual-write (docs/superpowers/specs/2026-09-06-shared-train-identity-design.md
    // §2 Step A): only possible when this backlog carried an Activation for
    // this train_id (train_uid is Some) -- a Movement/Cancellation-only
    // backfill has no natural key to create a trains row against, matching
    // Step B's own accepted gap.
    if let Some(train_uid) = &train_uid {
        let trains_id =
            crate::data::trains::find_or_create_train(pool, train_uid, service_date).await?;
        crate::data::trains::mark_train_resolved(pool, trains_id, &train_id).await?;
        sqlx::query("UPDATE train_subscriptions SET trains_id = $2 WHERE id = $1")
            .bind(tracked_train_id)
            .bind(trains_id)
            .execute(pool)
            .await?;
    }

    Ok(true)
}

/// The periodic backlog-match sweep's own entry point -- the fix for the
/// gap this module's own top-level doc comment does NOT (and, until this
/// function, never did) name: `attempt_backlog_match` above was, in
/// production, only ever reached once, synchronously, from
/// `routes::train::post_track` at pin-creation time. A pin created before
/// the tracked train has departed sees an empty (or merely
/// not-yet-relevant) `trust_event_backlog` at that single attempt, and
/// `trust-consumer::matching::resolve_origin_departure`'s own live match
/// only succeeds within `common::MATCH_TOLERANCE` of the pin's scheduled
/// departure -- so a train that departs more than 20 minutes early or late
/// (routine under disruption) has no path left to resolve at all, despite
/// the exact backlog row a retry would match filling in over the next few
/// hours as TRUST movements actually arrive. Mirrors
/// `schedule_matching::run_schedule_match_sweep`'s own shape closely: same
/// "list candidates, attempt each independently, one bad row logs and
/// moves on, return the count matched" structure, same
/// `train_tracking`-owned candidate query pattern.
pub async fn run_backlog_match_sweep(pool: &PgPool) -> anyhow::Result<u64> {
    let rows = train_tracking::list_pending_pins_for_backlog_match(pool).await?;
    let mut matched = 0u64;
    for row in rows {
        let (Some(pin_origin_crs), Some(pin_scheduled_departure)) =
            (row.pin_origin_crs.as_deref(), row.pin_scheduled_departure)
        else {
            tracing::warn!(
                tracked_train_id = row.id,
                "pending backlog pin missing origin CRS or scheduled departure; skipping \
                 (list_pending_pins_for_backlog_match should have already excluded this row)"
            );
            continue;
        };
        match attempt_backlog_match(
            pool,
            row.id,
            pin_origin_crs,
            pin_scheduled_departure,
            row.service_date,
        )
        .await
        {
            Ok(true) => matched += 1,
            Ok(false) => {}
            Err(err) => {
                tracing::warn!(
                    error = ?err,
                    tracked_train_id = row.id,
                    "backlog match attempt failed for this pin; will retry next sweep"
                );
            }
        }
    }
    Ok(matched)
}

/// What a successful [`attempt_backlog_match_by_uid`] replay recovered.
#[derive(Debug, Clone)]
pub struct BacklogReplayOutcome {
    /// TRUST's own daily identifier for this train, recovered from the
    /// backlog's Activation row.
    pub train_id: String,
    /// How many backlog rows were replayed through `upsert_train_event`.
    pub replayed_rows: usize,
    /// The `(CRS, planned departure)` of the earliest origin-shaped
    /// DEPARTURE in the replayed history, when there was one.
    ///
    /// This is the whole reason a bare-`train_uid` subscription can get
    /// schedule data at all. `schedule_query::match_pin` -- and therefore
    /// every schedule lookup in this codebase -- is keyed on
    /// `(origin CRS, departure time)`, which a `train_uid` alone does not
    /// give you (the design spec's own §1 accepted gap). The backlog's own
    /// first DEPARTURE row supplies exactly that pair for a train that has
    /// already run, so the caller can hand it to
    /// `schedule_matching::attempt_schedule_match_for_shared_train`.
    /// `None` when the retained history holds no located DEPARTURE (an
    /// Activation-only window, or one that starts mid-journey).
    pub origin_departure: Option<(String, DateTime<Utc>)>,
}

/// The identity-first counterpart to [`attempt_backlog_match`]: replays a
/// train's retained TRUST history when its `(train_uid, service_date)` is
/// ALREADY known, rather than discovering it from a CRS+time pin.
///
/// This is what `POST /Train/by-uid/{uid}/{date}/track` needs and what
/// review finding I1 found missing entirely. `find_train_id_by_uid` (Task
/// 15) was built for exactly this and, until this fix, had no production
/// caller anywhere -- so a subscription created via that endpoint after the
/// train's live TRUST window had closed received nothing at all: no
/// movement history, no `train_id`, no `resolved_at`, and (via the
/// `origin_departure` this returns) no route to schedule data either.
///
/// Everything below the lookup is deliberately the SAME machinery
/// [`attempt_backlog_match`] uses -- `fetch_backlog_history` +
/// `replay_backlog_history` + the Step A dual-write -- so a backlog replay
/// leaves the database in one shape, reached two ways, rather than two
/// subtly different ones. The only differences are the first step (a
/// `train_uid` lookup instead of a CRS+time one, which also means
/// `train_uid` is always `Some` here and the dual-write is unconditional)
/// and the `origin_departure` this returns for the caller's schedule
/// match.
///
/// Idempotent: `replay_backlog_history`'s writes are
/// `ON CONFLICT ... DO NOTHING`/`DO UPDATE` on the shared tables, and
/// `find_or_create_train`/`mark_train_resolved` are both re-runnable, so
/// calling this again for the same subscription is a no-op beyond the
/// re-read. `Ok(None)` means the backlog holds no Activation for this
/// identity (never emitted, or already pruned past its retention window) --
/// an honest, expected outcome, exactly as `Ok(false)` is for
/// [`attempt_backlog_match`].
pub async fn attempt_backlog_match_by_uid(
    pool: &PgPool,
    tracked_train_id: i64,
    train_uid: &str,
    service_date: NaiveDate,
) -> anyhow::Result<Option<BacklogReplayOutcome>> {
    let Some(train_id) = find_train_id_by_uid(pool, train_uid, service_date).await? else {
        return Ok(None);
    };

    let history = fetch_backlog_history(pool, &train_id, service_date).await?;
    if history.is_empty() {
        return Ok(None);
    }

    // Captured BEFORE the replay consumes `history`. `planned_timestamp`,
    // not `actual_timestamp`: a schedule lookup matches against the BOOKED
    // departure time, and a delayed train's actual time can easily fall
    // outside `MATCH_TOLERANCE` of it. Falls back to `actual_timestamp`
    // only when TRUST sent no planned time at all.
    let origin_departure = history
        .iter()
        .find(|row| {
            row.msg_type == "0003"
                && row.event_type.as_deref() == Some("DEPARTURE")
                && row.crs.is_some()
                && (row.planned_timestamp.is_some() || row.actual_timestamp.is_some())
        })
        .map(|row| {
            (
                row.crs.clone().expect("filtered on crs.is_some()"),
                row.planned_timestamp
                    .or(row.actual_timestamp)
                    .expect("filtered on one of the two being present"),
            )
        });

    let replayed_rows = history.len();
    replay_backlog_history(pool, tracked_train_id, Some(train_uid), history).await?;

    // Step A dual-write, same as `attempt_backlog_match` above -- but
    // unconditional here, because this path's `train_uid` is an input, not
    // something that may or may not have been discovered.
    let trains_id =
        crate::data::trains::find_or_create_train(pool, train_uid, service_date).await?;
    crate::data::trains::mark_train_resolved(pool, trains_id, &train_id).await?;
    sqlx::query("UPDATE train_subscriptions SET trains_id = $2 WHERE id = $1")
        .bind(tracked_train_id)
        .bind(trains_id)
        .execute(pool)
        .await?;

    Ok(Some(BacklogReplayOutcome {
        train_id,
        replayed_rows,
        origin_departure,
    }))
}

#[cfg(test)]
mod db_tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    async fn connect() -> PgPool {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres")
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                attempt_backlog_match -- --ignored --test-threads=1`"]
    async fn a_full_activation_plus_movement_backlog_resolves_the_pin_to_resolved() {
        let pool = connect().await;
        let user_id = "TEST-BACKLOG-MATCH-USER";
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind("backlog-match@example.com")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");

        let service_date: chrono::NaiveDate = "2026-09-05".parse().unwrap();
        let scheduled: DateTime<Utc> = "2026-09-05T18:15:00Z".parse().unwrap();

        // Faithful to Task 9's real producer behavior, NOT a shortcut:
        // the Activation row (msg_type '0001') is the ONLY row that ever
        // carries a real `train_uid` and the ONLY row with `crs = NULL`;
        // the Movement row (msg_type '0003') carries the real `crs` +
        // timing data but `train_uid = NULL` -- Task 9's own consumer
        // never correlates the two in-process, `attempt_backlog_match`
        // does that at read time instead (see `find_backlog_match`'s own
        // doc comment). An earlier draft of this test set `train_uid` on
        // the Movement row directly, which papered over a real bug in
        // this plan's own backfill query -- caught and fixed during this
        // plan's second review pass (see Task 1's migration and this
        // module's `fetch_backlog_history`).
        sqlx::query(
            "INSERT INTO trust_event_backlog \
                (crs, train_uid, train_id, service_date, msg_type, event_type, \
                 planned_timestamp, actual_timestamp, variation_status, dedup_key) \
             VALUES (NULL, $1, $2, $3, '0001', NULL, NULL, NULL, NULL, $4), \
                    ($5, NULL, $2, $3, '0003', 'DEPARTURE', $6, $6, 'ON TIME', $7)",
        )
        .bind("C99999")
        .bind("TEST-BACKLOG-TRAIN-ID")
        .bind(service_date)
        .bind("test-backlog-dedup-activation")
        .bind("EUS")
        .bind(scheduled)
        .bind("test-backlog-dedup-movement")
        .execute(&pool)
        .await
        .expect("seed backlog rows");

        let (tracked_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions (user_id, service_date, pin_origin_crs, pin_scheduled_departure) \
             VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(user_id)
        .bind(service_date)
        .bind("EUS")
        .bind(scheduled)
        .fetch_one(&pool)
        .await
        .expect("seed tracked_trains row");

        let matched =
            attempt_backlog_match(&pool, tracked_train_id, "EUS", scheduled, service_date)
                .await
                .expect("attempt_backlog_match");
        assert!(matched);

        // `tracked_trains` no longer has its own `train_uid` column (Task
        // 22 dropped it) -- the resolved identity now lives exclusively on
        // the shared `trains` row, joined via `trains_id`.
        let (resolution_status, train_uid): (String, Option<String>) = sqlx::query_as(
            "SELECT tt.resolution_status, tr.train_uid \
             FROM train_subscriptions tt LEFT JOIN trains tr ON tr.id = tt.trains_id \
             WHERE tt.id = $1",
        )
        .bind(tracked_train_id)
        .fetch_one(&pool)
        .await
        .expect("read back tracked_trains joined to its resolved trains row");
        assert_eq!(resolution_status, "resolved");
        assert_eq!(train_uid, Some("C99999".to_string()));

        // Real bug caught while running this plan's own end-to-end
        // verification (Task 13): this test's own fixture cleanup, as
        // specced, deleted the trust_event_backlog rows but never the
        // tracked_trains row it inserted above. tracked_trains has a real
        // UNIQUE(train_uid, service_date) WHERE train_uid IS NOT NULL
        // constraint (tracked_trains_resolved_identity, added by the
        // schedule-first design) -- re-running this test without deleting
        // that row made the SECOND run's own INSERT INTO tracked_trains
        // violate that constraint against the FIRST run's leftover row
        // (both resolve to the same train_uid=C99999/service_date). Delete
        // it here too so this test is idempotent across repeated runs, not
        // just its own single first execution.
        sqlx::query("DELETE FROM train_subscriptions WHERE id = $1")
            .bind(tracked_train_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trust_event_backlog WHERE train_id = 'TEST-BACKLOG-TRAIN-ID'")
            .execute(&pool)
            .await
            .ok();
        // Same class of leak as the tracked_trains one documented just
        // above, discovered the same way (Task 8's own end-to-end
        // verification): `attempt_backlog_match`'s own Step A dual-write
        // creates a shared `trains` row for this C99999/2026-09-05 identity
        // too, and this test never cleaned it up. That identity is also
        // used by `schedule_matching::db_tests`'s own EUS fixture -- an
        // uncleaned row here corrupted that unrelated test's `train_id`
        // assertion once Step C started reading `train_id` through the
        // joined `trains` row instead of `tracked_trains`' own column.
        sqlx::query("DELETE FROM trains WHERE train_uid = 'C99999' AND service_date = $1")
            .bind(service_date)
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                attempt_backlog_match_with_no_matching_rows -- --ignored --test-threads=1`"]
    async fn no_matching_backlog_rows_leaves_the_pin_untouched() {
        let pool = connect().await;
        let user_id = "TEST-BACKLOG-MATCH-EMPTY-USER";
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind("backlog-match-empty@example.com")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");

        let service_date: chrono::NaiveDate = "2026-09-05".parse().unwrap();
        let scheduled: DateTime<Utc> = "2026-09-05T09:00:00Z".parse().unwrap();
        let (tracked_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions (user_id, service_date, pin_origin_crs, pin_scheduled_departure) \
             VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(user_id)
        .bind(service_date)
        .bind("ZZZ-NOWHERE")
        .bind(scheduled)
        .fetch_one(&pool)
        .await
        .expect("seed tracked_trains row");

        let matched = attempt_backlog_match(
            &pool,
            tracked_train_id,
            "ZZZ-NOWHERE",
            scheduled,
            service_date,
        )
        .await
        .expect("attempt_backlog_match");
        assert!(!matched);

        let (resolution_status,): (String,) =
            sqlx::query_as("SELECT resolution_status FROM train_subscriptions WHERE id = $1")
                .bind(tracked_train_id)
                .fetch_one(&pool)
                .await
                .expect("read back tracked_trains");
        assert_eq!(resolution_status, "pending");

        sqlx::query("DELETE FROM train_subscriptions WHERE id = $1")
            .bind(tracked_train_id)
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                a_backlog_match_with_an_activation_also_dual_writes_the_shared_trains_row -- --ignored --test-threads=1`"]
    async fn a_backlog_match_with_an_activation_also_dual_writes_the_shared_trains_row() {
        let pool = connect().await;
        let user_id = "TEST-BACKLOG-DUAL-WRITE-USER";
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind("backlog-dual-write@example.com")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");

        let service_date: chrono::NaiveDate = "2026-09-06".parse().unwrap();
        let scheduled: DateTime<Utc> = "2026-09-06T18:15:00Z".parse().unwrap();

        sqlx::query(
            "INSERT INTO trust_event_backlog \
                (crs, train_uid, train_id, service_date, msg_type, event_type, \
                 planned_timestamp, actual_timestamp, variation_status, dedup_key) \
             VALUES (NULL, $1, $2, $3, '0001', NULL, NULL, NULL, NULL, $4), \
                    ($5, NULL, $2, $3, '0003', 'DEPARTURE', $6, $6, 'ON TIME', $7)",
        )
        .bind("TEST-DW-BACKLOG-UID")
        .bind("TEST-DW-BACKLOG-TRAIN-ID")
        .bind(service_date)
        .bind("test-dw-backlog-dedup-activation")
        .bind("EUS")
        .bind(scheduled)
        .bind("test-dw-backlog-dedup-movement")
        .execute(&pool)
        .await
        .expect("seed backlog rows");

        let (tracked_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions (user_id, service_date, pin_origin_crs, pin_scheduled_departure) \
             VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(user_id)
        .bind(service_date)
        .bind("EUS")
        .bind(scheduled)
        .fetch_one(&pool)
        .await
        .expect("seed tracked_trains row");

        let matched =
            attempt_backlog_match(&pool, tracked_train_id, "EUS", scheduled, service_date)
                .await
                .expect("attempt_backlog_match");
        assert!(matched);

        let (trains_id,): (Option<i64>,) =
            sqlx::query_as("SELECT trains_id FROM train_subscriptions WHERE id = $1")
                .bind(tracked_train_id)
                .fetch_one(&pool)
                .await
                .expect("read back trains_id");
        let trains_id =
            trains_id.expect("a backlog match with a found Activation must set trains_id");

        let (train_uid,): (String,) = sqlx::query_as("SELECT train_uid FROM trains WHERE id = $1")
            .bind(trains_id)
            .fetch_one(&pool)
            .await
            .expect("read back the shared trains row");
        assert_eq!(train_uid, "TEST-DW-BACKLOG-UID");

        sqlx::query("DELETE FROM train_subscriptions WHERE id = $1")
            .bind(tracked_train_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trains WHERE train_uid = 'TEST-DW-BACKLOG-UID'")
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trust_event_backlog WHERE train_id = 'TEST-DW-BACKLOG-TRAIN-ID'")
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api find_train_id_by_uid_resolves_via_the_activation_row -- --ignored --test-threads=1`"]
    async fn find_train_id_by_uid_resolves_via_the_activation_row() {
        let pool = connect().await;
        let service_date: chrono::NaiveDate = "2026-09-06".parse().unwrap();
        sqlx::query(
            "INSERT INTO trust_event_backlog \
                (crs, train_uid, train_id, service_date, msg_type, dedup_key) \
             VALUES (NULL, $1, $2, $3, '0001', $4)",
        )
        .bind("TEST-FIND-BY-UID")
        .bind("TEST-FIND-BY-UID-TRAIN-ID")
        .bind(service_date)
        .bind("test-find-by-uid-dedup-activation")
        .execute(&pool)
        .await
        .expect("seed an Activation row");

        let train_id = find_train_id_by_uid(&pool, "TEST-FIND-BY-UID", service_date)
            .await
            .expect("find_train_id_by_uid");
        assert_eq!(train_id, Some("TEST-FIND-BY-UID-TRAIN-ID".to_string()));

        let miss = find_train_id_by_uid(&pool, "TEST-FIND-BY-UID-NO-SUCH-ROW", service_date)
            .await
            .expect("find_train_id_by_uid miss");
        assert_eq!(miss, None);

        sqlx::query("DELETE FROM trust_event_backlog WHERE train_id = 'TEST-FIND-BY-UID-TRAIN-ID'")
            .execute(&pool)
            .await
            .ok();
    }

    /// `run_backlog_match_sweep`'s own headline scenario, proven
    /// end-to-end: the exact regression this sweep exists to fix. A pin is
    /// created (and, per `routes::train::post_track`'s real sequencing,
    /// `attempt_backlog_match` is tried once) BEFORE the matching backlog
    /// row ever lands -- exactly what happens when a pin is created before
    /// its train has departed, or when the live departure falls outside
    /// `resolve_origin_departure`'s `MATCH_TOLERANCE` window and TRUST's
    /// own backlog only fills in afterwards. That first attempt must fail
    /// honestly (`Ok(false)`), leaving the pin `'pending'` with no retry
    /// mechanism prior to this fix. Once the backlog row exists, a later
    /// call to `run_backlog_match_sweep` -- exactly what `main.rs`'s
    /// periodic loop performs -- must find and resolve it, proving the
    /// sweep (not just `attempt_backlog_match` in isolation) closes this
    /// gap.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                run_backlog_match_sweep_resolves_a_pin_the_backlog_had_nothing_for_at_creation_time \
                -- --ignored --test-threads=1`"]
    async fn run_backlog_match_sweep_resolves_a_pin_the_backlog_had_nothing_for_at_creation_time() {
        let pool = connect().await;
        let user_id = "TEST-BACKLOG-SWEEP-E2E-USER";
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind("backlog-sweep-e2e@example.com")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");

        let service_date: chrono::NaiveDate = chrono::Utc::now().date_naive();
        let scheduled: DateTime<Utc> = service_date
            .and_hms_opt(18, 15, 0)
            .expect("valid time")
            .and_utc();

        let (tracked_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions (user_id, service_date, pin_origin_crs, pin_scheduled_departure) \
             VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(user_id)
        .bind(service_date)
        .bind("EUS")
        .bind(scheduled)
        .fetch_one(&pool)
        .await
        .expect("seed tracked_trains row");

        // Step 1: the pin-creation-time attempt, before any backlog data
        // exists -- must honestly fail and leave the pin 'pending', same as
        // `no_matching_backlog_rows_leaves_the_pin_untouched` above.
        let first_attempt =
            attempt_backlog_match(&pool, tracked_train_id, "EUS", scheduled, service_date)
                .await
                .expect("first attempt_backlog_match, before any backlog data exists");
        assert!(
            !first_attempt,
            "no backlog row exists yet; the pin-creation-time attempt must honestly fail"
        );

        // Step 2: the delayed/early departure's TRUST movement now lands in
        // the backlog, minutes to hours later -- exactly what the real
        // trust-backlog-consumer does continuously.
        sqlx::query(
            "INSERT INTO trust_event_backlog \
                (crs, train_uid, train_id, service_date, msg_type, event_type, \
                 planned_timestamp, actual_timestamp, variation_status, dedup_key) \
             VALUES (NULL, $1, $2, $3, '0001', NULL, NULL, NULL, NULL, $4), \
                    ($5, NULL, $2, $3, '0003', 'DEPARTURE', $6, $6, 'LATE', $7)",
        )
        .bind("TEST-BACKLOG-SWEEP-E2E-UID")
        .bind("TEST-BACKLOG-SWEEP-E2E-TRAIN-ID")
        .bind(service_date)
        .bind("test-backlog-sweep-e2e-dedup-activation")
        .bind("EUS")
        .bind(scheduled)
        .bind("test-backlog-sweep-e2e-dedup-movement")
        .execute(&pool)
        .await
        .expect("seed backlog rows arriving after pin creation");

        // Step 3: the periodic sweep -- not a second direct call to
        // attempt_backlog_match -- is what must find and resolve it now.
        let matched = run_backlog_match_sweep(&pool)
            .await
            .expect("run_backlog_match_sweep");
        assert!(
            matched >= 1,
            "at least this fixture's pin must be matched by the sweep"
        );

        let resolution_status: String =
            sqlx::query_scalar("SELECT resolution_status FROM train_subscriptions WHERE id = $1")
                .bind(tracked_train_id)
                .fetch_one(&pool)
                .await
                .expect("read back resolution_status");
        assert_eq!(
            resolution_status, "resolved",
            "the sweep must resolve a pin the backlog had nothing for at pin-creation time, \
             once the matching backlog row later exists"
        );

        sqlx::query("DELETE FROM train_subscriptions WHERE id = $1")
            .bind(tracked_train_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query(
            "DELETE FROM trust_event_backlog WHERE train_id = 'TEST-BACKLOG-SWEEP-E2E-TRAIN-ID'",
        )
        .execute(&pool)
        .await
        .ok();
        sqlx::query("DELETE FROM trains WHERE train_uid = 'TEST-BACKLOG-SWEEP-E2E-UID'")
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .ok();
    }
}
