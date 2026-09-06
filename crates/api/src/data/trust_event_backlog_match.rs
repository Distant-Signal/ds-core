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
    Ok(true)
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
            "INSERT INTO tracked_trains (user_id, service_date, pin_origin_crs, pin_scheduled_departure) \
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

        let (resolution_status, train_uid): (String, Option<String>) =
            sqlx::query_as("SELECT resolution_status, train_uid FROM tracked_trains WHERE id = $1")
                .bind(tracked_train_id)
                .fetch_one(&pool)
                .await
                .expect("read back tracked_trains");
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
        sqlx::query("DELETE FROM tracked_trains WHERE id = $1")
            .bind(tracked_train_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trust_event_backlog WHERE train_id = 'TEST-BACKLOG-TRAIN-ID'")
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
            "INSERT INTO tracked_trains (user_id, service_date, pin_origin_crs, pin_scheduled_departure) \
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
            sqlx::query_as("SELECT resolution_status FROM tracked_trains WHERE id = $1")
                .bind(tracked_train_id)
                .fetch_one(&pool)
                .await
                .expect("read back tracked_trains");
        assert_eq!(resolution_status, "pending");

        sqlx::query("DELETE FROM tracked_trains WHERE id = $1")
            .bind(tracked_train_id)
            .execute(&pool)
            .await
            .ok();
    }
}
