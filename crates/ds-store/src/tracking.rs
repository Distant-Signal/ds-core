//! The ingest half of `train_tracking.rs`: train movement and event
//! upserts, `reopen_subscriptions_after_reinstatement`,
//! `apply_schedule_match`, the pending-pin listers and
//! `list_active_tracked_trains`; and
//! `notifier_forward_queue::insert_forward_signals` ([`forward_queue`]).
//! The user half (pins, subscriptions, tickets, the `TrackedTrainState`
//! read model) stays in the api's `data::train_tracking`, which
//! re-exports these.

pub mod forward_queue;
pub mod outbox;

pub use forward_queue::{insert_forward_signals, insert_forward_signals_on};

use chrono::{DateTime, Utc};
use common::{TrackedTrainRef, TrainMovementEventMessage};
use sqlx::{Connection, PgConnection, PgPool};

/// How far ahead a pin's or a track-by-uid's `service_date` may be (API-6):
/// four weeks, raised from 7 on 2026-10-08 to match the timetable's publish
/// window. `schedule-reference` publishes its per-date products at least
/// this many days ahead -- its `SCHEDULE_FORWARD_PUBLISH_DAYS` takes this
/// constant as its lower bound -- so a tracked train inside this bound always
/// has its stops (`schedule_calling_points_full`) and its true origin
/// (`schedule_destination_departures`) published. The departure instant
/// gets one more day for a service that runs past midnight.
///
/// A pin's schedule MATCH is a different product:
/// `sweeps::schedule_matching` reads `schedule_line_population`, which is
/// published for today and tomorrow only. A far pin is accepted, stays
/// `pending`, and matches on the first sweep after its date's (or the day
/// before's) population lands -- `run_schedule_match_sweep` skips it until
/// then without querying per pin.
pub const PIN_MAX_DAYS_AHEAD: i64 = 28;

/// Row shape for `list_active_tracked_trains`'s query -- identical fields
/// to `common::TrackedTrainRef`, but with `sqlx::FromRow` derived, since
/// that derive can't live on `TrackedTrainRef` itself (`crates/common` has
/// no `sqlx` dependency at all). Crate-private: `reads` decodes the
/// `ingest_active_tracked_trains` view (the same SELECT) into it. See `crates/api/src/data/queries.rs`'s `TflLineSummaryRow`/
/// `row_to_report` for the precedent this mirrors.
#[derive(Debug, Clone, sqlx::FromRow)]
pub(crate) struct TrackedTrainRow {
    id: i64,
    service_date: chrono::NaiveDate,
    /// `Option`, not `String` -- as of Task 20's `create_subscription_for_train`
    /// (the NR-primary path), a row can legitimately have `NULL` here (the
    /// design spec's own accepted §1 gap: a bare `train_uid` with no
    /// schedule match yet). See `common::TrackedTrainRef::pin_origin_crs`'s
    /// own doc comment -- this row shape's whole reason to exist is
    /// carrying that value through to it unchanged.
    pin_origin_crs: Option<String>,
    /// See `pin_origin_crs`'s own doc comment on this same struct.
    pin_scheduled_departure: Option<DateTime<Utc>>,
    resolution_status: String,
    train_uid: Option<String>,
    train_id: Option<String>,
    trains_id: Option<i64>,
    /// See `common::TrackedTrainRef::destination_crs`'s own doc comment.
    destination_crs: Option<String>,
}

impl From<TrackedTrainRow> for TrackedTrainRef {
    fn from(row: TrackedTrainRow) -> Self {
        TrackedTrainRef {
            id: row.id,
            service_date: row.service_date,
            pin_origin_crs: row.pin_origin_crs,
            pin_scheduled_departure: row.pin_scheduled_departure,
            resolution_status: row.resolution_status,
            train_uid: row.train_uid,
            train_id: row.train_id,
            trains_id: row.trains_id,
            destination_crs: row.destination_crs,
        }
    }
}

/// What `trust-consumer` needs for its periodic reference reload (Task
/// 14): pending pins to attempt resolving, and already-resolved ones to
/// recognize incoming TRUST messages against, after a restart or on its
/// periodic reload. "Active" excludes `completed`/`cancelled` rows in
/// `train_current_state` and `unresolved` rows in `tracked_trains` --
/// there is nothing further for trust-consumer to do with either.
///
/// `train_uid`/`train_id` now come from a `LEFT JOIN trains`, not
/// `tracked_trains`' own (as of this task, no-longer-written) columns --
/// this is the change that finally lets an NR-primary subscription
/// (Task 20's `create_subscription_for_train`, which sets `trains_id`
/// immediately but deliberately never touches this table's own legacy
/// `train_uid` column) populate `trust-consumer`'s `by_train_uid`
/// direct-match fast path (Task 16) at all, including for a SECOND
/// subscriber sharing the same physical train's `trains_id` -- exactly
/// the scenario the unique `tracked_trains_resolved_identity` index made
/// impossible to express via `tracked_trains.train_uid` itself. `LEFT
/// JOIN`, not `JOIN`: a row whose `trains_id` is still `NULL` (no
/// schedule/backlog/live match has ever run) must still come back, just
/// with `train_uid`/`train_id` both `None` -- `TrackedTrainRow`/
/// `TrackedTrainRef` already type both fields `Option` for exactly this
/// reason.
///
/// **`service_date` floor, added for the 2026-09-25 review finding (Medium
/// 8).** "Active" used to rest entirely on two exclusions that between them
/// excluded almost nothing over time:
/// * `resolution_status != 'unresolved'` -- at the time of the 2026-09-25
///   review's Medium 8 finding, `'unresolved'` was a legal value of the
///   CHECK constraint (`20260828120000_train_tracking.sql`,
///   `20260905150000_schedule_matched_resolution.sql`) that NO code path
///   anywhere ever wrote, so this clause had never excluded a single row in
///   production. Kept anyway at the time ("a future writer of it would mean
///   exactly this"), and as of that same review's Low finding #2,
///   [`mark_subscriptions_unresolved_on_cancellation`] is now that writer: a
///   subscription cancelled before ever resolving flips here, precisely the
///   "nothing further to do with it" case this exclusion always anticipated.
/// * the `train_current_state` status check -- which stops applying the
///   moment `aggregator::queries::prune_trains` deletes the `trains` row at
///   30 days: `trains_id` is `ON DELETE SET NULL`, so the `LEFT JOIN`s go
///   `NULL` and `cs.status IS NULL` makes the row "active" again, forever.
///
/// The result was that every subscription ever created came back on every
/// reference reload -- an unbounded set that `trust-consumer` rebuilds its
/// whole in-memory index from, periodically, for the life of the deployment.
/// A `service_date` floor bounds it by the only thing that actually decides
/// whether TRUST can still say anything about a train: its service date.
/// `CURRENT_DATE - INTERVAL '2 days'` matches this file's other two sweep
/// bounds (`list_pending_pins_for_backlog_match`,
/// `list_pending_pins_for_schedule_match`) rather than inventing a third
/// figure, and is generously past the point where TRUST's live Train
/// Movements stream -- the only feed this set exists to recognize messages
/// from -- still carries anything for a service (`MAX_PIN_AGE` is 6 hours;
/// an overnight service spans one calendar boundary, not two).
///
/// **`service_date` ceiling, added for the 2026-09-26 review finding H3.**
/// The floor above bounds the PAST but this `WHERE` clause had no bound on
/// the FUTURE at all: a recurring daily `train_uid` (a Mon-Fri commute, or
/// any "track tomorrow's departure" journey template) gets its NEXT running
/// pinned well before that running's own day arrives, and with no ceiling
/// that subscription was already "active" -- already sitting in
/// `trust-consumer`'s `by_train_uid` direct-match index -- for however many
/// days out it was created. Combined with `activation_is_for_service_date`'s
/// now-closed D+1 date-arithmetic gap, TODAY's Activation of that same uid
/// could claim TOMORROW's subscription outright. `CURRENT_DATE + INTERVAL
/// '1 day'` is the right ceiling rather than `CURRENT_DATE` itself: it is
/// exactly the window `activation_is_for_service_date`'s legitimate D+1
/// (post-midnight-service) branch needs a subscription to already be
/// present for, and finding H3's OWN fix (this subscription's
/// `pin_scheduled_departure` must itself fall in the Activation's rail day)
/// is what now keeps an ordinary tomorrow-daytime subscription from being
/// misattributed while still `service_date <= CURRENT_DATE + 1`. A
/// subscription for several days out has no legitimate reason to be in this
/// set yet; it becomes "active" the day this floor/ceiling window reaches
/// it, same as any other.
///
/// **Buses and ferries are excluded (2026-10-06).** A subscription whose
/// shared row is a bus or ferry on its date (`schedule_services.mode <>
/// 'train'`) is timetable-only: TRUST never activates or reports one, so
/// carrying it in `trust-consumer`'s index only cost a lookup per reload
/// for something that can never match. A row with no `schedule_services`
/// entry stays in, as a train.
pub async fn list_active_tracked_trains(pool: &PgPool) -> anyhow::Result<Vec<TrackedTrainRef>> {
    let rows = sqlx::query_as::<_, TrackedTrainRow>(
        "SELECT tt.id, tt.service_date, tt.pin_origin_crs, tt.pin_scheduled_departure, \
                tt.resolution_status, tr.train_uid, tr.train_id, tt.trains_id, \
                tr.destination_crs \
         FROM train_subscriptions tt \
         LEFT JOIN trains tr ON tr.id = tt.trains_id \
         LEFT JOIN train_current_state cs ON cs.trains_id = tt.trains_id \
         WHERE tt.resolution_status != 'unresolved' \
           AND tt.service_date >= CURRENT_DATE - INTERVAL '2 days' \
           AND tt.service_date <= CURRENT_DATE + INTERVAL '1 day' \
           AND (cs.status IS NULL OR cs.status NOT IN ('completed', 'cancelled')) \
           AND NOT EXISTS ( \
               SELECT 1 FROM schedule_services ss \
               WHERE ss.service_date = tr.service_date AND ss.uid = tr.train_uid \
                 AND ss.mode <> 'train')",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(TrackedTrainRef::from).collect())
}

/// Writes one TRUST-derived event into the SHARED, per-physical-train
/// tables. Callable for ANY `trains_id`, regardless of whether any
/// `tracked_trains` row (subscription) references it at all -- this is
/// the primary write path once trust-backlog-consumer becomes the primary
/// movement-event writer (Task 14), and it's also what `upsert_train_event`
/// below now delegates to for the legacy per-subscription path.
/// `event.tracked_train_id` is ignored here on purpose -- this function's
/// entire point is to not require one.
///
/// **Event-time monotonicity guard on the `train_current_state` write.**
/// This function is the one place `trust-consumer`'s live write path and
/// `trust-backlog-consumer`'s `ingest_shared_movement` write path converge
/// (see `crates/api/src/data/trust_event_backlog.rs`) -- both are
/// independent, continuously-running processes that can write the same
/// `trains_id` in either order, with no coordination between them. Without
/// a guard, whichever process's `UPDATE` commits last always wins,
/// regardless of which one actually carries the more recent real-world
/// event -- a lagging writer's stale update can silently overwrite a
/// fresher one. Full analysis, including why this is a real (not
/// theoretical) production risk and why the guard lives here rather than
/// in either caller:
/// `docs/superpowers/specs/2026-09-07-shared-train-status-write-race-design.md`
/// (Option C).
///
/// The mechanism is **event-time monotonicity, not wall-clock/commit
/// order**: `event_time` below is `COALESCE(event.actual_timestamp,
/// event.planned_timestamp)` -- the incoming event's own real-world
/// timestamp, not `NOW()` (which `updated_at` already records, and which
/// provides no protection at all since it always advances forward
/// regardless of which writer produced it). The `ON CONFLICT DO UPDATE`'s
/// `WHERE EXCLUDED.event_time IS NULL OR EXCLUDED.event_time >=
/// train_current_state.event_time OR train_current_state.event_time IS
/// NULL` clause makes the whole `UPDATE` a no-op whenever the incoming
/// event is OLDER than what's already stored, independent of commit order
/// between the two writers -- so this is NOT dead code or a redundant
/// restatement of the `ON CONFLICT` target; it is the actual fix.
///
/// **A no-timestamp incoming event always applies, but never regresses a
/// known `event_time`.** Not every message this function is called for
/// actually carries a timestamp -- e.g. a Cancellation with a missing or
/// malformed `canx_timestamp` (both `crates/trust-consumer/src/process.rs`
/// and `crates/trust-backlog-consumer/src/process.rs` build a
/// Cancellation's `actual_timestamp` from
/// `common::trust_timestamp::parse_trust_epoch_millis_pair(None,
/// canx_timestamp.as_deref(), ...).actual`, `None` on either a missing or
/// an unparseable value, with `planned_timestamp`
/// always `None` for that message shape) -- so `event_time` here can
/// legitimately be `NULL` even once the stored row's `event_time` is
/// already known. The first `EXCLUDED.event_time IS NULL` branch exists
/// specifically for that case: an event we have no real-world time for is
/// still the best information available and must still apply (matching
/// this function's pre-guard behaviour for exactly that case), rather than
/// being silently and PERMANENTLY dropped -- without this branch, `NULL >=
/// x` is SQL's UNKNOWN, `train_current_state.event_time IS NULL` is FALSE
/// once a real `event_time` is stored, and `UNKNOWN OR FALSE` never
/// satisfies `WHERE`, so every future write to that `trains_id` (including
/// ones that DO carry a real, newer timestamp) would silently no-op
/// forever -- worse than having no guard at all. Symmetrically, `SET
/// event_time = COALESCE(EXCLUDED.event_time, train_current_state.event_time)`
/// (rather than a bare `EXCLUDED.event_time`) means a no-timestamp write
/// updates every other column normally but leaves the stored `event_time`
/// exactly as it was -- if it instead clobbered `event_time` back to
/// `NULL`, that would re-open the `train_current_state.event_time IS NULL`
/// branch for every subsequent call, permanently defeating the guard for
/// this `trains_id` after just one no-timestamp event.
///
/// **No-op guard (DB2-3).** trust-consumer fans one movement out as one
/// message per subscriber of the same train, so the same state arrived N
/// times and was rewritten N times (plus `updated_at` and a dead tuple
/// each). The `IS DISTINCT FROM` clause skips the write when nothing but
/// `updated_at` would change; `updated_at` therefore records the last
/// real change.
pub async fn upsert_train_movement(
    pool: &PgPool,
    trains_id: i64,
    event: &TrainMovementEventMessage,
) -> anyhow::Result<bool> {
    let mut conn = pool.acquire().await?;
    upsert_train_movement_on(&mut conn, trains_id, event).await
}

/// [`upsert_train_movement`] on a caller-supplied connection, which may
/// already be inside a transaction (`post_train_events` runs each event
/// behind its own savepoint). The two writes run in one (nested)
/// transaction of their own, so a failure in the `train_current_state`
/// write no longer leaves the `train_movement_events` row committed on its
/// own -- a redelivery of that event would otherwise hit `ON CONFLICT DO
/// NOTHING` for the movement and never repair the stale current state
/// (DB2-2).
///
/// Returns whether the `train_movement_events` row was newly inserted:
/// `false` means this `(trains_id, dedup_key)` was already stored, so the
/// event is a redelivery or a replay. Callers gate their one-shot side
/// effects (reopening or closing subscriptions) on it; see
/// [`upsert_train_event_on`].
pub async fn upsert_train_movement_on(
    conn: &mut PgConnection,
    trains_id: i64,
    event: &TrainMovementEventMessage,
) -> anyhow::Result<bool> {
    let event_time = event.actual_timestamp.or(event.planned_timestamp);
    let mut tx = conn.begin().await?;

    let inserted = sqlx::query(
        "INSERT INTO train_movement_events \
            (trains_id, dedup_key, msg_type, event_type, loc_stanox, loc_crs, \
             planned_timestamp, actual_timestamp, variation_status, raw_body, gbtt_timestamp) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
         ON CONFLICT (trains_id, dedup_key) WHERE trains_id IS NOT NULL DO NOTHING",
    )
    .bind(trains_id)
    .bind(&event.dedup_key)
    .bind(&event.msg_type)
    .bind(&event.event_type)
    .bind(&event.loc_stanox)
    .bind(&event.loc_crs)
    .bind(event.planned_timestamp)
    .bind(event.actual_timestamp)
    .bind(&event.variation_status)
    .bind(&event.raw_body)
    .bind(event.gbtt_timestamp)
    .execute(&mut *tx)
    .await?
    .rows_affected()
        > 0;

    sqlx::query(
        "INSERT INTO train_current_state \
            (trains_id, status, last_reported_location, last_event_type, \
             delay_minutes, next_calling_point, eta_next, eta_source, event_time, updated_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW()) \
         ON CONFLICT (trains_id) WHERE trains_id IS NOT NULL DO UPDATE SET \
            status                  = EXCLUDED.status, \
            last_reported_location  = EXCLUDED.last_reported_location, \
            last_event_type         = EXCLUDED.last_event_type, \
            delay_minutes            = EXCLUDED.delay_minutes, \
            next_calling_point       = EXCLUDED.next_calling_point, \
            eta_next                 = EXCLUDED.eta_next, \
            eta_source               = EXCLUDED.eta_source, \
            event_time               = COALESCE(EXCLUDED.event_time, train_current_state.event_time), \
            updated_at               = NOW() \
         WHERE (EXCLUDED.event_time IS NULL \
            OR EXCLUDED.event_time >= train_current_state.event_time \
            OR train_current_state.event_time IS NULL) \
           AND (train_current_state.status, train_current_state.last_reported_location, \
                train_current_state.last_event_type, train_current_state.delay_minutes, \
                train_current_state.next_calling_point, train_current_state.eta_next, \
                train_current_state.eta_source, train_current_state.event_time) \
               IS DISTINCT FROM \
               (EXCLUDED.status, EXCLUDED.last_reported_location, \
                EXCLUDED.last_event_type, EXCLUDED.delay_minutes, \
                EXCLUDED.next_calling_point, EXCLUDED.eta_next, \
                EXCLUDED.eta_source, COALESCE(EXCLUDED.event_time, train_current_state.event_time))",
    )
    .bind(trains_id)
    .bind(&event.status)
    .bind(&event.last_reported_location)
    .bind(&event.last_event_type)
    .bind(event.delay_minutes)
    .bind(&event.next_calling_point)
    .bind(event.eta_next)
    .bind(&event.eta_source)
    .bind(event_time)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(inserted)
}

/// Legacy per-subscription resolution flip only -- as of this task, it no
/// longer writes `train_movement_events`/`train_current_state` itself
/// (that's `upsert_train_movement`'s job now). Advances
/// `tracked_trains.resolution_status`, mirrors the resolution onto the
/// shared `trains` row (same dual-write Task 5 introduced), and returns
/// the resolved `trains_id` so the caller can feed the same event into
/// `upsert_train_movement`. Returns [`LegacyResolution::NoIdentity`] in the
/// accepted-gap case: no `trains_id` was already known AND this call carries
/// no `resolved_train_uid` either (this process never saw the Activation) --
/// the pin still flips to `'resolved'` for this user's own tracking
/// purposes, but no shared `trains` row can be created or updated without
/// a known identity. Returns [`LegacyResolution::UidMismatch`] when the
/// identity this resolution claims disagrees with the one the subscription's
/// already-linked shared row carries -- see that variant and the guard
/// below.
///
/// As of Task 22 (Step D's final cutover), this `UPDATE` writes ONLY
/// `resolution_status` -- `tracked_trains.train_uid`/`train_id`/
/// `resolved_at` no longer exist as columns at all (dropped by this same
/// task's migration), so the old `train_uid = COALESCE($2, train_uid),
/// train_id = $3, ..., resolved_at = NOW()` write this UPDATE used to do
/// is gone entirely, not merely stopped. This is also the direct fix for
/// the risk Task 21's review flagged: that old per-subscription
/// `train_uid` write could collide with `tracked_trains_resolved_identity`
/// (a `UNIQUE (train_uid, service_date) WHERE train_uid IS NOT NULL`
/// index) the moment two subscribers shared one physical train (Task 20's
/// own headline scenario) and a process restart re-delivered an Activation
/// for the second one -- both writes would race to set the same
/// `(train_uid, service_date)` pair on two different `tracked_trains` rows.
/// That index is dropped by this same migration, and this UPDATE no longer
/// attempts the write that could have hit it -- the shared identity link
/// lives exclusively on `trains_id` from here on.
///
/// Because the returned row no longer carries a `train_uid` column to fall
/// back on, `trains_id` derivation below now reads directly off THIS
/// call's own `resolved_train_uid` parameter instead of a value the
/// `UPDATE` read back post-write. A previously-schedule-matched pin no
/// longer needs that fallback anyway: schedule matching (Task 3) already
/// links `trains_id` directly on `tracked_trains` the moment it succeeds,
/// so `existing_trains_id` (the `trains_id` column itself) is already
/// `Some` by the time any live-TRUST resolution reaches this function for
/// such a pin.
///
/// **Identity date (Repeater Signal M7 leftover, 2026-09-27).** A newly
/// created `trains` row is keyed on `identity_date` (the resolving
/// Activation's `tp_origin_timestamp`, carried by trust-consumer) when it is
/// present and plausible, and on the subscription's own `service_date`
/// otherwise. The two differ for a pin at an intermediate stop after
/// midnight on a train that left its origin before midnight: the pin is
/// dated D+1, and `trains(uid, D+1)` is the NEXT day's run of the same
/// service, so the movements would land on another train's shared row. See
/// [`identity_date_for`] for the plausibility rule.
#[expect(
    clippy::similar_names,
    reason = "the similar names are distinct domain terms"
)]
async fn flip_legacy_resolution(
    conn: &mut PgConnection,
    tracked_train_id: i64,
    resolved_train_uid: Option<&str>,
    resolved_train_id: &str,
    identity_date: Option<chrono::NaiveDate>,
) -> anyhow::Result<LegacyResolution> {
    // **Single-transaction fix (2026-09-26 review, Medium finding 6).**
    // Before this fix, the `resolution_status = 'resolved'` write below and
    // the shared-row write in the match arms that follow it (`mark_train_
    // resolved`/`find_or_create_train`) were two independent statements
    // against the bare pool -- so a failure in the SECOND write (most
    // plausibly a `trains_train_id_service_date` unique-index collision:
    // two subscriptions' cross-matched resolutions both claiming the same
    // `(train_id, service_date)`) left the FIRST write's
    // `resolution_status = 'resolved'` permanently committed with nothing
    // to back it up. That reads as "resolved" everywhere this subscription
    // is displayed, while `trains_id` may still be NULL (or pointing at a
    // row that never actually got this `train_id`) -- and because neither
    // sweep re-selects an already-`'resolved'` row, nothing would ever
    // retry it. Doing both writes inside one transaction, committed only
    // once every arm below has succeeded, means a failure in the shared-row
    // write rolls the status flip back too, leaving the subscription
    // exactly where it started (still `'pending'`/`'schedule_matched'`, still
    // picked up by the next sweep/event) instead of stranded.
    //
    // `conn.begin()` is a real `BEGIN` on a bare connection and a
    // `SAVEPOINT` when the caller already holds a transaction on it
    // (`post_train_events`' per-event savepoints), so this stays atomic
    // either way.
    let mut tx = conn.begin().await?;

    // The scalar subquery reads the ALREADY-LINKED shared row's own
    // `train_uid` in the same round trip as the status flip -- the value the
    // `UidMismatch` guard below compares against. Cheap (`trains.id` is the
    // primary key) and, unlike a follow-up `SELECT`, guaranteed to describe
    // the same `trains_id` this statement just returned.
    let row: Option<(Option<i64>, chrono::NaiveDate, Option<String>)> = sqlx::query_as(
        "UPDATE train_subscriptions tt SET resolution_status = 'resolved', unresolved_from = NULL \
         WHERE tt.id = $1 \
         RETURNING tt.trains_id, tt.service_date, \
                   (SELECT tr.train_uid FROM trains tr WHERE tr.id = tt.trains_id)",
    )
    .bind(tracked_train_id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((existing_trains_id, service_date, existing_train_uid)) = row else {
        tx.commit().await?;
        return Ok(LegacyResolution::NoIdentity);
    };

    let outcome = match (existing_trains_id, resolved_train_uid) {
        // **The uid-disagreement guard (2026-09-25 review finding, High 2).**
        // This subscription is already linked to a shared `trains` row for
        // one identity, and this resolution claims a DIFFERENT one. Before
        // this guard, the `(Some(id), _)` arm below took the existing
        // `trains_id` unconditionally and the caller then wrote this event's
        // movement onto it -- so one mis-resolved subscription could
        // attribute a foreign train's movements to an already-correctly-
        // matched shared row, which is visible to EVERY subscriber of that
        // row (and feeds journey view, delay-repay evidence and
        // notifications), not just the one whose pin was mis-resolved.
        //
        // Same posture as this codebase's other uid-mismatch guard
        // (`schedule_matching::attempt_schedule_match_for_shared_train`):
        // warn with both uids and decline, rather than write something we
        // know to be wrong. The status flip above is deliberately left in
        // place (it still commits below) -- it is this user's own
        // per-subscription bookkeeping and carries no cross-subscriber
        // identity claim, unlike the shared-row writes this arm refuses.
        (Some(existing_id), Some(resolved))
            if existing_train_uid
                .as_deref()
                .is_some_and(|existing| existing != resolved) =>
        {
            tracing::warn!(
                tracked_train_id,
                trains_id = existing_id,
                existing_train_uid = existing_train_uid.as_deref(),
                resolved_train_uid = resolved,
                resolved_train_id,
                "live TRUST resolution disagrees with the train_uid this subscription's shared \
                 row already carries; refusing to attribute this train's movements to it"
            );
            LegacyResolution::UidMismatch
        }
        (Some(id), _) => {
            crate::trains::mark_train_resolved(&mut *tx, id, resolved_train_id).await?;
            LegacyResolution::Applied(id)
        }
        (None, Some(train_uid)) => {
            let train_date = identity_date_for(tracked_train_id, service_date, identity_date);
            let id = crate::trains::find_or_create_train(&mut *tx, train_uid, train_date).await?;
            sqlx::query("UPDATE train_subscriptions SET trains_id = $2 WHERE id = $1")
                .bind(tracked_train_id)
                .bind(id)
                .execute(&mut *tx)
                .await?;
            crate::trains::mark_train_resolved(&mut *tx, id, resolved_train_id).await?;
            LegacyResolution::Applied(id)
        }
        (None, None) => LegacyResolution::NoIdentity,
    };

    // Only reached once every write above has succeeded -- any `?` earlier
    // in this function (most importantly a unique-index violation from
    // `mark_train_resolved`) returns before this point and drops `tx`
    // un-committed, rolling back the `resolution_status = 'resolved'` write
    // alongside whatever partial shared-row write also failed.
    tx.commit().await?;
    Ok(outcome)
}

/// The date [`flip_legacy_resolution`] keys a new `trains` row on.
///
/// `identity_date` wins only when it is the subscription's own
/// `service_date` or the day before it: a pin is dated by its own departure,
/// which is never before the train's origin date and at most one day after
/// it (an overnight train). Anything else is implausible -- a mis-parsed or
/// mis-attributed Activation -- and falls back to the subscription's date,
/// the pre-existing behaviour, with a warning. Absent (an older
/// trust-consumer, or no parked Activation) falls back silently.
fn identity_date_for(
    tracked_train_id: i64,
    service_date: chrono::NaiveDate,
    identity_date: Option<chrono::NaiveDate>,
) -> chrono::NaiveDate {
    match identity_date {
        None => service_date,
        Some(date) if date == service_date || date == service_date - chrono::Duration::days(1) => {
            date
        }
        Some(date) => {
            tracing::warn!(
                tracked_train_id,
                %service_date,
                identity_date = %date,
                "resolution's identity_date is neither the subscription's date nor the day \
                 before it; keying the trains row on the subscription's date instead"
            );
            service_date
        }
    }
}

/// [`flip_legacy_resolution`]'s outcome. A bare `Option<i64>` could not
/// express the third case: "we know which shared row this subscription
/// points at, and we are deliberately NOT writing to it." Returning `None`
/// for that would be actively wrong, because [`upsert_train_event`] treats
/// `None` as "no identity known yet" and falls back to reading `trains_id`
/// straight off the subscription -- which is the very row the guard just
/// refused, so the refused write would happen anyway one line later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LegacyResolution {
    /// The resolution was applied to this `trains_id`; the caller should
    /// write this event's movement against it.
    Applied(i64),
    /// No shared identity exists or could be created (this process never
    /// saw the Activation, and the subscription has no `trains_id` yet) --
    /// the documented accepted gap. The caller may still fall back to the
    /// subscription's own `trains_id` if one appeared by another route.
    NoIdentity,
    /// The resolution's `train_uid` disagrees with the one the already-linked
    /// shared row carries. Nothing was written, and the caller must NOT
    /// write this event anywhere -- see the guard in
    /// [`flip_legacy_resolution`].
    UidMismatch,
}

/// **Low finding #2 of the 2026-09-25 review's own fix.** A real
/// Cancellation for a train a `'pending'`/`'schedule_matched'` subscription
/// is tracking never carries `resolved_train_uid`/`resolved_train_id` --
/// `trust-consumer::process.rs`'s `TrustMessage::Cancellation` handler has
/// no new identity to report, only the fact that this journey is over -- so
/// [`flip_legacy_resolution`] never runs for it and `resolution_status`
/// stayed `'pending'` forever, even though there is no train left to ever
/// resolve to. Worse than mere display staleness: `list_pending_pins_for_schedule_match`/
/// `list_pending_pins_for_backlog_match` both re-select every still-`'pending'`,
/// still-`trains_id IS NULL` row on every sweep tick, so a subscription
/// whose train was cancelled before ever departing its origin (no Movement,
/// so no `resolve_origin_departure` match either) was retried forever for a
/// train that will never produce another event.
///
/// Flips it to `'unresolved'` instead -- a value the `CHECK` constraint has
/// allowed since `20260905150000_schedule_matched_resolution.sql`, and which
/// `list_active_tracked_trains`'s own doc comment already named as
/// deliberately unwritten, "kept anyway... a future writer of it would mean
/// exactly this": exactly this case, a subscription with nothing further to
/// do. Only from `'pending'`/`'schedule_matched'` -- an already-`'resolved'`
/// subscription (the train departed, then was cancelled mid-journey) is left
/// alone: it is more informative than `'unresolved'` (the train WAS found),
/// isn't in either sweep's `WHERE resolution_status = 'pending'` anyway, and
/// downgrading it would be exactly finding #1's "regress an already-advanced
/// status" mistake played out one layer up.
///
/// Closes the subscription `tracked_train_id` names and every subscription
/// linked to `trains_id`, mirroring [`reopen_subscriptions_after_reinstatement`].
/// The train-wide half matters since the 2026-10-09 replay fix: callers run
/// this only for a newly inserted movement row (see
/// [`upsert_train_event_on`]), and trust-consumer's per-subscriber fan-out
/// and trust-backlog-consumer's copy all share one `dedup_key`, so whichever
/// write lands first has to close every linked subscription. Returns how
/// many rows were closed.
pub(crate) async fn mark_subscriptions_unresolved_on_cancellation(
    conn: &mut PgConnection,
    tracked_train_id: Option<i64>,
    trains_id: Option<i64>,
) -> anyhow::Result<u64> {
    if tracked_train_id.is_none() && trains_id.is_none() {
        return Ok(0);
    }
    // `unresolved_from` keeps the status this cancellation replaced (the
    // right-hand side of a `SET` reads the row's old values), so
    // [`reopen_subscriptions_after_reinstatement`] can restore it.
    let result = sqlx::query(
        "UPDATE train_subscriptions \
         SET resolution_status = 'unresolved', unresolved_from = resolution_status \
         WHERE resolution_status IN ('pending', 'schedule_matched') \
           AND (id = $1 OR trains_id = $2)",
    )
    .bind(tracked_train_id)
    .bind(trains_id)
    .execute(conn)
    .await?;
    Ok(result.rows_affected())
}

/// The undo of [`mark_subscriptions_unresolved_on_cancellation`] (H4
/// residual, 2026-10-01 verification pass). A Reinstatement (`0005`) means
/// the train runs after all, so every subscription a Cancellation of it
/// moved to `'unresolved'` goes back to the status it had then
/// (`unresolved_from`), and `list_active_tracked_trains` and the pending
/// sweeps pick it up again. Before this, nothing reversed the move:
/// recovery depended on `trust-consumer`'s in-memory state surviving until
/// the train's next Movement, and a restart in between stranded it.
///
/// Reopens the subscription `tracked_train_id` names (the live
/// `trust-consumer` path, which may not know a `trains_id`) and every
/// subscription linked to `trains_id` (the backlog ingest path, and other
/// subscribers of the same train). Only rows with `unresolved_from` set:
/// `'unresolved'` written any other way, or before this column existed,
/// stays as it is. Returns how many rows were reopened.
pub async fn reopen_subscriptions_after_reinstatement(
    conn: &mut PgConnection,
    tracked_train_id: Option<i64>,
    trains_id: Option<i64>,
) -> anyhow::Result<u64> {
    if tracked_train_id.is_none() && trains_id.is_none() {
        return Ok(0);
    }
    let result = sqlx::query(
        "UPDATE train_subscriptions \
         SET resolution_status = unresolved_from, unresolved_from = NULL \
         WHERE resolution_status = 'unresolved' AND unresolved_from IS NOT NULL \
           AND (id = $1 OR trains_id = $2)",
    )
    .bind(tracked_train_id)
    .bind(trains_id)
    .execute(conn)
    .await?;
    Ok(result.rows_affected())
}

/// Idempotent, same overall contract as before this task: resolves the pin
/// (if `resolved_train_id` is `Some`) and writes the shared movement/
/// current-state tables. As of this task, that write ALWAYS goes through
/// [`upsert_train_movement`], keyed on `trains_id` -- never directly on
/// `tracked_train_id` -- so an event for a subscription whose identity is
/// still entirely unknown (no schedule/backlog match ever ran, and this
/// call itself carries no `resolved_train_uid`) has nothing to key a
/// shared-table write on and is dropped with a warning, matching the
/// accepted gap `flip_legacy_resolution` documents.
pub async fn upsert_train_event(
    pool: &PgPool,
    event: &TrainMovementEventMessage,
) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    upsert_train_event_on(&mut conn, event).await
}

/// What [`upsert_train_events_batch`] did with one batch.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct TrainEventsBatchOutcome {
    /// Events whose writes committed (including the documented no-op
    /// cases: no identity known yet, or a refused uid mismatch).
    pub upserted: u64,
    /// Events refused for a data error, each rolled back in full.
    pub rejected: Vec<common::RejectedTrustBacklogRow>,
}

/// `POST /private/train-events`' write (DB2-2): the whole batch in one
/// transaction, each event behind its own savepoint -- the same pattern as
/// `trust_event_backlog::upsert_trust_event_backlog_batch`'s fallback path.
///
/// * A **data error** (SQLSTATE class 22/23, see
///   `trust_event_backlog::classify_data_error`) rolls back exactly that
///   event -- its resolution flip, its movement row and its current-state
///   write together, so nothing is left half-applied -- and is reported in
///   [`TrainEventsBatchOutcome::rejected`]. No retry could ever fix it.
/// * **Any other error** (a dropped connection, a pool timeout, a
///   serialization failure or deadlock, a lock or statement timeout, an
///   unexpected SQLSTATE) returns `Err` straight away, rolling back the
///   whole batch, so the route answers 500 and `trust-consumer` leaves the
///   batch un-ACKed and retries it. Before this, every per-event error was
///   logged and skipped behind a 200, so a transient failure lost the event
///   for good -- including the one message that carries a pin's
///   resolution. Every write here is idempotent (`dedup_key`,
///   `ON CONFLICT`), so the retry is safe.
pub async fn upsert_train_events_batch(
    pool: &PgPool,
    events: &[TrainMovementEventMessage],
) -> anyhow::Result<TrainEventsBatchOutcome> {
    if events.is_empty() {
        return Ok(TrainEventsBatchOutcome::default());
    }
    let mut tx = pool.begin().await?;
    let outcome = upsert_train_events_batch_in(&mut tx, events).await?;
    tx.commit().await?;
    Ok(outcome)
}

/// [`upsert_train_events_batch`] inside the caller's open transaction
/// `tx`, which the caller commits: trust-consumer's DB sink (plan 3b.3)
/// writes the batch's forward signals in the same transaction. Each event
/// runs behind its own savepoint exactly as there; an `Err` leaves `tx` to
/// be rolled back.
pub async fn upsert_train_events_batch_in(
    tx: &mut PgConnection,
    events: &[TrainMovementEventMessage],
) -> anyhow::Result<TrainEventsBatchOutcome> {
    let mut outcome = TrainEventsBatchOutcome::default();
    for (index, event) in events.iter().enumerate() {
        // A nested `begin` on a connection already in a transaction is a
        // `SAVEPOINT`; its `commit`/`rollback` are `RELEASE`/`ROLLBACK TO`.
        let mut savepoint = Connection::begin(&mut *tx).await?;
        match upsert_train_event_on(&mut savepoint, event).await {
            Ok(()) => {
                savepoint.commit().await?;
                outcome.upserted += 1;
            }
            Err(err) => {
                let Some(data_error) = crate::backlog::classify_anyhow_data_error(&err) else {
                    return Err(err);
                };
                savepoint.rollback().await?;
                outcome
                    .rejected
                    .push(data_error.into_rejected_row(index, &event.dedup_key));
            }
        }
    }
    Ok(outcome)
}

/// [`upsert_train_event`] on a caller-supplied connection, which may already
/// be inside a transaction: `post_train_events` runs every event of a batch
/// behind its own savepoint so a data error rolls back exactly that event
/// (and nothing it half-wrote), while any other error fails the request.
pub async fn upsert_train_event_on(
    conn: &mut PgConnection,
    event: &TrainMovementEventMessage,
) -> anyhow::Result<()> {
    let resolved = match &event.resolved_train_id {
        Some(train_id) => {
            flip_legacy_resolution(
                &mut *conn,
                event.tracked_train_id,
                event.resolved_train_uid.as_deref(),
                train_id,
                event.identity_date,
            )
            .await?
        }
        None => LegacyResolution::NoIdentity,
    };

    // A refused resolution (High 2's uid-disagreement guard) must not fall
    // through to the `SELECT trains_id` below: that would read back exactly
    // the shared row the guard just declined to attribute this event to, and
    // write the movement onto it anyway. Dropping the event is the same
    // posture the "no identity at all" branch further down already takes --
    // losing one movement is recoverable, mis-attributing another train's
    // movements to a shared row every subscriber reads is not.
    if resolved == LegacyResolution::UidMismatch {
        return Ok(());
    }

    let trains_id = match resolved {
        LegacyResolution::Applied(id) => Some(id),
        _ => sqlx::query_scalar::<_, Option<i64>>(
            "SELECT trains_id FROM train_subscriptions WHERE id = $1",
        )
        .bind(event.tracked_train_id)
        .fetch_optional(&mut *conn)
        .await?
        .flatten(),
    };

    // Whether this event is new to the shared store. A redelivered or
    // replayed event (its `(trains_id, dedup_key)` row already stored) must
    // not re-run the one-shot subscription changes below: replaying a
    // cancel -> reinstate -> cancel range would otherwise reopen, at the
    // replayed Reinstatement, a subscription the later Cancellation had
    // closed (2026-10-09 redelivery review). With no `trains_id` there is
    // no row to deduplicate on, so the changes run as before; a full,
    // in-order replay still ends in the right state.
    let fresh = if let Some(trains_id) = trains_id {
        upsert_train_movement_on(&mut *conn, trains_id, event).await?
    } else {
        tracing::warn!(
            tracked_train_id = event.tracked_train_id,
            "no trains_id known yet for this subscription; movement event dropped \
             from the shared store until its identity is resolved"
        );
        true
    };
    if !fresh {
        return Ok(());
    }

    // Low finding #2: a Cancellation carries `event.status == "cancelled"`
    // (set by `trust_schema::journey::apply_cancellation`) but, per the
    // above, never a `resolved_train_id` -- so this is the one place left to
    // stop such a subscription being retried forever. Independent of the
    // `trains_id`/movement write above: even when this subscription's
    // identity was never established at all (so the movement itself is
    // dropped), the subscription-level bookkeeping must still advance --
    // there is nothing further any sweep can do for it either way.
    if event.status == "cancelled" {
        mark_subscriptions_unresolved_on_cancellation(
            &mut *conn,
            Some(event.tracked_train_id),
            trains_id,
        )
        .await?;
    }

    // The other half of the block above: a Reinstatement reopens what a
    // Cancellation of the same train closed.
    if event.msg_type == "0005" {
        reopen_subscriptions_after_reinstatement(
            &mut *conn,
            Some(event.tracked_train_id),
            trains_id,
        )
        .await?;
    }

    Ok(())
}

/// Flips `resolution_status` to `'schedule_matched'` AND links the
/// subscription to the shared `trains` row the match resolved, in ONE
/// statement. Every schedule column this function once wrote lives
/// exclusively on that shared row now
/// (`schedule_matching::attempt_schedule_match`'s own
/// `find_or_create_train_with_schedule_match` call, Task 3).
///
/// Guarded on `trains_id IS NULL` rather than the old `train_uid IS NULL` --
/// since Task 8's read cutover, `tracked_trains.train_uid` is no longer the
/// signal anything trusts for "has this pin been schedule-matched yet."
/// Still safe to call from BOTH the synchronous pin-creation path and the
/// periodic sweep without a race clobbering a row that has since moved on
/// (a live TRUST Movement resolved it first -- setting `trains_id` via
/// `flip_legacy_resolution` -- or an earlier sweep tick already matched
/// it) -- `rows_affected() == 0` in either of those cases is not an error,
/// just a no-op, which is why this returns `bool` rather than erroring on
/// zero rows affected.
///
/// **`trains_id` is written HERE, in the same `UPDATE` as the status flip
/// (2026-09-25 review finding, Medium 5).** It used to be a separate
/// `UPDATE train_subscriptions SET trains_id = $2` statement issued by
/// `schedule_matching::attempt_schedule_match` AFTER this one, on the same
/// pool and outside any transaction. A crash between the two left the row
/// `'schedule_matched'` with `trains_id NULL` permanently: the retry sweep
/// only ever selects `resolution_status = 'pending'` rows
/// (`list_pending_pins_for_schedule_match`), and every read path resolves
/// identity through `trains_id`, so the subscription was both unrepairable
/// and permanently schedule-less. Postgres applies a single `UPDATE`
/// atomically, so that intermediate state no longer exists -- this is why
/// the parameter is here rather than in a caller-side follow-up write. See
/// `attempt_schedule_match`'s own doc comment for the full ordering account.
pub async fn apply_schedule_match(
    pool: &PgPool,
    tracked_train_id: i64,
    trains_id: i64,
) -> anyhow::Result<bool> {
    let result = sqlx::query(
        "UPDATE train_subscriptions \
         SET resolution_status = 'schedule_matched', trains_id = $2 \
         WHERE id = $1 AND trains_id IS NULL AND resolution_status = 'pending'",
    )
    .bind(tracked_train_id)
    .bind(trains_id)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Row shape for `list_pending_pins_for_schedule_match`'s query -- every
/// still-`pending`, never-schedule-matched row, the periodic sweep's own
/// input set (Decision 3's "also run this same attempt periodically").
///
/// `pin_origin_crs`/`pin_scheduled_departure` are `Option`, not bare
/// `String`/`DateTime<Utc>`, even though the query below already filters
/// NULLs out of its `WHERE` clause: `train_subscriptions.trains_id` is
/// `ON DELETE SET NULL` (`20260906100000_trains.sql:38`), so a still-
/// `pending` NR-primary subscription (`create_subscription_for_train`,
/// whose `pin_*` columns are `NULL` by design until a schedule match ever
/// happens -- the design's own accepted §1 gap) can have its `trains_id`
/// nulled out from under it once `aggregator::queries::prune_trains`
/// deletes the `trains` row it pointed at, landing it right back in this
/// query's result set with NULL `pin_*` columns. Decoding those as
/// non-`Option` used to make sqlx error on EVERY row of EVERY sweep tick,
/// forever, the moment a single row reached that state -- not just fail to
/// process that one row. See
/// `list_pending_pins_for_schedule_match_excludes_a_pruned_nr_primary_row_with_null_pins`
/// in this module's own `db_tests`.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PendingSchedulePin {
    pub id: i64,
    pub service_date: chrono::NaiveDate,
    pub pin_origin_crs: Option<String>,
    pub pin_scheduled_departure: Option<DateTime<Utc>>,
    /// The destination the user's own departure-board pick named. Carried
    /// through the sweep purely so a pin resolved by a LATER sweep tick gets
    /// the same same-minute tie-break the synchronous attempt at pin-creation
    /// time already had -- see `schedule_matching::find_schedule_match`'s
    /// round 4(a). `None` for an origin-only pin.
    pub pin_destination_crs: Option<String>,
    /// See `common::TrackPinRequest.skipped_stations`'s own doc comment --
    /// this row's own captured snapshot, carried through the sweep to
    /// `schedule_matching::attempt_schedule_match` so a pin the SYNCHRONOUS
    /// attempt at creation time didn't resolve doesn't lose this signal by
    /// the time the periodic sweep resolves it instead. `NOT NULL DEFAULT
    /// '{}'` on the column, so this is a bare `Vec`, never an `Option`.
    pub pin_skipped_stations: Vec<String>,
    /// Same idea as `pin_skipped_stations` immediately above, for the
    /// origin platform snapshot instead -- see
    /// `common::TrackPinRequest.platform`/`planned_platform`'s own doc
    /// comments. Nullable (unlike the array above): a single scalar has no
    /// safe non-`NULL` "no signal" sentinel to invent.
    pub pin_platform: Option<String>,
    pub pin_planned_platform: Option<String>,
}

/// Every row the periodic schedule-match sweep should retry: still
/// `pending` AND still lacking a `trains_id` -- a `schedule_matched` row
/// (or one resolved via live TRUST) already has one and is excluded, same
/// as a `resolved`/`unresolved` row. As of this task, `trains_id IS NULL`
/// is the ONLY identity guard here (the old, redundant `train_uid IS NULL`
/// was dropped along with `apply_schedule_match`'s own write of that
/// column -- `resolution_status = 'pending'` alone already excluded every
/// `schedule_matched` row, since `apply_schedule_match` always flips both
/// together in the same `UPDATE`).
///
/// `trains_id IS NULL` is a no-op for every legacy row (which never has a
/// `trains_id` without also going through `apply_schedule_match`/live
/// resolution first, both of which also flip `resolution_status` away from
/// `'pending'`), but load-bearing as of Task 20's NR-primary path
/// (`create_subscription_for_train`): that function sets `trains_id`
/// immediately but leaves `resolution_status` at its `'pending'` default,
/// so such a row would otherwise be swept into a schedule-match attempt
/// that exists only to *discover* a `trains_id`, which this row already
/// has -- pointlessly.
///
/// **Re-examined for review finding I1, and deliberately left as it is.**
/// The question was whether this should instead pick up NR-primary rows
/// whose linked `trains` row still lacks schedule data. It should not, for
/// a concrete reason rather than a stylistic one: this sweep's only tool is
/// `attempt_schedule_match`, which is keyed on `(origin CRS, departure
/// time)` -- exactly the pair such a row does not have and cannot derive.
/// Widening the `WHERE` would select rows the sweep can do nothing with.
///
/// **Re-examined again for the finding that `trains_id IS NULL` alone is
/// NOT sufficient to guarantee non-NULL `pin_*` columns.** `trains_id` is
/// `ON DELETE SET NULL` (`20260906100000_trains.sql:38`): once
/// `aggregator::queries::prune_trains` deletes a `trains` row, any
/// still-`pending` subscription pointing at it (an NR-primary row that
/// never got a schedule match, e.g. a train that never ran) has its
/// `trains_id` nulled out and reappears in this very query's result --
/// now indistinguishable, by `trains_id` alone, from a fresh NR-primary
/// row, but with `pin_origin_crs`/`pin_scheduled_departure` NULL. The
/// `WHERE` clause below now also excludes those explicitly (rather than
/// relying solely on `PendingSchedulePin`'s fields being `Option` to avoid
/// a decode error), since this sweep has nothing to do with such a row
/// either way -- same reasoning as the paragraph above, just reached via a
/// different route into this table's state space. See
/// `list_pending_pins_for_schedule_match_excludes_a_pruned_nr_primary_row_with_null_pins`.
///
/// What those rows get instead:
/// * LIVE data -- `trust-consumer` sees them through
///   `list_active_tracked_trains`' `LEFT JOIN trains`, matches their
///   `train_uid` straight off an Activation, and their movements flow
///   normally. Proven end to end by
///   `an_nr_primary_subscription_receives_live_movement_events` in this
///   module's own `db_tests`, not assumed.
/// * HISTORICAL data -- `routes::train::enrich_shared_train` replays the
///   retained `trust_event_backlog` at track time, and uses the replayed
///   origin departure's own `(CRS, time)` to run a real schedule match
///   (`schedule_matching::attempt_schedule_match_for_shared_train`).
///
/// The one residual gap, named rather than hidden: a train tracked
/// NR-primary that has NOT yet run (nothing in the backlog) acquires
/// schedule data only if something else supplies a `(CRS, time)` for it
/// later -- another subscriber's legacy pin, or a re-track. Live TRUST
/// still resolves its `train_id` and movements; it is only origin/
/// destination/calling points that stay `NULL`. Closing that needs a
/// schedule lookup keyed on `train_uid` alone, which no index in this
/// codebase supports today; out of scope for this fix.
///
/// **`service_date` floor, added for the 2026-09-25 review finding (Medium
/// 7), mirroring `list_pending_pins_for_backlog_match`'s own identical
/// bound.** This query had none: a pin that can never match -- its
/// `schedule_line_population` was pruned, its origin CRS is on no line, its
/// schedule simply does not exist -- was re-selected and re-attempted on
/// EVERY sweep tick, forever, for as long as the row stayed `'pending'`.
/// Each attempt is a `list_stanox_crs_for_crs` read plus one
/// `schedule_line_population` read AND a full JSONB parse per candidate line
/// (a whole day of schedules for that line), so the futile work is
/// substantial, unbounded in time, and grows monotonically with every such
/// pin a user ever creates. `CURRENT_DATE - INTERVAL '2 days'` is the same
/// figure and the same reasoning as the backlog sibling (see its doc
/// comment): one full day of margin past the shortest realistic retention
/// window, so a boundary row is never dropped moments before it would have
/// matched. Nothing is lost past the floor that was reachable anyway -- a
/// schedule match needs that date's `schedule_line_population`, and
/// `schedule-reference` only publishes a rolling window of upcoming dates.
///
/// **Upper bound, API-6.** Both sweeps also skip a pin dated more than
/// [`SWEEP_MAX_DAYS_AHEAD`] days ahead: no pin that far out can be created,
/// and a pin dated 2090 (possible before `validate_pin` bounded the future)
/// was swept every 300 s for ever. With the 2-day floor this gives every pin
/// a sweep life of at most about a month, whatever its date. Inside the
/// bound, `schedule_matching::run_schedule_match_sweep` skips a pin whose
/// date has no `schedule_line_population` yet before any per-pin query,
/// so a far pin costs this list query's row and nothing else.
pub async fn list_pending_pins_for_schedule_match(
    pool: &PgPool,
) -> anyhow::Result<Vec<PendingSchedulePin>> {
    let rows = sqlx::query_as::<_, PendingSchedulePin>(
        "SELECT id, service_date, pin_origin_crs, pin_scheduled_departure, pin_destination_crs, \
                pin_skipped_stations, pin_platform, pin_planned_platform \
         FROM train_subscriptions WHERE trains_id IS NULL AND resolution_status = 'pending' \
         AND pin_origin_crs IS NOT NULL AND pin_scheduled_departure IS NOT NULL \
         AND service_date >= CURRENT_DATE - INTERVAL '2 days' \
         AND service_date <= CURRENT_DATE + $1::int",
    )
    .bind(SWEEP_MAX_DAYS_AHEAD)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// The sweeps' future bound: [`PIN_MAX_DAYS_AHEAD`] plus one day, since
/// `CURRENT_DATE` here is the database's (UTC) date, not London's.
#[expect(clippy::cast_possible_truncation, reason = "a small constant")]
pub const SWEEP_MAX_DAYS_AHEAD: i32 = PIN_MAX_DAYS_AHEAD as i32 + 1;

/// Row shape for `list_pending_pins_for_backlog_match`'s query -- a
/// separate type from `PendingSchedulePin` even though its fields are
/// identical, matching this codebase's own convention of one type per
/// sweep concern (`reconciliation.rs`'s `EnrichmentCandidate` next to this
/// same `PendingSchedulePin`) rather than one shared type two unrelated
/// sweeps both happen to decode into.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PendingBacklogPin {
    pub id: i64,
    pub service_date: chrono::NaiveDate,
    pub pin_origin_crs: Option<String>,
    pub pin_scheduled_departure: Option<DateTime<Utc>>,
}

/// Every row the periodic backlog-match sweep should retry:
/// `trust_event_backlog_match::attempt_backlog_match`'s own candidate set.
///
/// Fixes a real, confirmed gap: `attempt_backlog_match` is otherwise only
/// ever invoked once, synchronously, at pin-creation time
/// (`routes::train::post_track`) -- normally before the tracked train has
/// even departed, when `trust_event_backlog` has nothing for it yet. A
/// pin whose real departure lands outside `common::MATCH_TOLERANCE` of
/// `trust-consumer::matching::resolve_origin_departure`'s own one-shot
/// check (a common occurrence under disruption -- more than 20 minutes
/// early or late) then has no retry at all, even though the exact backlog
/// row a later `attempt_backlog_match` call would match fills in over the
/// following hours as TRUST movements actually arrive. This sweep is that
/// retry.
///
/// Same three exclusions as `list_pending_pins_for_schedule_match`, for
/// the same reasons:
/// * `resolution_status = 'pending'` -- a `schedule_matched`/`resolved`/
///   `unresolved` row has already advanced past what a backlog match
///   could do for it.
/// * `trains_id IS NULL` -- a row that already has a shared `trains` row
///   (schedule-matched, or an NR-primary subscription) has nothing this
///   sweep's only tool, `attempt_backlog_match`, can usefully add; that
///   function is keyed on `(origin CRS, departure time)`, not `trains_id`.
/// * `pin_origin_crs`/`pin_scheduled_departure IS NOT NULL` -- an
///   NR-primary row (or one whose `trains_id` was later nulled out by
///   `aggregator::queries::prune_trains`'s `ON DELETE SET NULL`) has
///   neither, and `attempt_backlog_match` has no use for it either
///   (`attempt_backlog_match_by_uid` is the identity-first counterpart for
///   that shape, and is not swept periodically -- see this table's own
///   `PendingSchedulePin` doc comment for why widening this sweep to that
///   shape would select rows it can do nothing with).
///
/// **One addition `list_pending_pins_for_schedule_match` doesn't need**: a
/// `service_date` floor, `CURRENT_DATE - INTERVAL '2 days'`. Without it, a
/// pin that can never resolve this way (its `service_date` predates
/// `trust_event_backlog`'s own retention window -- 1 day by default,
/// `crates/aggregator/src/config.rs`'s `trust_event_backlog_retention_days`,
/// pending an RDM licence confirmation before it can ever be raised) would
/// still be re-selected, and re-fail `attempt_backlog_match`'s query, on
/// every single sweep tick for as long as the row stays `'pending'` --
/// guaranteed-futile repeated work, unbounded in time. 2 days, not 1,
/// mirrors `list_trains_needing_schedule_enrichment`'s own identical-shaped
/// bound in `reconciliation.rs` and the "one extra day of margin" reasoning
/// `schedule_destination_departures_retention_days`'s own doc comment uses
/// (`crates/aggregator/src/config.rs`): one full day of safety margin
/// beyond the shortest realistic retention window, so a boundary row isn't
/// dropped from this sweep moments before it would otherwise have matched.
pub async fn list_pending_pins_for_backlog_match(
    pool: &PgPool,
) -> anyhow::Result<Vec<PendingBacklogPin>> {
    let rows = sqlx::query_as::<_, PendingBacklogPin>(
        "SELECT id, service_date, pin_origin_crs, pin_scheduled_departure \
         FROM train_subscriptions WHERE trains_id IS NULL AND resolution_status = 'pending' \
         AND pin_origin_crs IS NOT NULL AND pin_scheduled_departure IS NOT NULL \
         AND service_date >= CURRENT_DATE - INTERVAL '2 days' \
         AND service_date <= CURRENT_DATE + $1::int",
    )
    .bind(SWEEP_MAX_DAYS_AHEAD)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

#[cfg(test)]
#[expect(
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::unnecessary_wraps,
    reason = "test code: paired test values share names; scenario tests read top to bottom; fakes mirror the signatures they stand in for"
)]
mod db_tests {
    use super::*;
    use crate::test_support::{
        cleanup_user, connect, db_today, fixture_event, seed_backlog_candidate_pin,
        seed_tracked_train, seed_user,
    };
    use chrono::NaiveDate;

    // --- H4 residual (2026-10-01): a Reinstatement reopens what a Cancellation closed ---

    async fn resolution_status_of(pool: &PgPool, tracked_train_id: i64) -> String {
        sqlx::query_scalar("SELECT resolution_status FROM train_subscriptions WHERE id = $1")
            .bind(tracked_train_id)
            .fetch_one(pool)
            .await
            .expect("read resolution_status")
    }

    fn at(raw: &str) -> Option<DateTime<Utc>> {
        Some(raw.parse().unwrap())
    }

    async fn is_listed_active(pool: &PgPool, tracked_train_id: i64) -> bool {
        list_active_tracked_trains(pool)
            .await
            .expect("list_active_tracked_trains")
            .iter()
            .any(|tracked| tracked.id == tracked_train_id)
    }

    /// Cancel -> trust-consumer restart (nothing in memory, so it never
    /// forwards the Reinstatement) -> the Reinstatement arrives only through
    /// trust-backlog-consumer's ingest -> the next Movement resolves the
    /// subscription. `backlog_knows_uid` is whether trust-backlog-consumer
    /// still had the train's Activation parked; without it the shared row
    /// is found by the `train_id` already written onto it.
    async fn reinstatement_after_restart_reopens_and_a_movement_resolves(backlog_knows_uid: bool) {
        let pool = connect().await;
        let user_id = if backlog_knows_uid {
            "TEST-H4-REOPEN-UID"
        } else {
            "TEST-H4-REOPEN-NOUID"
        };
        let train_uid = if backlog_knows_uid {
            "TH4RU1"
        } else {
            "TH4RN1"
        };
        let train_id = if backlog_knows_uid {
            "TH4RU1ID01"
        } else {
            "TH4RN1ID01"
        };
        sqlx::query("DELETE FROM trains WHERE train_uid = $1")
            .bind(train_uid)
            .execute(&pool)
            .await
            .expect("pre-clean trains");
        cleanup_user(&pool, user_id).await;
        seed_user(&pool, user_id).await;
        let today = db_today(&pool).await;

        let trains_id = crate::trains::find_or_create_train(&pool, train_uid, today)
            .await
            .expect("seed the shared trains row");
        crate::trains::mark_train_resolved(&pool, trains_id, train_id)
            .await
            .expect("seed its train_id, as the backlog Activation ingest does");
        let (tracked_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions \
                (user_id, service_date, pin_origin_crs, pin_scheduled_departure, trains_id, \
                 resolution_status) \
             VALUES ($1, $2, 'EUS', $3, $4, 'schedule_matched') RETURNING id",
        )
        .bind(user_id)
        .bind(today)
        .bind(today.and_hms_opt(18, 15, 0).unwrap().and_utc())
        .bind(trains_id)
        .fetch_one(&pool)
        .await
        .expect("seed a schedule-matched subscription");

        // The Cancellation, through trust-consumer's live path.
        let mut cancel = fixture_event(tracked_train_id, "h4-reopen-cancel");
        cancel.msg_type = "0002".to_string();
        cancel.event_type = None;
        cancel.planned_timestamp = None;
        cancel.actual_timestamp = at("2026-09-05T18:05:00Z");
        cancel.status = "cancelled".to_string();
        upsert_train_event(&pool, &cancel).await.expect("cancel");
        assert_eq!(
            resolution_status_of(&pool, tracked_train_id).await,
            "unresolved"
        );
        assert!(!is_listed_active(&pool, tracked_train_id).await);

        // trust-consumer restarts here. It never sees this subscription
        // again (not listed), so only trust-backlog-consumer forwards the
        // Reinstatement.
        let reinstatement = common::TrustBacklogEventMessage {
            crs: None,
            train_uid: backlog_knows_uid.then(|| train_uid.to_string()),
            train_id: train_id.to_string(),
            service_date: today,
            msg_type: "0005".to_string(),
            event_type: None,
            planned_timestamp: None,
            actual_timestamp: at("2026-09-05T18:10:00Z"),
            variation_status: None,
            delay_minutes: None,
            dedup_key: format!("h4-reopen-reinstate-{user_id}"),
            gbtt_timestamp: None,
        };
        let results = crate::backlog::ingest_shared_movements_batch(
            &pool,
            std::slice::from_ref(&reinstatement),
        )
        .await;
        assert!(results.iter().all(Result::is_ok), "{results:?}");
        assert_eq!(
            resolution_status_of(&pool, tracked_train_id).await,
            "schedule_matched",
            "the reinstatement restores the status the cancellation replaced"
        );
        let status: String =
            sqlx::query_scalar("SELECT status FROM train_current_state WHERE trains_id = $1")
                .bind(trains_id)
                .fetch_one(&pool)
                .await
                .expect("read train_current_state");
        assert_eq!(status, "en_route");
        assert!(
            is_listed_active(&pool, tracked_train_id).await,
            "listed again, so a restarted trust-consumer can claim it"
        );

        // The next Movement, as the restarted trust-consumer's pin claim
        // sends it.
        let mut movement = fixture_event(tracked_train_id, "h4-reopen-movement");
        movement.resolved_train_uid = Some(train_uid.to_string());
        movement.resolved_train_id = Some(train_id.to_string());
        movement.actual_timestamp = at("2026-09-05T18:20:00Z");
        upsert_train_event(&pool, &movement)
            .await
            .expect("movement");
        assert_eq!(
            resolution_status_of(&pool, tracked_train_id).await,
            "resolved"
        );

        cleanup_user(&pool, user_id).await;
        sqlx::query("DELETE FROM trains WHERE train_uid = $1")
            .bind(train_uid)
            .execute(&pool)
            .await
            .expect("clean trains");
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                a_backlog_reinstatement -- --ignored --test-threads=1`"]
    async fn a_backlog_reinstatement_reopens_a_cancelled_subscription_after_a_restart() {
        reinstatement_after_restart_reopens_and_a_movement_resolves(true).await;
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                a_backlog_reinstatement -- --ignored --test-threads=1`"]
    async fn a_backlog_reinstatement_without_a_parked_activation_still_reopens() {
        reinstatement_after_restart_reopens_and_a_movement_resolves(false).await;
    }

    /// The live path: trust-consumer still holds the train in memory and
    /// forwards the Reinstatement itself, for a subscription with no
    /// `trains_id` yet. It goes back to `'pending'`. A row made
    /// `'unresolved'` some other way (no `unresolved_from`) is left alone.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                a_live_reinstatement -- --ignored --test-threads=1`"]
    async fn a_live_reinstatement_reopens_only_what_a_cancellation_closed() {
        let pool = connect().await;
        let user_id = "TEST-H4-REOPEN-LIVE";
        cleanup_user(&pool, user_id).await;
        seed_user(&pool, user_id).await;
        let cancelled = seed_tracked_train(&pool, user_id).await;
        let otherwise_unresolved = seed_tracked_train(&pool, user_id).await;
        sqlx::query(
            "UPDATE train_subscriptions SET resolution_status = 'unresolved' WHERE id = $1",
        )
        .bind(otherwise_unresolved)
        .execute(&pool)
        .await
        .expect("seed an unresolved row with no cancellation behind it");

        let mut cancel = fixture_event(cancelled, "h4-live-cancel");
        cancel.msg_type = "0002".to_string();
        cancel.status = "cancelled".to_string();
        upsert_train_event(&pool, &cancel).await.expect("cancel");
        assert_eq!(resolution_status_of(&pool, cancelled).await, "unresolved");

        for tracked_train_id in [cancelled, otherwise_unresolved] {
            let mut reinstate = fixture_event(
                tracked_train_id,
                &format!("h4-live-reinstate-{tracked_train_id}"),
            );
            reinstate.msg_type = "0005".to_string();
            reinstate.status = "en_route".to_string();
            upsert_train_event(&pool, &reinstate)
                .await
                .expect("reinstate");
        }
        assert_eq!(resolution_status_of(&pool, cancelled).await, "pending");
        assert_eq!(
            resolution_status_of(&pool, otherwise_unresolved).await,
            "unresolved"
        );

        cleanup_user(&pool, user_id).await;
    }

    // --- 2026-10-09 redelivery review: a replay must not reopen what a later cancellation closed ---

    /// A shared `trains` row with its `train_id`, and `count` subscriptions
    /// linked to it in `'schedule_matched'`. Returns `(trains_id, ids)`.
    async fn seed_linked_subscriptions(
        pool: &PgPool,
        user_id: &str,
        train_uid: &str,
        train_id: &str,
        count: usize,
    ) -> (i64, Vec<i64>) {
        sqlx::query("DELETE FROM trains WHERE train_uid = $1")
            .bind(train_uid)
            .execute(pool)
            .await
            .expect("pre-clean trains");
        cleanup_user(pool, user_id).await;
        seed_user(pool, user_id).await;
        let today = db_today(pool).await;
        let trains_id = crate::trains::find_or_create_train(pool, train_uid, today)
            .await
            .expect("seed the shared trains row");
        crate::trains::mark_train_resolved(pool, trains_id, train_id)
            .await
            .expect("seed its train_id");
        let mut ids = Vec::new();
        for _ in 0..count {
            let id: i64 = sqlx::query_scalar(
                "INSERT INTO train_subscriptions \
                    (user_id, service_date, pin_origin_crs, pin_scheduled_departure, trains_id, \
                     resolution_status) \
                 VALUES ($1, $2, 'EUS', $3, $4, 'schedule_matched') RETURNING id",
            )
            .bind(user_id)
            .bind(today)
            .bind(today.and_hms_opt(18, 15, 0).unwrap().and_utc())
            .bind(trains_id)
            .fetch_one(pool)
            .await
            .expect("seed a schedule-matched subscription");
            ids.push(id);
        }
        (trains_id, ids)
    }

    async fn clean_linked_subscriptions(pool: &PgPool, user_id: &str, train_uid: &str) {
        cleanup_user(pool, user_id).await;
        sqlx::query("DELETE FROM trains WHERE train_uid = $1")
            .bind(train_uid)
            .execute(pool)
            .await
            .expect("clean trains");
    }

    fn backlog_event(
        train: (&str, &str),
        service_date: NaiveDate,
        msg_type: &str,
        dedup_key: &str,
        actual: &str,
    ) -> common::TrustBacklogEventMessage {
        common::TrustBacklogEventMessage {
            crs: None,
            train_uid: Some(train.0.to_string()),
            train_id: train.1.to_string(),
            service_date,
            msg_type: msg_type.to_string(),
            event_type: None,
            planned_timestamp: None,
            actual_timestamp: at(actual),
            variation_status: None,
            delay_minutes: None,
            dedup_key: dedup_key.to_string(),
            gbtt_timestamp: None,
        }
    }

    async fn ingest_backlog(pool: &PgPool, events: &[common::TrustBacklogEventMessage]) {
        let results = crate::backlog::ingest_shared_movements_batch(pool, events).await;
        assert!(results.iter().all(Result::is_ok), "{results:?}");
    }

    /// trust-backlog-consumer's path: cancel -> reinstate -> cancel again,
    /// then the range is replayed (a consumer-group rewind). The replayed
    /// Reinstatement used to reopen the subscription the second
    /// Cancellation had closed.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                a_replayed_backlog_range -- --ignored --test-threads=1`"]
    async fn a_replayed_backlog_range_keeps_a_recancelled_subscription_closed() {
        let pool = connect().await;
        let (user_id, train) = ("TEST-REPLAY-BACKLOG", ("TRPB01", "TRPB01ID01"));
        let (_, ids) = seed_linked_subscriptions(&pool, user_id, train.0, train.1, 1).await;
        let tracked_train_id = ids[0];
        let today = db_today(&pool).await;
        let range = [
            backlog_event(train, today, "0002", "replay-b-c1", "2026-09-05T18:05:00Z"),
            backlog_event(train, today, "0005", "replay-b-r1", "2026-09-05T18:10:00Z"),
            backlog_event(train, today, "0002", "replay-b-c2", "2026-09-05T18:20:00Z"),
        ];

        ingest_backlog(&pool, &range[..1]).await;
        assert_eq!(
            resolution_status_of(&pool, tracked_train_id).await,
            "unresolved",
            "the backlog path closes it on a newly stored cancellation"
        );
        ingest_backlog(&pool, &range[1..2]).await;
        assert_eq!(
            resolution_status_of(&pool, tracked_train_id).await,
            "schedule_matched"
        );
        ingest_backlog(&pool, &range[2..]).await;
        assert_eq!(
            resolution_status_of(&pool, tracked_train_id).await,
            "unresolved",
            "the second cancellation closes it again"
        );

        // The rewind: each event on its own, then the range as one batch.
        for event in &range {
            ingest_backlog(&pool, std::slice::from_ref(event)).await;
            assert_eq!(
                resolution_status_of(&pool, tracked_train_id).await,
                "unresolved",
                "replaying {} must not reopen it",
                event.dedup_key
            );
        }
        ingest_backlog(&pool, &range).await;
        assert_eq!(
            resolution_status_of(&pool, tracked_train_id).await,
            "unresolved"
        );

        clean_linked_subscriptions(&pool, user_id, train.0).await;
    }

    /// trust-consumer's path (`POST /private/train-events` and the outbox):
    /// the same cancel -> reinstate -> cancel sequence, then a redelivery of
    /// the Reinstatement alone (an un-ACKed batch) and of the whole range.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                a_replayed_live_range -- --ignored --test-threads=1`"]
    async fn a_replayed_live_range_keeps_a_recancelled_subscription_closed() {
        let pool = connect().await;
        let (user_id, train_uid, train_id) = ("TEST-REPLAY-LIVE", "TRPL01", "TRPL01ID01");
        let (_, ids) = seed_linked_subscriptions(&pool, user_id, train_uid, train_id, 1).await;
        let tracked_train_id = ids[0];
        let event = |msg_type: &str, status: &str, key: &str, actual: &str| {
            let mut event = fixture_event(tracked_train_id, key);
            event.msg_type = msg_type.to_string();
            event.event_type = None;
            event.planned_timestamp = None;
            event.actual_timestamp = at(actual);
            event.status = status.to_string();
            event
        };
        let cancel = event("0002", "cancelled", "replay-l-c1", "2026-09-05T18:05:00Z");
        let reinstate = event("0005", "en_route", "replay-l-r1", "2026-09-05T18:10:00Z");
        let cancel_again = event("0002", "cancelled", "replay-l-c2", "2026-09-05T18:20:00Z");

        upsert_train_event(&pool, &cancel).await.expect("cancel");
        assert_eq!(
            resolution_status_of(&pool, tracked_train_id).await,
            "unresolved"
        );
        upsert_train_event(&pool, &reinstate)
            .await
            .expect("reinstate");
        assert_eq!(
            resolution_status_of(&pool, tracked_train_id).await,
            "schedule_matched"
        );
        upsert_train_event(&pool, &cancel_again)
            .await
            .expect("cancel again");
        assert_eq!(
            resolution_status_of(&pool, tracked_train_id).await,
            "unresolved"
        );

        upsert_train_event(&pool, &reinstate)
            .await
            .expect("redelivered reinstatement");
        assert_eq!(
            resolution_status_of(&pool, tracked_train_id).await,
            "unresolved",
            "a redelivered reinstatement must not reopen it"
        );
        let outcome = upsert_train_events_batch(
            &pool,
            &[cancel.clone(), reinstate.clone(), cancel_again.clone()],
        )
        .await
        .expect("replayed batch");
        assert!(outcome.rejected.is_empty(), "{outcome:?}");
        assert_eq!(
            resolution_status_of(&pool, tracked_train_id).await,
            "unresolved"
        );

        clean_linked_subscriptions(&pool, user_id, train_uid).await;
    }

    /// trust-consumer fans one Cancellation out per subscriber with one
    /// `dedup_key`, and trust-backlog-consumer stores the same key. Only the
    /// first write of the row runs the subscription change, so it closes
    /// (or reopens) every subscription of the train; a later copy changes
    /// nothing.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                a_cancellation_stored_first -- --ignored --test-threads=1`"]
    async fn a_cancellation_stored_first_by_either_path_closes_every_linked_subscription() {
        let pool = connect().await;
        let (user_id, train) = ("TEST-REPLAY-FANOUT", ("TRPF01", "TRPF01ID01"));
        let (_, ids) = seed_linked_subscriptions(&pool, user_id, train.0, train.1, 2).await;
        let today = db_today(&pool).await;

        // The backlog copy lands first.
        ingest_backlog(
            &pool,
            &[backlog_event(
                train,
                today,
                "0002",
                "fanout-c1",
                "2026-09-05T18:05:00Z",
            )],
        )
        .await;
        for &id in &ids {
            assert_eq!(resolution_status_of(&pool, id).await, "unresolved");
        }

        // Then trust-consumer's per-subscriber copies, and a live
        // Reinstatement: its first copy reopens both.
        for &id in &ids {
            let mut cancel = fixture_event(id, "fanout-c1");
            cancel.msg_type = "0002".to_string();
            cancel.status = "cancelled".to_string();
            upsert_train_event(&pool, &cancel)
                .await
                .expect("live cancel");
        }
        for &id in &ids {
            let mut reinstate = fixture_event(id, "fanout-r1");
            reinstate.msg_type = "0005".to_string();
            reinstate.actual_timestamp = at("2026-09-05T18:10:00Z");
            upsert_train_event(&pool, &reinstate)
                .await
                .expect("live reinstate");
        }
        for &id in &ids {
            assert_eq!(resolution_status_of(&pool, id).await, "schedule_matched");
        }

        // A live cancellation stored first closes both too.
        let mut cancel = fixture_event(ids[0], "fanout-c2");
        cancel.msg_type = "0002".to_string();
        cancel.status = "cancelled".to_string();
        cancel.actual_timestamp = at("2026-09-05T18:20:00Z");
        upsert_train_event(&pool, &cancel)
            .await
            .expect("live cancel again");
        for &id in &ids {
            assert_eq!(resolution_status_of(&pool, id).await, "unresolved");
        }

        clean_linked_subscriptions(&pool, user_id, train.0).await;
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                a_live_resolution_with_a_known_train_uid_dual_writes_the_shared_trains_row -- --ignored --test-threads=1`"]
    async fn a_live_resolution_with_a_known_train_uid_dual_writes_the_shared_trains_row() {
        let pool = connect().await;
        let user_id = "TEST-LIVE-RESOLUTION-DUAL-WRITE";
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind("live-resolution@example.com")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");

        let service_date: NaiveDate = "2026-09-06".parse().unwrap();
        let (tracked_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions (user_id, service_date, pin_origin_crs, pin_scheduled_departure) \
             VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(user_id)
        .bind(service_date)
        .bind("WAT")
        .bind(service_date.and_hms_opt(18, 32, 0).unwrap().and_utc())
        .fetch_one(&pool)
        .await
        .expect("seed tracked_trains row");

        let event = TrainMovementEventMessage {
            tracked_train_id,
            resolved_train_uid: Some("TEST-LIVE-UID".to_string()),
            resolved_train_id: Some("221832406".to_string()),
            identity_date: None,
            dedup_key: "test-live-dual-write-dedup".to_string(),
            msg_type: "0003".to_string(),
            event_type: Some("DEPARTURE".to_string()),
            loc_stanox: Some("87212".to_string()),
            loc_crs: Some("WAT".to_string()),
            planned_timestamp: None,
            gbtt_timestamp: None,
            actual_timestamp: None,
            variation_status: None,
            raw_body: serde_json::json!({}),
            status: "en_route".to_string(),
            last_reported_location: Some("WAT".to_string()),
            last_event_type: Some("DEPARTURE".to_string()),
            delay_minutes: Some(0),
            next_calling_point: None,
            eta_next: None,
            eta_source: None,
        };

        upsert_train_event(&pool, &event)
            .await
            .expect("upsert_train_event");

        let (trains_id,): (Option<i64>,) =
            sqlx::query_as("SELECT trains_id FROM train_subscriptions WHERE id = $1")
                .bind(tracked_train_id)
                .fetch_one(&pool)
                .await
                .expect("read back trains_id");
        let trains_id = trains_id.expect("a resolution with a known train_uid must set trains_id");

        let (train_uid, train_id): (String, Option<String>) =
            sqlx::query_as("SELECT train_uid, train_id FROM trains WHERE id = $1")
                .bind(trains_id)
                .fetch_one(&pool)
                .await
                .expect("read back the shared trains row");
        assert_eq!(train_uid, "TEST-LIVE-UID");
        assert_eq!(train_id, Some("221832406".to_string()));

        sqlx::query("DELETE FROM train_subscriptions WHERE user_id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trains WHERE train_uid = 'TEST-LIVE-UID'")
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .ok();
    }

    // --- Live resolution keyed on the train's identity date (M7 leftover) ---
    //
    // A train leaves its origin at 23:30 BST on D and calls at an
    // intermediate stop at 00:30 BST on D+1. The pin there is dated D+1 (its
    // own departure's date); trust-consumer claims it and sends the
    // Activation's origin date D as `identity_date`. The shared row must be
    // `trains(uid, D)` -- `(uid, D+1)` is the next day's run.

    /// Seeds a pending pin dated `pin_date` (no `trains_id`) and returns its id.
    async fn seed_post_midnight_pin(pool: &PgPool, user_id: &str, pin_date: NaiveDate) -> i64 {
        seed_user(pool, user_id).await;
        let (id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions \
                (user_id, service_date, pin_origin_crs, pin_scheduled_departure) \
             VALUES ($1, $2, 'ZIM', $3) RETURNING id",
        )
        .bind(user_id)
        .bind(pin_date)
        // 00:30 BST on `pin_date`.
        .bind(
            (pin_date - chrono::Duration::days(1))
                .and_hms_opt(23, 30, 0)
                .unwrap()
                .and_utc(),
        )
        .fetch_one(pool)
        .await
        .expect("seed a post-midnight pending pin");
        id
    }

    fn post_midnight_resolution(
        tracked_train_id: i64,
        train_uid: &str,
        identity_date: Option<NaiveDate>,
    ) -> TrainMovementEventMessage {
        let mut event = fixture_event(tracked_train_id, &format!("{train_uid}-resolve"));
        event.resolved_train_uid = Some(train_uid.to_string());
        event.resolved_train_id = Some(format!("{train_uid}-TID"));
        event.identity_date = identity_date;
        event
    }

    /// `(trains.train_uid, trains.service_date)` behind a subscription.
    async fn linked_identity(pool: &PgPool, tracked_train_id: i64) -> Option<(String, NaiveDate)> {
        sqlx::query_as(
            "SELECT tr.train_uid, tr.service_date FROM train_subscriptions ts \
             JOIN trains tr ON tr.id = ts.trains_id WHERE ts.id = $1",
        )
        .bind(tracked_train_id)
        .fetch_optional(pool)
        .await
        .expect("read the linked trains row")
    }

    async fn trains_row_exists(pool: &PgPool, train_uid: &str, date: NaiveDate) -> bool {
        sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM trains WHERE train_uid = $1 AND service_date = $2)",
        )
        .bind(train_uid)
        .bind(date)
        .fetch_one(pool)
        .await
        .expect("check for a trains row")
    }

    async fn cleanup_identity_fixture(pool: &PgPool, user_ids: &[&str], train_uid: &str) {
        for user_id in user_ids {
            sqlx::query("DELETE FROM train_subscriptions WHERE user_id = $1")
                .bind(user_id)
                .execute(pool)
                .await
                .ok();
        }
        sqlx::query("DELETE FROM trains WHERE train_uid = $1")
            .bind(train_uid)
            .execute(pool)
            .await
            .ok();
        for user_id in user_ids {
            cleanup_user(pool, user_id).await;
        }
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                a_post_midnight_live_resolution -- --ignored --test-threads=1`"]
    async fn a_post_midnight_live_resolution_creates_the_trains_row_on_the_identity_date() {
        let pool = connect().await;
        let user_id = "TEST-M7L-CREATE-USER";
        let train_uid = "TEST-M7L-CREATE";
        cleanup_identity_fixture(&pool, &[user_id], train_uid).await;

        let origin_date: NaiveDate = "2026-09-12".parse().unwrap();
        let pin_date: NaiveDate = "2026-09-13".parse().unwrap();
        let pin = seed_post_midnight_pin(&pool, user_id, pin_date).await;

        upsert_train_event(
            &pool,
            &post_midnight_resolution(pin, train_uid, Some(origin_date)),
        )
        .await
        .expect("upsert_train_event");

        assert_eq!(
            linked_identity(&pool, pin).await,
            Some((train_uid.to_string(), origin_date)),
            "the shared row is the train's own (uid, origin date)"
        );
        assert!(
            !trains_row_exists(&pool, train_uid, pin_date).await,
            "no trains row for (uid, pin date): that is the next day's run"
        );
        let movements: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM train_movement_events m \
             JOIN trains tr ON tr.id = m.trains_id \
             WHERE tr.train_uid = $1 AND tr.service_date = $2",
        )
        .bind(train_uid)
        .bind(origin_date)
        .fetch_one(&pool)
        .await
        .expect("count movements");
        assert_eq!(movements, 1, "the resolving movement lands on (uid, D)");

        cleanup_identity_fixture(&pool, &[user_id], train_uid).await;
    }

    /// A second subscriber at the post-midnight stop joins the SAME shared
    /// row the first one (or a schedule match) already created for `(uid, D)`.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                a_post_midnight_live_resolution -- --ignored --test-threads=1`"]
    async fn a_post_midnight_live_resolution_uses_an_existing_trains_row_on_the_identity_date() {
        let pool = connect().await;
        let user_id = "TEST-M7L-REUSE-USER";
        let train_uid = "TEST-M7L-REUSE";
        cleanup_identity_fixture(&pool, &[user_id], train_uid).await;

        let origin_date: NaiveDate = "2026-09-12".parse().unwrap();
        let pin_date: NaiveDate = "2026-09-13".parse().unwrap();
        let existing = crate::trains::find_or_create_train(&pool, train_uid, origin_date)
            .await
            .expect("seed the origin-date trains row");
        let pin = seed_post_midnight_pin(&pool, user_id, pin_date).await;

        upsert_train_event(
            &pool,
            &post_midnight_resolution(pin, train_uid, Some(origin_date)),
        )
        .await
        .expect("upsert_train_event");

        let trains_id: Option<i64> =
            sqlx::query_scalar("SELECT trains_id FROM train_subscriptions WHERE id = $1")
                .bind(pin)
                .fetch_one(&pool)
                .await
                .expect("read trains_id");
        assert_eq!(trains_id, Some(existing));
        assert!(!trains_row_exists(&pool, train_uid, pin_date).await);

        cleanup_identity_fixture(&pool, &[user_id], train_uid).await;
    }

    /// Backward compatibility: a message with no `identity_date` (an older
    /// trust-consumer, or no parked Activation) keeps the old behaviour and
    /// keys on the subscription's own date; an implausible one (not the
    /// pin's date or the day before) is ignored the same way.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                a_live_resolution_without_a_plausible_identity_date -- --ignored --test-threads=1`"]
    async fn a_live_resolution_without_a_plausible_identity_date_keys_on_the_subscription_date() {
        let pool = connect().await;
        let absent_user = "TEST-M7L-ABSENT-USER";
        let implausible_user = "TEST-M7L-IMPLAUSIBLE-USER";
        let absent_uid = "TEST-M7L-ABSENT";
        let implausible_uid = "TEST-M7L-IMPLAUSIBLE";
        cleanup_identity_fixture(&pool, &[absent_user], absent_uid).await;
        cleanup_identity_fixture(&pool, &[implausible_user], implausible_uid).await;

        let pin_date: NaiveDate = "2026-09-13".parse().unwrap();

        let absent_pin = seed_post_midnight_pin(&pool, absent_user, pin_date).await;
        upsert_train_event(
            &pool,
            &post_midnight_resolution(absent_pin, absent_uid, None),
        )
        .await
        .expect("upsert_train_event");
        assert_eq!(
            linked_identity(&pool, absent_pin).await,
            Some((absent_uid.to_string(), pin_date))
        );

        let implausible_pin = seed_post_midnight_pin(&pool, implausible_user, pin_date).await;
        upsert_train_event(
            &pool,
            &post_midnight_resolution(
                implausible_pin,
                implausible_uid,
                Some("2026-09-10".parse().unwrap()),
            ),
        )
        .await
        .expect("upsert_train_event");
        assert_eq!(
            linked_identity(&pool, implausible_pin).await,
            Some((implausible_uid.to_string(), pin_date))
        );

        cleanup_identity_fixture(&pool, &[absent_user], absent_uid).await;
        cleanup_identity_fixture(&pool, &[implausible_user], implausible_uid).await;
    }

    #[test]
    fn identity_date_for_accepts_only_the_pins_date_or_the_day_before() {
        let pin_date: NaiveDate = "2026-09-13".parse().unwrap();
        let day = |s: &str| s.parse::<NaiveDate>().unwrap();
        assert_eq!(identity_date_for(1, pin_date, None), pin_date);
        assert_eq!(
            identity_date_for(1, pin_date, Some(day("2026-09-12"))),
            day("2026-09-12")
        );
        assert_eq!(identity_date_for(1, pin_date, Some(pin_date)), pin_date);
        assert_eq!(
            identity_date_for(1, pin_date, Some(day("2026-09-14"))),
            pin_date
        );
        assert_eq!(
            identity_date_for(1, pin_date, Some(day("2026-09-11"))),
            pin_date
        );
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                upsert_train_movement_writes_a_row_for_a_trains_id_with_no_subscriber_at_all \
                -- --ignored --test-threads=1`"]
    async fn upsert_train_movement_writes_a_row_for_a_trains_id_with_no_subscriber_at_all() {
        let pool = connect().await;
        let service_date: NaiveDate = "2026-09-06".parse().unwrap();
        let trains_id = crate::trains::find_or_create_train(&pool, "NOSUB-UID", service_date)
            .await
            .expect("find_or_create_train");
        // Deliberately: no tracked_trains row is ever created for this trains_id.

        let event = TrainMovementEventMessage {
            tracked_train_id: 0, // unused by upsert_train_movement -- see its own doc comment
            resolved_train_uid: None,
            resolved_train_id: None,
            identity_date: None,
            dedup_key: "test-nosub-dedup".to_string(),
            msg_type: "0003".to_string(),
            event_type: Some("DEPARTURE".to_string()),
            loc_stanox: Some("87212".to_string()),
            loc_crs: Some("WAT".to_string()),
            planned_timestamp: None,
            gbtt_timestamp: None,
            actual_timestamp: None,
            variation_status: None,
            raw_body: serde_json::json!({}),
            status: "en_route".to_string(),
            last_reported_location: Some("WAT".to_string()),
            last_event_type: Some("DEPARTURE".to_string()),
            delay_minutes: Some(0),
            next_calling_point: None,
            eta_next: None,
            eta_source: None,
        };

        upsert_train_movement(&pool, trains_id, &event)
            .await
            .expect("upsert_train_movement for an unsubscribed train");

        let (status,): (String,) =
            sqlx::query_as("SELECT status FROM train_current_state WHERE trains_id = $1")
                .bind(trains_id)
                .fetch_one(&pool)
                .await
                .expect(
                    "a current-state row must exist for this trains_id even with zero subscribers",
                );
        assert_eq!(status, "en_route");

        // Also verify the movement-event row itself landed, trains_id-keyed
        // -- the whole point of this function's split from
        // upsert_train_event. As of Task 22, `train_movement_events` no
        // longer has a `tracked_train_id` column at all to assert `NULL`
        // on -- "with no tracked_train_id at all" is now structurally
        // guaranteed by the schema itself, not just this row's own value.
        let (dedup_key,): (String,) =
            sqlx::query_as("SELECT dedup_key FROM train_movement_events WHERE trains_id = $1")
                .bind(trains_id)
                .fetch_one(&pool)
                .await
                .expect(
                    "a movement-event row must exist for this trains_id even with zero subscribers",
                );
        assert_eq!(dedup_key, "test-nosub-dedup");

        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                upsert_train_movement_is_idempotent_on_a_redelivered_dedup_key \
                -- --ignored --test-threads=1`"]
    async fn upsert_train_movement_is_idempotent_on_a_redelivered_dedup_key() {
        // Same real code path called twice against non-reset state (not a
        // duplicated inline copy) -- proving ON CONFLICT (trains_id, dedup_key)
        // actually dedups the movement-event insert for this new,
        // trains_id-only write path, the same guarantee upsert_train_event
        // already had for the legacy tracked_train_id-keyed path.
        let pool = connect().await;
        let service_date: NaiveDate = "2026-09-06".parse().unwrap();
        let trains_id = crate::trains::find_or_create_train(&pool, "IDEMPOTENT-UID", service_date)
            .await
            .expect("find_or_create_train");

        let mut event = TrainMovementEventMessage {
            tracked_train_id: 0,
            resolved_train_uid: None,
            resolved_train_id: None,
            identity_date: None,
            dedup_key: "test-idempotent-dedup".to_string(),
            msg_type: "0003".to_string(),
            event_type: Some("DEPARTURE".to_string()),
            loc_stanox: Some("87212".to_string()),
            loc_crs: Some("WAT".to_string()),
            planned_timestamp: None,
            gbtt_timestamp: None,
            actual_timestamp: None,
            variation_status: None,
            raw_body: serde_json::json!({}),
            status: "en_route".to_string(),
            last_reported_location: Some("WAT".to_string()),
            last_event_type: Some("DEPARTURE".to_string()),
            delay_minutes: Some(0),
            next_calling_point: None,
            eta_next: None,
            eta_source: None,
        };

        upsert_train_movement(&pool, trains_id, &event)
            .await
            .expect("first upsert_train_movement call");
        // A redelivered Kafka message: same dedup_key, but the current-state
        // fields have moved on (a later, real-world snapshot of the same
        // train) -- proving the event-row dedup and the current-state
        // upsert are independent concerns, exactly as upsert_train_event's
        // own doc comment already established for the legacy path.
        event.status = "en_route".to_string();
        event.delay_minutes = Some(5);
        event.last_reported_location = Some("CLJ".to_string());
        upsert_train_movement(&pool, trains_id, &event)
            .await
            .expect("second, redelivered upsert_train_movement call");

        let rows: Vec<(i64,)> = sqlx::query_as(
            "SELECT id FROM train_movement_events WHERE trains_id = $1 AND dedup_key = $2",
        )
        .bind(trains_id)
        .bind("test-idempotent-dedup")
        .fetch_all(&pool)
        .await
        .expect("read back movement-event rows");
        assert_eq!(
            rows.len(),
            1,
            "the redelivered event must be deduped, not inserted a second time"
        );

        let (delay_minutes, last_reported_location): (Option<i32>, Option<String>) = sqlx::query_as(
            "SELECT delay_minutes, last_reported_location FROM train_current_state WHERE trains_id = $1",
        )
        .bind(trains_id)
        .fetch_one(&pool)
        .await
        .expect("read back current-state row");
        assert_eq!(
            delay_minutes,
            Some(5),
            "current-state upsert must still apply the second call's fresher values"
        );
        assert_eq!(last_reported_location, Some("CLJ".to_string()));

        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
    }

    /// DB2-3: the same state delivered again (one message per subscriber
    /// of a shared train) must not rewrite `train_current_state`; a real
    /// change still must. `xmin` changes on every row version, so it shows
    /// whether an UPDATE actually happened.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                an_identical_state_does_not_rewrite_current_state \
                -- --ignored --test-threads=1`"]
    async fn an_identical_state_does_not_rewrite_current_state() {
        let pool = connect().await;
        let service_date: NaiveDate = "2026-09-06".parse().unwrap();
        let trains_id = crate::trains::find_or_create_train(&pool, "DB2-3-NOOP-UID", service_date)
            .await
            .expect("find_or_create_train");
        let at: DateTime<Utc> = "2026-09-06T08:00:00Z".parse().unwrap();
        let event = |dedup_key: &str, delay: i32| TrainMovementEventMessage {
            tracked_train_id: 0,
            resolved_train_uid: None,
            resolved_train_id: None,
            identity_date: None,
            dedup_key: dedup_key.to_string(),
            msg_type: "0003".to_string(),
            event_type: Some("DEPARTURE".to_string()),
            loc_stanox: Some("87212".to_string()),
            loc_crs: Some("WAT".to_string()),
            planned_timestamp: None,
            gbtt_timestamp: None,
            actual_timestamp: Some(at),
            variation_status: None,
            raw_body: serde_json::json!({}),
            status: "en_route".to_string(),
            last_reported_location: Some("WAT".to_string()),
            last_event_type: Some("DEPARTURE".to_string()),
            delay_minutes: Some(delay),
            next_calling_point: None,
            eta_next: None,
            eta_source: None,
        };
        let xmin = |pool: PgPool| async move {
            let (xmin,): (String,) =
                sqlx::query_as("SELECT xmin::text FROM train_current_state WHERE trains_id = $1")
                    .bind(trains_id)
                    .fetch_one(&pool)
                    .await
                    .expect("read xmin");
            xmin
        };

        upsert_train_movement(&pool, trains_id, &event("db2-3-a", 2))
            .await
            .unwrap();
        let first = xmin(pool.clone()).await;
        // Two more subscribers' copies of the same movement.
        upsert_train_movement(&pool, trains_id, &event("db2-3-a", 2))
            .await
            .unwrap();
        upsert_train_movement(&pool, trains_id, &event("db2-3-a", 2))
            .await
            .unwrap();
        assert_eq!(
            xmin(pool.clone()).await,
            first,
            "an identical state must not be rewritten"
        );

        // Same event_time, new state: still applies.
        upsert_train_movement(&pool, trains_id, &event("db2-3-b", 4))
            .await
            .unwrap();
        assert_ne!(
            xmin(pool.clone()).await,
            first,
            "a real change must still be written"
        );
        let (delay,): (Option<i32>,) =
            sqlx::query_as("SELECT delay_minutes FROM train_current_state WHERE trains_id = $1")
                .bind(trains_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(delay, Some(4));

        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
    }

    // --- Option C: event-time monotonicity guard, see
    // docs/superpowers/specs/2026-09-07-shared-train-status-write-race-design.md
    // -----------------------------------------------------------------------

    /// The direct discriminating proof for the guard `upsert_train_movement`'s
    /// own doc comment describes: `trust-consumer` and `trust-backlog-consumer`
    /// both call this same function for the same `trains_id`, with no
    /// coordination between them, so an OLDER event can genuinely reach
    /// Postgres AFTER a NEWER one already wrote the row (one process lagging
    /// behind the other -- see the design doc's §2). Before the `WHERE
    /// EXCLUDED.event_time >= train_current_state.event_time OR
    /// train_current_state.event_time IS NULL` guard existed, this second,
    /// commit-order-later call would blindly win and regress the row back to
    /// stale data; this test proves it no longer does, on
    /// `status`/`last_reported_location`/`delay_minutes`/`event_time` alike.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                an_out_of_order_event_does_not_regress_current_state \
                -- --ignored --test-threads=1`"]
    async fn an_out_of_order_event_does_not_regress_current_state() {
        let pool = connect().await;
        let service_date: NaiveDate = "2026-09-06".parse().unwrap();
        let trains_id =
            crate::trains::find_or_create_train(&pool, "OUT-OF-ORDER-UID", service_date)
                .await
                .expect("find_or_create_train");

        let newer_event = TrainMovementEventMessage {
            tracked_train_id: 0,
            resolved_train_uid: None,
            resolved_train_id: None,
            identity_date: None,
            dedup_key: "test-out-of-order-newer-dedup".to_string(),
            msg_type: "0003".to_string(),
            event_type: Some("ARRIVAL".to_string()),
            loc_stanox: Some("87212".to_string()),
            loc_crs: Some("MKC".to_string()),
            planned_timestamp: Some("2026-09-06T19:45:00Z".parse().unwrap()),
            gbtt_timestamp: None,
            actual_timestamp: Some("2026-09-06T19:45:00Z".parse().unwrap()),
            variation_status: Some("ON TIME".to_string()),
            raw_body: serde_json::json!({}),
            status: "en_route".to_string(),
            last_reported_location: Some("MKC".to_string()),
            last_event_type: Some("ARRIVAL".to_string()),
            delay_minutes: Some(0),
            next_calling_point: Some("BHM".to_string()),
            eta_next: None,
            eta_source: None,
        };
        upsert_train_movement(&pool, trains_id, &newer_event)
            .await
            .expect("the newer event's own write must succeed");

        // An OLDER event (an earlier actual_timestamp), arriving SECOND --
        // e.g. a lagging trust-consumer catching up on a Movement
        // trust-backlog-consumer's own faster path already superseded.
        let older_event = TrainMovementEventMessage {
            tracked_train_id: 0,
            resolved_train_uid: None,
            resolved_train_id: None,
            identity_date: None,
            dedup_key: "test-out-of-order-older-dedup".to_string(),
            msg_type: "0003".to_string(),
            event_type: Some("DEPARTURE".to_string()),
            loc_stanox: Some("72410".to_string()),
            loc_crs: Some("EUS".to_string()),
            planned_timestamp: Some("2026-09-06T19:15:00Z".parse().unwrap()),
            gbtt_timestamp: None,
            actual_timestamp: Some("2026-09-06T19:15:00Z".parse().unwrap()),
            variation_status: Some("LATE".to_string()),
            raw_body: serde_json::json!({}),
            status: "cancelled".to_string(),
            last_reported_location: Some("EUS".to_string()),
            last_event_type: Some("DEPARTURE".to_string()),
            delay_minutes: Some(99),
            next_calling_point: Some("CRE".to_string()),
            eta_next: None,
            eta_source: None,
        };
        upsert_train_movement(&pool, trains_id, &older_event)
            .await
            .expect("the older event's call must succeed (a guarded no-op is not an error)");

        let (status, last_reported_location, delay_minutes, event_time): (
            String,
            Option<String>,
            Option<i32>,
            Option<DateTime<Utc>>,
        ) = sqlx::query_as(
            "SELECT status, last_reported_location, delay_minutes, event_time \
             FROM train_current_state WHERE trains_id = $1",
        )
        .bind(trains_id)
        .fetch_one(&pool)
        .await
        .expect("read back current-state row");

        assert_eq!(
            status, "en_route",
            "the older event's status must not have overwritten the newer event's"
        );
        assert_eq!(
            last_reported_location,
            Some("MKC".to_string()),
            "the older event's location must not have overwritten the newer event's"
        );
        assert_eq!(
            delay_minutes,
            Some(0),
            "the older event's delay_minutes must not have overwritten the newer event's"
        );
        assert_eq!(
            event_time,
            Some("2026-09-06T19:45:00Z".parse::<DateTime<Utc>>().unwrap()),
            "event_time itself must still reflect the newer event, not have regressed either"
        );

        // Also directly verify the movement-event ROWS themselves both
        // landed -- the guard is scoped to the train_current_state upsert
        // only, never to train_movement_events' own append-only insert.
        let movement_events_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM train_movement_events WHERE trains_id = $1")
                .bind(trains_id)
                .fetch_one(&pool)
                .await
                .expect("count train_movement_events");
        assert_eq!(
            movement_events_count, 2,
            "both events' own movement-event rows must still be recorded regardless of the \
             current-state guard"
        );

        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
    }

    /// Same-shape companion to the out-of-order test above, proving the new
    /// guard does NOT interfere with the existing, expected in-order case:
    /// each call carries a `event_time` newer than (or equal to) the last,
    /// so every call's write must still apply normally, exactly as before
    /// this guard existed.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                in_order_events_still_update_current_state_normally \
                -- --ignored --test-threads=1`"]
    async fn in_order_events_still_update_current_state_normally() {
        let pool = connect().await;
        let service_date: NaiveDate = "2026-09-06".parse().unwrap();
        let trains_id = crate::trains::find_or_create_train(&pool, "IN-ORDER-UID", service_date)
            .await
            .expect("find_or_create_train");

        let first_event = TrainMovementEventMessage {
            tracked_train_id: 0,
            resolved_train_uid: None,
            resolved_train_id: None,
            identity_date: None,
            dedup_key: "test-in-order-first-dedup".to_string(),
            msg_type: "0003".to_string(),
            event_type: Some("DEPARTURE".to_string()),
            loc_stanox: Some("72410".to_string()),
            loc_crs: Some("EUS".to_string()),
            planned_timestamp: Some("2026-09-06T19:15:00Z".parse().unwrap()),
            gbtt_timestamp: None,
            actual_timestamp: Some("2026-09-06T19:15:00Z".parse().unwrap()),
            variation_status: Some("ON TIME".to_string()),
            raw_body: serde_json::json!({}),
            status: "en_route".to_string(),
            last_reported_location: Some("EUS".to_string()),
            last_event_type: Some("DEPARTURE".to_string()),
            delay_minutes: Some(0),
            next_calling_point: Some("MKC".to_string()),
            eta_next: None,
            eta_source: None,
        };
        upsert_train_movement(&pool, trains_id, &first_event)
            .await
            .expect("first, in-order event must succeed");

        // A genuinely NEWER event, arriving second -- the ordinary,
        // overwhelmingly common case this guard must not disturb.
        let second_event = TrainMovementEventMessage {
            tracked_train_id: 0,
            resolved_train_uid: None,
            resolved_train_id: None,
            identity_date: None,
            dedup_key: "test-in-order-second-dedup".to_string(),
            msg_type: "0003".to_string(),
            event_type: Some("ARRIVAL".to_string()),
            loc_stanox: Some("87212".to_string()),
            loc_crs: Some("MKC".to_string()),
            planned_timestamp: Some("2026-09-06T19:45:00Z".parse().unwrap()),
            gbtt_timestamp: None,
            actual_timestamp: Some("2026-09-06T19:45:00Z".parse().unwrap()),
            variation_status: Some("LATE".to_string()),
            raw_body: serde_json::json!({}),
            status: "en_route".to_string(),
            last_reported_location: Some("MKC".to_string()),
            last_event_type: Some("ARRIVAL".to_string()),
            delay_minutes: Some(3),
            next_calling_point: Some("BHM".to_string()),
            eta_next: None,
            eta_source: None,
        };
        upsert_train_movement(&pool, trains_id, &second_event)
            .await
            .expect("second, newer event must succeed");

        let (status, last_reported_location, delay_minutes, event_time): (
            String,
            Option<String>,
            Option<i32>,
            Option<DateTime<Utc>>,
        ) = sqlx::query_as(
            "SELECT status, last_reported_location, delay_minutes, event_time \
             FROM train_current_state WHERE trains_id = $1",
        )
        .bind(trains_id)
        .fetch_one(&pool)
        .await
        .expect("read back current-state row");

        assert_eq!(
            status, "en_route",
            "the in-order case is unaffected by the new guard"
        );
        assert_eq!(last_reported_location, Some("MKC".to_string()));
        assert_eq!(delay_minutes, Some(3));
        assert_eq!(
            event_time,
            Some("2026-09-06T19:45:00Z".parse::<DateTime<Utc>>().unwrap())
        );

        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
    }

    /// Regression test for a critical bug review found in this guard's
    /// first version: `WHERE EXCLUDED.event_time >=
    /// train_current_state.event_time OR train_current_state.event_time IS
    /// NULL`, with no `EXCLUDED.event_time IS NULL` branch, silently and
    /// PERMANENTLY blocked every future write to a `trains_id` once (a) its
    /// stored `event_time` had become non-NULL, and (b) a later incoming
    /// event itself carried `event_time = NULL` (a real, reachable case --
    /// see this function's own doc comment on Cancellations with a missing
    /// or malformed `canx_timestamp`). `NULL >= x` is SQL's UNKNOWN, `... IS
    /// NULL` is FALSE once a real value is stored, and `UNKNOWN OR FALSE`
    /// never satisfies `WHERE` -- so the whole `ON CONFLICT DO UPDATE`
    /// became a no-op, worse than the pre-fix blind-overwrite behaviour it
    /// replaced (which at least always applied). This test proves a
    /// no-timestamp event still applies normally even against a row whose
    /// `event_time` is already known.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                a_null_event_time_event_still_applies_even_with_a_known_stored_event_time \
                -- --ignored --test-threads=1`"]
    async fn a_null_event_time_event_still_applies_even_with_a_known_stored_event_time() {
        let pool = connect().await;
        let service_date: NaiveDate = "2026-09-06".parse().unwrap();
        let trains_id =
            crate::trains::find_or_create_train(&pool, "NULL-EVENT-TIME-APPLIES-UID", service_date)
                .await
                .expect("find_or_create_train");

        // First, a normal, well-timed event -- establishes a known, non-NULL
        // stored event_time.
        let timed_event = TrainMovementEventMessage {
            tracked_train_id: 0,
            resolved_train_uid: None,
            resolved_train_id: None,
            identity_date: None,
            dedup_key: "test-null-event-time-timed-dedup".to_string(),
            msg_type: "0003".to_string(),
            event_type: Some("DEPARTURE".to_string()),
            loc_stanox: Some("72410".to_string()),
            loc_crs: Some("EUS".to_string()),
            planned_timestamp: Some("2026-09-06T19:15:00Z".parse().unwrap()),
            gbtt_timestamp: None,
            actual_timestamp: Some("2026-09-06T19:15:00Z".parse().unwrap()),
            variation_status: Some("ON TIME".to_string()),
            raw_body: serde_json::json!({}),
            status: "en_route".to_string(),
            last_reported_location: Some("EUS".to_string()),
            last_event_type: Some("DEPARTURE".to_string()),
            delay_minutes: Some(0),
            next_calling_point: Some("MKC".to_string()),
            eta_next: None,
            eta_source: None,
        };
        upsert_train_movement(&pool, trains_id, &timed_event)
            .await
            .expect("the first, well-timed event must succeed");

        // A CANCELLATION with a missing/malformed canx_timestamp -- both
        // planned_timestamp and actual_timestamp are None, exactly as
        // trust-consumer/trust-backlog-consumer's own Cancellation
        // construction produces for that case. Before the fix, this call
        // would have silently no-op'd (INSERT 0 0) instead of applying.
        let no_timestamp_cancellation = TrainMovementEventMessage {
            tracked_train_id: 0,
            resolved_train_uid: None,
            resolved_train_id: None,
            identity_date: None,
            dedup_key: "test-null-event-time-cancel-dedup".to_string(),
            msg_type: "0002".to_string(),
            event_type: None,
            loc_stanox: None,
            loc_crs: None,
            planned_timestamp: None,
            gbtt_timestamp: None,
            actual_timestamp: None,
            variation_status: None,
            raw_body: serde_json::json!({}),
            status: "cancelled".to_string(),
            last_reported_location: Some("EUS".to_string()),
            last_event_type: Some("DEPARTURE".to_string()),
            delay_minutes: Some(0),
            next_calling_point: Some("MKC".to_string()),
            eta_next: None,
            eta_source: None,
        };
        upsert_train_movement(&pool, trains_id, &no_timestamp_cancellation)
            .await
            .expect("the no-timestamp event's own call must succeed");

        let (status,): (String,) =
            sqlx::query_as("SELECT status FROM train_current_state WHERE trains_id = $1")
                .bind(trains_id)
                .fetch_one(&pool)
                .await
                .expect("read back current-state row");
        assert_eq!(
            status, "cancelled",
            "a no-timestamp event must still apply normally, even against a row whose \
             event_time is already known -- this is the direct regression proof for the bug \
             review found in this guard's first version"
        );

        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
    }

    /// Companion to the regression test above, proving the fix's OTHER half:
    /// a no-timestamp write must not clobber the stored `event_time` back to
    /// `NULL` (which would re-open the `train_current_state.event_time IS
    /// NULL` branch and permanently defeat the guard for this `trains_id`
    /// after just one no-timestamp event). Proves both halves directly: (1)
    /// `event_time` survives the no-timestamp write unchanged, and (2) a
    /// SUBSEQUENT, genuinely-stale, well-timed write is still correctly
    /// guarded (blocked) afterwards -- i.e. the guard keeps working, not
    /// merely that the column value looks right in isolation.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                a_null_event_time_write_does_not_clobber_the_stored_event_time \
                -- --ignored --test-threads=1`"]
    async fn a_null_event_time_write_does_not_clobber_the_stored_event_time() {
        let pool = connect().await;
        let service_date: NaiveDate = "2026-09-06".parse().unwrap();
        let trains_id = crate::trains::find_or_create_train(
            &pool,
            "NULL-EVENT-TIME-NO-CLOBBER-UID",
            service_date,
        )
        .await
        .expect("find_or_create_train");

        let timed_event = TrainMovementEventMessage {
            tracked_train_id: 0,
            resolved_train_uid: None,
            resolved_train_id: None,
            identity_date: None,
            dedup_key: "test-no-clobber-timed-dedup".to_string(),
            msg_type: "0003".to_string(),
            event_type: Some("DEPARTURE".to_string()),
            loc_stanox: Some("72410".to_string()),
            loc_crs: Some("EUS".to_string()),
            planned_timestamp: Some("2026-09-06T19:15:00Z".parse().unwrap()),
            gbtt_timestamp: None,
            actual_timestamp: Some("2026-09-06T19:15:00Z".parse().unwrap()),
            variation_status: Some("ON TIME".to_string()),
            raw_body: serde_json::json!({}),
            status: "en_route".to_string(),
            last_reported_location: Some("EUS".to_string()),
            last_event_type: Some("DEPARTURE".to_string()),
            delay_minutes: Some(0),
            next_calling_point: Some("MKC".to_string()),
            eta_next: None,
            eta_source: None,
        };
        upsert_train_movement(&pool, trains_id, &timed_event)
            .await
            .expect("the first, well-timed event must succeed");

        let no_timestamp_event = TrainMovementEventMessage {
            tracked_train_id: 0,
            resolved_train_uid: None,
            resolved_train_id: None,
            identity_date: None,
            dedup_key: "test-no-clobber-no-timestamp-dedup".to_string(),
            msg_type: "0002".to_string(),
            event_type: None,
            loc_stanox: None,
            loc_crs: None,
            planned_timestamp: None,
            gbtt_timestamp: None,
            actual_timestamp: None,
            variation_status: None,
            raw_body: serde_json::json!({}),
            status: "cancelled".to_string(),
            last_reported_location: Some("EUS".to_string()),
            last_event_type: Some("DEPARTURE".to_string()),
            delay_minutes: Some(0),
            next_calling_point: Some("MKC".to_string()),
            eta_next: None,
            eta_source: None,
        };
        upsert_train_movement(&pool, trains_id, &no_timestamp_event)
            .await
            .expect("the no-timestamp event's own call must succeed");

        let (event_time_after_no_timestamp_write,): (Option<DateTime<Utc>>,) =
            sqlx::query_as("SELECT event_time FROM train_current_state WHERE trains_id = $1")
                .bind(trains_id)
                .fetch_one(&pool)
                .await
                .expect("read back current-state row");
        assert_eq!(
            event_time_after_no_timestamp_write,
            Some("2026-09-06T19:15:00Z".parse::<DateTime<Utc>>().unwrap()),
            "a no-timestamp write must not clobber the stored event_time back to NULL"
        );

        // Now a genuinely STALE, well-timed event (older than the row's
        // still-intact stored event_time) -- must still be correctly
        // guarded (blocked), proving the earlier no-timestamp write didn't
        // permanently defeat the guard for this trains_id.
        let stale_timed_event = TrainMovementEventMessage {
            tracked_train_id: 0,
            resolved_train_uid: None,
            resolved_train_id: None,
            identity_date: None,
            dedup_key: "test-no-clobber-stale-dedup".to_string(),
            msg_type: "0003".to_string(),
            event_type: Some("DEPARTURE".to_string()),
            loc_stanox: Some("11111".to_string()),
            loc_crs: Some("XXX".to_string()),
            planned_timestamp: Some("2026-09-06T18:00:00Z".parse().unwrap()),
            gbtt_timestamp: None,
            actual_timestamp: Some("2026-09-06T18:00:00Z".parse().unwrap()),
            variation_status: Some("ON TIME".to_string()),
            raw_body: serde_json::json!({}),
            status: "en_route".to_string(),
            last_reported_location: Some("XXX".to_string()),
            last_event_type: Some("DEPARTURE".to_string()),
            delay_minutes: Some(0),
            next_calling_point: Some("YYY".to_string()),
            eta_next: None,
            eta_source: None,
        };
        upsert_train_movement(&pool, trains_id, &stale_timed_event)
            .await
            .expect("the stale event's own call must succeed (a guarded no-op is not an error)");

        let (status, last_reported_location, event_time): (
            String,
            Option<String>,
            Option<DateTime<Utc>>,
        ) = sqlx::query_as(
            "SELECT status, last_reported_location, event_time \
             FROM train_current_state WHERE trains_id = $1",
        )
        .bind(trains_id)
        .fetch_one(&pool)
        .await
        .expect("read back current-state row");
        assert_eq!(
            status, "cancelled",
            "the stale, well-timed event must still be guarded (blocked) after the intervening \
             no-timestamp write -- the guard must not have been permanently defeated"
        );
        assert_eq!(last_reported_location, Some("EUS".to_string()));
        assert_eq!(
            event_time,
            Some("2026-09-06T19:15:00Z".parse::<DateTime<Utc>>().unwrap()),
            "event_time itself must still reflect the last real timestamp, unaffected by \
             either the no-timestamp write or the subsequently-blocked stale write"
        );

        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                upsert_train_event_delegates_to_upsert_train_movement_for_an_already_resolved_pin \
                -- --ignored --test-threads=1`"]
    async fn upsert_train_event_delegates_to_upsert_train_movement_for_an_already_resolved_pin() {
        // A pin already resolved (trains_id known from an earlier message),
        // receiving a plain follow-up event that itself carries neither
        // resolved_train_uid nor resolved_train_id. upsert_train_event must
        // still look up the existing trains_id and delegate the shared-table
        // write to upsert_train_movement -- the legacy per-subscription path
        // and the shared path must end up writing the exact same row.
        let pool = connect().await;
        let user_id = "TEST-DELEGATES-ALREADY-RESOLVED";
        seed_user(&pool, user_id).await;
        let service_date: NaiveDate = "2026-09-06".parse().unwrap();
        let trains_id = crate::trains::find_or_create_train(&pool, "DELEGATE-UID", service_date)
            .await
            .expect("find_or_create_train");
        let (tracked_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions \
                (user_id, service_date, pin_origin_crs, pin_scheduled_departure, trains_id, \
                 resolution_status) \
             VALUES ($1, $2, 'EUS', $3, $4, 'resolved') RETURNING id",
        )
        .bind(user_id)
        .bind(service_date)
        .bind(service_date.and_hms_opt(19, 15, 0).unwrap().and_utc())
        .bind(trains_id)
        .fetch_one(&pool)
        .await
        .expect("seed an already-resolved tracked_trains row");

        let mut event = fixture_event(tracked_train_id, "test-delegate-dedup");
        event.resolved_train_uid = None;
        event.resolved_train_id = None; // no fresh resolution info on this event

        upsert_train_event(&pool, &event)
            .await
            .expect("upsert_train_event");

        let (status,): (String,) =
            sqlx::query_as("SELECT status FROM train_current_state WHERE trains_id = $1")
                .bind(trains_id)
                .fetch_one(&pool)
                .await
                .expect(
                    "upsert_train_event must delegate to upsert_train_movement, keyed on trains_id",
                );
        assert_eq!(status, "en_route");

        sqlx::query("DELETE FROM train_subscriptions WHERE id = $1")
            .bind(tracked_train_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .ok();
    }

    /// Task 17's own reason `list_active_tracked_trains` needed to start
    /// selecting `tt.trains_id`: trust-consumer's `apply_reference_reload`
    /// seeds `trains_id_by_tracked_train_id` straight off the field on
    /// `TrackedTrainRef` this query returns -- if the query silently
    /// dropped it, no forwarding signal (Task 17) could ever be built for
    /// this ref, even though the pin is otherwise fully wired up
    /// (`resolved`, with a real `trains_id`).
    #[tokio::test]
    #[ignore = "requires a live database; see the plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p ds-store \
                list_active_tracked_trains -- --ignored --test-threads=1`"]
    async fn list_active_tracked_trains_carries_the_trains_id_through_for_a_resolved_ref() {
        let pool = connect().await;
        let user_id = "TEST-LIST-ACTIVE-TRAINS-ID-USER";
        seed_user(&pool, user_id).await;
        // TODAY, not a hardcoded past date: as of the 2026-09-25 Medium 8 fix,
        // `list_active_tracked_trains` applies a `service_date` floor, so a
        // fixture dated weeks in the past is (correctly) not active and would
        // make this test assert the wrong thing. Same change, same reason, in
        // every `list_active_tracked_trains` test below.
        let service_date = db_today(&pool).await;
        let trains_id = crate::trains::find_or_create_train(
            &pool,
            "TEST-LIST-ACTIVE-TRAINS-ID-UID",
            service_date,
        )
        .await
        .expect("find_or_create_train");

        let (tracked_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions \
                (user_id, service_date, pin_origin_crs, pin_scheduled_departure, \
                 trains_id, resolution_status) \
             VALUES ($1, $2, 'WAT', $3, $4, 'resolved') \
             RETURNING id",
        )
        .bind(user_id)
        .bind(service_date)
        .bind(service_date.and_hms_opt(19, 15, 0).unwrap().and_utc())
        .bind(trains_id)
        .fetch_one(&pool)
        .await
        .expect("seed a resolved tracked_trains row with a real trains_id");

        let refs = list_active_tracked_trains(&pool)
            .await
            .expect("list_active_tracked_trains");
        let seeded = refs
            .into_iter()
            .find(|r| r.id == tracked_train_id)
            .expect("the seeded ref should be active (resolved, no current-state row)");
        assert_eq!(
            seeded.trains_id,
            Some(trains_id),
            "trains_id must round-trip through list_active_tracked_trains, not be dropped"
        );

        sqlx::query("DELETE FROM train_subscriptions WHERE id = $1")
            .bind(tracked_train_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
        cleanup_user(&pool, user_id).await;
    }

    /// `trust-consumer`'s confirmed-arrival detection
    /// (`trust_schema::journey::apply_movement`'s `destination_crs` param)
    /// needs `TrackedTrainRef::destination_crs` to actually round-trip
    /// through this query, the same way `trains_id` already does above --
    /// otherwise the live processing loop would never learn a schedule-
    /// matched train's own terminus and could never recognize a genuine
    /// terminus ARRIVAL at all.
    #[tokio::test]
    #[ignore = "requires a live database; see the plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p ds-store \
                list_active_tracked_trains -- --ignored --test-threads=1`"]
    async fn list_active_tracked_trains_carries_the_destination_crs_through_for_a_schedule_matched_ref()
     {
        let pool = connect().await;
        let user_id = "TEST-LIST-ACTIVE-TRAINS-DEST-USER";
        seed_user(&pool, user_id).await;
        // Today, for the `service_date` floor -- see the sibling test above.
        let service_date = db_today(&pool).await;
        let scheduled_departure = service_date.and_hms_opt(18, 32, 0).unwrap().and_utc();
        let trains_id = crate::trains::find_or_create_train_with_schedule_match(
            &pool,
            "TEST-LIST-ACTIVE-TRAINS-DEST-UID",
            service_date,
            Some("WAT"),
            Some(scheduled_departure),
            Some("WOK"),
            "line-a",
            &serde_json::json!([]),
            &[],
            None,
            None,
        )
        .await
        .expect("find_or_create_train_with_schedule_match");

        let (tracked_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions \
                (user_id, service_date, pin_origin_crs, pin_scheduled_departure, \
                 trains_id, resolution_status) \
             VALUES ($1, $2, 'WAT', $3, $4, 'schedule_matched') \
             RETURNING id",
        )
        .bind(user_id)
        .bind(service_date)
        .bind(scheduled_departure)
        .bind(trains_id)
        .fetch_one(&pool)
        .await
        .expect("seed a schedule_matched tracked_trains row with a real trains_id");

        let refs = list_active_tracked_trains(&pool)
            .await
            .expect("list_active_tracked_trains");
        let seeded = refs
            .into_iter()
            .find(|r| r.id == tracked_train_id)
            .expect("the seeded ref should be active (schedule_matched, no current-state row)");
        assert_eq!(
            seeded.destination_crs,
            Some("WOK".to_string()),
            "destination_crs must round-trip through list_active_tracked_trains, not be dropped"
        );

        sqlx::query("DELETE FROM train_subscriptions WHERE id = $1")
            .bind(tracked_train_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
        cleanup_user(&pool, user_id).await;
    }

    /// The direct fix for the risk Task 21's review flagged, proven rather
    /// than merely argued in a doc comment: before this task,
    /// `flip_legacy_resolution`'s `UPDATE` wrote
    /// `tracked_trains.train_uid = COALESCE($2, train_uid)` per
    /// subscription row -- once two DIFFERENT subscribers shared one
    /// physical train (Task 20's own headline scenario) and each one's
    /// pin was independently resolved via live TRUST (e.g. a process
    /// restart re-delivering the same Activation and causing both
    /// subscribers' pins to resolve against the same `train_uid` and
    /// `service_date`), the SECOND subscriber's `UPDATE` would collide
    /// with `tracked_trains_resolved_identity`'s
    /// `UNIQUE (train_uid, service_date) WHERE train_uid IS NOT NULL`
    /// index and fail outright.
    ///
    /// This test seeds exactly that scenario -- two independent, still-
    /// `pending` pins for two different users, sharing the same
    /// `service_date` (both via `seed_tracked_train`) -- and resolves BOTH
    /// with the identical `resolved_train_uid`/`resolved_train_id`, back
    /// to back, with no cleanup in between. Both calls must succeed: the
    /// index itself is dropped by this task's migration, and
    /// `flip_legacy_resolution` no longer attempts the write that could
    /// have hit it in the first place -- the shared identity link now
    /// lives exclusively on `trains_id`, and `find_or_create_train`'s own
    /// `ON CONFLICT (train_uid, service_date) DO UPDATE` makes a second
    /// resolution against the same identity a safe, idempotent no-op.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                two_subscribers_sharing_a_physical_train_each_resolve_via_live_trust_without_a_unique_constraint_collision \
                -- --ignored --test-threads=1`"]
    async fn two_subscribers_sharing_a_physical_train_each_resolve_via_live_trust_without_a_unique_constraint_collision()
     {
        let pool = connect().await;
        let first_user_id = "TEST-POST-DROP-COLLISION-USER-1";
        let second_user_id = "TEST-POST-DROP-COLLISION-USER-2";
        seed_user(&pool, first_user_id).await;
        seed_user(&pool, second_user_id).await;

        let first_tracking_id = seed_tracked_train(&pool, first_user_id).await;
        let second_tracking_id = seed_tracked_train(&pool, second_user_id).await;

        let mut first_event = fixture_event(first_tracking_id, "dedup-post-drop-collision-1");
        first_event.resolved_train_uid = Some("TEST-POST-DROP-COLLISION-UID".to_string());
        first_event.resolved_train_id = Some("TEST-POST-DROP-COLLISION-TRAIN-ID".to_string());
        upsert_train_event(&pool, &first_event)
            .await
            .expect("the FIRST subscriber's live-TRUST resolution must succeed");

        // Same train_uid, same service_date (both fixtures share
        // seed_tracked_train's hardcoded "2026-09-02") -- exactly the
        // collision shape Task 21's review flagged. Before this task, this
        // second call's own UPDATE would have hit
        // tracked_trains_resolved_identity's UNIQUE constraint.
        let mut second_event = fixture_event(second_tracking_id, "dedup-post-drop-collision-2");
        second_event.resolved_train_uid = Some("TEST-POST-DROP-COLLISION-UID".to_string());
        second_event.resolved_train_id = Some("TEST-POST-DROP-COLLISION-TRAIN-ID".to_string());
        upsert_train_event(&pool, &second_event).await.expect(
            "the SECOND subscriber sharing the same physical train must ALSO resolve, with \
                 no unique-constraint collision -- this is the direct proof of Task 21's fix",
        );

        let (first_status, first_trains_id): (String, Option<i64>) = sqlx::query_as(
            "SELECT resolution_status, trains_id FROM train_subscriptions WHERE id = $1",
        )
        .bind(first_tracking_id)
        .fetch_one(&pool)
        .await
        .expect("read back first subscriber's row");
        let (second_status, second_trains_id): (String, Option<i64>) = sqlx::query_as(
            "SELECT resolution_status, trains_id FROM train_subscriptions WHERE id = $1",
        )
        .bind(second_tracking_id)
        .fetch_one(&pool)
        .await
        .expect("read back second subscriber's row");

        assert_eq!(first_status, "resolved");
        assert_eq!(second_status, "resolved");
        let trains_id = first_trains_id.expect("first subscriber must have a linked trains_id");
        assert_eq!(
            second_trains_id,
            Some(trains_id),
            "both subscribers must end up linked to the exact SAME shared trains row"
        );

        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
        cleanup_user(&pool, first_user_id).await;
        cleanup_user(&pool, second_user_id).await;
    }

    /// The sweep's headline case: a still-`pending`, never-schedule-matched
    /// pin with real origin/departure data and a recent `service_date` must
    /// be surfaced as a retry candidate. This is exactly the row shape the
    /// bug left permanently stuck: `resolve_origin_departure`'s one-shot
    /// live check missed it, `attempt_backlog_match`'s own one-shot
    /// pin-creation-time call found nothing yet, and until this sweep
    /// existed nothing ever asked again.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                list_pending_pins_for_backlog_match_includes_a_plain_pending_pin \
                -- --ignored --test-threads=1`"]
    async fn list_pending_pins_for_backlog_match_includes_a_plain_pending_pin() {
        let pool = connect().await;
        let user_id = "TEST-BACKLOG-SWEEP-CANDIDATE";
        seed_user(&pool, user_id).await;

        let service_date = db_today(&pool).await;
        let id = seed_backlog_candidate_pin(
            &pool,
            user_id,
            service_date,
            Some("EUS"),
            Some("2026-09-09T18:15:00Z".parse().unwrap()),
            "pending",
            None,
        )
        .await;

        let pending = list_pending_pins_for_backlog_match(&pool)
            .await
            .expect("list_pending_pins_for_backlog_match");
        assert!(
            pending.iter().any(|row| row.id == id),
            "a plain still-pending pin with real origin/departure data must be a retry candidate"
        );

        sqlx::query("DELETE FROM train_subscriptions WHERE id = $1")
            .bind(id)
            .execute(&pool)
            .await
            .ok();
        cleanup_user(&pool, user_id).await;
    }

    /// A `pending` row that already has a `trains_id` (the NR-primary
    /// shape, `create_subscription_for_train`) has nothing
    /// `attempt_backlog_match` -- keyed on `(origin CRS, departure time)`,
    /// not `trains_id` -- can usefully do with it. Same exclusion
    /// `list_pending_pins_for_schedule_match` already applies, for the
    /// identical reason.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                list_pending_pins_for_backlog_match_excludes_a_row_with_a_trains_id \
                -- --ignored --test-threads=1`"]
    async fn list_pending_pins_for_backlog_match_excludes_a_row_with_a_trains_id() {
        let pool = connect().await;
        let user_id = "TEST-BACKLOG-SWEEP-HAS-TRAINS-ID";
        seed_user(&pool, user_id).await;

        let trains_id = crate::trains::find_or_create_train(
            &pool,
            "TEST-BACKLOG-SWEEP-TRAINS-ID-UID",
            "2026-09-09".parse().unwrap(),
        )
        .await
        .expect("seed a trains row");

        let service_date = db_today(&pool).await;
        let id = seed_backlog_candidate_pin(
            &pool,
            user_id,
            service_date,
            Some("EUS"),
            Some("2026-09-09T18:15:00Z".parse().unwrap()),
            "pending",
            Some(trains_id),
        )
        .await;

        let pending = list_pending_pins_for_backlog_match(&pool)
            .await
            .expect("list_pending_pins_for_backlog_match");
        assert!(
            !pending.iter().any(|row| row.id == id),
            "a row that already has a trains_id must not be re-selected"
        );

        sqlx::query("DELETE FROM train_subscriptions WHERE id = $1")
            .bind(id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
        cleanup_user(&pool, user_id).await;
    }

    /// The pruned-NR-primary shape (`ON DELETE SET NULL` on `trains_id`,
    /// see `list_pending_pins_for_schedule_match_excludes_a_pruned_nr_primary_row_with_null_pins`
    /// just above): NULL `pin_origin_crs`/`pin_scheduled_departure` gives
    /// `attempt_backlog_match` nothing to look up by, and decoding these
    /// columns as non-`Option` would poison every sweep tick the moment any
    /// row reached this state -- same failure mode this sibling query was
    /// already fixed for.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                list_pending_pins_for_backlog_match_excludes_a_row_with_null_pins \
                -- --ignored --test-threads=1`"]
    async fn list_pending_pins_for_backlog_match_excludes_a_row_with_null_pins() {
        let pool = connect().await;
        let user_id = "TEST-BACKLOG-SWEEP-NULL-PINS";
        seed_user(&pool, user_id).await;

        let service_date = db_today(&pool).await;
        let id =
            seed_backlog_candidate_pin(&pool, user_id, service_date, None, None, "pending", None)
                .await;

        let pending = list_pending_pins_for_backlog_match(&pool).await.expect(
            "must not error even though a row with NULL pin columns is present in the table",
        );
        assert!(
            !pending.iter().any(|row| row.id == id),
            "a row with NULL pin_origin_crs/pin_scheduled_departure must be excluded, not \
             surfaced for a backlog-match attempt it cannot make"
        );

        sqlx::query("DELETE FROM train_subscriptions WHERE id = $1")
            .bind(id)
            .execute(&pool)
            .await
            .ok();
        cleanup_user(&pool, user_id).await;
    }

    /// The staleness bound this query adds beyond
    /// `list_pending_pins_for_schedule_match`'s own shape: a pending pin
    /// whose `service_date` is well outside `trust_event_backlog`'s
    /// retention window (1 day by default) has no honest chance of ever
    /// matching, and must not be swept forever. `10` days ago is
    /// comfortably past the `CURRENT_DATE - INTERVAL '2 days'` floor
    /// regardless of when this test runs.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                list_pending_pins_for_backlog_match_excludes_a_stale_service_date \
                -- --ignored --test-threads=1`"]
    async fn list_pending_pins_for_backlog_match_excludes_a_stale_service_date() {
        let pool = connect().await;
        let user_id = "TEST-BACKLOG-SWEEP-STALE";
        seed_user(&pool, user_id).await;

        let service_date = db_today(&pool).await - chrono::Duration::days(10);
        let id = seed_backlog_candidate_pin(
            &pool,
            user_id,
            service_date,
            Some("EUS"),
            Some((service_date.and_hms_opt(18, 15, 0).unwrap()).and_utc()),
            "pending",
            None,
        )
        .await;

        let pending = list_pending_pins_for_backlog_match(&pool)
            .await
            .expect("list_pending_pins_for_backlog_match");
        assert!(
            !pending.iter().any(|row| row.id == id),
            "a pin more than 2 days stale must not be swept forever with no honest chance of \
             ever matching trust_event_backlog's own short retention window"
        );

        sqlx::query("DELETE FROM train_subscriptions WHERE id = $1")
            .bind(id)
            .execute(&pool)
            .await
            .ok();
        cleanup_user(&pool, user_id).await;
    }

    /// The inverse of the stale-exclusion test above, guarding against an
    /// off-by-one that would make the floor too aggressive: a pin exactly
    /// at "yesterday" (well inside the `CURRENT_DATE - INTERVAL '2 days'`
    /// floor) must still be a candidate.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                list_pending_pins_for_backlog_match_includes_a_recent_but_not_todays_service_date \
                -- --ignored --test-threads=1`"]
    async fn list_pending_pins_for_backlog_match_includes_a_recent_but_not_todays_service_date() {
        let pool = connect().await;
        let user_id = "TEST-BACKLOG-SWEEP-RECENT";
        seed_user(&pool, user_id).await;

        let service_date = db_today(&pool).await - chrono::Duration::days(1);
        let id = seed_backlog_candidate_pin(
            &pool,
            user_id,
            service_date,
            Some("EUS"),
            Some((service_date.and_hms_opt(18, 15, 0).unwrap()).and_utc()),
            "pending",
            None,
        )
        .await;

        let pending = list_pending_pins_for_backlog_match(&pool)
            .await
            .expect("list_pending_pins_for_backlog_match");
        assert!(
            pending.iter().any(|row| row.id == id),
            "a pin from yesterday is well inside the 2-day floor and must still be a candidate"
        );

        sqlx::query("DELETE FROM train_subscriptions WHERE id = $1")
            .bind(id)
            .execute(&pool)
            .await
            .ok();
        cleanup_user(&pool, user_id).await;
    }

    /// **The 2026-09-25 Medium 7 regression test**: the SCHEDULE-match sweep's
    /// own missing `service_date` floor, the exact hazard its backlog sibling
    /// directly above already documented and handled.
    ///
    /// A pin whose service date is long past can never schedule-match (that
    /// date's `schedule_line_population` is gone, if it was ever published),
    /// but nothing excluded it: every sweep tick re-selected it and re-ran a
    /// full `attempt_schedule_match` -- a crosswalk read plus a
    /// whole-day-of-schedules JSONB parse per candidate line -- forever, for
    /// every such pin ever created.
    ///
    /// Asserts both sides of the floor, so it cannot pass by excluding
    /// everything: yesterday's pin is still a candidate, the ten-day-old one
    /// is not.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                list_pending_pins_for_schedule_match_applies_a_service_date_floor \
                -- --ignored --test-threads=1`"]
    async fn list_pending_pins_for_schedule_match_applies_a_service_date_floor() {
        let pool = connect().await;
        let user_id = "TEST-SCHEDULE-SWEEP-FLOOR";
        seed_user(&pool, user_id).await;

        let recent = db_today(&pool).await - chrono::Duration::days(1);
        let stale = db_today(&pool).await - chrono::Duration::days(10);
        let recent_id = seed_backlog_candidate_pin(
            &pool,
            user_id,
            recent,
            Some("EUS"),
            Some(recent.and_hms_opt(18, 15, 0).unwrap().and_utc()),
            "pending",
            None,
        )
        .await;
        let stale_id = seed_backlog_candidate_pin(
            &pool,
            user_id,
            stale,
            Some("EUS"),
            Some(stale.and_hms_opt(18, 15, 0).unwrap().and_utc()),
            "pending",
            None,
        )
        .await;

        let pending = list_pending_pins_for_schedule_match(&pool)
            .await
            .expect("list_pending_pins_for_schedule_match");
        assert!(
            pending.iter().any(|row| row.id == recent_id),
            "yesterday's pending pin is inside the 2-day floor and must still be retried"
        );
        assert!(
            !pending.iter().any(|row| row.id == stale_id),
            "a ten-day-old pending pin can never schedule-match and must not be retried on \
             every sweep tick forever"
        );

        cleanup_user(&pool, user_id).await;
    }

    /// API-6: both sweeps skip a pin dated beyond the timetable horizon, so
    /// a far-future pin (legal before `validate_pin` bounded it) is no
    /// longer retried every 300 s until its date comes round.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                pending_pin_sweeps_skip_pins_beyond_the_timetable_horizon \
                -- --ignored --test-threads=1`"]
    async fn pending_pin_sweeps_skip_pins_beyond_the_timetable_horizon() {
        let pool = connect().await;
        let user_id = "TEST-API6-SWEEP-HORIZON";
        cleanup_user(&pool, user_id).await;
        seed_user(&pool, user_id).await;

        let today = db_today(&pool).await;
        let mut ids = Vec::new();
        for date in [
            today + chrono::Duration::days(1),
            today + chrono::Duration::days(i64::from(SWEEP_MAX_DAYS_AHEAD) + 1),
            "2090-01-01".parse().unwrap(),
            // 2026-10-08: the horizon is four weeks, so a pin 20 days out
            // is swept (it matches once its date's population lands).
            today + chrono::Duration::days(20),
        ] {
            ids.push(
                seed_backlog_candidate_pin(
                    &pool,
                    user_id,
                    date,
                    Some("EUS"),
                    Some(date.and_hms_opt(18, 15, 0).unwrap().and_utc()),
                    "pending",
                    None,
                )
                .await,
            );
        }
        let schedule: Vec<i64> = list_pending_pins_for_schedule_match(&pool)
            .await
            .expect("schedule sweep")
            .into_iter()
            .map(|r| r.id)
            .collect();
        let backlog: Vec<i64> = list_pending_pins_for_backlog_match(&pool)
            .await
            .expect("backlog sweep")
            .into_iter()
            .map(|r| r.id)
            .collect();
        for swept in [&schedule, &backlog] {
            assert!(
                swept.contains(&ids[0]),
                "tomorrow's pin must still be swept"
            );
            assert!(
                !swept.contains(&ids[1]),
                "a pin past the horizon must not be swept yet"
            );
            assert!(!swept.contains(&ids[2]), "a 2090 pin must not be swept");
            assert!(swept.contains(&ids[3]), "a pin 20 days ahead must be swept");
        }

        cleanup_user(&pool, user_id).await;
    }

    /// The sweep must also carry the pin's own destination through, or a pin
    /// that only ever resolves via the periodic retry silently loses the
    /// same-minute tie-break the synchronous attempt at pin-creation time had
    /// -- see `schedule_matching::find_schedule_match`'s round 4(a). Cheap to
    /// assert, and the kind of column that is easy to add to a struct and
    /// forget in the `SELECT`.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                list_pending_pins_for_schedule_match_carries_the_pin_destination \
                -- --ignored --test-threads=1`"]
    async fn list_pending_pins_for_schedule_match_carries_the_pin_destination() {
        let pool = connect().await;
        let user_id = "TEST-SCHEDULE-SWEEP-DEST";
        seed_user(&pool, user_id).await;

        let service_date = db_today(&pool).await;
        let (id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions \
                (user_id, service_date, pin_origin_crs, pin_scheduled_departure, \
                 pin_destination_crs) \
             VALUES ($1, $2, 'BHM', $3, 'EUS') RETURNING id",
        )
        .bind(user_id)
        .bind(service_date)
        .bind(service_date.and_hms_opt(16, 6, 0).unwrap().and_utc())
        .fetch_one(&pool)
        .await
        .expect("seed a pending pin with a destination");

        let pending = list_pending_pins_for_schedule_match(&pool)
            .await
            .expect("list_pending_pins_for_schedule_match");
        let row = pending
            .iter()
            .find(|row| row.id == id)
            .expect("the seeded pin must be a sweep candidate");
        assert_eq!(row.pin_destination_crs.as_deref(), Some("EUS"));

        cleanup_user(&pool, user_id).await;
    }

    /// **The 2026-09-25 Medium 8 regression test**: "active tracked trains"
    /// used to mean "every subscription ever created."
    ///
    /// Neither of the two exclusions bounded it. `resolution_status !=
    /// 'unresolved'` excludes a value nothing in this workspace ever writes
    /// (grepped: only the two migrations' CHECK constraints mention it), and
    /// the `train_current_state` status check stops applying entirely once
    /// `prune_trains` deletes the `trains` row at 30 days -- `trains_id` is
    /// `ON DELETE SET NULL`, so the `LEFT JOIN`s go NULL and `cs.status IS
    /// NULL` readmits the row for good. `trust-consumer` rebuilt its whole
    /// in-memory reference index from this monotonically-growing set on every
    /// periodic reload.
    ///
    /// Both sides asserted: today's subscription is still active, a
    /// ten-day-old one (with no `train_current_state` row at all, which is
    /// exactly the post-prune shape) is not.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                list_active_tracked_trains_excludes_a_stale_service_date \
                -- --ignored --test-threads=1`"]
    async fn list_active_tracked_trains_excludes_a_stale_service_date() {
        let pool = connect().await;
        let user_id = "TEST-ACTIVE-STALE";
        seed_user(&pool, user_id).await;

        let today = db_today(&pool).await;
        let stale = today - chrono::Duration::days(10);
        let today_id = seed_backlog_candidate_pin(
            &pool,
            user_id,
            today,
            Some("WAT"),
            Some(today.and_hms_opt(18, 32, 0).unwrap().and_utc()),
            "resolved",
            None,
        )
        .await;
        let stale_id = seed_backlog_candidate_pin(
            &pool,
            user_id,
            stale,
            Some("WAT"),
            Some(stale.and_hms_opt(18, 32, 0).unwrap().and_utc()),
            "resolved",
            None,
        )
        .await;

        let refs = list_active_tracked_trains(&pool)
            .await
            .expect("list_active_tracked_trains");
        assert!(
            refs.iter().any(|r| r.id == today_id),
            "today's subscription is exactly what this set exists for"
        );
        assert!(
            !refs.iter().any(|r| r.id == stale_id),
            "a ten-day-old subscription has nothing left for trust-consumer's live stream to \
             say about it and must not stay in the active set forever"
        );

        cleanup_user(&pool, user_id).await;
    }

    /// **The 2026-09-26 review's finding H3 regression test**, the future
    /// half of the same gap: the floor above bounds the past, but nothing
    /// bounded the future, so a subscription for a recurring uid's running
    /// several days from now was already "active" -- already reachable via
    /// `trust-consumer`'s `by_train_uid` direct-match index -- long before
    /// its own day arrived. Combined with `activation_is_for_service_date`'s
    /// now-fixed D+1 gap this let TODAY's Activation of a shared `train_uid`
    /// misattribute a completely different, not-yet-run day's subscription.
    ///
    /// Three subscriptions asserted: today's is active (as ever), TOMORROW's
    /// stays active too (finding H3's OWN fix, the `pin_scheduled_departure`
    /// timing check in `activation_is_for_service_date`, is what now keeps
    /// that legitimately in-range without being misattributed), and one ten
    /// days out is excluded.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                list_active_tracked_trains_excludes_a_far_future_service_date \
                -- --ignored --test-threads=1`"]
    async fn list_active_tracked_trains_excludes_a_far_future_service_date() {
        let pool = connect().await;
        let user_id = "TEST-ACTIVE-FUTURE";
        seed_user(&pool, user_id).await;

        let today = db_today(&pool).await;
        let tomorrow = today + chrono::Duration::days(1);
        let far_future = today + chrono::Duration::days(10);
        let today_id = seed_backlog_candidate_pin(
            &pool,
            user_id,
            today,
            Some("WAT"),
            Some(today.and_hms_opt(18, 32, 0).unwrap().and_utc()),
            "resolved",
            None,
        )
        .await;
        let tomorrow_id = seed_backlog_candidate_pin(
            &pool,
            user_id,
            tomorrow,
            Some("WAT"),
            Some(tomorrow.and_hms_opt(17, 30, 0).unwrap().and_utc()),
            "resolved",
            None,
        )
        .await;
        let far_future_id = seed_backlog_candidate_pin(
            &pool,
            user_id,
            far_future,
            Some("WAT"),
            Some(far_future.and_hms_opt(18, 32, 0).unwrap().and_utc()),
            "resolved",
            None,
        )
        .await;

        let refs = list_active_tracked_trains(&pool)
            .await
            .expect("list_active_tracked_trains");
        assert!(
            refs.iter().any(|r| r.id == today_id),
            "today's subscription is exactly what this set exists for"
        );
        assert!(
            refs.iter().any(|r| r.id == tomorrow_id),
            "tomorrow's subscription must stay in range -- the legitimate D+1 case needs it, \
             and finding H3's timing check (not this floor/ceiling) is what keeps it from being \
             misattributed"
        );
        assert!(
            !refs.iter().any(|r| r.id == far_future_id),
            "a subscription several days out has no legitimate reason to already be in the \
             active set and must not sit there until its own day arrives"
        );

        cleanup_user(&pool, user_id).await;
    }

    /// **The 2026-09-25 High 2 regression test**: a live-TRUST resolution
    /// whose `train_uid` DISAGREES with the one this subscription's shared
    /// `trains` row already carries must be refused outright, not glued onto
    /// that row.
    ///
    /// The shape, which is reachable in production: a subscription is already
    /// correctly linked to train A (schedule-matched, or resolved earlier),
    /// and `trust-consumer`'s CRS+time heuristic then matches the same pin to
    /// train B at a busy station and calls this with B's uid. The
    /// `(Some(existing_trains_id), _)` arm took the existing `trains_id`
    /// unconditionally and never compared the uids, so
    /// `mark_train_resolved` stamped B's TRUST `train_id` onto A's row and
    /// `upsert_train_movement` wrote B's movements into A's movement/current-
    /// state tables -- visible to EVERY subscriber of A, and feeding A's
    /// journey view, ETA and delay-repay evidence.
    ///
    /// Asserts the refusal is total: A's `train_id` stays NULL, and no
    /// movement or current-state row appears for A at all.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                upsert_train_event_refuses_a_resolution_whose_uid_disagrees \
                -- --ignored --test-threads=1`"]
    async fn upsert_train_event_refuses_a_resolution_whose_uid_disagrees() {
        let pool = connect().await;
        let user_id = "TEST-UID-DISAGREE";
        cleanup_user(&pool, user_id).await;
        // Cleanup FIRST as well as last, the same posture
        // `an_nr_primary_subscription_receives_live_movement_events` already
        // takes: this test's whole assertion is "nothing was written to this
        // shared row", and `find_or_create_train` is idempotent per
        // `(train_uid, service_date)` -- so a leftover row from an earlier
        // FAILED run (which never reaches its own cleanup) would be reused
        // here with its `train_id` already stamped, failing this test for a
        // reason that has nothing to do with the code under test.
        cleanup_disagreement_fixture_trains(&pool).await;
        seed_user(&pool, user_id).await;
        let service_date = db_today(&pool).await;

        // Train A: the identity this subscription is ALREADY correctly linked
        // to, deliberately left unresolved (`train_id` NULL) so the assertions
        // below can tell "nothing was written" from "something was".
        let trains_id =
            crate::trains::find_or_create_train(&pool, "TEST-DISAGREE-EXISTING", service_date)
                .await
                .expect("find_or_create_train for the existing identity");

        let tracked_train_id = seed_backlog_candidate_pin(
            &pool,
            user_id,
            service_date,
            Some("BHM"),
            Some(service_date.and_hms_opt(16, 6, 0).unwrap().and_utc()),
            "schedule_matched",
            Some(trains_id),
        )
        .await;

        // Train B's resolution arriving for train A's subscription.
        let mut event = fixture_event(tracked_train_id, "test-uid-disagree-dedup");
        event.resolved_train_uid = Some("TEST-DISAGREE-OTHER".to_string());
        event.resolved_train_id = Some("999999999".to_string());

        upsert_train_event(&pool, &event)
            .await
            .expect("upsert_train_event must not error -- it declines, loudly, and returns Ok");

        let (train_id,): (Option<String>,) =
            sqlx::query_as("SELECT train_id FROM trains WHERE id = $1")
                .bind(trains_id)
                .fetch_one(&pool)
                .await
                .expect("read back the existing shared row");
        assert_eq!(
            train_id, None,
            "the disagreeing resolution must not stamp another train's TRUST train_id onto this \
             already-correctly-matched shared row"
        );

        let (movements,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM train_movement_events WHERE trains_id = $1")
                .bind(trains_id)
                .fetch_one(&pool)
                .await
                .expect("count movement rows");
        assert_eq!(
            movements, 0,
            "a refused resolution must not fall through to a movement write against the same \
             shared row -- that was the whole harm"
        );
        let (states,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM train_current_state WHERE trains_id = $1")
                .bind(trains_id)
                .fetch_one(&pool)
                .await
                .expect("count current-state rows");
        assert_eq!(states, 0, "nor a current-state write");

        cleanup_user(&pool, user_id).await;
        cleanup_disagreement_fixture_trains(&pool).await;
    }

    /// Deletes both `trains` identities the uid-disagreement test uses, plus
    /// anything keyed on them. Called at the START of that test as well as the
    /// end -- see its own comment for why a leftover row would otherwise fail
    /// it for the wrong reason.
    async fn cleanup_disagreement_fixture_trains(pool: &PgPool) {
        let ids: Vec<i64> = sqlx::query_scalar(
            "SELECT id FROM trains WHERE train_uid IN ('TEST-DISAGREE-EXISTING', \
             'TEST-DISAGREE-OTHER')",
        )
        .fetch_all(pool)
        .await
        .unwrap_or_default();
        for id in ids {
            sqlx::query("DELETE FROM train_current_state WHERE trains_id = $1")
                .bind(id)
                .execute(pool)
                .await
                .ok();
            sqlx::query("DELETE FROM train_movement_events WHERE trains_id = $1")
                .bind(id)
                .execute(pool)
                .await
                .ok();
            sqlx::query("DELETE FROM trains WHERE id = $1")
                .bind(id)
                .execute(pool)
                .await
                .ok();
        }
    }

    /// The other side of the same guard, so it cannot pass by refusing
    /// everything: when the resolution's uid AGREES with the shared row's, the
    /// resolution applies exactly as it always did -- `train_id` stamped,
    /// movement and current-state rows written.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                upsert_train_event_applies_a_resolution_whose_uid_agrees \
                -- --ignored --test-threads=1`"]
    async fn upsert_train_event_applies_a_resolution_whose_uid_agrees() {
        let pool = connect().await;
        let user_id = "TEST-UID-AGREE";
        // Same cleanup-first posture as its sibling above: this asserts an
        // exact movement-row count, which a leftover row from a failed earlier
        // run would distort.
        cleanup_user(&pool, user_id).await;
        seed_user(&pool, user_id).await;
        let service_date = db_today(&pool).await;

        sqlx::query(
            "DELETE FROM train_movement_events WHERE trains_id IN \
             (SELECT id FROM trains WHERE train_uid = 'TEST-AGREE-UID')",
        )
        .execute(&pool)
        .await
        .ok();
        sqlx::query(
            "DELETE FROM train_current_state WHERE trains_id IN \
             (SELECT id FROM trains WHERE train_uid = 'TEST-AGREE-UID')",
        )
        .execute(&pool)
        .await
        .ok();
        sqlx::query("DELETE FROM trains WHERE train_uid = 'TEST-AGREE-UID'")
            .execute(&pool)
            .await
            .ok();
        let trains_id = crate::trains::find_or_create_train(&pool, "TEST-AGREE-UID", service_date)
            .await
            .expect("find_or_create_train");
        let tracked_train_id = seed_backlog_candidate_pin(
            &pool,
            user_id,
            service_date,
            Some("BHM"),
            Some(service_date.and_hms_opt(16, 6, 0).unwrap().and_utc()),
            "schedule_matched",
            Some(trains_id),
        )
        .await;

        let mut event = fixture_event(tracked_train_id, "test-uid-agree-dedup");
        event.resolved_train_uid = Some("TEST-AGREE-UID".to_string());
        event.resolved_train_id = Some("111111111".to_string());

        upsert_train_event(&pool, &event)
            .await
            .expect("upsert_train_event");

        let (train_id,): (Option<String>,) =
            sqlx::query_as("SELECT train_id FROM trains WHERE id = $1")
                .bind(trains_id)
                .fetch_one(&pool)
                .await
                .expect("read back the shared row");
        assert_eq!(train_id, Some("111111111".to_string()));
        let (movements,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM train_movement_events WHERE trains_id = $1")
                .bind(trains_id)
                .fetch_one(&pool)
                .await
                .expect("count movement rows");
        assert_eq!(movements, 1);

        sqlx::query("DELETE FROM train_current_state WHERE trains_id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM train_movement_events WHERE trains_id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
        cleanup_user(&pool, user_id).await;
        sqlx::query("DELETE FROM trains WHERE train_uid = 'TEST-AGREE-UID'")
            .execute(&pool)
            .await
            .ok();
    }

    /// 2026-09-26 review, Medium finding 6: reproduces the cross-match
    /// residual this task's transaction fix closes. Two DIFFERENT physical
    /// trains (different `train_uid`s) end up reported under the SAME
    /// TRUST `train_id` for the SAME `service_date` -- a real-world
    /// cross-matching incident, not a contrived input (see
    /// `20260925221500_close_out_trains_train_id_service_date_collisions.sql`'s
    /// own header for the 2026-09-25 incident this exact shape came from).
    /// The second subscriber's own resolution hits
    /// `trains_train_id_service_date`'s unique index the moment
    /// `mark_train_resolved` tries to write the SAME `train_id` onto a
    /// SECOND `trains` row for that date.
    ///
    /// Before this task's fix, `flip_legacy_resolution` first committed
    /// `resolution_status = 'resolved'` on the second subscriber's row as
    /// its own independent statement, then attempted the colliding write
    /// separately -- so the second subscriber's row was left reading
    /// `'resolved'` forever, with `trains_id` still `NULL`: "resolved" but
    /// never actually claimed, and invisible to every sweep that only
    /// re-checks still-`'pending'` rows. This proves that can no longer
    /// happen: the second call must fail outright (surfacing the
    /// collision instead of burying it), and the second subscriber's row
    /// must come back completely unchanged -- still `'pending'`, still no
    /// `trains_id` -- so a later, correct resolution can still claim it.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                a_cross_matched_second_resolution_does_not_strand_the_subscription_as_resolved_with_no_trains_id \
                -- --ignored --test-threads=1`"]
    async fn a_cross_matched_second_resolution_does_not_strand_the_subscription_as_resolved_with_no_trains_id()
     {
        let pool = connect().await;
        let first_user_id = "TEST-M6-COLLISION-USER-1";
        let second_user_id = "TEST-M6-COLLISION-USER-2";
        seed_user(&pool, first_user_id).await;
        seed_user(&pool, second_user_id).await;

        let first_tracking_id = seed_tracked_train(&pool, first_user_id).await;
        let second_tracking_id = seed_tracked_train(&pool, second_user_id).await;

        // First subscriber resolves cleanly to its own physical train.
        let mut first_event = fixture_event(first_tracking_id, "dedup-m6-collision-1");
        first_event.resolved_train_uid = Some("TEST-M6-UID-A".to_string());
        first_event.resolved_train_id = Some("TEST-M6-SHARED-TRAIN-ID".to_string());
        upsert_train_event(&pool, &first_event)
            .await
            .expect("the first subscriber's live-TRUST resolution must succeed");

        // Second subscriber resolves to a DIFFERENT physical train (a
        // different `train_uid`) but the SAME `train_id` and the same
        // `service_date` (both fixtures share `seed_tracked_train`'s
        // hardcoded "2026-09-02") -- the cross-match collision. This must
        // fail: `find_or_create_train` mints a fresh, distinct `trains` row
        // for `TEST-M6-UID-B` (a different `train_uid` never collides on
        // its own upsert), but the subsequent `mark_train_resolved` write
        // that stamps `TEST-M6-SHARED-TRAIN-ID` onto THAT row collides with
        // the first subscriber's row for the same `(train_id, service_date)`.
        let mut second_event = fixture_event(second_tracking_id, "dedup-m6-collision-2");
        second_event.resolved_train_uid = Some("TEST-M6-UID-B".to_string());
        second_event.resolved_train_id = Some("TEST-M6-SHARED-TRAIN-ID".to_string());
        let second_result = upsert_train_event(&pool, &second_event).await;
        assert!(
            second_result.is_err(),
            "a genuine cross-match collision must surface as an error, not be silently \
             swallowed"
        );

        // The heart of this fix: the second subscriber's OWN row must be
        // completely unaffected by its own failed resolution attempt --
        // never left reading 'resolved' with no trains_id to back it up.
        let (second_status, second_trains_id): (String, Option<i64>) = sqlx::query_as(
            "SELECT resolution_status, trains_id FROM train_subscriptions WHERE id = $1",
        )
        .bind(second_tracking_id)
        .fetch_one(&pool)
        .await
        .expect("read back second subscriber's row");
        assert_eq!(
            second_status, "pending",
            "a failed resolution must not leave the subscription stuck reading 'resolved'"
        );
        assert!(
            second_trains_id.is_none(),
            "a failed resolution must not leave a dangling trains_id link either"
        );

        // The doomed candidate `trains` row itself must not have survived
        // the rollback -- proving the INSERT inside the same failed
        // transaction was undone, not merely orphaned.
        let (uid_b_count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM trains WHERE train_uid = 'TEST-M6-UID-B'")
                .fetch_one(&pool)
                .await
                .expect("count TEST-M6-UID-B rows");
        assert_eq!(
            uid_b_count, 0,
            "the rolled-back transaction must not leave behind the candidate trains row either"
        );

        // The FIRST subscriber, unrelated to the second's failed attempt,
        // must remain completely unaffected.
        let (first_status, first_trains_id): (String, Option<i64>) = sqlx::query_as(
            "SELECT resolution_status, trains_id FROM train_subscriptions WHERE id = $1",
        )
        .bind(first_tracking_id)
        .fetch_one(&pool)
        .await
        .expect("read back first subscriber's row");
        assert_eq!(first_status, "resolved");
        let trains_id = first_trains_id.expect("first subscriber must have a linked trains_id");

        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
        cleanup_user(&pool, first_user_id).await;
        cleanup_user(&pool, second_user_id).await;
    }

    /// A row that already advanced past `'pending'` (schedule-matched,
    /// resolved live, or given up on) has nothing left for a backlog match
    /// to do -- must never be re-selected.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                list_pending_pins_for_backlog_match_excludes_a_non_pending_row \
                -- --ignored --test-threads=1`"]
    async fn list_pending_pins_for_backlog_match_excludes_a_non_pending_row() {
        let pool = connect().await;
        let user_id = "TEST-BACKLOG-SWEEP-NON-PENDING";
        seed_user(&pool, user_id).await;

        let service_date = db_today(&pool).await;
        let id = seed_backlog_candidate_pin(
            &pool,
            user_id,
            service_date,
            Some("EUS"),
            Some("2026-09-09T18:15:00Z".parse().unwrap()),
            "schedule_matched",
            None,
        )
        .await;

        let pending = list_pending_pins_for_backlog_match(&pool)
            .await
            .expect("list_pending_pins_for_backlog_match");
        assert!(
            !pending.iter().any(|row| row.id == id),
            "a row already past 'pending' must not be re-selected for a backlog match retry"
        );

        sqlx::query("DELETE FROM train_subscriptions WHERE id = $1")
            .bind(id)
            .execute(&pool)
            .await
            .ok();
        cleanup_user(&pool, user_id).await;
    }
}
