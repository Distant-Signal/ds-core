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
//! 2. Full backfill: every backlog row for that `train_id` on the matched
//!    row's own `service_date` (never the pin's -- see `find_backlog_match`
//!    on absolute-time matching), in `received_at` order. Keyed on `train_id`, NOT `train_uid` --
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
//! it).
//!
//! **M9 finding, 2026-09-26 review: this module now DOES define its own
//! local tolerance constant after all, but a different, TIGHTER one for a
//! different purpose.** `common::MATCH_TOLERANCE` (20 minutes) is sized for
//! comparing a SCHEDULED time against an ACTUAL one, where lateness is
//! genuinely expected -- but `find_backlog_match`'s own CRS+time lookup
//! compares TWO scheduled times (a Movement's `planned_timestamp`, i.e. WTT,
//! against a pin's own `pin_scheduled_departure`), exactly the comparison
//! `trust-consumer::matching::resolve_origin_departure`'s own
//! `SCHEDULED_DEPARTURE_TOLERANCE` already exists for, and that live matcher
//! was deliberately tightened from `common::MATCH_TOLERANCE` down to 5
//! minutes (see that constant's own doc comment: "there is no lateness
//! between two timetabled values," and a wide window at a busy terminus
//! "routinely contains a dozen other services"). This backlog-replay
//! sibling did the identical scheduled-vs-scheduled comparison but was never
//! brought in line -- seen twice in this file's own regression fixtures
//! before this fix: the real `Y80926`/`W34058` mis-attribution this
//! module's own contradiction filter now also guards, and the `trains_id=713729`
//! ARRIVAL mis-attribution, both real production incidents that a 20-minute
//! scheduled-vs-scheduled window at a busy terminus made possible in the
//! first place. See [`SCHEDULED_DEPARTURE_TOLERANCE`]'s own doc comment.
//! `common::MATCH_TOLERANCE` has no remaining use in this module and its
//! `use` is dropped. The plausibility guard's own skew threshold is a
//! separate constant entirely (`common::trust_timestamp`'s
//! `MAX_TIMESTAMP_SKEW_AHEAD_OF_RECEIPT`), untouched by this fix.
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

use chrono::{DateTime, Duration, NaiveDate, Utc};
use sqlx::PgPool;
use trust_schema::journey::{self, DerivedState};
use trust_schema::schema::Movement;

use crate::data::train_tracking;

/// How far a backlog Movement's own SCHEDULED (`planned_timestamp`, i.e.
/// WTT) departure time may sit from a pin's `pin_scheduled_departure` and
/// still be believed to describe the same booked service.
///
/// **M9 finding, 2026-09-26 review.** This used to be `common::MATCH_TOLERANCE`
/// (20 minutes) -- see this module's own top-level doc comment for the full
/// reasoning this constant now mirrors from
/// `trust-consumer::matching::SCHEDULED_DEPARTURE_TOLERANCE` exactly:
/// `find_backlog_match` compares two SCHEDULED times, not a scheduled time
/// against an actual one, so there is no lateness to absorb and the only
/// real slack needed is the small, couple-of-minutes disagreement between a
/// Darwin-sourced pin's public timetable (GBTT) and TRUST's own working
/// timetable (WTT). A 20-minute window at a busy terminus routinely
/// contains several OTHER trains' scheduled departures -- exactly the shape
/// of two real, confirmed production mis-attributions this file's own
/// regression tests document (`a_backlog_candidate_naming_a_different_train_uid_never_repoints_an_identified_pin`'s
/// `Y80926`/`W34058` incident, and `an_unrelated_arrival_event_never_falsely_matches_a_pins_scheduled_departure`'s
/// `trains_id=713729` one) -- and the live matcher was tightened to 5
/// minutes for exactly this reason. This backlog-replay sibling, doing the
/// identical scheduled-vs-scheduled comparison, was never brought in line
/// until now.
const SCHEDULED_DEPARTURE_TOLERANCE: Duration = Duration::minutes(5);

#[derive(Debug, Clone, sqlx::FromRow)]
struct BacklogRow {
    train_id: String,
    // The row's own date. Used as `dedup_key`'s date component on replay,
    // since one history can span two dates (see `fetch_backlog_history`).
    service_date: NaiveDate,
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
/// [`SCHEDULED_DEPARTURE_TOLERANCE`] of `pin_scheduled_departure`, exist? Returns that
/// row's `train_id` (TRUST's own daily identifier -- present on every row
/// this table ever stores, per Task 9) plus, opportunistically, a
/// `train_uid` (CIF's own identifier) if an Activation row for that same
/// `train_id` is also present somewhere in the backlog (it may not be --
/// see this module's own doc comment and this plan's "Dependency on the
/// schedule-first plan" section on why that's an accepted, named gap, not
/// a bug). Arbitrary among ties among genuine DEPARTURE candidates.
///
/// **`event_type = 'DEPARTURE'` is required in the SQL below, mirroring
/// `trust-consumer::process.rs`'s own live-matching guard
/// (`if movement.event_type != "DEPARTURE" { return Vec::new(); }`).**
/// An earlier version of this function's own doc comment claimed this
/// table "already excludes PASS and only Activation/Cancellation/Movement
/// rows exist here at all", and treated that as reason enough to skip
/// `resolve_origin_departure`'s "only a DEPARTURE may claim" refinement --
/// but excluding PASS does nothing about ARRIVAL, which this table DOES
/// store (see `trust_event_backlog`'s own migration: `event_type CHECK
/// (event_type IS NULL OR event_type IN ('ARRIVAL', 'DEPARTURE'))`, and
/// `trust-backlog-consumer::process::process_message`'s own Movement arm,
/// which keeps a Movement "only if its `event_type` is `ARRIVAL` or
/// `DEPARTURE`"). Without this filter, a pin's scheduled departure could
/// match an UNRELATED train's ARRIVAL event at the same CRS inside the
/// same `SCHEDULED_DEPARTURE_TOLERANCE` window -- a routine occurrence at any
/// turnback/interchange station -- and that wrong train's entire movement
/// history would then get replayed onto the pin's `trains_id` via
/// `fetch_backlog_history`/`replay_backlog_history` below. Confirmed as a
/// real, live-production data-correctness bug (a correctly schedule-matched
/// `trains_id` received 29 movement events belonging to the wrong,
/// opposite-direction train this exact way) -- see
/// `an_unrelated_arrival_event_never_falsely_matches_a_pins_scheduled_departure`
/// and `a_real_departure_is_still_found_despite_a_more_favorably_sorted_unrelated_arrival`
/// below for the regression coverage.
///
/// Deliberately does NOT look at this matching row's own `train_uid`
/// column: a Movement/Cancellation row's `train_uid` is always NULL as
/// written by Task 9's own consumer (only an Activation row ever carries
/// one), so the matching row found here is realistically always a
/// Movement (the only kept type that carries a `crs`) and its `train_uid`
/// column is realistically always NULL. The real `train_uid` lookup is the
/// second, explicit query below, by `train_id`.
///
/// **Plausibility guard (defense-in-depth against the still-unconfirmed
/// TRUST timestamp corruption -- see `common::trust_timestamp`'s own doc
/// comment for the full background):** the SQL itself now excludes any row
/// whose `actual_timestamp` is implausibly ahead of its own `received_at`
/// (this consumer's own wall-clock receipt time, reliable and unaffected by
/// the corruption -- see `trust_event_backlog`'s migration), matching
/// `common::trust_timestamp::is_plausible_actual_timestamp`'s own threshold
/// (`common::trust_timestamp::MAX_TIMESTAMP_SKEW_AHEAD_OF_RECEIPT`).
///
/// **Finding #4: filtered in the WHERE clause, not rejected after the
/// fact.** An earlier version of this function selected the single
/// best-CRS+time-matching row via `ORDER BY planned_timestamp LIMIT 1` and
/// rejected it in Rust if implausible, returning `Ok(None)` -- but the NEXT
/// sweep would deterministically re-select and re-reject that exact SAME
/// row forever (nothing about the query changes between sweeps), never
/// reaching a second, potentially-plausible candidate in the same window.
/// The pin would then only ever self-heal once the table's 1-day retention
/// aged that row out, not on the "next sweep" this function's own docs (and
/// the plan's) always claimed. Excluding the implausible row in the SQL
/// itself means an implausible candidate simply doesn't compete for
/// `ORDER BY planned_timestamp LIMIT 1` at all, so a second, plausible
/// candidate in the same window is found immediately, on the very next
/// sweep, exactly as documented. `actual_timestamp IS NULL` rows (not
/// every kept row carries one) are never excluded by this filter -- there
/// is nothing to guard in that case, so they remain eligible unchanged.
///
/// **Closest-to-scheduled, not earliest-in-window (2026-09-25 review,
/// same root-cause class as `Y80908` -- see `schedule_matching.rs`'s own
/// "Round 3" doc comment for that incident's full account).** Before this
/// fix the query below ended `ORDER BY planned_timestamp LIMIT 1`, i.e.
/// whichever candidate DEPARTURE in the `SCHEDULED_DEPARTURE_TOLERANCE`
/// window happened to depart EARLIEST -- not the one actually closest to this pin's own
/// `pin_scheduled_departure`. At a busy multi-departure station that is a
/// different train's schedule, picked arbitrarily by clock time rather
/// than by relevance to the pin: exactly the same "closest wins, not
/// first-by-some-unrelated-order wins" bug `schedule_matching.rs`'s own
/// Round 3 fix closed for the live-matching path, just manifesting here on
/// the backlog-replay path instead. Ordering by absolute distance from
/// `pin_scheduled_departure` means a service departing 1 minute from the
/// pin's own booked time is always preferred over one departing 19 minutes
/// away, regardless of which one happens to be chronologically earlier.
///
/// **Matched on absolute time, not on `service_date` (Repeater Signal M7
/// residual, 2026-09-27).** The pin's `pin_scheduled_departure` is a real
/// UTC instant and so is every backlog row's `planned_timestamp`, so the
/// `SCHEDULED_DEPARTURE_TOLERANCE` window already says which day a
/// candidate belongs to: two runs of the same service on consecutive days
/// are 24 hours apart, far outside a 5-minute window. This query used to
/// add `service_date = <pin's service_date>` on top of that, but the two
/// dates follow different conventions for one class of train, and the
/// filter then excluded the right answer:
///
/// - a pin's `service_date` is the London calendar date of its OWN
///   departure (`TrackTrainForm.tsx` takes the picked wall-clock string's
///   date), so a pin at an intermediate stop at 00:30 is dated D+1;
/// - a backlog row's `service_date` is the train's ORIGIN date, since
///   commit 97ccd3ea dates an Activation by `tp_origin_timestamp` and its
///   Movements inherit that date. A train that left its origin at 23:30 on
///   D is filed under D.
///
/// So every intermediate-stop pin after midnight on a train that started
/// before midnight never matched. The filter was added (Low finding #2,
/// 2026-09-25) so that this lookup and [`fetch_backlog_history`] agreed on
/// a date. That agreement now comes from the other direction: the matched
/// row's OWN `service_date` is returned in [`BacklogCandidate`] and is what
/// the history fetch and the train identity use, never the pin's.
///
/// The predicate keeps exactly the shape of the expression index
/// `trust_event_backlog_upper_crs_time (UPPER(crs), planned_timestamp)
/// WHERE crs IS NOT NULL` (20260926182000): an equality on `UPPER(crs)` and
/// a range on `planned_timestamp`, both index conditions.
#[expect(
    clippy::similar_names,
    reason = "the similar names are distinct domain terms"
)]
async fn find_backlog_match(
    pool: &PgPool,
    pin_origin_crs: &str,
    pin_scheduled_departure: DateTime<Utc>,
) -> anyhow::Result<Option<BacklogCandidate>> {
    let window_start = pin_scheduled_departure - SCHEDULED_DEPARTURE_TOLERANCE;
    let window_end = pin_scheduled_departure + SCHEDULED_DEPARTURE_TOLERANCE;

    // The skew threshold is interpolated as a plain integer (not bound as a
    // parameter) because `chrono::Duration` has no `sqlx::Encode` for
    // Postgres' `INTERVAL` type in this workspace's dependency set, and
    // there is no user input anywhere near this string -- `num_minutes()`
    // reads a `const` from `common::trust_timestamp`, so this stays exactly
    // as safe as a hand-written literal while never drifting from the
    // threshold `common::trust_timestamp::is_plausible_actual_timestamp`
    // itself uses.
    //
    // `UPPER(crs)` is served by the expression index
    // `trust_event_backlog_upper_crs_time (UPPER(crs), planned_timestamp)`
    // (20260926182000); keep the predicate's shape in step with it, or this
    // falls back to a sequential scan of the whole backlog.
    let query = format!(
        "SELECT train_id, service_date FROM trust_event_backlog \
         WHERE UPPER(crs) = UPPER($1) AND planned_timestamp BETWEEN $2 AND $3 \
         AND event_type = 'DEPARTURE' \
         AND (actual_timestamp IS NULL \
              OR actual_timestamp <= received_at + INTERVAL '{} minutes') \
         ORDER BY ABS(EXTRACT(EPOCH FROM (planned_timestamp - $4))) LIMIT 1",
        common::trust_timestamp::MAX_TIMESTAMP_SKEW_AHEAD_OF_RECEIPT.num_minutes()
    );
    let row: Option<(String, NaiveDate)> = sqlx::query_as(&query)
        .bind(pin_origin_crs)
        .bind(window_start)
        .bind(window_end)
        .bind(pin_scheduled_departure)
        .fetch_optional(pool)
        .await?;

    let Some((train_id, service_date)) = row else {
        return Ok(None);
    };

    // Look for an Activation row for the SAME train_id, unscoped by CRS (an
    // Activation carries no location at all) -- the only row type in this
    // table that ever carries a train_uid.
    //
    // **Scoped to the matched row's own date or the day before it
    // (2026-09-27).** TRUST recycles `train_id`s monthly, so an unscoped
    // lookup would attach the wrong train's uid as soon as retention grew
    // past a month (db-review part 2). Not scoped to the exact date, though:
    // a Movement processed with no parked Activation (a consumer restart
    // between the two) is dated by its own calendar date, which after
    // midnight is one day later than its Activation's origin date. Both
    // dates are within one day of each other and a `train_id` never repeats
    // within a day, so this window cannot pick up another train. The exact
    // date is preferred when both exist.
    //
    // **`ORDER BY ... received_at DESC`, not a bare `LIMIT 1` (Low finding,
    // 2026-09-25 review).** A re-activation of the SAME `train_id` (a
    // corrected/duplicate Activation resend) means more than one row can
    // match. `received_at` (this consumer's own reliable wall-clock receipt
    // time, never subject to the TRUST timestamp corruption
    // `common::trust_timestamp` documents) descending picks the MOST RECENT
    // Activation deterministically, which is also the more likely to be a
    // correction than an earlier, possibly-superseded one.
    let activation: Option<(String, NaiveDate)> = sqlx::query_as(
        "SELECT train_uid, service_date FROM trust_event_backlog \
         WHERE train_id = $1 AND service_date BETWEEN $2::date - 1 AND $2 \
         AND msg_type = '0001' AND train_uid IS NOT NULL \
         ORDER BY service_date = $2 DESC, received_at DESC LIMIT 1",
    )
    .bind(&train_id)
    .bind(service_date)
    .fetch_optional(pool)
    .await?;
    let (train_uid, identity_date) = match activation {
        Some((uid, activation_date)) => (Some(uid), activation_date),
        None => (None, service_date),
    };
    Ok(Some(BacklogCandidate {
        train_id,
        service_date,
        train_uid,
        identity_date,
    }))
}

/// What [`find_backlog_match`] found for one pin.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BacklogCandidate {
    /// TRUST's own daily identifier of the matched DEPARTURE row.
    train_id: String,
    /// The matched DEPARTURE row's OWN `service_date` -- the train's origin
    /// date, which for a pin after midnight on a train that started before
    /// it is the day BEFORE the pin's own `service_date`.
    service_date: NaiveDate,
    /// CIF's identifier, when an Activation for `train_id` is in the
    /// backlog.
    train_uid: Option<String>,
    /// The date the train's identity is keyed on: the Activation's own
    /// `service_date` when there is one (that row states the
    /// `(train_uid, date)` pair directly, and is the CIF schedule's date by
    /// construction since 97ccd3ea), otherwise `service_date`. The two only
    /// differ for a train whose Movements were dated without a parked
    /// Activation -- see `find_backlog_match`'s Activation lookup.
    identity_date: NaiveDate,
}

/// The already-known CIF identity of one subscription, or `None` when it
/// has none yet.
///
/// A subscription's resolved identity does not live on
/// `train_subscriptions` itself (Task 22 dropped that column) -- it lives
/// exclusively on the shared `trains` row reached through
/// `train_subscriptions.trains_id`, which is `NULL` for any pin no
/// schedule match, NR-primary creation or earlier backlog replay has
/// bound yet. `trains.train_uid` is itself `NOT NULL` (see
/// `20260906100000_trains.sql`), so the only reason this returns `None` is
/// a subscription with no `trains_id` at all: the honest "this pin has no
/// identity of its own yet", which is exactly the case the contradiction
/// filter below must leave alone.
async fn known_train_uid_for_subscription(
    pool: &PgPool,
    tracked_train_id: i64,
) -> anyhow::Result<Option<String>> {
    let train_uid: Option<String> = sqlx::query_scalar(
        "SELECT tr.train_uid FROM train_subscriptions ts \
         JOIN trains tr ON tr.id = ts.trains_id \
         WHERE ts.id = $1",
    )
    .bind(tracked_train_id)
    .fetch_optional(pool)
    .await?;
    Ok(train_uid)
}

/// Is a CRS+time backlog candidate PROVABLY not the train a subscription
/// is already known to be tracking?
///
/// The exact same shape, and the exact same reasoning, as the
/// contradiction filter `trust-consumer::process::process_message` applies
/// in front of `matching::resolve_origin_departure` (commit
/// `4340c97f`, "a parked Activation's `train_uid` vetoes a wrong CRS+time
/// claim"): only a comparison between two identities that are BOTH already
/// known can ever prove a mismatch. When either side is unknown the
/// underlying CRS+time heuristic is all there is, and it must run exactly
/// as it did before -- this filter can only ever remove a candidate TRUST's
/// own data has already contradicted, never change an outcome that was
/// previously correct.
///
/// Case-insensitive on both sides, same posture as every other
/// `train_uid` comparison in this codebase (and as the trust-consumer
/// filter this mirrors): casing alone is not a contradiction.
fn is_provable_identity_contradiction(
    subscription_train_uid: Option<&str>,
    candidate_train_uid: Option<&str>,
) -> bool {
    match (subscription_train_uid, candidate_train_uid) {
        (Some(known), Some(candidate)) => !known.eq_ignore_ascii_case(candidate),
        _ => false,
    }
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
    // **`ORDER BY received_at DESC`, not a bare `LIMIT 1` (Low finding,
    // 2026-09-25 review) -- same reasoning as `find_backlog_match`'s own
    // Activation lookup just above.** A re-activation of the same
    // `(train_uid, service_date)` pair within retention (TRUST resending a
    // corrected/duplicate Activation) means more than one row can match
    // here; an unordered `LIMIT 1` would pick a Postgres-planner-dependent
    // row rather than deterministically the most recent one.
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT train_id FROM trust_event_backlog \
         WHERE train_uid = $1 AND service_date = $2 AND msg_type = '0001' \
         ORDER BY received_at DESC LIMIT 1",
    )
    .bind(train_uid)
    .bind(service_date)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(train_id,)| train_id))
}

/// Decision 3 step 3: every backlog row for `train_id` on any of
/// `service_dates`, in `received_at` order -- the entire observed history
/// for this train.
///
/// Usually one date. [`attempt_backlog_match`] passes two when the matched
/// DEPARTURE row and the train's Activation were filed under different
/// dates (a Movement processed with no parked Activation is dated by its
/// own calendar date -- see `find_backlog_match`), and
/// [`attempt_backlog_match_by_uid`] passes its identity date and the day
/// after it (unless that day holds another run's Activation), so the history is not cut in half at midnight. A `train_id` never repeats within a day, so
/// two adjacent dates cannot mix two trains.
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
    service_dates: &[NaiveDate],
) -> anyhow::Result<Vec<BacklogRow>> {
    let rows = sqlx::query_as::<_, BacklogRow>(
        "SELECT train_id, service_date, msg_type, event_type, crs, planned_timestamp, \
                actual_timestamp, variation_status \
         FROM trust_event_backlog \
         WHERE train_id = $1 AND service_date = ANY($2) \
         ORDER BY received_at",
    )
    .bind(train_id)
    .bind(service_dates)
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
///
/// Each row's own `service_date` is `dedup_key`'s date component (see the
/// call below, and `trust_schema::dedup::dedup_key`'s own doc comment on why
/// that component exists at all: TRUST recycles `train_id`s monthly).
#[expect(
    clippy::similar_names,
    clippy::too_many_lines,
    reason = "the similar names are distinct domain terms; long but linear; splitting it would scatter its shared state across helpers"
)]
async fn replay_backlog_history(
    pool: &PgPool,
    tracked_train_id: i64,
    train_uid: Option<&str>,
    destination_crs: Option<&str>,
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
                    timetable_variation: None,
                };
                let mut derived = journey::apply_movement(
                    &previous,
                    &movement,
                    row.crs.as_deref(),
                    destination_crs,
                );
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
                    // Finding #4 (2026-09-25 review): same guard as the
                    // live consumer path (`trust_event_backlog.rs`) --
                    // see `common::trust_timestamp::plausible_delay_minutes`'s
                    // own doc comment for why `None` (keep `apply_movement`'s
                    // coarser estimate) rather than clamping to a
                    // fabricated-but-bounded number.
                    if let Some(delay) = common::trust_timestamp::plausible_delay_minutes(a, p) {
                        derived.delay_minutes = Some(delay);
                    }
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
            // "0005" (Reinstatement, confirmed by the H4 fix of the
            // 2026-09-26 review): un-sticks a "cancelled" journey back to
            // "en_route" during replay too, same as the live paths --
            // without this arm, a resolved subscription that only backlog-
            // matches AFTER a cancel -> reinstate sequence would replay the
            // Cancellation but silently skip the Reinstatement (falling into
            // `_ => continue` below) and land back on a stale "cancelled".
            "0005" => (
                journey::apply_reinstatement(&previous),
                None,
                None,
                row.actual_timestamp, // reinstatement_timestamp, stored like canx_timestamp
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
        // `row.service_date` as the key's date component, rather than the
        // "rail day this message was processed on" every LIVE consumer passes
        // (see `trust_schema::dedup::dedup_key`). This function replays rows
        // that were stored days ago, so there is no live processing day to
        // speak of, and the stored row's own `service_date` is both available
        // and the more meaningful value. That makes these keys differ from a
        // live consumer's for the same real event -- exactly the divergence
        // the paragraph above already documents and accepts for
        // `loc_stanox`, for the same reason: `ON CONFLICT (tracked_train_id,
        // dedup_key) DO NOTHING` only ever needs to make THIS replay
        // idempotent against itself.
        //
        // A Cancellation or Reinstatement has no planned time; its own
        // event time (`actual_timestamp`) fills the key's timestamp slot
        // instead, so cancel -> reinstate -> cancel -> reinstate on one day
        // replays as four history rows rather than two (H4 residual,
        // 2026-10-01).
        let key_timestamp = match row.msg_type.as_str() {
            "0002" | "0005" => actual,
            _ => planned,
        };
        let dedup = trust_schema::dedup::dedup_key(
            &row.train_id,
            &row.msg_type,
            event_type.as_deref(),
            None,
            key_timestamp
                .map(|t| t.timestamp_millis().to_string())
                .as_deref(),
            row.service_date,
        );

        let (resolved_train_uid, resolved_train_id) = if resolution_claimed {
            (None, None)
        } else {
            resolution_claimed = true;
            (train_uid.map(str::to_string), Some(row.train_id.clone()))
        };

        let event = common::TrainMovementEventMessage {
            tracked_train_id,
            resolved_train_uid,
            resolved_train_id,
            identity_date: None,
            dedup_key: dedup,
            msg_type: row.msg_type.clone(),
            event_type,
            loc_stanox: None, // never persisted by trust_event_backlog -- see the dedup_key note above
            loc_crs: row.crs.clone(),
            planned_timestamp: planned,
            gbtt_timestamp: None,
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
/// `service_date`) -- exactly Decision 3 step 8's "no regression, no new
/// failure mode" posture: a pin left `Ok(false)` here is exactly as it
/// would have been without this feature at all. Since the contradiction
/// filter below, `Ok(false)` also covers "the CRS+time candidate is
/// provably a different train than this subscription already knows itself
/// to be" -- same "left exactly as it was" outcome, for a reason that is a
/// correctness guarantee rather than an absence of data.
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
) -> anyhow::Result<bool> {
    // No `service_date` parameter: the pin's own date is deliberately not
    // consulted anywhere below. The match is on absolute time, and every
    // later step uses the matched row's own dates -- see
    // `find_backlog_match`'s doc comment (Repeater Signal M7 residual).
    let Some(candidate) = find_backlog_match(pool, pin_origin_crs, pin_scheduled_departure).await?
    else {
        return Ok(false);
    };
    let BacklogCandidate {
        train_id,
        service_date,
        train_uid,
        identity_date,
    } = candidate;

    // CONTRADICTION FILTER -- the same fix, for the same class of bug, as
    // commit `4340c97f`'s ("a parked Activation's train_uid vetoes a wrong
    // CRS+time claim") guard in front of
    // `trust-consumer::matching::resolve_origin_departure`. That fix closed
    // this shape on the LIVE matching path; this closes it on the BACKLOG
    // replay path, which had the identical gap and, if anything, a worse
    // blast radius.
    //
    // `find_backlog_match` above is a pure CRS + `planned_timestamp`-window
    // lookup with no notion of train identity: it answers "which train_id
    // left this CRS near this time", and at a busy origin inside a
    // +/-`SCHEDULED_DEPARTURE_TOLERANCE` window that can still contain more
    // than one train (the M9 fix, 2026-09-26 review, tightened this from a
    // 20-minute window to 5 -- see this module's own top-level doc comment
    // -- which shrinks but does not eliminate the risk this filter guards
    // against). It then opportunistically discovers that candidate's
    // REAL `train_uid` from TRUST's own Activation (`0001`) row -- so by
    // this point the candidate's identity is frequently already known for
    // certain. Until this filter, that known identity was used only to look
    // up a destination and to drive the Step A dual-write below; it was
    // never compared against the identity the subscription ALREADY had.
    //
    // What that cost, concretely. A pin that `attempt_schedule_match`
    // resolved a moment earlier (`routes::train::post_track` and
    // `routes::journeys::post_journey` both call that first, then this,
    // unconditionally) already points at a `trains` row whose `train_uid`
    // came from a real CIF schedule match. If this CRS+time lookup then
    // landed on a DIFFERENT train that happened to leave the same station
    // inside the same window, `replay_backlog_history` below replayed that
    // other train's entire movement history onto the user's subscription,
    // and the Step A dual-write at the end of this function then
    // `UPDATE train_subscriptions SET trains_id = ...` -- repointing the
    // subscription away from its correctly-matched train onto the wrong
    // one. There is no unwind path for either write. That is exactly the
    // mis-attribution shape confirmed in production on 2026-09-25 (a user's
    // Euston -> Birmingham New Street service showing an Avanti
    // Euston -> Liverpool service's movements, and reading "En route"
    // before it had left), and the same shape as the earlier confirmed
    // `trains_id=713729`/`C17876` incident this module's own ARRIVAL-filter
    // regression test documents.
    //
    // So: when the subscription's own identity is already known AND this
    // candidate's identity is already known AND they differ, the candidate
    // is provably not this train. Reject before anything is written --
    // before `fetch_backlog_history`, before the replay, before the
    // repoint. `Ok(false)` is the honest outcome and exactly what every
    // other "nothing in the backlog for this pin" path already returns: the
    // pin is left precisely as it was, and `run_backlog_match_sweep` will
    // not re-select it anyway (its candidate query already excludes any row
    // with a `trains_id`).
    //
    // Named residual, deliberately left as-is rather than tightened: when
    // this candidate has NO Activation row in the retention window its
    // `train_uid` is `None`, nothing is provable, and the pre-existing
    // behavior is preserved untouched -- same "either side unknown means
    // the heuristic runs exactly as before" posture the trust-consumer fix
    // took, and for the same reason. Tightening that case into "an
    // already-identified pin may only match a candidate TRUST confirms is
    // the same train" would also break the legitimate case it is
    // indistinguishable from: a correctly schedule-matched pin whose real
    // train's Movement rows are retained but whose Activation has already
    // aged out. See this fix's report for the follow-up that would close it
    // properly (routing an already-identified subscription through
    // `attempt_backlog_match_by_uid`, the identity-first counterpart, instead
    // of this CRS+time discovery function at all).
    //
    // **M9 finding, 2026-09-26 review, investigated and confirmed NOT a
    // gap**: a plain `pending` pin (no `trains_id` at all yet) is the OTHER
    // "either side unknown" case, distinct from the residual just above (a
    // known-identity pin against a candidate with no Activation).
    // `known_train_uid_for_subscription`'s own `JOIN train_subscriptions ...
    // trains` finds no row at all when `trains_id IS NULL`, so it already
    // returns `None` for exactly this pin -- and `is_provable_identity_contradiction`'s
    // own `(None, _) => false` arm already treats that `None` as "nothing to
    // contradict," letting the CRS+time heuristic run unchanged. This is
    // the exact same treatment `trust-consumer::process`'s own live
    // contradiction filter gives a pin with no `train_uid` of its own
    // (`PendingPin::train_uid: None` falls into that filter's own `_ =>
    // true` arm) -- symmetric by construction, not a gap this filter leaves
    // open. See `a_pin_with_no_identity_of_its_own_is_unaffected_by_the_contradiction_filter`
    // below for the regression coverage.
    let subscription_train_uid = known_train_uid_for_subscription(pool, tracked_train_id).await?;
    if is_provable_identity_contradiction(subscription_train_uid.as_deref(), train_uid.as_deref()) {
        tracing::warn!(
            tracked_train_id,
            subscription_train_uid = ?subscription_train_uid,
            candidate_train_uid = ?train_uid,
            candidate_train_id = %train_id,
            "backlog CRS+time candidate names a different train_uid than this subscription is \
             already known to be tracking; rejecting rather than replaying and repointing it"
        );
        return Ok(false);
    }

    let history_dates = if identity_date == service_date {
        vec![service_date]
    } else {
        vec![identity_date, service_date]
    };
    let history = fetch_backlog_history(pool, &train_id, &history_dates).await?;
    if history.is_empty() {
        return Ok(false);
    }

    // Read-only precheck, same posture as `shared_train_enrichment_state`:
    // only possible when this backlog carried an Activation for this
    // train_id (`train_uid` is `Some`) -- without one there's no natural
    // key to look a `trains` row up by, so `destination_crs` stays `None`
    // (the honest "unknown", not a bug) and every replayed ARRIVAL in this
    // history falls back to the existing "may have finished" inference.
    let destination_crs = match &train_uid {
        Some(train_uid) => {
            crate::data::trains::destination_crs_for_train(pool, train_uid, identity_date).await?
        }
        None => None,
    };

    // Step A dual-write (docs/superpowers/specs/2026-09-06-shared-train-identity-design.md
    // §2 Step A): only possible when this backlog carried an Activation for
    // this train_id (train_uid is Some) -- a Movement/Cancellation-only
    // backfill has no natural key to create a trains row against, matching
    // Step B's own accepted gap.
    //
    // Keyed on `identity_date` (the train's own origin date), NOT the pin's
    // `service_date`. For a pin after midnight on a train that started
    // before it, the pin's date is the NEXT day, and `(train_uid, next day)`
    // is a different real train: the same service's run a day later.
    //
    // **Done BEFORE the replay, not after it (2026-09-27).** The replay's
    // first event goes through `upsert_train_event` ->
    // `flip_legacy_resolution`, which, for a subscription with no
    // `trains_id` yet, creates `trains(train_uid, <subscription's
    // service_date>)` (the replay sends no `identity_date`) and writes every
    // replayed movement there. For the pin
    // above that is the NEXT day's run of the same service, so its
    // movements would land on another train's shared row, visible to that
    // train's subscribers, before this write repointed the subscription.
    // Binding the subscription to the right row first makes
    // `flip_legacy_resolution` take its "already has a trains_id" arm and
    // write there instead. Nothing is lost by the reorder: the contradiction
    // filter above has already run, and the replay alone already bound
    // `trains_id` via `flip_legacy_resolution` before this change, so a
    // failure part-way through leaves the same kind of state as before.
    if let Some(train_uid) = &train_uid {
        let trains_id =
            crate::data::trains::find_or_create_train(pool, train_uid, identity_date).await?;
        crate::data::trains::mark_train_resolved(pool, trains_id, &train_id).await?;
        if !crate::data::trains::bind_subscription_unless_other_train(
            pool,
            tracked_train_id,
            trains_id,
        )
        .await?
        {
            tracing::warn!(
                tracked_train_id,
                trains_id,
                "subscription was bound to a different train (or deleted) during this backlog \
                 match; not repointing it or replaying this history (DB2-5)"
            );
            return Ok(false);
        }
    }

    replay_backlog_history(
        pool,
        tracked_train_id,
        train_uid.as_deref(),
        destination_crs.as_deref(),
        history,
    )
    .await?;

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
/// only succeeds within its own `SCHEDULED_DEPARTURE_TOLERANCE` (5 minutes,
/// comparing booked time against booked time -- or the wider
/// `common::MATCH_TOLERANCE`, 20 minutes, on the rarer fallback path where a
/// Movement carries no `planned_timestamp` at all) of the pin's scheduled
/// departure -- so a train that departs more than that has no path left to
/// resolve at all, despite
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
        match attempt_backlog_match(pool, row.id, pin_origin_crs, pin_scheduled_departure).await {
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
/// [`attempt_backlog_match`] -- or that the subscription is bound to a
/// different train, which this never repoints (DB2-5; logged at warn).
#[expect(
    clippy::expect_used,
    clippy::similar_names,
    reason = "the invariant is established just above; the expect message names it; the similar names are distinct domain terms"
)]
pub async fn attempt_backlog_match_by_uid(
    pool: &PgPool,
    tracked_train_id: i64,
    train_uid: &str,
    service_date: NaiveDate,
) -> anyhow::Result<Option<BacklogReplayOutcome>> {
    let Some(train_id) = find_train_id_by_uid(pool, train_uid, service_date).await? else {
        return Ok(None);
    };

    // `service_date` here is the train's identity date (its Activation's
    // `service_date`, the origin date since 97ccd3ea). Its Movements are
    // usually filed under the same date, but one processed with no parked
    // Activation (a trust-backlog-consumer restart between the two) is
    // dated by its own calendar date, which after midnight is the NEXT day.
    // So read both, exactly as `attempt_backlog_match` reads
    // `{identity_date, service_date}` from the other direction (M7 leftover,
    // 2026-09-27). TRUST's `train_id` carries the origin day of month, so
    // real D+1 rows with this `train_id` are this train's. Belt and braces:
    // if D+1 holds an Activation of its own for this `train_id`, those rows
    // are another run's and D+1 is left out.
    let next_day = service_date + Duration::days(1);
    let next_day_is_another_run: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM trust_event_backlog \
         WHERE train_id = $1 AND service_date = $2 AND msg_type = '0001')",
    )
    .bind(&train_id)
    .bind(next_day)
    .fetch_one(pool)
    .await?;
    let history_dates = if next_day_is_another_run {
        vec![service_date]
    } else {
        vec![service_date, next_day]
    };
    let history = fetch_backlog_history(pool, &train_id, &history_dates).await?;
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

    // Same read-only precheck as `attempt_backlog_match` -- `train_uid` is
    // always known here (it's this function's own input), so this only
    // ever reports "unknown" when no schedule has ever matched this
    // identity, never for a missing-Activation reason.
    let destination_crs =
        crate::data::trains::destination_crs_for_train(pool, train_uid, service_date).await?;

    // Step A dual-write, same as `attempt_backlog_match` above -- but
    // unconditional here, because this path's `train_uid` is an input, not
    // something that may or may not have been discovered.
    //
    // Guarded and done BEFORE the replay, exactly as in
    // `attempt_backlog_match` (DB2-5, applied to this path at integration,
    // 2026-09-27): a bare `UPDATE ... SET trains_id` here would repoint a
    // subscription that is bound to a DIFFERENT train (a concurrent schedule
    // match, or any other writer, between the caller creating it and this
    // call), after the replay had already written this train's history onto
    // that other train's shared row. `bind_subscription_unless_other_train`
    // only binds an unbound subscription, one already on this row, or one on
    // a same-uid row; otherwise nothing is replayed and this returns
    // `Ok(None)` (the caller then leaves the train to live trust-consumer
    // resolution, as for an empty backlog). Binding first also makes the
    // replay's `flip_legacy_resolution` write onto this row.
    let trains_id =
        crate::data::trains::find_or_create_train(pool, train_uid, service_date).await?;
    crate::data::trains::mark_train_resolved(pool, trains_id, &train_id).await?;
    if !crate::data::trains::bind_subscription_unless_other_train(pool, tracked_train_id, trains_id)
        .await?
    {
        tracing::warn!(
            tracked_train_id,
            trains_id,
            train_uid,
            "subscription is bound to a different train (or was deleted); not repointing it or \
             replaying this train's history onto it (DB2-5, uid path)"
        );
        return Ok(None);
    }

    let replayed_rows = history.len();
    replay_backlog_history(
        pool,
        tracked_train_id,
        Some(train_uid),
        destination_crs.as_deref(),
        history,
    )
    .await?;

    Ok(Some(BacklogReplayOutcome {
        train_id,
        replayed_rows,
        origin_departure,
    }))
}

/// Pure coverage of the contradiction predicate itself -- no database, so
/// these run in the default `cargo test --workspace` pass rather than only
/// behind `--ignored`. The DB-gated tests below prove the predicate is
/// actually WIRED into `attempt_backlog_match` (and that the writes it
/// guards really don't happen); these prove its truth table, including the
/// three "not provable, leave the heuristic alone" cases that are the whole
/// reason this is a filter and not a rewrite.
#[cfg(test)]
mod contradiction_filter_tests {
    use super::is_provable_identity_contradiction;

    #[test]
    fn two_different_known_train_uids_are_a_provable_contradiction() {
        // The real 2026-09-25 pair: the user's London Northwestern service
        // versus the Avanti service whose origin departure fell inside the
        // same tolerance window.
        assert!(is_provable_identity_contradiction(
            Some("Y80926"),
            Some("W34058")
        ));
    }

    #[test]
    fn the_same_known_train_uid_is_never_a_contradiction() {
        assert!(!is_provable_identity_contradiction(
            Some("Y80926"),
            Some("Y80926")
        ));
    }

    #[test]
    fn casing_alone_is_never_a_contradiction() {
        assert!(!is_provable_identity_contradiction(
            Some("y80926"),
            Some("Y80926")
        ));
        assert!(!is_provable_identity_contradiction(
            Some("Y80926"),
            Some("y80926")
        ));
    }

    /// The three unknown-side cases, pinned so a future change to any of
    /// them is a deliberate one: nothing is provable without both
    /// identities, and the CRS+time heuristic must behave exactly as it did
    /// before this filter existed.
    #[test]
    fn an_unknown_identity_on_either_side_is_never_a_contradiction() {
        assert!(
            !is_provable_identity_contradiction(None, Some("W34058")),
            "a pin with no identity of its own has only the heuristic, same as always"
        );
        assert!(
            !is_provable_identity_contradiction(Some("Y80926"), None),
            "a candidate with no Activation in the retention window cannot be contradicted"
        );
        assert!(!is_provable_identity_contradiction(None, None));
    }
}

#[cfg(test)]
#[expect(
    clippy::similar_names,
    clippy::too_many_lines,
    reason = "test code: paired test values share names; scenario tests read top to bottom"
)]
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
                a_full_activation_plus_movement_backlog_resolves_the_pin_to_resolved -- --ignored --test-threads=1`"]
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

        let service_date: NaiveDate = "2026-09-05".parse().unwrap();
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

        let matched = attempt_backlog_match(&pool, tracked_train_id, "EUS", scheduled)
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
                no_matching_backlog_rows_leaves_the_pin_untouched -- --ignored --test-threads=1`"]
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

        let service_date: NaiveDate = "2026-09-05".parse().unwrap();
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

        let matched = attempt_backlog_match(&pool, tracked_train_id, "ZZZ-NOWHERE", scheduled)
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

    /// Layer 1's plausibility guard, exercised against the real
    /// `find_backlog_match` query: a backlog row that would otherwise match
    /// cleanly on CRS+time is rejected when its own `actual_timestamp` is
    /// implausibly ahead of its `received_at` -- exactly the shape of the
    /// still-unconfirmed TRUST timestamp corruption documented in
    /// `common::trust_timestamp`. The pin must be left `'pending'`, not
    /// bound to this row's `train_id`, so `run_backlog_match_sweep` can
    /// retry it later.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                an_implausible_actual_timestamp_is_rejected_and_leaves_the_pin_pending -- --ignored --test-threads=1`"]
    async fn an_implausible_actual_timestamp_is_rejected_and_leaves_the_pin_pending() {
        let pool = connect().await;
        let user_id = "TEST-BACKLOG-IMPLAUSIBLE-USER";
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind("backlog-implausible@example.com")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");

        let service_date: NaiveDate = "2026-09-05".parse().unwrap();
        let scheduled: DateTime<Utc> = "2026-09-05T18:15:00Z".parse().unwrap();
        // `planned_timestamp` is inside the pin's SCHEDULED_DEPARTURE_TOLERANCE
        // window -- `find_backlog_match`'s own SQL WHERE clause finds this row -- but
        // `actual_timestamp` is 60 minutes AHEAD of `received_at`, exactly
        // the shape of the still-unconfirmed corruption this guard exists
        // to catch. `received_at` is set explicitly (rather than left to
        // its `DEFAULT NOW()`) so the fixture is deterministic regardless
        // of when this test runs.
        let received_at: DateTime<Utc> = "2026-09-05T17:16:00Z".parse().unwrap();
        sqlx::query(
            "INSERT INTO trust_event_backlog \
                (crs, train_uid, train_id, service_date, msg_type, event_type, \
                 planned_timestamp, actual_timestamp, variation_status, dedup_key, received_at) \
             VALUES ($1, NULL, $2, $3, '0003', 'DEPARTURE', $4, $4, 'ON TIME', $5, $6)",
        )
        .bind("EUS")
        .bind("TEST-BACKLOG-IMPLAUSIBLE-TRAIN-ID")
        .bind(service_date)
        .bind(scheduled)
        .bind("test-backlog-implausible-dedup-movement")
        .bind(received_at)
        .execute(&pool)
        .await
        .expect("seed an implausible backlog row");

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

        let matched = attempt_backlog_match(&pool, tracked_train_id, "EUS", scheduled)
            .await
            .expect("attempt_backlog_match");
        assert!(
            !matched,
            "an implausible actual_timestamp must not resolve the pin, even though CRS+time \
             alone would otherwise match cleanly"
        );

        let (resolution_status,): (String,) =
            sqlx::query_as("SELECT resolution_status FROM train_subscriptions WHERE id = $1")
                .bind(tracked_train_id)
                .fetch_one(&pool)
                .await
                .expect("read back tracked_trains");
        assert_eq!(
            resolution_status, "pending",
            "the pin must be left untouched for the next backlog-match sweep to retry"
        );

        sqlx::query("DELETE FROM train_subscriptions WHERE id = $1")
            .bind(tracked_train_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query(
            "DELETE FROM trust_event_backlog WHERE train_id = 'TEST-BACKLOG-IMPLAUSIBLE-TRAIN-ID'",
        )
        .execute(&pool)
        .await
        .ok();
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .ok();
    }

    /// Finding #4's own regression test: an implausible row and a genuinely
    /// plausible one both fall inside the same CRS+time window. Before this
    /// fix, `find_backlog_match`'s `ORDER BY planned_timestamp LIMIT 1`
    /// selected whichever row sorted first REGARDLESS of plausibility,
    /// rejected it in Rust, and returned `Ok(None)` -- so a later sweep
    /// would deterministically re-select and re-reject that exact same row
    /// forever, never reaching the plausible second candidate sitting right
    /// next to it. Excluding the implausible row directly in the SQL means
    /// the plausible candidate is found and resolves the pin on the very
    /// first attempt, not merely "eventually, once the implausible row ages
    /// out of retention."
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                a_plausible_candidate_is_found_even_when_a_more_favorably_sorted_implausible_row_exists \
                -- --ignored --test-threads=1`"]
    async fn a_plausible_candidate_is_found_even_when_a_more_favorably_sorted_implausible_row_exists()
     {
        let pool = connect().await;
        let user_id = "TEST-BACKLOG-FALLTHROUGH-USER";
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind("backlog-fallthrough@example.com")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");

        let service_date: NaiveDate = "2026-09-05".parse().unwrap();
        let scheduled: DateTime<Utc> = "2026-09-05T18:15:00Z".parse().unwrap();

        // Candidate 1: sorts FIRST by planned_timestamp (exactly on time),
        // but its actual_timestamp is implausibly ahead of its own
        // received_at -- must be excluded from the query entirely, not just
        // rejected after being selected.
        let received_at_implausible: DateTime<Utc> = "2026-09-05T17:16:00Z".parse().unwrap();
        // Candidate 2: sorts SECOND (4 minutes after scheduled, comfortably
        // within the M9-tightened SCHEDULED_DEPARTURE_TOLERANCE of 5), but is
        // genuinely plausible -- this is the row that must actually resolve
        // the pin.
        let plausible_planned: DateTime<Utc> = "2026-09-05T18:19:00Z".parse().unwrap();
        let received_at_plausible: DateTime<Utc> = "2026-09-05T18:20:00Z".parse().unwrap();

        sqlx::query(
            "INSERT INTO trust_event_backlog \
                (crs, train_uid, train_id, service_date, msg_type, event_type, \
                 planned_timestamp, actual_timestamp, variation_status, dedup_key, received_at) \
             VALUES \
                ($1, NULL, $2, $3, '0003', 'DEPARTURE', $4, $4, 'ON TIME', $5, $6), \
                ($1, NULL, $7, $3, '0003', 'DEPARTURE', $8, $8, 'ON TIME', $9, $10), \
                (NULL, $11, $7, $3, '0001', NULL, NULL, NULL, NULL, $12, $10)",
        )
        .bind("EUS")
        .bind("TEST-BACKLOG-FALLTHROUGH-IMPLAUSIBLE-TRAIN-ID")
        .bind(service_date)
        .bind(scheduled)
        .bind("test-backlog-fallthrough-dedup-implausible")
        .bind(received_at_implausible)
        .bind("TEST-BACKLOG-FALLTHROUGH-PLAUSIBLE-TRAIN-ID")
        .bind(plausible_planned)
        .bind("test-backlog-fallthrough-dedup-plausible")
        .bind(received_at_plausible)
        // An Activation row for the PLAUSIBLE train_id only -- so
        // find_backlog_match's own Activation lookup discovers a train_uid
        // and the Step A dual-write sets trains_id, letting this test
        // verify identity via the joined `trains` row (same pattern as
        // `a_full_activation_plus_movement_backlog_resolves_the_pin_to_resolved`
        // above). The implausible train_id deliberately has NO Activation
        // row -- it's excluded from the CRS+time query before an Activation
        // lookup would even run for it.
        .bind("TEST-DW-FALLTHROUGH-UID")
        .bind("test-backlog-fallthrough-dedup-activation")
        .execute(&pool)
        .await
        .expect("seed both backlog rows plus an Activation for the plausible one");

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

        let matched = attempt_backlog_match(&pool, tracked_train_id, "EUS", scheduled)
            .await
            .expect("attempt_backlog_match");
        assert!(
            matched,
            "the plausible second candidate must be found even though the implausible row \
             would otherwise have sorted first"
        );

        let (resolution_status,): (String,) =
            sqlx::query_as("SELECT resolution_status FROM train_subscriptions WHERE id = $1")
                .bind(tracked_train_id)
                .fetch_one(&pool)
                .await
                .expect("read back tracked_trains");
        assert_eq!(resolution_status, "resolved");

        let (trains_id, train_id): (i64, String) = sqlx::query_as(
            "SELECT tr.id, tr.train_id FROM train_subscriptions tt \
             JOIN trains tr ON tr.id = tt.trains_id \
             WHERE tt.id = $1",
        )
        .bind(tracked_train_id)
        .fetch_one(&pool)
        .await
        .expect("read back the resolved train_id");
        assert_eq!(
            train_id, "TEST-BACKLOG-FALLTHROUGH-PLAUSIBLE-TRAIN-ID",
            "must resolve to the plausible candidate, never the implausible one"
        );

        sqlx::query("DELETE FROM train_subscriptions WHERE id = $1")
            .bind(tracked_train_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query(
            "DELETE FROM trust_event_backlog WHERE train_id IN \
             ('TEST-BACKLOG-FALLTHROUGH-IMPLAUSIBLE-TRAIN-ID', 'TEST-BACKLOG-FALLTHROUGH-PLAUSIBLE-TRAIN-ID')",
        )
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

    /// Finding #2's own regression test (2026-09-25 review), modeled on
    /// `schedule_matching.rs`'s own `Y80908` busy-station fixtures: a busy
    /// station with TWO plausible DEPARTURE candidates inside the same
    /// `SCHEDULED_DEPARTURE_TOLERANCE` window, where the EARLIER-departing
    /// one is a completely different, unrelated train and the
    /// LATER-departing one is actually the closest to the pin's own
    /// `pin_scheduled_departure`.
    ///
    /// Before this fix, `find_backlog_match`'s `ORDER BY planned_timestamp
    /// LIMIT 1` always won on chronological order, not proximity -- so the
    /// earlier, unrelated train's entire movement history would have been
    /// replayed onto this pin. This asserts the CLOSER candidate is the one
    /// actually chosen, even though it sorts second by plain
    /// `planned_timestamp` order.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                a_busy_stations_closest_departure_wins_over_an_earlier_unrelated_one \
                -- --ignored --test-threads=1`"]
    async fn a_busy_stations_closest_departure_wins_over_an_earlier_unrelated_one() {
        let pool = connect().await;
        let user_id = "TEST-BACKLOG-CLOSEST-USER";
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind("backlog-closest@example.com")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");

        let service_date: NaiveDate = "2026-09-25".parse().unwrap();
        // The pin's own scheduled departure.
        let scheduled: DateTime<Utc> = "2026-09-25T18:15:00Z".parse().unwrap();

        // EARLIER candidate: sorts FIRST by plain `planned_timestamp`
        // ascending (4 minutes before the pin's own scheduled departure,
        // still inside the M9-tightened +/-5 minute
        // SCHEDULED_DEPARTURE_TOLERANCE window), but it's a completely
        // different, unrelated train -- exactly the "busy station, wrong
        // train picked because it happened to depart first" shape the
        // finding describes.
        let earlier_planned: DateTime<Utc> = "2026-09-25T18:11:00Z".parse().unwrap();
        // CLOSER candidate: only 1 minute after the pin's own scheduled
        // departure -- the train this pin actually belongs to -- but sorts
        // SECOND by plain ascending `planned_timestamp`.
        let closer_planned: DateTime<Utc> = "2026-09-25T18:16:00Z".parse().unwrap();

        sqlx::query(
            "INSERT INTO trust_event_backlog \
                (crs, train_uid, train_id, service_date, msg_type, event_type, \
                 planned_timestamp, actual_timestamp, variation_status, dedup_key) \
             VALUES \
                ($1, NULL, $2, $3, '0003', 'DEPARTURE', $4, $4, 'ON TIME', $5), \
                ($1, NULL, $6, $3, '0003', 'DEPARTURE', $7, $7, 'ON TIME', $8), \
                (NULL, $9, $6, $3, '0001', NULL, NULL, NULL, NULL, $10)",
        )
        .bind("EUS")
        .bind("TEST-BACKLOG-CLOSEST-EARLIER-TRAIN-ID")
        .bind(service_date)
        .bind(earlier_planned)
        .bind("test-backlog-closest-dedup-earlier")
        .bind("TEST-BACKLOG-CLOSEST-CLOSER-TRAIN-ID")
        .bind(closer_planned)
        .bind("test-backlog-closest-dedup-closer")
        // An Activation row for the CLOSER train_id only, so this test can
        // confirm identity via the joined `trains` row, same pattern as
        // the fallthrough test above.
        .bind("TEST-DW-CLOSEST-UID")
        .bind("test-backlog-closest-dedup-activation")
        .execute(&pool)
        .await
        .expect("seed both backlog rows plus an Activation for the closer one");

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

        let matched = attempt_backlog_match(&pool, tracked_train_id, "EUS", scheduled)
            .await
            .expect("attempt_backlog_match");
        assert!(matched);

        let (train_id,): (String,) = sqlx::query_as(
            "SELECT tr.train_id FROM train_subscriptions tt \
             JOIN trains tr ON tr.id = tt.trains_id \
             WHERE tt.id = $1",
        )
        .bind(tracked_train_id)
        .fetch_one(&pool)
        .await
        .expect("read back the resolved train_id");
        assert_eq!(
            train_id, "TEST-BACKLOG-CLOSEST-CLOSER-TRAIN-ID",
            "must resolve to the candidate closest to the pin's own scheduled departure, never \
             whichever candidate merely departs earliest in the window"
        );

        sqlx::query("DELETE FROM train_subscriptions WHERE id = $1")
            .bind(tracked_train_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query(
            "DELETE FROM trust_event_backlog WHERE train_id IN \
             ('TEST-BACKLOG-CLOSEST-EARLIER-TRAIN-ID', 'TEST-BACKLOG-CLOSEST-CLOSER-TRAIN-ID')",
        )
        .execute(&pool)
        .await
        .ok();
        sqlx::query(
            "DELETE FROM trains WHERE train_uid = 'TEST-DW-CLOSEST-UID' AND service_date = $1",
        )
        .bind(service_date)
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

        let service_date: NaiveDate = "2026-09-06".parse().unwrap();
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

        let matched = attempt_backlog_match(&pool, tracked_train_id, "EUS", scheduled)
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
        let service_date: NaiveDate = "2026-09-06".parse().unwrap();
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
    /// `resolve_origin_departure`'s `SCHEDULED_DEPARTURE_TOLERANCE` window
    /// and TRUST's own backlog only fills in afterwards. That first attempt must fail
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

        // Deliberately NOT a fixed wall-clock time (e.g. "today at 18:15") --
        // `find_backlog_match`'s plausibility guard below rejects any
        // `actual_timestamp` more than
        // `common::trust_timestamp::MAX_TIMESTAMP_SKEW_AHEAD_OF_RECEIPT`
        // (10 minutes) ahead of `received_at`, which the backlog row below
        // defaults to `NOW()` at INSERT time. A fixed future-or-past-
        // depending-on-when-CI-runs wall-clock time made this test's outcome
        // depend on what time of day it happened to run, and it did in fact
        // fail in CI outside a ~10-minute-wide window around that hour --
        // see the incident this comment now documents. Anchoring to "30
        // minutes before whenever this test actually runs" keeps
        // `actual_timestamp` safely behind `received_at` regardless of the
        // clock, the same way every fixed-date sibling test in this file
        // (e.g. "2026-09-05T18:15:00Z") is safely in the past by construction.
        let scheduled: DateTime<Utc> = Utc::now() - Duration::minutes(30);
        let service_date: NaiveDate = scheduled.date_naive();

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
        let first_attempt = attempt_backlog_match(&pool, tracked_train_id, "EUS", scheduled)
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

    /// Regression test for the confirmed live-production bug: `find_backlog_match`'s
    /// own SQL had no `event_type` filter, unlike the live-matching path
    /// (`trust-consumer::process.rs`'s `if movement.event_type != "DEPARTURE"
    /// { return Vec::new(); }` guard). A pin's scheduled departure could
    /// therefore match an UNRELATED train's ARRIVAL event at the same CRS,
    /// inside the same `SCHEDULED_DEPARTURE_TOLERANCE` window -- a routine occurrence at
    /// any turnback/interchange station. Once matched, the wrong train's
    /// entire movement history gets replayed onto the pin's already-correct
    /// `trains_id` (confirmed real-world instance: `trains_id=713729`,
    /// correctly identified as `train_uid=C17876`, received 29 movement
    /// events belonging to `C18017`, the opposite-direction service, this
    /// exact way).
    ///
    /// Here only the unrelated train's ARRIVAL row exists in the window --
    /// no DEPARTURE anywhere -- so the honest outcome is `Ok(false)`/pin
    /// left `'pending'`, never a match manufactured from the ARRIVAL row.
    /// Confirmed this test would have caught the bug: reverting the
    /// `event_type = 'DEPARTURE'` filter in `find_backlog_match` and
    /// re-running this test fails it (`matched` becomes `true` and the pin
    /// resolves against the ARRIVAL row's `train_id`), exactly the
    /// production failure mode above.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                an_unrelated_arrival_event_never_falsely_matches_a_pins_scheduled_departure \
                -- --ignored --test-threads=1`"]
    async fn an_unrelated_arrival_event_never_falsely_matches_a_pins_scheduled_departure() {
        let pool = connect().await;
        let user_id = "TEST-BACKLOG-ARRIVAL-GUARD-USER";
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind("backlog-arrival-guard@example.com")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");

        let service_date: NaiveDate = "2026-09-05".parse().unwrap();
        let scheduled: DateTime<Utc> = "2026-09-05T18:15:00Z".parse().unwrap();

        // An unrelated train's ARRIVAL at the pin's own origin CRS, exactly
        // on the pin's scheduled departure time -- well inside
        // SCHEDULED_DEPARTURE_TOLERANCE, and (before this fix) the only row
        // `find_backlog_match`'s CRS+time WHERE clause needed to match.
        sqlx::query(
            "INSERT INTO trust_event_backlog \
                (crs, train_uid, train_id, service_date, msg_type, event_type, \
                 planned_timestamp, actual_timestamp, variation_status, dedup_key) \
             VALUES ($1, NULL, $2, $3, '0003', 'ARRIVAL', $4, $4, 'ON TIME', $5)",
        )
        .bind("EUS")
        .bind("TEST-BACKLOG-ARRIVAL-GUARD-TRAIN-ID")
        .bind(service_date)
        .bind(scheduled)
        .bind("test-backlog-arrival-guard-dedup-arrival")
        .execute(&pool)
        .await
        .expect("seed the unrelated ARRIVAL row");

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

        let matched = attempt_backlog_match(&pool, tracked_train_id, "EUS", scheduled)
            .await
            .expect("attempt_backlog_match");
        assert!(
            !matched,
            "an ARRIVAL-only backlog must never resolve a pin's scheduled DEPARTURE match"
        );

        let (resolution_status,): (String,) =
            sqlx::query_as("SELECT resolution_status FROM train_subscriptions WHERE id = $1")
                .bind(tracked_train_id)
                .fetch_one(&pool)
                .await
                .expect("read back tracked_trains");
        assert_eq!(
            resolution_status, "pending",
            "the pin must be left untouched, not bound to the unrelated ARRIVAL's train_id"
        );

        sqlx::query("DELETE FROM train_subscriptions WHERE id = $1")
            .bind(tracked_train_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query(
            "DELETE FROM trust_event_backlog WHERE train_id = 'TEST-BACKLOG-ARRIVAL-GUARD-TRAIN-ID'",
        )
        .execute(&pool)
        .await
        .ok();
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .ok();
    }

    /// Companion to the test above, proving the fix does more than just
    /// reject a match -- it must still find the CORRECT DEPARTURE match when
    /// one genuinely exists alongside an unrelated ARRIVAL that sorts more
    /// favorably (`ORDER BY planned_timestamp LIMIT 1` would otherwise pick
    /// the ARRIVAL row first, exactly as `an_implausible_actual_timestamp...`'s
    /// sibling test proved for the plausibility guard). The unrelated
    /// train's ARRIVAL lands exactly on the pin's scheduled time (sorts
    /// first); the real train's own DEPARTURE lands 4 minutes later (sorts
    /// second, still inside the M9-tightened +/-5 minute
    /// `SCHEDULED_DEPARTURE_TOLERANCE`). Only the DEPARTURE may resolve
    /// the pin.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                a_real_departure_is_still_found_despite_a_more_favorably_sorted_unrelated_arrival \
                -- --ignored --test-threads=1`"]
    async fn a_real_departure_is_still_found_despite_a_more_favorably_sorted_unrelated_arrival() {
        let pool = connect().await;
        let user_id = "TEST-BACKLOG-ARRIVAL-FALLTHROUGH-USER";
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind("backlog-arrival-fallthrough@example.com")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");

        let service_date: NaiveDate = "2026-09-05".parse().unwrap();
        let scheduled: DateTime<Utc> = "2026-09-05T18:15:00Z".parse().unwrap();

        // Unrelated train: ARRIVAL, sorts FIRST by planned_timestamp
        // (exactly on the pin's scheduled time).
        // Real train: DEPARTURE, sorts SECOND (4 minutes later, still
        // within SCHEDULED_DEPARTURE_TOLERANCE), plus its own Activation so
        // identity can be verified via the dual-written `trains` row.
        let departure_planned: DateTime<Utc> = "2026-09-05T18:19:00Z".parse().unwrap();

        sqlx::query(
            "INSERT INTO trust_event_backlog \
                (crs, train_uid, train_id, service_date, msg_type, event_type, \
                 planned_timestamp, actual_timestamp, variation_status, dedup_key) \
             VALUES \
                ($1, NULL, $2, $3, '0003', 'ARRIVAL', $4, $4, 'ON TIME', $5), \
                ($1, NULL, $6, $3, '0003', 'DEPARTURE', $7, $7, 'ON TIME', $8), \
                (NULL, $9, $6, $3, '0001', NULL, NULL, NULL, NULL, $10)",
        )
        .bind("EUS")
        .bind("TEST-BACKLOG-ARRIVAL-FALLTHROUGH-UNRELATED-TRAIN-ID")
        .bind(service_date)
        .bind(scheduled)
        .bind("test-backlog-arrival-fallthrough-dedup-arrival")
        .bind("TEST-BACKLOG-ARRIVAL-FALLTHROUGH-REAL-TRAIN-ID")
        .bind(departure_planned)
        .bind("test-backlog-arrival-fallthrough-dedup-departure")
        .bind("TEST-DW-ARRIVAL-FALLTHROUGH-UID")
        .bind("test-backlog-arrival-fallthrough-dedup-activation")
        .execute(&pool)
        .await
        .expect("seed both the unrelated ARRIVAL and the real train's DEPARTURE+Activation");

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

        let matched = attempt_backlog_match(&pool, tracked_train_id, "EUS", scheduled)
            .await
            .expect("attempt_backlog_match");
        assert!(
            matched,
            "the real DEPARTURE must still resolve the pin even though an unrelated ARRIVAL \
             sorts first"
        );

        let (trains_id, train_id): (i64, String) = sqlx::query_as(
            "SELECT tr.id, tr.train_id FROM train_subscriptions tt \
             JOIN trains tr ON tr.id = tt.trains_id \
             WHERE tt.id = $1",
        )
        .bind(tracked_train_id)
        .fetch_one(&pool)
        .await
        .expect("read back the resolved train_id");
        assert_eq!(
            train_id, "TEST-BACKLOG-ARRIVAL-FALLTHROUGH-REAL-TRAIN-ID",
            "must resolve to the real DEPARTURE's train_id, never the unrelated ARRIVAL's"
        );

        sqlx::query("DELETE FROM train_subscriptions WHERE id = $1")
            .bind(tracked_train_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query(
            "DELETE FROM trust_event_backlog WHERE train_id IN \
             ('TEST-BACKLOG-ARRIVAL-FALLTHROUGH-UNRELATED-TRAIN-ID', \
              'TEST-BACKLOG-ARRIVAL-FALLTHROUGH-REAL-TRAIN-ID')",
        )
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

    /// Seeds one subscription that is ALREADY bound to a shared `trains`
    /// row (the schedule-matched shape: `resolution_status =
    /// 'schedule_matched'`, `trains_id` pointing at a `trains` row whose
    /// `train_uid` came from a real CIF match), plus one backlog train that
    /// departs the same CRS inside the same `SCHEDULED_DEPARTURE_TOLERANCE`
    /// window and carries its own TRUST Activation naming its `train_uid`
    /// unambiguously.
    ///
    /// Shared by both sides of the filter: pass a `backlog_train_uid` that
    /// DIFFERS from `pin_train_uid` for the production shape this fix exists
    /// for (two identities, both known for certain, one pin), or the same one
    /// for the agreement case that must still replay. Returns
    /// `(tracked_train_id, trains_id)`.
    ///
    /// The fixture's own incidental values -- the user's email, the backlog
    /// rows' `dedup_key`s, the `service_date`, and the backlog train's
    /// `planned_timestamp` -- are DERIVED here rather than passed in, both to
    /// keep this under `clippy::too_many_arguments` and because each caller
    /// would otherwise be restating a value it has no reason to choose
    /// differently. **The derived departure offset was the real incident's
    /// own 14 minutes before the pin's booked time until the M9 fix
    /// (2026-09-26 review) tightened `find_backlog_match`'s own window from
    /// `common::MATCH_TOLERANCE` (20 minutes) to `SCHEDULED_DEPARTURE_TOLERANCE`
    /// (5)** -- 14 minutes would now fall OUTSIDE that window and never
    /// reach the contradiction filter this fixture exists to exercise at
    /// all (`find_backlog_match` would simply return `None`, and this
    /// fixture's own tests would then be proving nothing about the filter).
    /// 4 minutes keeps the fixture comfortably inside the new, tighter
    /// window while still testing the identical contradiction-filter logic.
    async fn seed_identified_pin_and_a_backlog_train(
        pool: &PgPool,
        user_id: &str,
        pin_train_uid: &str,
        backlog_train_uid: &str,
        backlog_train_id: &str,
        pin_scheduled: DateTime<Utc>,
    ) -> (i64, i64) {
        let service_date = pin_scheduled.date_naive();
        let backlog_planned = pin_scheduled - Duration::minutes(4);
        let dedup_prefix = backlog_train_id;

        // Defensive pre-clean, not belt-and-braces: `trust_event_backlog` has
        // a real `UNIQUE (dedup_key)`, and these fixtures' dedup keys are
        // fixed strings, so a PREVIOUS run that panicked before reaching its
        // own cleanup (exactly what happens while a test is being written, or
        // when an assertion legitimately fails) would otherwise make every
        // later run fail on the seed instead of on the assertion -- hiding
        // the real outcome behind a duplicate-key error. Same class of
        // re-runnability bug this module's
        // `a_full_activation_plus_movement_backlog_resolves_the_pin_to_resolved`
        // documents for its own leftover `train_subscriptions` row.
        for train_uid in [pin_train_uid, backlog_train_uid] {
            sqlx::query("DELETE FROM trains WHERE train_uid = $1")
                .bind(train_uid)
                .execute(pool)
                .await
                .ok();
        }
        sqlx::query("DELETE FROM trust_event_backlog WHERE train_id = $1")
            .bind(backlog_train_id)
            .execute(pool)
            .await
            .ok();

        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind(format!("{user_id}@example.com"))
        .bind(user_id)
        .execute(pool)
        .await
        .expect("seed fixture user");

        // The pin's OWN identity, on the shared `trains` row -- where a
        // subscription's resolved identity actually lives (Task 22 dropped
        // `train_subscriptions.train_uid`). `schedule_matched_at` set, no
        // `train_id`: precisely what `attempt_schedule_match` leaves behind
        // a moment before `attempt_backlog_match` is called at pin creation.
        let (trains_id,): (i64,) = sqlx::query_as(
            "INSERT INTO trains (train_uid, service_date, origin_crs, scheduled_departure, \
                                 schedule_matched_at) \
             VALUES ($1, $2, 'EUS', $3, NOW()) RETURNING id",
        )
        .bind(pin_train_uid)
        .bind(service_date)
        .bind(pin_scheduled)
        .fetch_one(pool)
        .await
        .expect("seed the pin's own already-matched trains row");

        let (tracked_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions \
                (user_id, service_date, pin_origin_crs, pin_scheduled_departure, \
                 trains_id, resolution_status) \
             VALUES ($1, $2, 'EUS', $3, $4, 'schedule_matched') RETURNING id",
        )
        .bind(user_id)
        .bind(service_date)
        .bind(pin_scheduled)
        .bind(trains_id)
        .fetch_one(pool)
        .await
        .expect("seed the already-identified subscription");

        // A DIFFERENT train's backlog history at the same origin, inside the
        // pin's window: an Activation naming its real train_uid (so
        // `find_backlog_match` discovers that identity for certain) plus the
        // located DEPARTURE that makes it a CRS+time candidate at all. Same
        // faithful row shapes as this module's other fixtures -- train_uid
        // on the Activation only, crs/timings on the Movement only.
        sqlx::query(
            "INSERT INTO trust_event_backlog \
                (crs, train_uid, train_id, service_date, msg_type, event_type, \
                 planned_timestamp, actual_timestamp, variation_status, dedup_key) \
             VALUES (NULL, $1, $2, $3, '0001', NULL, NULL, NULL, NULL, $4), \
                    ('EUS', NULL, $2, $3, '0003', 'DEPARTURE', $5, $5, 'ON TIME', $6)",
        )
        .bind(backlog_train_uid)
        .bind(backlog_train_id)
        .bind(service_date)
        .bind(format!("{dedup_prefix}-activation"))
        .bind(backlog_planned)
        .bind(format!("{dedup_prefix}-movement"))
        .execute(pool)
        .await
        .expect("seed the conflicting backlog train's Activation + DEPARTURE");

        (tracked_train_id, trains_id)
    }

    async fn cleanup_identity_fixture(
        pool: &PgPool,
        user_id: &str,
        tracked_train_id: i64,
        trains_id: i64,
        backlog_train_id: &str,
        backlog_train_uid: &str,
    ) {
        // `train_movement_events`/`train_current_state` are keyed on
        // `trains_id`, not on the subscription (Step D's cutover) -- and
        // `trains`' own FKs are `ON DELETE CASCADE`, so deleting the `trains`
        // rows below takes both with them. Deleted explicitly first anyway,
        // for the same reason this module's other fixtures clean up
        // defensively: a row left behind here would show up as another
        // test's phantom movement.
        sqlx::query("DELETE FROM train_movement_events WHERE trains_id = $1")
            .bind(trains_id)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM train_current_state WHERE trains_id = $1")
            .bind(trains_id)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM train_subscriptions WHERE id = $1")
            .bind(tracked_train_id)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trust_event_backlog WHERE train_id = $1")
            .bind(backlog_train_id)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(pool)
            .await
            .ok();
        // The backlog train's own `trains` row only exists if the match was
        // (correctly) allowed to proceed -- harmless no-op otherwise.
        sqlx::query("DELETE FROM trains WHERE train_uid = $1")
            .bind(backlog_train_uid)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(pool)
            .await
            .ok();
    }

    /// THE REGRESSION TEST for this fix, and the backlog-path twin of
    /// trust-consumer's own
    /// `a_parked_activation_for_a_different_schedule_cannot_claim_a_pin_by_crs_and_time`.
    ///
    /// A subscription is already correctly identified (`trains_id` -> a
    /// `trains` row with `train_uid` `Y80926`, exactly what
    /// `attempt_schedule_match` leaves behind at pin creation). The backlog
    /// holds a DIFFERENT train (`W34058`) whose own TRUST Activation names
    /// it unambiguously and whose DEPARTURE from the same origin falls 14
    /// minutes inside the pin's +/-20-minute window -- so the pure CRS+time
    /// `find_backlog_match` lookup selects it, just as it did in production
    /// on 2026-09-25.
    ///
    /// Before the contradiction filter, every one of the writes asserted
    /// against below actually happened, and none of them has an unwind path:
    ///
    /// * `replay_backlog_history` replayed `W34058`'s movements through
    ///   `upsert_train_event`, whose `flip_legacy_resolution` returns the
    ///   subscription's EXISTING `trains_id` -- so the wrong train's
    ///   movements and `train_current_state` landed on the CORRECT train's
    ///   shared `trains` row, visible to every subscriber of that train, not
    ///   just this one;
    /// * that same `flip_legacy_resolution` advanced
    ///   `resolution_status` to `'resolved'` with no guard at all; and
    /// * the Step A dual-write then created a `trains` row for `W34058` and
    ///   repointed `train_subscriptions.trains_id` at it.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                a_backlog_candidate_naming_a_different_train_uid_never_repoints_an_identified_pin \
                -- --ignored --test-threads=1`"]
    async fn a_backlog_candidate_naming_a_different_train_uid_never_repoints_an_identified_pin() {
        let pool = connect().await;
        let user_id = "TEST-BACKLOG-UID-CONTRADICTION-USER";
        // The real incident's own booked departure time (the
        // Euston -> Birmingham New Street service); the seeding helper puts
        // the other train's DEPARTURE 4 minutes earlier (the real incident's
        // own TRUST-reported Avanti offset was 14 minutes, but that no
        // longer fits inside the M9-tightened `SCHEDULED_DEPARTURE_TOLERANCE`
        // window -- see `seed_identified_pin_and_a_backlog_train`'s own doc
        // comment).
        //
        // The DATE, though, is deliberately a fixed one several days in the
        // past rather than the incident's own 2026-09-25, and each of this
        // fix's three tests deliberately uses a different HOUR.
        // `find_backlog_match` filters on CRS + `planned_timestamp` window
        // ONLY -- never on `service_date` -- so two fixtures sharing an
        // origin CRS and a departure time within `SCHEDULED_DEPARTURE_TOLERANCE` of each
        // other compete for the same `ORDER BY planned_timestamp LIMIT 1`
        // even across different service dates. Both hazards were real: an
        // earlier draft of these tests shared one timestamp (so the second
        // test matched the first's leftover row) and used "today", which put
        // them inside the `Utc::now() - 30 minutes` window
        // `run_backlog_match_sweep_resolves_a_pin_the_backlog_had_nothing_for_at_creation_time`
        // anchors itself to, breaking that unrelated test depending on the
        // hour the suite ran.
        let pin_scheduled: DateTime<Utc> = "2026-09-20T17:56:00Z".parse().unwrap();
        let backlog_train_id = "TEST-BACKLOG-UID-CONTRADICTION-TRAIN-ID";
        let backlog_train_uid = "TEST-CONTRADICTION-W34058";

        let (tracked_train_id, trains_id) = seed_identified_pin_and_a_backlog_train(
            &pool,
            user_id,
            "TEST-CONTRADICTION-Y80926",
            backlog_train_uid,
            backlog_train_id,
            pin_scheduled,
        )
        .await;

        let matched = attempt_backlog_match(&pool, tracked_train_id, "EUS", pin_scheduled)
            .await
            .expect("attempt_backlog_match");
        assert!(
            !matched,
            "a backlog train TRUST itself names as a different train_uid must never match a \
             subscription already known to be tracking another, however well its origin \
             departure lines up"
        );

        let (bound_trains_id, resolution_status): (Option<i64>, String) = sqlx::query_as(
            "SELECT trains_id, resolution_status FROM train_subscriptions WHERE id = $1",
        )
        .bind(tracked_train_id)
        .fetch_one(&pool)
        .await
        .expect("read back the subscription");
        assert_eq!(
            bound_trains_id,
            Some(trains_id),
            "the subscription must still point at its own correctly-matched trains row -- the \
             Step A dual-write's repoint has no unwind path"
        );
        assert_eq!(
            resolution_status, "schedule_matched",
            "the pin's own resolution must be left exactly as it was"
        );

        // Keyed on `trains_id`, which is where the real damage landed: the
        // shared movement log of the train this pin is CORRECTLY matched to.
        let replayed: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM train_movement_events WHERE trains_id = $1")
                .bind(trains_id)
                .fetch_one(&pool)
                .await
                .expect("count replayed movement events");
        assert_eq!(
            replayed, 0,
            "rejected BEFORE fetch_backlog_history/replay_backlog_history -- not one of the \
             other train's movements may reach this train's shared movement log"
        );

        let wrong_train_row: Option<i64> =
            sqlx::query_scalar("SELECT id FROM trains WHERE train_uid = $1")
                .bind(backlog_train_uid)
                .fetch_optional(&pool)
                .await
                .expect("look for a trains row for the contradicting identity");
        assert_eq!(
            wrong_train_row, None,
            "the Step A dual-write must not even have created the other train's trains row"
        );

        cleanup_identity_fixture(
            &pool,
            user_id,
            tracked_train_id,
            trains_id,
            backlog_train_id,
            backlog_train_uid,
        )
        .await;
    }

    /// The other side of the same filter, and the reason it is a
    /// contradiction filter rather than a rewrite: when the backlog
    /// candidate's Activation names the SAME `train_uid` the subscription
    /// already knows itself to be, the replay must still happen. This is the
    /// whole legitimate purpose of calling `attempt_backlog_match` after a
    /// successful `attempt_schedule_match` -- a schedule match supplies
    /// timetable data but no TRUST movements, and an already-departed train's
    /// movements only exist in the backlog. Mirrors trust-consumer's own
    /// `a_parked_activation_for_the_same_schedule_still_allows_the_crs_and_time_claim`.
    ///
    /// Both sides use the identical `train_uid` here, deliberately: the
    /// filter's case-insensitivity is covered by the pure predicate tests
    /// above, and feeding a differing-case `train_uid` through this DB path
    /// would additionally exercise `find_or_create_train`'s own
    /// case-SENSITIVE `UNIQUE (train_uid, service_date)` key -- an unrelated
    /// pre-existing behavior this test has no business asserting on.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                a_backlog_candidate_naming_the_same_train_uid_still_replays_onto_an_identified_pin \
                -- --ignored --test-threads=1`"]
    async fn a_backlog_candidate_naming_the_same_train_uid_still_replays_onto_an_identified_pin() {
        let pool = connect().await;
        let user_id = "TEST-BACKLOG-UID-AGREEMENT-USER";
        // A different hour from its sibling tests -- see the timestamp note
        // on `a_backlog_candidate_naming_a_different_train_uid_never_repoints_an_identified_pin`.
        let pin_scheduled: DateTime<Utc> = "2026-09-20T09:56:00Z".parse().unwrap();
        let backlog_train_id = "TEST-BACKLOG-UID-AGREEMENT-TRAIN-ID";
        // The very same identity the pin already knows itself to be.
        let backlog_train_uid = "TEST-AGREEMENT-Y80926";

        let (tracked_train_id, trains_id) = seed_identified_pin_and_a_backlog_train(
            &pool,
            user_id,
            backlog_train_uid,
            backlog_train_uid,
            backlog_train_id,
            pin_scheduled,
        )
        .await;

        let matched = attempt_backlog_match(&pool, tracked_train_id, "EUS", pin_scheduled)
            .await
            .expect("attempt_backlog_match");
        assert!(
            matched,
            "the same train's own retained history must still replay -- this backfill is the \
             whole reason the pin-creation call sites run this unconditionally after a \
             successful schedule match"
        );

        let replayed: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM train_movement_events WHERE trains_id = $1")
                .bind(trains_id)
                .fetch_one(&pool)
                .await
                .expect("count replayed movement events");
        assert_eq!(
            replayed, 1,
            "the DEPARTURE row must have been replayed onto this train's own shared movement \
             log (the Activation row is a no-op replay step)"
        );

        cleanup_identity_fixture(
            &pool,
            user_id,
            tracked_train_id,
            trains_id,
            backlog_train_id,
            backlog_train_uid,
        )
        .await;
    }

    /// The named residual, pinned so narrowing it later is a deliberate
    /// choice rather than an accident: a subscription with NO identity of
    /// its own (`trains_id IS NULL`, `'pending'` -- the ordinary
    /// pre-resolution shape, and every row
    /// `list_pending_pins_for_backlog_match` selects) is completely
    /// unaffected by this filter. There is nothing to contradict, so the
    /// CRS+time heuristic remains the only thing it has ever had, and it
    /// must still resolve the pin exactly as before.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                a_pin_with_no_identity_of_its_own_is_unaffected_by_the_contradiction_filter \
                -- --ignored --test-threads=1`"]
    async fn a_pin_with_no_identity_of_its_own_is_unaffected_by_the_contradiction_filter() {
        let pool = connect().await;
        let user_id = "TEST-BACKLOG-UID-UNKNOWN-PIN-USER";
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind("backlog-uid-unknown-pin@example.com")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");

        // A third distinct hour, for the reason the timestamp note on
        // `a_backlog_candidate_naming_a_different_train_uid_never_repoints_an_identified_pin`
        // gives. 4 minutes, not the real incident's own 14, for the same
        // M9-tightened-tolerance reason
        // `seed_identified_pin_and_a_backlog_train`'s own doc comment gives.
        let pin_scheduled: DateTime<Utc> = "2026-09-20T12:56:00Z".parse().unwrap();
        let service_date = pin_scheduled.date_naive();
        let backlog_planned = pin_scheduled - Duration::minutes(4);

        sqlx::query(
            "INSERT INTO trust_event_backlog \
                (crs, train_uid, train_id, service_date, msg_type, event_type, \
                 planned_timestamp, actual_timestamp, variation_status, dedup_key) \
             VALUES (NULL, $1, $2, $3, '0001', NULL, NULL, NULL, NULL, $4), \
                    ('EUS', NULL, $2, $3, '0003', 'DEPARTURE', $5, $5, 'ON TIME', $6)",
        )
        .bind("TEST-UNKNOWN-PIN-UID")
        .bind("TEST-BACKLOG-UID-UNKNOWN-PIN-TRAIN-ID")
        .bind(service_date)
        .bind("test-backlog-uid-unknown-pin-activation")
        .bind(backlog_planned)
        .bind("test-backlog-uid-unknown-pin-movement")
        .execute(&pool)
        .await
        .expect("seed the backlog train");

        let (tracked_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions \
                (user_id, service_date, pin_origin_crs, pin_scheduled_departure) \
             VALUES ($1, $2, 'EUS', $3) RETURNING id",
        )
        .bind(user_id)
        .bind(service_date)
        .bind(pin_scheduled)
        .fetch_one(&pool)
        .await
        .expect("seed a pin with no identity of its own");

        let matched = attempt_backlog_match(&pool, tracked_train_id, "EUS", pin_scheduled)
            .await
            .expect("attempt_backlog_match");
        assert!(
            matched,
            "a pin with no known train_uid must still be matchable by CRS+time alone -- this \
             filter only ever removes a candidate TRUST's own data has already contradicted"
        );

        let (trains_id,): (Option<i64>,) =
            sqlx::query_as("SELECT trains_id FROM train_subscriptions WHERE id = $1")
                .bind(tracked_train_id)
                .fetch_one(&pool)
                .await
                .expect("read back trains_id");
        let trains_id = trains_id.expect("the dual-write must still have bound a trains row");

        cleanup_identity_fixture(
            &pool,
            user_id,
            tracked_train_id,
            trains_id,
            "TEST-BACKLOG-UID-UNKNOWN-PIN-TRAIN-ID",
            "TEST-UNKNOWN-PIN-UID",
        )
        .await;
    }

    /// M9 (Repeater Signal, 2026-09-26) boundary pin: `find_backlog_match`
    /// compares booked time against booked time, so its window is
    /// `SCHEDULED_DEPARTURE_TOLERANCE` (5 minutes), not `common::MATCH_TOLERANCE`
    /// (20). A DEPARTURE booked 10 minutes from the pin's departure (inside
    /// the old window) must not be matched; one booked 4 minutes away must.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                find_backlog_match_ignores_a_departure_outside_the_five_minute_window \
                -- --ignored --test-threads=1`"]
    async fn find_backlog_match_ignores_a_departure_outside_the_five_minute_window() {
        let pool = connect().await;
        let far_train_id = "TEST-M9-FAR-TRAIN-ID";
        let near_train_id = "TEST-M9-NEAR-TRAIN-ID";
        let cleanup = || async {
            sqlx::query("DELETE FROM trust_event_backlog WHERE train_id IN ($1, $2)")
                .bind(far_train_id)
                .bind(near_train_id)
                .execute(&pool)
                .await
                .ok();
        };
        cleanup().await;

        let pin_scheduled: DateTime<Utc> = "2026-09-19T07:31:00Z".parse().unwrap();
        let service_date = pin_scheduled.date_naive();
        let insert_departure =
            |train_id: &'static str, planned: DateTime<Utc>, dedup: &'static str| {
                let pool = pool.clone();
                async move {
                    sqlx::query(
                        "INSERT INTO trust_event_backlog \
                        (crs, train_uid, train_id, service_date, msg_type, event_type, \
                         planned_timestamp, actual_timestamp, variation_status, dedup_key) \
                     VALUES ('ZZM', NULL, $1, $2, '0003', 'DEPARTURE', $3, $3, 'ON TIME', $4)",
                    )
                    .bind(train_id)
                    .bind(service_date)
                    .bind(planned)
                    .bind(dedup)
                    .execute(&pool)
                    .await
                    .expect("seed backlog departure");
                }
            };

        insert_departure(
            far_train_id,
            pin_scheduled - Duration::minutes(10),
            "test-m9-far-departure",
        )
        .await;
        let far_only = find_backlog_match(&pool, "ZZM", pin_scheduled)
            .await
            .expect("find_backlog_match");
        assert_eq!(
            far_only, None,
            "a departure booked 10 minutes away is outside the 5-minute window (it was inside \
             the old 20-minute one)"
        );

        insert_departure(
            near_train_id,
            pin_scheduled - Duration::minutes(4),
            "test-m9-near-departure",
        )
        .await;
        let with_near = find_backlog_match(&pool, "ZZM", pin_scheduled)
            .await
            .expect("find_backlog_match");
        assert_eq!(
            with_near.map(|candidate| candidate.train_id).as_deref(),
            Some(near_train_id),
            "a departure booked 4 minutes away is inside the window"
        );

        cleanup().await;
    }

    // ---------------------------------------------------------------------
    // Absolute-time matching (Repeater Signal M7 residual, 2026-09-27).
    //
    // Every fixture below uses BST dates in September 2026, so London is
    // UTC+1: a train leaving its origin at 23:30 London on D is at 22:30Z,
    // and its intermediate stop at 00:30 London on D+1 is at 23:30Z on D.
    // Each test uses its own synthetic CRS so fixtures never compete for
    // the same CRS+time window.
    // ---------------------------------------------------------------------

    /// One run of a train in `trust_event_backlog`, as the backlog consumer
    /// files it since 97ccd3ea: an Activation carrying the uid, then a
    /// DEPARTURE at `crs` and an ARRIVAL at a synthetic terminus 30 minutes
    /// later, all under `service_date` (the origin date).
    async fn seed_backlog_run(
        pool: &PgPool,
        train_id: &str,
        train_uid: &str,
        service_date: NaiveDate,
        crs: &str,
        departure: DateTime<Utc>,
    ) {
        let arrival = departure + Duration::minutes(30);
        let received = departure - Duration::hours(1);
        sqlx::query(
            "INSERT INTO trust_event_backlog \
                (crs, train_uid, train_id, service_date, msg_type, event_type, \
                 planned_timestamp, actual_timestamp, variation_status, dedup_key, received_at) \
             VALUES (NULL, $1, $2, $3, '0001', NULL, NULL, NULL, NULL, $4, $5), \
                    ($6, NULL, $2, $3, '0003', 'DEPARTURE', $7, $7, 'ON TIME', $8, $9), \
                    ('ZZT', NULL, $2, $3, '0003', 'ARRIVAL', $10, $10, 'ON TIME', $11, $12)",
        )
        .bind(train_uid)
        .bind(train_id)
        .bind(service_date)
        .bind(format!("{train_id}-{service_date}-activation"))
        .bind(received)
        .bind(crs)
        .bind(departure)
        .bind(format!("{train_id}-{service_date}-departure"))
        .bind(departure)
        .bind(arrival)
        .bind(format!("{train_id}-{service_date}-arrival"))
        .bind(arrival)
        .execute(pool)
        .await
        .expect("seed a backlog run");
    }

    async fn seed_pending_pin(
        pool: &PgPool,
        user_id: &str,
        service_date: NaiveDate,
        crs: &str,
        scheduled: DateTime<Utc>,
    ) -> i64 {
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind(format!("{user_id}@example.com"))
        .bind(user_id)
        .execute(pool)
        .await
        .expect("seed fixture user");
        let (id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions \
                (user_id, service_date, pin_origin_crs, pin_scheduled_departure) \
             VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(user_id)
        .bind(service_date)
        .bind(crs)
        .bind(scheduled)
        .fetch_one(pool)
        .await
        .expect("seed a pending pin");
        id
    }

    /// `(trains.train_uid, trains.service_date, resolution_status)` for a
    /// subscription, via its `trains_id`.
    async fn subscription_identity(
        pool: &PgPool,
        tracked_train_id: i64,
    ) -> (Option<String>, Option<NaiveDate>, String) {
        sqlx::query_as(
            "SELECT tr.train_uid, tr.service_date, ts.resolution_status \
             FROM train_subscriptions ts LEFT JOIN trains tr ON tr.id = ts.trains_id \
             WHERE ts.id = $1",
        )
        .bind(tracked_train_id)
        .fetch_one(pool)
        .await
        .expect("read back the subscription's identity")
    }

    /// How many movement events the shared `trains` row for
    /// `(train_uid, service_date)` holds; `None` when that row doesn't exist.
    async fn movement_count_for(
        pool: &PgPool,
        train_uid: &str,
        service_date: NaiveDate,
    ) -> Option<i64> {
        let trains_id: Option<i64> =
            sqlx::query_scalar("SELECT id FROM trains WHERE train_uid = $1 AND service_date = $2")
                .bind(train_uid)
                .bind(service_date)
                .fetch_optional(pool)
                .await
                .expect("look up trains row");
        let trains_id = trains_id?;
        Some(
            sqlx::query_scalar("SELECT COUNT(*) FROM train_movement_events WHERE trains_id = $1")
                .bind(trains_id)
                .fetch_one(pool)
                .await
                .expect("count movement events"),
        )
    }

    async fn cleanup_absolute_time_fixture(
        pool: &PgPool,
        user_ids: &[&str],
        train_ids: &[&str],
        train_uid: &str,
    ) {
        for user_id in user_ids {
            sqlx::query("DELETE FROM train_subscriptions WHERE user_id = $1")
                .bind(user_id)
                .execute(pool)
                .await
                .ok();
        }
        for train_id in train_ids {
            sqlx::query("DELETE FROM trust_event_backlog WHERE train_id = $1")
                .bind(train_id)
                .execute(pool)
                .await
                .ok();
        }
        // `train_movement_events`/`train_current_state` cascade from `trains`.
        sqlx::query("DELETE FROM trains WHERE train_uid = $1")
            .bind(train_uid)
            .execute(pool)
            .await
            .ok();
        for user_id in user_ids {
            sqlx::query("DELETE FROM users WHERE id = $1")
                .bind(user_id)
                .execute(pool)
                .await
                .ok();
        }
    }

    /// THE regression test for the M7 residual. A train leaves its origin
    /// at 23:30 on D and calls at an intermediate station at 00:30 on D+1.
    /// The pin at that station is dated D+1 (its own departure's calendar
    /// date); the backlog files the whole train under D (its origin date).
    /// Before the fix `find_backlog_match` required the two dates to be
    /// equal, so this pin never matched. It must now match on absolute time,
    /// and the identity must be the train's own `(uid, D)`, never
    /// `(uid, D+1)` -- that is the next day's run of the same service.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                an_intermediate_stop_pin_after_midnight -- --ignored --test-threads=1`"]
    async fn an_intermediate_stop_pin_after_midnight_matches_its_pre_midnight_origin_trains_backlog()
     {
        let pool = connect().await;
        let user_id = "TEST-M7R-AFTER-MIDNIGHT-USER";
        let train_id = "TEST-M7R-A-TID";
        let train_uid = "TEST-M7R-A";
        cleanup_absolute_time_fixture(&pool, &[user_id], &[train_id], train_uid).await;

        let origin_date: NaiveDate = "2026-09-12".parse().unwrap();
        let pin_date: NaiveDate = "2026-09-13".parse().unwrap();
        // 00:30 BST on 2026-09-13.
        let intermediate_departure: DateTime<Utc> = "2026-09-12T23:30:00Z".parse().unwrap();
        seed_backlog_run(
            &pool,
            train_id,
            train_uid,
            origin_date,
            "ZMA",
            intermediate_departure,
        )
        .await;

        // M9's tolerance still applies across midnight: 10 minutes off is
        // not the same booked departure.
        assert_eq!(
            find_backlog_match(&pool, "ZMA", intermediate_departure + Duration::minutes(10))
                .await
                .expect("find_backlog_match"),
            None,
            "a pin 10 minutes off must not match, whatever the dates"
        );

        let candidate = find_backlog_match(&pool, "ZMA", intermediate_departure)
            .await
            .expect("find_backlog_match")
            .expect("the pin's own departure must match across the date convention gap");
        assert_eq!(
            candidate,
            BacklogCandidate {
                train_id: train_id.to_string(),
                service_date: origin_date,
                train_uid: Some(train_uid.to_string()),
                identity_date: origin_date,
            }
        );

        let tracked_train_id =
            seed_pending_pin(&pool, user_id, pin_date, "ZMA", intermediate_departure).await;
        let matched = attempt_backlog_match(&pool, tracked_train_id, "ZMA", intermediate_departure)
            .await
            .expect("attempt_backlog_match");
        assert!(matched, "the post-midnight pin must match its backlog rows");

        assert_eq!(
            subscription_identity(&pool, tracked_train_id).await,
            (
                Some(train_uid.to_string()),
                Some(origin_date),
                "resolved".to_string()
            ),
            "the identity is the train's own origin date, not the pin's"
        );
        assert_eq!(
            movement_count_for(&pool, train_uid, origin_date).await,
            Some(2),
            "the DEPARTURE and ARRIVAL filed under the origin date are both replayed"
        );
        assert_eq!(
            movement_count_for(&pool, train_uid, pin_date).await,
            None,
            "no trains row may be created for (uid, pin date): that is the next day's run"
        );

        cleanup_absolute_time_fixture(&pool, &[user_id], &[train_id], train_uid).await;
    }

    /// The critical guard: the same service runs on two consecutive days,
    /// and (worst case) both runs carry the SAME `train_id`. Each pin at
    /// the intermediate stop must bind to its own run -- the right
    /// `(uid, date)` identity, and only that run's movements -- never the
    /// adjacent day's. Absolute time separates the runs by 24 hours; the
    /// history fetch and the identity then key on the matched row's date.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                the_same_uid_on_consecutive_days -- --ignored --test-threads=1`"]
    async fn the_same_uid_on_consecutive_days_never_cross_matches() {
        let pool = connect().await;
        let first_user = "TEST-M7R-CROSS-DAY-USER-1";
        let second_user = "TEST-M7R-CROSS-DAY-USER-2";
        let train_id = "TEST-M7R-B-TID";
        let train_uid = "TEST-M7R-B";
        cleanup_absolute_time_fixture(&pool, &[first_user, second_user], &[train_id], train_uid)
            .await;

        let day_one: NaiveDate = "2026-09-14".parse().unwrap();
        let day_two: NaiveDate = "2026-09-15".parse().unwrap();
        let day_three: NaiveDate = "2026-09-16".parse().unwrap();
        // 00:30 BST on day two and on day three respectively.
        let day_one_run_at_stop: DateTime<Utc> = "2026-09-14T23:30:00Z".parse().unwrap();
        let day_two_run_at_stop: DateTime<Utc> = "2026-09-15T23:30:00Z".parse().unwrap();
        seed_backlog_run(
            &pool,
            train_id,
            train_uid,
            day_one,
            "ZMB",
            day_one_run_at_stop,
        )
        .await;
        seed_backlog_run(
            &pool,
            train_id,
            train_uid,
            day_two,
            "ZMB",
            day_two_run_at_stop,
        )
        .await;

        // Pin on day one's run, dated day two. Its date equals day two's
        // run's `service_date`, which is exactly the false match an exact
        // date filter would invite if the time check were loose.
        let first_pin =
            seed_pending_pin(&pool, first_user, day_two, "ZMB", day_one_run_at_stop).await;
        assert!(
            attempt_backlog_match(&pool, first_pin, "ZMB", day_one_run_at_stop)
                .await
                .expect("attempt_backlog_match")
        );
        assert_eq!(
            subscription_identity(&pool, first_pin).await,
            (
                Some(train_uid.to_string()),
                Some(day_one),
                "resolved".to_string()
            )
        );
        assert_eq!(
            movement_count_for(&pool, train_uid, day_one).await,
            Some(2),
            "only day one's two movements, not day two's as well"
        );
        assert_eq!(
            movement_count_for(&pool, train_uid, day_two).await,
            None,
            "day two's run must not be created or written by a day-one pin"
        );

        // Pin on day two's run, dated day three.
        let second_pin =
            seed_pending_pin(&pool, second_user, day_three, "ZMB", day_two_run_at_stop).await;
        assert!(
            attempt_backlog_match(&pool, second_pin, "ZMB", day_two_run_at_stop)
                .await
                .expect("attempt_backlog_match")
        );
        assert_eq!(
            subscription_identity(&pool, second_pin).await,
            (
                Some(train_uid.to_string()),
                Some(day_two),
                "resolved".to_string()
            )
        );
        assert_eq!(movement_count_for(&pool, train_uid, day_two).await, Some(2));
        assert_eq!(
            movement_count_for(&pool, train_uid, day_one).await,
            Some(2),
            "day one's row is untouched by the day-two pin"
        );

        cleanup_absolute_time_fixture(&pool, &[first_user, second_user], &[train_id], train_uid)
            .await;
    }

    /// The ordinary case is unchanged: a daytime pin whose date and the
    /// backlog's date agree still matches, and its identity is that date.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                a_same_day_pin_still_matches -- --ignored --test-threads=1`"]
    async fn a_same_day_pin_still_matches_on_absolute_time() {
        let pool = connect().await;
        let user_id = "TEST-M7R-SAME-DAY-USER";
        let train_id = "TEST-M7R-C-TID";
        let train_uid = "TEST-M7R-C";
        cleanup_absolute_time_fixture(&pool, &[user_id], &[train_id], train_uid).await;

        let service_date: NaiveDate = "2026-09-17".parse().unwrap();
        let departure: DateTime<Utc> = "2026-09-17T11:04:00Z".parse().unwrap();
        seed_backlog_run(&pool, train_id, train_uid, service_date, "ZMC", departure).await;

        // Booked 3 minutes apart (GBTT vs WTT), inside M9's 5 minutes.
        let pin_scheduled = departure + Duration::minutes(3);
        let tracked_train_id =
            seed_pending_pin(&pool, user_id, service_date, "ZMC", pin_scheduled).await;
        assert!(
            attempt_backlog_match(&pool, tracked_train_id, "ZMC", pin_scheduled)
                .await
                .expect("attempt_backlog_match")
        );
        assert_eq!(
            subscription_identity(&pool, tracked_train_id).await,
            (
                Some(train_uid.to_string()),
                Some(service_date),
                "resolved".to_string()
            )
        );
        assert_eq!(
            movement_count_for(&pool, train_uid, service_date).await,
            Some(2)
        );

        cleanup_absolute_time_fixture(&pool, &[user_id], &[train_id], train_uid).await;
    }

    /// A Movement processed with no parked Activation (a backlog-consumer
    /// restart between the two) is dated by its own calendar date, so after
    /// midnight it is filed one day later than its Activation. The match
    /// must still find the Activation (and so the uid and the identity
    /// date), and the history must include both dates.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                a_movement_filed_a_day_after_its_activation -- --ignored --test-threads=1`"]
    async fn a_movement_filed_a_day_after_its_activation_still_resolves_to_the_origin_date() {
        let pool = connect().await;
        let user_id = "TEST-M7R-SPLIT-USER";
        let train_id = "TEST-M7R-D-TID";
        let train_uid = "TEST-M7R-D";
        cleanup_absolute_time_fixture(&pool, &[user_id], &[train_id], train_uid).await;

        let origin_date: NaiveDate = "2026-09-18".parse().unwrap();
        let next_date: NaiveDate = "2026-09-19".parse().unwrap();
        let origin_departure: DateTime<Utc> = "2026-09-18T22:30:00Z".parse().unwrap();
        let stop_departure: DateTime<Utc> = "2026-09-18T23:30:00Z".parse().unwrap();
        sqlx::query(
            "INSERT INTO trust_event_backlog \
                (crs, train_uid, train_id, service_date, msg_type, event_type, \
                 planned_timestamp, actual_timestamp, variation_status, dedup_key, received_at) \
             VALUES (NULL, $1, $2, $3, '0001', NULL, NULL, NULL, NULL, 'test-m7r-d-act', $5), \
                    ('ZMX', NULL, $2, $3, '0003', 'DEPARTURE', $5, $5, 'ON TIME', 'test-m7r-d-origin', $5), \
                    ('ZMD', NULL, $2, $4, '0003', 'DEPARTURE', $6, $6, 'ON TIME', 'test-m7r-d-stop', $6)",
        )
        .bind(train_uid)
        .bind(train_id)
        .bind(origin_date)
        .bind(next_date)
        .bind(origin_departure)
        .bind(stop_departure)
        .execute(&pool)
        .await
        .expect("seed a split-date backlog train");

        let tracked_train_id =
            seed_pending_pin(&pool, user_id, next_date, "ZMD", stop_departure).await;
        assert!(
            attempt_backlog_match(&pool, tracked_train_id, "ZMD", stop_departure)
                .await
                .expect("attempt_backlog_match")
        );
        assert_eq!(
            subscription_identity(&pool, tracked_train_id).await,
            (
                Some(train_uid.to_string()),
                Some(origin_date),
                "resolved".to_string()
            )
        );
        assert_eq!(
            movement_count_for(&pool, train_uid, origin_date).await,
            Some(2),
            "both halves of the split history are replayed"
        );

        cleanup_absolute_time_fixture(&pool, &[user_id], &[train_id], train_uid).await;
    }

    /// B5 (migration/test hygiene review, 2026-09-27): the identity-first
    /// path had no test at all. The same uid runs on two consecutive days
    /// with the same `train_id` (worst case); asking for day one must
    /// replay day one's run only, bind the subscription to `(uid, day one)`,
    /// and report the run's origin departure. A date with no Activation in
    /// the backlog is an honest `None`.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                attempt_backlog_match_by_uid -- --ignored --test-threads=1`"]
    async fn attempt_backlog_match_by_uid_replays_only_the_requested_days_run() {
        let pool = connect().await;
        let user_id = "TEST-M7R-BY-UID-USER";
        let train_id = "TEST-M7R-U-TID";
        let train_uid = "TEST-M7R-U";
        cleanup_absolute_time_fixture(&pool, &[user_id], &[train_id], train_uid).await;

        let day_one: NaiveDate = "2026-09-20".parse().unwrap();
        let day_two: NaiveDate = "2026-09-21".parse().unwrap();
        let day_one_departure: DateTime<Utc> = "2026-09-20T22:30:00Z".parse().unwrap();
        let day_two_departure: DateTime<Utc> = "2026-09-21T22:30:00Z".parse().unwrap();
        seed_backlog_run(
            &pool,
            train_id,
            train_uid,
            day_one,
            "ZMU",
            day_one_departure,
        )
        .await;
        seed_backlog_run(
            &pool,
            train_id,
            train_uid,
            day_two,
            "ZMU",
            day_two_departure,
        )
        .await;

        // What `POST /Train/by-uid/{uid}/{date}/track` sets up before calling
        // this: a subscription already bound to the `(uid, date)` row.
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind(format!("{user_id}@example.com"))
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");
        let trains_id = crate::data::trains::find_or_create_train(&pool, train_uid, day_one)
            .await
            .expect("find_or_create_train");
        let (tracked_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions (user_id, trains_id, service_date) \
             VALUES ($1, $2, $3) RETURNING id",
        )
        .bind(user_id)
        .bind(trains_id)
        .bind(day_one)
        .fetch_one(&pool)
        .await
        .expect("seed an NR-primary subscription");

        let outcome = attempt_backlog_match_by_uid(&pool, tracked_train_id, train_uid, day_one)
            .await
            .expect("attempt_backlog_match_by_uid")
            .expect("day one's Activation is in the backlog");
        assert_eq!(outcome.train_id, train_id);
        assert_eq!(
            outcome.replayed_rows, 3,
            "day one's Activation, DEPARTURE and ARRIVAL -- not day two's as well"
        );
        assert_eq!(
            outcome.origin_departure,
            Some(("ZMU".to_string(), day_one_departure))
        );
        assert_eq!(
            subscription_identity(&pool, tracked_train_id).await,
            (
                Some(train_uid.to_string()),
                Some(day_one),
                "resolved".to_string()
            )
        );
        assert_eq!(movement_count_for(&pool, train_uid, day_one).await, Some(2));
        assert_eq!(
            movement_count_for(&pool, train_uid, day_two).await,
            None,
            "day two's run is never touched"
        );
        let resolved_train_id: Option<String> =
            sqlx::query_scalar("SELECT train_id FROM trains WHERE id = $1")
                .bind(trains_id)
                .fetch_one(&pool)
                .await
                .expect("read back trains.train_id");
        assert_eq!(resolved_train_id.as_deref(), Some(train_id));

        let no_activation: NaiveDate = "2026-09-22".parse().unwrap();
        let miss = attempt_backlog_match_by_uid(&pool, tracked_train_id, train_uid, no_activation)
            .await
            .expect("attempt_backlog_match_by_uid miss");
        assert!(
            miss.is_none(),
            "no Activation for that date means no replay, not the adjacent day's"
        );

        cleanup_absolute_time_fixture(&pool, &[user_id], &[train_id], train_uid).await;
    }

    /// M7 leftover (2026-09-27): the uid path reads `{date, date+1}`. After
    /// a backlog-consumer restart between a train's Activation and its
    /// post-midnight Movements, those Movements are filed under D+1 while
    /// the Activation (and the identity) is D. Asking for `(uid, D)` must
    /// replay both halves onto `trains(uid, D)`.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                attempt_backlog_match_by_uid -- --ignored --test-threads=1`"]
    async fn attempt_backlog_match_by_uid_includes_the_next_day_half_of_a_split_history() {
        let pool = connect().await;
        let user_id = "TEST-M7L-BY-UID-SPLIT-USER";
        let train_id = "TEST-M7L-US-TID";
        let train_uid = "TEST-M7L-US";
        cleanup_absolute_time_fixture(&pool, &[user_id], &[train_id], train_uid).await;

        let origin_date: NaiveDate = "2026-09-23".parse().unwrap();
        let next_date: NaiveDate = "2026-09-24".parse().unwrap();
        // 23:30 BST on D, then 00:30 BST on D+1.
        let origin_departure: DateTime<Utc> = "2026-09-23T22:30:00Z".parse().unwrap();
        let stop_departure: DateTime<Utc> = "2026-09-23T23:30:00Z".parse().unwrap();
        sqlx::query(
            "INSERT INTO trust_event_backlog \
                (crs, train_uid, train_id, service_date, msg_type, event_type, \
                 planned_timestamp, actual_timestamp, variation_status, dedup_key, received_at) \
             VALUES (NULL, $1, $2, $3, '0001', NULL, NULL, NULL, NULL, 'test-m7l-us-act', $5), \
                    ('ZMV', NULL, $2, $3, '0003', 'DEPARTURE', $5, $5, 'ON TIME', 'test-m7l-us-origin', $5), \
                    ('ZMW', NULL, $2, $4, '0003', 'DEPARTURE', $6, $6, 'ON TIME', 'test-m7l-us-stop', $6)",
        )
        .bind(train_uid)
        .bind(train_id)
        .bind(origin_date)
        .bind(next_date)
        .bind(origin_departure)
        .bind(stop_departure)
        .execute(&pool)
        .await
        .expect("seed a split-date backlog train");

        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind(format!("{user_id}@example.com"))
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");
        let trains_id = crate::data::trains::find_or_create_train(&pool, train_uid, origin_date)
            .await
            .expect("find_or_create_train");
        let (tracked_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions (user_id, trains_id, service_date) \
             VALUES ($1, $2, $3) RETURNING id",
        )
        .bind(user_id)
        .bind(trains_id)
        .bind(origin_date)
        .fetch_one(&pool)
        .await
        .expect("seed an NR-primary subscription");

        let outcome = attempt_backlog_match_by_uid(&pool, tracked_train_id, train_uid, origin_date)
            .await
            .expect("attempt_backlog_match_by_uid")
            .expect("the Activation is in the backlog");
        assert_eq!(
            outcome.replayed_rows, 3,
            "the Activation and origin DEPARTURE under D, and the D+1-dated stop DEPARTURE"
        );
        assert_eq!(
            outcome.origin_departure,
            Some(("ZMV".to_string(), origin_departure))
        );
        assert_eq!(
            movement_count_for(&pool, train_uid, origin_date).await,
            Some(2),
            "both halves land on (uid, D)"
        );
        assert_eq!(
            movement_count_for(&pool, train_uid, next_date).await,
            None,
            "no (uid, D+1) row is created"
        );

        cleanup_absolute_time_fixture(&pool, &[user_id], &[train_id], train_uid).await;
    }

    /// DB2-5, applied to the uid path at integration (2026-09-27): a
    /// subscription already bound to a DIFFERENT train is never repointed
    /// by `attempt_backlog_match_by_uid`, and that train's history is not
    /// replayed onto it. Before the guard, the replay wrote this uid's
    /// movements onto the other train's shared row and a bare UPDATE then
    /// repointed the subscription.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                attempt_backlog_match_by_uid -- --ignored --test-threads=1`"]
    async fn attempt_backlog_match_by_uid_never_repoints_a_subscription_bound_to_another_train() {
        let pool = connect().await;
        let user_id = "TEST-DB25-BY-UID-USER";
        let train_id = "TEST-DB25-U-TID";
        let train_uid = "TEST-DB25-U";
        let other_uid = "TEST-DB25-OTHER";
        cleanup_absolute_time_fixture(&pool, &[user_id], &[train_id], train_uid).await;
        cleanup_absolute_time_fixture(&pool, &[], &[], other_uid).await;

        let day: NaiveDate = "2026-09-25".parse().unwrap();
        let departure: DateTime<Utc> = "2026-09-25T08:30:00Z".parse().unwrap();
        seed_backlog_run(&pool, train_id, train_uid, day, "ZDU", departure).await;

        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind(format!("{user_id}@example.com"))
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");
        let other_trains_id = crate::data::trains::find_or_create_train(&pool, other_uid, day)
            .await
            .expect("find_or_create_train (other)");
        let (tracked_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions (user_id, trains_id, service_date) \
             VALUES ($1, $2, $3) RETURNING id",
        )
        .bind(user_id)
        .bind(other_trains_id)
        .bind(day)
        .fetch_one(&pool)
        .await
        .expect("seed a subscription bound to another train");

        let outcome = attempt_backlog_match_by_uid(&pool, tracked_train_id, train_uid, day)
            .await
            .expect("attempt_backlog_match_by_uid");
        assert!(
            outcome.is_none(),
            "a subscription bound to another train is not replayed onto"
        );
        let bound: Option<i64> =
            sqlx::query_scalar("SELECT trains_id FROM train_subscriptions WHERE id = $1")
                .bind(tracked_train_id)
                .fetch_one(&pool)
                .await
                .expect("read back trains_id");
        assert_eq!(bound, Some(other_trains_id), "never repointed");
        assert_eq!(
            movement_count_for(&pool, other_uid, day).await,
            Some(0),
            "the other train's shared row gets none of this train's history"
        );
        assert_eq!(
            movement_count_for(&pool, train_uid, day).await.unwrap_or(0),
            0,
            "nothing is replayed at all"
        );

        cleanup_absolute_time_fixture(&pool, &[user_id], &[train_id], train_uid).await;
        cleanup_absolute_time_fixture(&pool, &[], &[], other_uid).await;
    }

    /// H4 residual (2026-10-01): cancel -> reinstate -> cancel -> reinstate
    /// on one day, stored as four backlog rows (the consumers now key each
    /// reinstatement by its own `reinstatement_timestamp`), replays in
    /// `received_at` order to a running train, and keeps all four as their
    /// own history rows. Before the fix the replay keyed both
    /// Cancellations, and both Reinstatements, identically (no planned
    /// time), so the shared history held one of each.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                repeat_cancel_and_reinstate_replays -- --ignored --test-threads=1`"]
    async fn repeat_cancel_and_reinstate_replays_in_order_and_ends_running() {
        let pool = connect().await;
        let user_id = "TEST-H4R-USER";
        let train_id = "TEST-H4R-TID";
        let train_uid = "TEST-H4R";
        cleanup_absolute_time_fixture(&pool, &[user_id], &[train_id], train_uid).await;

        let service_date: NaiveDate = "2026-09-21".parse().unwrap();
        let departure: DateTime<Utc> = "2026-09-21T09:00:00Z".parse().unwrap();
        seed_backlog_run(&pool, train_id, train_uid, service_date, "ZHR", departure).await;
        // The run's ARRIVAL would complete the journey; drop it so the
        // sequence below decides the final status.
        sqlx::query(
            "DELETE FROM trust_event_backlog WHERE train_id = $1 AND event_type = 'ARRIVAL'",
        )
        .bind(train_id)
        .execute(&pool)
        .await
        .expect("drop the arrival");
        for (minutes, msg_type) in [(5, "0002"), (10, "0005"), (15, "0002"), (20, "0005")] {
            let at = departure + Duration::minutes(minutes);
            sqlx::query(
                "INSERT INTO trust_event_backlog \
                    (crs, train_uid, train_id, service_date, msg_type, actual_timestamp, \
                     dedup_key, received_at) \
                 VALUES (NULL, NULL, $1, $2, $3, $4, $5, $4)",
            )
            .bind(train_id)
            .bind(service_date)
            .bind(msg_type)
            .bind(at)
            .bind(format!("test-h4r-{msg_type}-{minutes}"))
            .execute(&pool)
            .await
            .expect("seed the cancel/reinstate sequence");
        }

        let tracked_train_id =
            seed_pending_pin(&pool, user_id, service_date, "ZHR", departure).await;
        assert!(
            attempt_backlog_match(&pool, tracked_train_id, "ZHR", departure)
                .await
                .expect("attempt_backlog_match")
        );
        let status: String = sqlx::query_scalar(
            "SELECT cs.status FROM train_current_state cs \
             JOIN trains tr ON tr.id = cs.trains_id \
             WHERE tr.train_uid = $1 AND tr.service_date = $2",
        )
        .bind(train_uid)
        .bind(service_date)
        .fetch_one(&pool)
        .await
        .expect("read the replayed status");
        assert_eq!(status, "en_route", "the last reinstatement wins");
        assert_eq!(
            movement_count_for(&pool, train_uid, service_date).await,
            Some(5),
            "the departure plus all four cancel/reinstate rows"
        );

        cleanup_absolute_time_fixture(&pool, &[user_id], &[train_id], train_uid).await;
    }
}
