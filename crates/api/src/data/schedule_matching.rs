//! Schedule-first resolution of a tracked-train pin's `train_uid`, per
//! docs/superpowers/specs/2026-09-05-schedule-first-train-tracking-design.md.
//! Attempted once at pin-creation time (`routes::train::post_track`) and
//! again periodically for every still-`pending` row
//! (`run_schedule_match_sweep`, `main.rs`'s new background loop) -- both
//! paths funnel through `attempt_schedule_match`, the only place this
//! crate ever calls `schedule_query::match_pin`.

use std::collections::HashMap;

use chrono::{DateTime, Duration, NaiveDate, Utc};
use common::LineDefinition;
use schedule_query::LinePopulationEntry;
use serde::Serialize;
use sqlx::PgPool;

use crate::data::eta_blend::london_to_utc;
use crate::data::{queries, train_tracking};

/// `CRS -> Vec<line_id>` (Decision 2 of the design spec), built from the
/// static `lines/*.toml` catalogue. Indexes EVERY catalogued station's CRS
/// to its line(s) regardless of whether that station's TOML `tiploc` field
/// is set, and regardless of whether any OTHER station on the same line has
/// one either -- as of the 2026-09-09 tiploc-schedule-matching-gap fix, the
/// TOML `tiploc` field is purely documentation/display metadata (see
/// `lines/SCHEMA.md`) and is never load-bearing for this index or for
/// whether a line participates in schedule matching at all.
///
/// This index only ever needs to answer "which line(s) claim this CRS," not
/// "what is this CRS's real TIPLOC" -- `find_schedule_match`, below,
/// resolves the actual TIPLOC(s) to match against from the real,
/// CIF-derived data via `queries::list_stanox_crs_for_crs`, which does not
/// depend on this TOML catalogue at all. As of Task 3 of
/// docs/superpowers/plans/2026-09-24-tiploc-crs-crosswalk-plan.md,
/// `list_stanox_crs_for_crs` itself reads the UNION of `stanox_crs` and the
/// newer, richer `tiploc_crs` table (preferring a `tiploc_crs` row when a
/// TIPLOC is present in both), so the TIPLOC(s) `find_schedule_match`
/// matches against for a station like Vauxhall or Clapham Junction can now
/// include EVERY real calling-point TIPLOC sharing that station's STANOX,
/// not just whichever one `stanox_crs`'s one-row-per-STANOX schema
/// happened to keep. Gating this index
/// on the TOML `tiploc` field used to make it an inaccurate proxy for "does
/// this station appear in real CIF data" -- ~83% of catalogued CRS codes
/// have no TOML `tiploc` set (39 of 109 `lines/*.toml` files have none at
/// all), so that gate silently excluded the vast majority of stations from
/// ever schedule-matching, producing a permanently-stuck "Waiting to hear
/// from Network Rail" status for any pin at one of them even though the
/// real `stanox_crs` table had everything needed to match it. Built once
/// at `AppState::init` from `app.config.lines` (already loaded there for
/// `full_coverage_enabled_for`'s own use -- this is a pure re-keying of
/// data already in memory, no new I/O).
pub fn crs_to_line_ids(lines: &[LineDefinition]) -> HashMap<String, Vec<String>> {
    let mut index: HashMap<String, Vec<String>> = HashMap::new();
    for line in lines {
        for station in &line.stations {
            let crs = station.crs.to_uppercase();
            let ids = index.entry(crs).or_default();
            if !ids.contains(&line.id) {
                ids.push(line.id.clone());
            }
        }
    }
    index
}

/// camelCase wire shape for one calling point, converted from
/// `schedule_query::CallingPoint` (whose own JSON keys are snake_case --
/// see this task's own note) BEFORE storage, so `schedule_calling_points`
/// is stored already camelCase and the read path can relay it verbatim.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ScheduleCallingPointDto {
    tiploc: String,
    kind: schedule_query::CallingPointKind,
    booked_arrival: Option<chrono::NaiveTime>,
    booked_departure: Option<chrono::NaiveTime>,
    is_half_minute_arrival: bool,
    is_half_minute_departure: bool,
    /// Carried through verbatim from `schedule_query::CallingPoint::day_offset`
    /// -- see that field's own doc comment. Stored on `trains.calling_points`
    /// so `journey::JourneyStop::from_calling_point` (the only reader of
    /// this JSONB blob) can correctly date a post-midnight calling point of
    /// an overnight schedule instead of unconditionally stamping every stop
    /// with the schedule's own `service_date`.
    day_offset: u8,
}

impl From<&schedule_query::CallingPoint> for ScheduleCallingPointDto {
    fn from(cp: &schedule_query::CallingPoint) -> Self {
        Self {
            tiploc: cp.tiploc.clone(),
            kind: cp.kind,
            booked_arrival: cp.booked_arrival,
            booked_departure: cp.booked_departure,
            is_half_minute_arrival: cp.is_half_minute_arrival,
            is_half_minute_departure: cp.is_half_minute_departure,
            day_offset: cp.day_offset,
        }
    }
}

/// One pin's schedule-match attempt (Decision 3 steps 1-5), called both
/// synchronously at creation (`routes::train::post_track`, Task 7) and
/// periodically for every still-`pending` row (`run_schedule_match_sweep`,
/// Task 8). Iterates `crs_line_index`'s candidate lines for
/// `pin_origin_crs` IN A FIXED ORDER and returns on the FIRST candidate
/// line whose own population yields any match at all (this plan's Open
/// Question 3 resolution: trusts that a second candidate line, if any,
/// would resolve the same UID/date identically, so there is nothing to
/// gain from fetching every candidate and reconciling). Calls
/// `find_schedule_match` with `expected_uid: None` -- this path has no
/// known identity to check a candidate against (discovering the identity
/// IS the point), so the first-candidate-wins heuristic above is the whole
/// contract here. Contrast [`attempt_schedule_match_for_shared_train`],
/// which DOES have a known identity and therefore passes `Some(train_uid)`
/// instead -- see that function's own doc comment for why "first candidate
/// line with any tolerance match" is the wrong contract once a caller
/// already knows which uid it wants.
///
/// Returns `Ok(true)` only if a match was found AND actually written
/// (i.e. the row was still eligible -- see `apply_schedule_match`'s own
/// guard). `Ok(false)` covers every other honest "still pending" outcome
/// uniformly: no candidate line, no `stanox_crs` rows for this CRS, no
/// `schedule_line_population` published yet for any candidate, or no
/// calling point within tolerance.
#[allow(clippy::too_many_arguments)]
pub async fn attempt_schedule_match(
    pool: &PgPool,
    tracked_train_id: i64,
    pin_origin_crs: &str,
    pin_scheduled_departure: DateTime<Utc>,
    service_date: NaiveDate,
    crs_line_index: &HashMap<String, Vec<String>>,
    // Darwin's own explicit skipped-calling-point snapshot, captured at
    // pin creation time from whichever departure-board row the user picked
    // (`common::TrackPinRequest.skipped_stations`) and carried through the
    // pending row's own `train_subscriptions.pin_skipped_stations` column
    // by every caller of this function (`post_track`'s synchronous
    // attempt, and `run_schedule_match_sweep`'s retry of the same pending
    // row) -- an empty slice for a pin with no such signal (the CIF-picker
    // or manual-entry path, or an older frontend build). See
    // `data::trains::find_or_create_train_with_schedule_match`'s own doc
    // comment for where this ends up.
    pin_skipped_stations: &[String],
    // Same idea as `pin_skipped_stations` immediately above, for the
    // picked row's origin-platform snapshot instead
    // (`common::TrackPinRequest.platform`/`planned_platform`, carried
    // through `train_subscriptions.pin_platform`/`pin_planned_platform`).
    pin_platform: Option<&str>,
    pin_planned_platform: Option<&str>,
) -> anyhow::Result<bool> {
    let Some(matched) = find_schedule_match(
        pool,
        pin_origin_crs,
        pin_scheduled_departure,
        service_date,
        crs_line_index,
        // No known identity yet -- discovering it IS the point of this
        // path, so the first candidate line with any tolerance match wins,
        // same as always. See `find_schedule_match`'s own doc comment.
        None,
    )
    .await?
    else {
        return Ok(false);
    };

    let matched_ok = train_tracking::apply_schedule_match(pool, tracked_train_id).await?;

    if matched_ok {
        // Step A dual-write (docs/superpowers/specs/2026-09-06-shared-train-identity-design.md
        // §2 Step A): mirror this schedule match onto the shared `trains`
        // row too, and point this subscription at it.
        let trains_id = crate::data::trains::find_or_create_train_with_schedule_match(
            pool,
            &matched.uid,
            service_date,
            pin_origin_crs,
            pin_scheduled_departure,
            matched.destination_crs.as_deref(),
            &matched.line_id,
            &matched.calling_points_json,
            pin_skipped_stations,
            pin_platform,
            pin_planned_platform,
        )
        .await?;
        sqlx::query("UPDATE train_subscriptions SET trains_id = $2 WHERE id = $1")
            .bind(tracked_train_id)
            .bind(trains_id)
            .execute(pool)
            .await?;
    }

    Ok(matched_ok)
}

/// What a successful `schedule_query::match_pin` lookup produced, already
/// resolved into the exact shapes the two writers below need. Extracted so
/// the read half of a schedule match (which is entirely a function of
/// `(origin CRS, departure time, date)`) is reusable by the shared-`trains`
/// writer as well as the per-subscription one, rather than being welded
/// into `attempt_schedule_match`'s own `train_subscriptions`-guarded write.
#[derive(Debug, Clone)]
pub struct ScheduleMatch {
    /// CIF's own identifier for the matched schedule. The caller of
    /// [`attempt_schedule_match_for_shared_train`] already knows the real
    /// `train_uid`, so it can (and does) reject a match whose `uid`
    /// disagrees -- a stronger check than the +/-20-minute CRS+time
    /// heuristic can make on its own.
    pub uid: String,
    pub line_id: String,
    pub destination_crs: Option<String>,
    pub calling_points_json: serde_json::Value,
}

/// The pure read half of a schedule match: no writes of any kind. Same
/// candidate-line iteration, same `MATCH_TOLERANCE`, same
/// first-line-that-matches-wins rule `attempt_schedule_match` has always
/// had when `expected_uid` is `None` -- that part of this code is lifted
/// out unchanged.
///
/// `expected_uid`: `None` for `attempt_schedule_match`'s legacy pin path,
/// which has no identity to check against (discovering it IS the point) --
/// the very first candidate line with ANY tolerance match wins, exactly as
/// before.
///
/// `Some(train_uid)` for [`attempt_schedule_match_for_shared_train`], which
/// already knows the real identity. Two distinct bugs have been found and
/// fixed on this path, in two rounds, both described below newest-first.
/// Round 3 is the one that actually kept train `Y80908` schedule-less in
/// production; round 2 is a real bug of the same family that simply was
/// not the one biting this train.
///
/// ---
///
/// **Round 3 (2026-09-24), the real `Y80908` root cause: an exact-minute
/// tie inside ONE candidate line's population.** Measured against live
/// production data, not reasoned about: `Y80908` (Birmingham New Street
/// 16:06 -> London Euston 18:18, 2026-09-24) has a perfectly good CIF
/// schedule -- `GET /public/trains/search?station=BHM&date=2026-09-24`
/// returns it, with `true_origin_crs` `BHM` -- and
/// `W75898` (Birmingham New Street -> Lichfield) departs Birmingham New
/// Street in the SAME MINUTE, 16:06. 32 further services depart within
/// `common::MATCH_TOLERANCE` of it.
///
/// `schedule_query::match_pin` returns the single globally-closest entry of
/// the population it is given and has no uid concept at all; on a tie it
/// keeps whichever entry it saw first. The correct schedule's delta here is
/// always exactly zero (`true_origin_departure`'s
/// `schedule_destination_departures.scheduled` and this function's
/// `schedule_line_population` `booked_departure` are two products of the
/// same CIF delivery describing the same calling point), so the correct
/// schedule can only ever tie for best -- never win outright. When the tie
/// goes the other way, `match_pin` hands back `W75898`, the uid check
/// rejects it, and the correct schedule is unreachable.
///
/// Round 2's candidate-line loop (below) cannot rescue this, which is
/// exactly why deploying it changed nothing for `Y80908`: `schedules_touching`
/// puts EVERY schedule calling at a station into the population of EVERY
/// line that lists that station, so `W75898` wins the same tie on every
/// candidate line for `BHM`. The loop just exhausts them all.
///
/// The live population-level confirmation, over the 267 schedules whose
/// true origin is `BHM` on 2026-09-24: of a 10-sample of the 161 with a
/// departure minute they hold alone, 10/10 had a matched schedule
/// (`originCrs` set); of a 10-sample of the 106 sharing their departure
/// minute with another service, 8/10 had none. The two exceptions are the
/// ones that happened to win their own tie. This is not a `Y80908` quirk --
/// it silently cost roughly 40% of every busy station's schedules.
///
/// Fixed by filtering the population to `expected_uid` BEFORE matching (see
/// the code below), so an identity-known match is actually identity-scoped
/// rather than a proximity contest the right answer can only draw. The uid
/// check further down is consequently unreachable now, and kept only as a
/// cheap invariant guard.
///
/// ---
///
/// **Round 2 (2026-09-24), a real bug, but not `Y80908`'s**:
/// `pin_origin_crs` can easily
/// have several candidate lines (`crs_line_index`'s `CRS -> Vec<line_id>`,
/// built from every `lines/*.toml` file that lists the station -- Birmingham
/// New Street alone appears on a dozen of them, from `cross-country` to
/// `wmr-cross-city`). The OLD code returned on the FIRST candidate line
/// whose population had ANY entry within the wide (20-minute,
/// `common::MATCH_TOLERANCE`) tolerance of the pin's own time, uid
/// unchecked -- fine for the untargeted legacy path, but wrong here: at a
/// busy multi-line terminus it is entirely normal for some OTHER real
/// service on an earlier-iterated candidate line (alphabetically,
/// `cross-country.toml` sorts before `lnwr-birmingham-crewe.toml`, the line
/// Y80908 actually runs on) to have a departure within 20 minutes of
/// Y80908's own. The caller only ever tried that ONE candidate line, saw
/// the uid disagreed, and gave up entirely -- `attempt_schedule_match_for_shared_train`'s
/// own uid check has always existed as a correctness guard, but nothing
/// ever gave it a SECOND candidate to check, so the guard itself is what
/// silently and permanently discarded the correct match. Now: when a
/// candidate line's match disagrees with `expected_uid`, this keeps
/// searching the REMAINING candidate lines instead of returning that wrong
/// match -- only `Ok(None)` (or a line whose match genuinely agrees) ends
/// the search. The untargeted (`None`) path is completely unaffected: it
/// still returns on the very first tolerance match, same as always.
///
/// Correction to that round's own account, for the record: it named
/// `Y80908` as its motivating case and guessed its route/line from the
/// symptom. Round 3 measured the real data -- `Y80908` runs Birmingham New
/// Street 16:06 to London Euston, and its rival for the match is on the
/// SAME line's population, not an earlier candidate line. Round 2 fixes a
/// genuine bug (a schedule reachable only from a later candidate line was
/// unreachable), and its own regression test exercises exactly that shape;
/// it simply was never what `Y80908` was hitting.
async fn find_schedule_match(
    pool: &PgPool,
    pin_origin_crs: &str,
    pin_scheduled_departure: DateTime<Utc>,
    service_date: NaiveDate,
    crs_line_index: &HashMap<String, Vec<String>>,
    expected_uid: Option<&str>,
) -> anyhow::Result<Option<ScheduleMatch>> {
    let Some(candidate_lines) = crs_line_index.get(&pin_origin_crs.to_uppercase()) else {
        return Ok(None);
    };

    let origin_tiplocs = queries::list_stanox_crs_for_crs(pool, pin_origin_crs).await?;
    if origin_tiplocs.is_empty() {
        return Ok(None);
    }
    let tiplocs: Vec<&str> = origin_tiplocs.iter().map(|r| r.tiploc.as_str()).collect();

    for line_id in candidate_lines {
        let Some(json) = queries::get_schedule_line_population(pool, line_id, service_date).await?
        else {
            continue;
        };
        let entries: Vec<LinePopulationEntry> = serde_json::from_value(json)?;

        // **The whole fix for the real Y80908 production bug** (2026-09-24
        // round-3 investigation -- see this function's own doc comment).
        // When the caller already knows which identity it wants, narrow the
        // population to THAT schedule BEFORE matching, instead of matching
        // across the whole line and checking the winner's uid afterwards.
        //
        // `schedule_query::match_pin` returns the single globally-CLOSEST
        // entry in the population it is handed, and knows nothing about
        // uids: on an exact tie it keeps the first entry it saw (its
        // `*best_delta <= delta` comparison). The correct schedule's own
        // delta is always exactly zero here -- `true_origin_departure` reads
        // `schedule_destination_departures.scheduled` and this reads
        // `schedule_line_population`'s `booked_departure`, two products
        // published from the SAME CIF delivery for the same calling point --
        // so the correct schedule can only ever TIE for best, never beat a
        // rival outright. Any other service departing the same station in
        // the same minute therefore wins the tie roughly half the time, and
        // the caller's uid check then threw the correct match away.
        // Filtering first removes the rivalry entirely.
        let entries: Vec<LinePopulationEntry> = match expected_uid {
            Some(expected) => entries
                .into_iter()
                .filter(|entry| entry.uid == expected)
                .collect(),
            None => entries,
        };

        let Some(matched) = schedule_query::match_pin(
            &entries,
            &tiplocs,
            pin_scheduled_departure,
            common::MATCH_TOLERANCE,
            // day_offset (see `schedule_query::CallingPoint::day_offset`)
            // shifts the base date forward for a calling point that falls
            // on a calendar day AFTER the schedule's own `service_date` --
            // required for a real overnight service (2026-09-09
            // investigation: c2c UID F49687 crosses midnight between
            // Stratford and Barking). Without this, every calling point
            // after a midnight crossing matched against the WRONG day,
            // permanently stuck "Waiting to hear from Network Rail" for any
            // pin on one of them.
            |t, day_offset| {
                london_to_utc((service_date + Duration::days(day_offset as i64)).and_time(t))
            },
        ) else {
            continue;
        };

        if let Some(expected) = expected_uid
            && matched.uid != expected
        {
            // Unreachable as of the round-3 fix above: the population this
            // matched against was already filtered to `expected`, so
            // `matched.uid` cannot disagree. Kept as a cheap invariant
            // guard -- if that filter is ever loosened, this restores
            // round 2's behavior (keep searching the remaining candidate
            // lines) rather than silently writing another train's calling
            // points onto this shared row. See this function's own doc
            // comment for both rounds' full account.
            tracing::debug!(
                expected_uid = expected,
                matched_uid = matched.uid,
                line_id = %line_id,
                "schedule match on this candidate line resolved a different uid; trying the \
                 next candidate line instead of giving up"
            );
            continue;
        }

        let calling_points: Vec<ScheduleCallingPointDto> = matched
            .calling_points
            .iter()
            .map(ScheduleCallingPointDto::from)
            .collect();
        let calling_points_json = serde_json::to_value(&calling_points)?;

        let destination_crs = match matched.calling_points.last() {
            Some(cp) => {
                queries::crs_for_tiploc(pool, schedule_query::normalize_tiploc(&cp.tiploc)).await?
            }
            None => None,
        }
        // See `queries::is_bookable_crs`'s own doc comment: this value
        // flows straight into `trains.destination_crs`, which the frontend
        // renders as `train.destinationName ?? train.destinationCrs` on the
        // single-train page (`frontend/lib/types.ts`'s `TrainDetail`) -- an
        // X-prefixed pseudo-CRS must not survive into that field any more
        // than it survives into a journey stop's own `crs`
        // (`journey::stops_from_calling_points`, the original call site of
        // this same filter). Blanked to `None` here, the same "treat like
        // unresolved" degrade every other call site uses.
        .filter(|crs| queries::is_bookable_crs(crs));

        return Ok(Some(ScheduleMatch {
            uid: matched.uid.clone(),
            line_id: line_id.clone(),
            destination_crs,
            calling_points_json,
        }));
    }

    Ok(None)
}

/// Fills in the SHARED `trains` row's schedule columns for a train whose
/// identity is already known -- the NR-primary path
/// (`POST /Train/by-uid/{uid}/{date}/track`), which starts from a real
/// `train_uid` and therefore never goes through the per-subscription
/// `pending -> schedule_matched` dance [`attempt_schedule_match`] drives.
///
/// This exists because [`attempt_schedule_match`] cannot serve that path at
/// all: its write is gated on `train_tracking::apply_schedule_match`, whose
/// `WHERE ... trains_id IS NULL AND resolution_status = 'pending'` is false
/// by construction for a subscription created by
/// `create_subscription_for_train` (which sets `trains_id` immediately).
/// The result was that an NR-primary subscription never acquired origin,
/// destination or calling points from ANY path -- review finding I1.
///
/// `origin_crs`/`scheduled_departure` are not the caller's own pin (there
/// isn't one); they come from the train's replayed TRUST history -- see
/// `trust_event_backlog_match::attempt_backlog_match_by_uid`'s
/// `origin_departure`.
///
/// Refuses a match whose `uid` isn't the `train_uid` we already know. The
/// CRS+time heuristic can legitimately land on a different service at a
/// busy terminus; for this path that is not an acceptable outcome, because
/// the write below would attach another train's calling points to THIS
/// train's shared row, visible to every subscriber of it. Returns
/// `Ok(false)` for that, same as for "no match at all".
///
/// Passes `Some(train_uid)` as `find_schedule_match`'s `expected_uid` --
/// see that function's own doc comment for the two real production bugs
/// (train `Y80908`, 2026-09-24) this closes. As of round 3 it narrows each
/// candidate line's population to `train_uid` BEFORE matching, so the
/// correct schedule can no longer lose an exact-minute tie to an unrelated
/// service departing the same station in the same minute (the actual
/// `Y80908` failure); as of round 2 it also keeps trying the remaining
/// candidate lines rather than giving up on the first one. The check just
/// below is therefore a defense-in-depth invariant now, not the primary
/// correctness mechanism it used to be -- `find_schedule_match` itself
/// should never hand this function a disagreeing uid any more; if this
/// branch ever fires, treat it as a bug in that guarantee, not an expected
/// "busy terminus" outcome.
///
/// Writes through `find_or_create_train_with_schedule_match`, whose every
/// column is `COALESCE`d against the existing value -- so this can never
/// clobber schedule data an earlier match already wrote, and is safe to
/// call repeatedly.
pub async fn attempt_schedule_match_for_shared_train(
    pool: &PgPool,
    train_uid: &str,
    origin_crs: &str,
    scheduled_departure: DateTime<Utc>,
    service_date: NaiveDate,
    crs_line_index: &HashMap<String, Vec<String>>,
) -> anyhow::Result<bool> {
    let Some(matched) = find_schedule_match(
        pool,
        origin_crs,
        scheduled_departure,
        service_date,
        crs_line_index,
        Some(train_uid),
    )
    .await?
    else {
        return Ok(false);
    };

    if matched.uid != train_uid {
        tracing::warn!(
            train_uid,
            matched_uid = matched.uid,
            origin_crs,
            "schedule match for a known-identity train resolved a DIFFERENT uid after checking \
             every candidate line -- this should be unreachable now that find_schedule_match \
             enforces expected_uid itself; discarding rather than writing another train's \
             calling points onto this shared row"
        );
        return Ok(false);
    }

    crate::data::trains::find_or_create_train_with_schedule_match(
        pool,
        train_uid,
        service_date,
        origin_crs,
        scheduled_departure,
        matched.destination_crs.as_deref(),
        &matched.line_id,
        &matched.calling_points_json,
        // The NR-primary path has no departure-board pin at all (see this
        // function's own doc comment) -- nothing to capture a Darwin skip
        // (or platform) snapshot from.
        &[],
        None,
        None,
    )
    .await?;
    Ok(true)
}

/// The periodic sweep's own entry point (Decision 3's "also run this same
/// attempt periodically"): re-runs `attempt_schedule_match` against every
/// still-`pending`, never-matched row. A single row's failure (e.g. a
/// malformed `schedule_line_population` JSONB for one line) is logged and
/// skipped, not propagated -- one bad row must never stop the sweep from
/// making progress on every other row. Returns the count of rows this
/// call actually matched, for the caller's own logging.
///
/// `PendingSchedulePin`'s `pin_origin_crs`/`pin_scheduled_departure` are
/// `Option`, not bare `String`/`DateTime<Utc>` (see that struct's own doc
/// comment for why -- a pruned NR-primary subscription can reach this
/// query with NULL pin columns). `list_pending_pins_for_schedule_match`'s
/// own `WHERE` already excludes such rows, so the `None` arm below should
/// never actually run; it exists so a row that somehow slips through is
/// skipped rather than panicking this whole sweep.
pub async fn run_schedule_match_sweep(
    pool: &PgPool,
    crs_line_index: &HashMap<String, Vec<String>>,
) -> anyhow::Result<u64> {
    let rows = train_tracking::list_pending_pins_for_schedule_match(pool).await?;
    let mut matched = 0u64;
    for row in rows {
        let (Some(pin_origin_crs), Some(pin_scheduled_departure)) =
            (row.pin_origin_crs.as_deref(), row.pin_scheduled_departure)
        else {
            tracing::warn!(
                tracked_train_id = row.id,
                "pending schedule pin missing origin CRS or scheduled departure; skipping \
                 (list_pending_pins_for_schedule_match should have already excluded this row)"
            );
            continue;
        };
        match attempt_schedule_match(
            pool,
            row.id,
            pin_origin_crs,
            pin_scheduled_departure,
            row.service_date,
            crs_line_index,
            &row.pin_skipped_stations,
            row.pin_platform.as_deref(),
            row.pin_planned_platform.as_deref(),
        )
        .await
        {
            Ok(true) => matched += 1,
            Ok(false) => {}
            Err(err) => {
                tracing::warn!(
                    error = ?err,
                    tracked_train_id = row.id,
                    "schedule match attempt failed for this pin; will retry next sweep"
                );
            }
        }
    }
    Ok(matched)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(id: &str, stations: Vec<(&str, Option<&str>)>) -> LineDefinition {
        LineDefinition {
            id: id.to_string(),
            name: id.to_string(),
            mode: "rail".to_string(),
            category: "national-rail".to_string(),
            operators: vec![],
            stations: stations
                .into_iter()
                .map(|(crs, tiploc)| common::Station {
                    crs: crs.to_string(),
                    tiploc: tiploc.map(str::to_string),
                    role: "minor".to_string(),
                    segment: None,
                })
                .collect(),
            sample_stations: vec![],
            match_keywords: vec![],
            excluded_keywords: vec![],
            severity_overrides: std::collections::HashMap::new(),
            exclusive_segments: vec![],
            destination_crs_filter: vec![],
            headcode_prefixes: vec![],
            full_coverage_enabled: false,
        }
    }

    #[test]
    fn a_tiploc_bearing_station_maps_its_crs_to_its_line() {
        let lines = vec![line("wcml", vec![("EUS", Some("EUSTON"))])];
        let index = crs_to_line_ids(&lines);
        assert_eq!(index.get("EUS"), Some(&vec!["wcml".to_string()]));
    }

    #[test]
    fn a_crs_with_no_tiploc_on_its_station_entry_is_indexed_anyway() {
        // The bug this rewritten test now guards against: the TOML `tiploc`
        // field is documentation/display metadata only, never load-bearing
        // for whether a station participates in schedule matching. A
        // station with no TOML `tiploc` set must still be indexed -- its
        // real TIPLOC(s), if any, are resolved separately from the
        // CIF-derived `stanox_crs` table (see `find_schedule_match`).
        let lines = vec![line("wcml", vec![("EUS", Some("EUSTON")), ("ZZZ", None)])];
        let index = crs_to_line_ids(&lines);
        assert_eq!(index.get("ZZZ"), Some(&vec!["wcml".to_string()]));
    }

    #[test]
    fn a_crs_shared_by_two_lines_maps_to_both() {
        let lines = vec![
            line("line-a", vec![("EUS", Some("EUSTON"))]),
            line("line-b", vec![("EUS", Some("EUSTON"))]),
        ];
        let index = crs_to_line_ids(&lines);
        let mut ids = index.get("EUS").cloned().unwrap_or_default();
        ids.sort();
        assert_eq!(ids, vec!["line-a".to_string(), "line-b".to_string()]);
    }

    #[test]
    fn a_line_with_no_tiploc_bearing_station_at_all_is_included_anyway() {
        // Previously this line was dropped from the index entirely -- the
        // exact bug that left every station on a fully tiploc-less line
        // (39 of 109 `lines/*.toml` files, e.g. all of ScotRail,
        // Southeastern, Merseyrail) permanently unable to schedule-match.
        let lines = vec![line("no-tiploc-line", vec![("ZZA", None), ("ZZB", None)])];
        let index = crs_to_line_ids(&lines);
        assert_eq!(index.get("ZZA"), Some(&vec!["no-tiploc-line".to_string()]));
        assert_eq!(index.get("ZZB"), Some(&vec!["no-tiploc-line".to_string()]));
    }

    #[test]
    fn a_lowercase_crs_on_a_station_entry_is_indexed_uppercased() {
        let lines = vec![line("wcml", vec![("eus", Some("EUSTON"))])];
        let index = crs_to_line_ids(&lines);
        assert_eq!(index.get("EUS"), Some(&vec!["wcml".to_string()]));
    }
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

    fn population_json(uid: &str, tiploc: &str, departure: &str) -> serde_json::Value {
        population_json_multi(&[(uid, tiploc, departure)])
    }

    /// [`population_json`]'s many-schedule sibling: ONE line's population
    /// carrying several schedules, in the given order. Needed because the
    /// real `Y80908` bug lives entirely INSIDE one line's population (two
    /// services departing the same station in the same minute), not across
    /// two lines -- see `find_schedule_match`'s own doc comment.
    fn population_json_multi(entries: &[(&str, &str, &str)]) -> serde_json::Value {
        serde_json::Value::Array(
            entries
                .iter()
                .map(|(uid, tiploc, departure)| {
                    serde_json::json!({
                        "uid": uid,
                        "calling_points": [{
                            "tiploc": tiploc,
                            "kind": "Origin",
                            "booked_arrival": null,
                            "booked_departure": departure,
                            "is_half_minute_arrival": false,
                            "is_half_minute_departure": false
                        }]
                    })
                })
                .collect(),
        )
    }

    #[tokio::test]
    #[ignore = "requires a live database; see this plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p api \
                attempt_schedule_match -- --ignored --test-threads=1`"]
    async fn attempt_schedule_match_reproduces_the_eus_bug_and_now_resolves_it() {
        let pool = connect().await;
        let user_id = "TEST-SCHEDULE-MATCH-EUS";
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind("schedule-match@example.com")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");

        sqlx::query(
            "INSERT INTO stanox_crs (stanox, crs, tiploc, station_name, source_sequence) \
             VALUES ('TEST-EUS-STANOX', 'EUS', 'EUSTON', 'LONDON EUSTON', 1) \
             ON CONFLICT (stanox) DO NOTHING",
        )
        .execute(&pool)
        .await
        .expect("seed stanox_crs");

        let service_date: chrono::NaiveDate = "2026-09-05".parse().unwrap();
        sqlx::query(
            "INSERT INTO schedule_line_population (line_id, service_date, population) \
             VALUES ('west-coast-main-line', $1, $2) \
             ON CONFLICT (line_id, service_date) DO UPDATE SET population = EXCLUDED.population",
        )
        .bind(service_date)
        .bind(population_json("C99999", "EUSTON ", "19:15"))
        .execute(&pool)
        .await
        .expect("seed schedule_line_population");

        // The exact reported bug: a pin created more than an hour after
        // its train's own origin-departure window (the pin's own
        // scheduled_departure is still 19:15 -- what changes is that no
        // live TRUST Movement for it will ever arrive within this
        // process's test window, exactly mirroring "pinned an hour late,
        // TRUST's own ±20-minute window already closed").
        let scheduled_departure: chrono::DateTime<chrono::Utc> =
            "2026-09-05T19:15:00+01:00".parse().unwrap(); // BST -> 18:15 UTC
        let (tracked_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions (user_id, service_date, pin_origin_crs, pin_scheduled_departure) \
             VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(user_id)
        .bind(service_date)
        .bind("EUS")
        .bind(scheduled_departure)
        .fetch_one(&pool)
        .await
        .expect("seed fixture tracked_trains row");

        let mut crs_line_index = HashMap::new();
        crs_line_index.insert("EUS".to_string(), vec!["west-coast-main-line".to_string()]);

        let matched = attempt_schedule_match(
            &pool,
            tracked_train_id,
            "EUS",
            scheduled_departure,
            service_date,
            &crs_line_index,
            &[],
            None,
            None,
        )
        .await
        .expect("attempt schedule match");
        assert!(matched, "the pin should schedule-match against C99999");

        let state = train_tracking::get_by_tracking_id(&pool, tracked_train_id)
            .await
            .expect("read tracked train")
            .expect("tracked train exists");
        assert_eq!(state.resolution_status, "schedule_matched");
        assert_eq!(state.train_uid, Some("C99999".to_string()));
        assert_eq!(state.train_id, None, "train_id must stay TRUST-exclusive");

        sqlx::query("DELETE FROM schedule_line_population WHERE line_id = 'west-coast-main-line' AND service_date = $1")
            .bind(service_date)
            .execute(&pool)
            .await
            .expect("cleanup population");
        sqlx::query("DELETE FROM stanox_crs WHERE stanox = 'TEST-EUS-STANOX'")
            .execute(&pool)
            .await
            .expect("cleanup stanox_crs");
        sqlx::query("DELETE FROM train_subscriptions WHERE user_id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("cleanup tracked_trains");
        // Step A's dual-write (attempt_schedule_match's own
        // find_or_create_train_with_schedule_match call) creates a shared
        // `trains` row for this identity too -- discovered as a real
        // cross-test leak during Task 8's own end-to-end verification: this
        // C99999/2026-09-05 identity is shared with
        // `trust_event_backlog_match::db_tests`'s own EUS fixture, and
        // neither test used to clean up its `trains` row, so whichever ran
        // second inherited the first's leftover `train_id`. Now that Step C
        // reads `train_id` through this row, an uncleaned leftover silently
        // corrupts an unrelated test's assertion. See the same fix applied
        // to `trust_event_backlog_match.rs`.
        sqlx::query("DELETE FROM trains WHERE train_uid = 'C99999' AND service_date = $1")
            .bind(service_date)
            .execute(&pool)
            .await
            .expect("cleanup trains");
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("cleanup user");
    }

    /// The exact live-confirmed midnight-crossing bug (2026-09-09
    /// investigation: c2c UID F49687, service_date 2026-09-05, Liverpool
    /// Street 23:48 -> Stratford 23:54/55 -> Barking 00:06/00:07 -> ... ->
    /// Shoeburyness 01:01 -- every calling point from Barking onward is
    /// really 2026-09-06 wall-clock), reproduced end to end through
    /// `attempt_schedule_match`: a pin dated with Barking's REAL actual
    /// calendar day (2026-09-06) must schedule-match against a population
    /// entry whose Barking calling point carries `day_offset: 1` relative
    /// to the schedule's own `service_date` (2026-09-05). Before this fix,
    /// `find_schedule_match`'s `to_utc` closure ignored `day_offset`
    /// entirely, so this pin -- correctly dated a full day after
    /// `service_date` -- would never land within `MATCH_TOLERANCE` of a
    /// candidate silently mis-stamped a day earlier, permanently stuck
    /// "Waiting to hear from Network Rail".
    #[tokio::test]
    #[ignore = "requires a live database; see this plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p api \
                attempt_schedule_match -- --ignored --test-threads=1`"]
    async fn attempt_schedule_match_matches_a_post_midnight_calling_point_via_its_day_offset() {
        let pool = connect().await;
        let user_id = "TEST-SCHEDULE-MATCH-MIDNIGHT";
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind("schedule-match-midnight@example.com")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");

        sqlx::query(
            "INSERT INTO stanox_crs (stanox, crs, tiploc, station_name, source_sequence) \
             VALUES ('TEST-BKG-STANOX', 'ZBK', 'BARKING', 'BARKING', 1) \
             ON CONFLICT (stanox) DO NOTHING",
        )
        .execute(&pool)
        .await
        .expect("seed stanox_crs");

        let service_date: chrono::NaiveDate = "2026-09-05".parse().unwrap();
        let population = serde_json::json!([{
            "uid": "TEST-F49687",
            "calling_points": [
                {
                    "tiploc": "LIVST  ",
                    "kind": "Origin",
                    "booked_arrival": null,
                    "booked_departure": "23:48:00",
                    "is_half_minute_arrival": false,
                    "is_half_minute_departure": false,
                    "day_offset": 0
                },
                {
                    "tiploc": "BARKING",
                    "kind": "Intermediate",
                    "booked_arrival": "00:06:00",
                    "booked_departure": "00:07:00",
                    "is_half_minute_arrival": false,
                    "is_half_minute_departure": false,
                    "day_offset": 1
                }
            ]
        }]);
        sqlx::query(
            "INSERT INTO schedule_line_population (line_id, service_date, population) \
             VALUES ('test-c2c-line', $1, $2) \
             ON CONFLICT (line_id, service_date) DO UPDATE SET population = EXCLUDED.population",
        )
        .bind(service_date)
        .bind(&population)
        .execute(&pool)
        .await
        .expect("seed schedule_line_population");

        // Barking's REAL booked_departure is 2026-09-06 00:07 Europe/London
        // (BST) = 2026-09-05T23:07:00Z -- a full calendar day after the
        // schedule's own service_date (2026-09-05), which is exactly what
        // day_offset: 1 says. The pin is dated with this REAL, correct
        // instant, as a genuine tracked-train pin would be.
        let scheduled_departure: chrono::DateTime<chrono::Utc> =
            "2026-09-06T00:07:00+01:00".parse().unwrap();
        let (tracked_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions (user_id, service_date, pin_origin_crs, pin_scheduled_departure) \
             VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(user_id)
        .bind(service_date)
        .bind("ZBK")
        .bind(scheduled_departure)
        .fetch_one(&pool)
        .await
        .expect("seed fixture tracked_trains row");

        let mut crs_line_index = HashMap::new();
        crs_line_index.insert("ZBK".to_string(), vec!["test-c2c-line".to_string()]);

        let matched = attempt_schedule_match(
            &pool,
            tracked_train_id,
            "ZBK",
            scheduled_departure,
            service_date,
            &crs_line_index,
            &[],
            None,
            None,
        )
        .await
        .expect("attempt schedule match");
        assert!(
            matched,
            "a pin correctly dated on Barking's REAL calendar day must schedule-match against \
             TEST-F49687's day_offset: 1 calling point"
        );

        let state = train_tracking::get_by_tracking_id(&pool, tracked_train_id)
            .await
            .expect("read tracked train")
            .expect("tracked train exists");
        assert_eq!(state.resolution_status, "schedule_matched");
        assert_eq!(state.train_uid, Some("TEST-F49687".to_string()));

        sqlx::query("DELETE FROM schedule_line_population WHERE line_id = 'test-c2c-line' AND service_date = $1")
            .bind(service_date)
            .execute(&pool)
            .await
            .expect("cleanup population");
        sqlx::query("DELETE FROM stanox_crs WHERE stanox = 'TEST-BKG-STANOX'")
            .execute(&pool)
            .await
            .expect("cleanup stanox_crs");
        sqlx::query("DELETE FROM train_subscriptions WHERE user_id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("cleanup tracked_trains");
        sqlx::query("DELETE FROM trains WHERE train_uid = 'TEST-F49687' AND service_date = $1")
            .bind(service_date)
            .execute(&pool)
            .await
            .expect("cleanup trains");
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("cleanup user");
    }

    /// Regression test for the destination-CRS half of the shared
    /// `queries::is_bookable_crs` filter -- see that function's own doc
    /// comment. `VICTRCR` (a real TIPLOC, a common empty-coaching-stock
    /// terminus) resolves to the X-prefixed pseudo-CRS `XVR` via the
    /// `tiploc_crs` crosswalk (the exact "widened resolution" case the
    /// 2026-09-24 `tiploc_crs` crosswalk plan introduced -- this TIPLOC
    /// previously returned `None` from `crs_for_tiploc`, not a pseudo-CRS,
    /// before that plan landed). Before this fix, `find_schedule_match`'s
    /// `destination_crs` carried `XVR` straight through into
    /// `trains.destination_crs`, which the single-train page renders as
    /// `train.destinationName ?? train.destinationCrs` -- so a tracked ECS
    /// working terminating at Victoria Carriage Sidings would have shown
    /// destination "XVR" instead of correctly showing nothing. Exercises
    /// `find_schedule_match` directly (private to this module, visible via
    /// `use super::*` in this same file) rather than the full
    /// `attempt_schedule_match` write path -- the bug is entirely in this
    /// pure read half.
    #[tokio::test]
    #[ignore = "requires a live database; see this plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p api \
                find_schedule_match -- --ignored --test-threads=1`"]
    async fn find_schedule_match_blanks_an_x_prefixed_pseudo_crs_destination() {
        let pool = connect().await;

        sqlx::query(
            "INSERT INTO tiploc_crs (tiploc, crs, station_name, stanox, source_sequence) \
             VALUES ('VICTRCR', 'XVR', 'VICTORIA C.S.', 'TEST-VICTRCR-STANOX', 1) \
             ON CONFLICT (tiploc) DO UPDATE SET crs = EXCLUDED.crs",
        )
        .execute(&pool)
        .await
        .expect("seed tiploc_crs");
        sqlx::query(
            "INSERT INTO stanox_crs (stanox, crs, tiploc, station_name, source_sequence) \
             VALUES ('TEST-ECSORIG-STANOX', 'ZEC', 'ECSORIG', 'TEST ECS ORIGIN', 1) \
             ON CONFLICT (stanox) DO UPDATE SET crs = EXCLUDED.crs",
        )
        .execute(&pool)
        .await
        .expect("seed stanox_crs");

        let service_date: chrono::NaiveDate = "2026-09-24".parse().unwrap();
        let population = serde_json::json!([{
            "uid": "TEST-ECSXVR",
            "calling_points": [
                {
                    "tiploc": "ECSORIG",
                    "kind": "Origin",
                    "booked_arrival": null,
                    "booked_departure": "23:10:00",
                    "is_half_minute_arrival": false,
                    "is_half_minute_departure": false,
                    "day_offset": 0
                },
                {
                    "tiploc": "VICTRCR",
                    "kind": "Terminate",
                    "booked_arrival": "23:40:00",
                    "booked_departure": null,
                    "is_half_minute_arrival": false,
                    "is_half_minute_departure": false,
                    "day_offset": 0
                }
            ]
        }]);
        sqlx::query(
            "INSERT INTO schedule_line_population (line_id, service_date, population) \
             VALUES ('test-ecs-line', $1, $2) \
             ON CONFLICT (line_id, service_date) DO UPDATE SET population = EXCLUDED.population",
        )
        .bind(service_date)
        .bind(&population)
        .execute(&pool)
        .await
        .expect("seed schedule_line_population");

        let scheduled_departure: chrono::DateTime<chrono::Utc> =
            "2026-09-24T23:10:00+01:00".parse().unwrap();
        let mut crs_line_index = HashMap::new();
        crs_line_index.insert("ZEC".to_string(), vec!["test-ecs-line".to_string()]);

        let matched = find_schedule_match(
            &pool,
            "ZEC",
            scheduled_departure,
            service_date,
            &crs_line_index,
            None,
        )
        .await
        .expect("find_schedule_match")
        .expect("should match the seeded TEST-ECSXVR population entry");

        assert_eq!(matched.uid, "TEST-ECSXVR");
        assert_eq!(
            matched.destination_crs, None,
            "VICTRCR resolves to the X-prefixed pseudo-CRS XVR, which must blank to None \
             here rather than leak into trains.destination_crs as if it were a real station"
        );

        sqlx::query(
            "DELETE FROM schedule_line_population WHERE line_id = 'test-ecs-line' AND service_date = $1",
        )
        .bind(service_date)
        .execute(&pool)
        .await
        .expect("cleanup population");
        sqlx::query("DELETE FROM stanox_crs WHERE stanox = 'TEST-ECSORIG-STANOX'")
            .execute(&pool)
            .await
            .expect("cleanup stanox_crs");
        sqlx::query("DELETE FROM tiploc_crs WHERE tiploc = 'VICTRCR'")
            .execute(&pool)
            .await
            .expect("cleanup tiploc_crs");
    }

    /// The mirror of the test directly above: a genuine, non-X-prefixed
    /// destination CRS must still resolve normally through the same
    /// `find_schedule_match` call -- `queries::is_bookable_crs` only
    /// excludes the `X`-prefixed convention, never a real station code.
    #[tokio::test]
    #[ignore = "requires a live database; see this plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p api \
                find_schedule_match -- --ignored --test-threads=1`"]
    async fn find_schedule_match_keeps_a_genuine_non_x_crs_destination() {
        let pool = connect().await;

        sqlx::query(
            "INSERT INTO stanox_crs (stanox, crs, tiploc, station_name, source_sequence) \
             VALUES ('TEST-BSKORIG-STANOX', 'ZBS', 'BSKORIG', 'TEST BSK ORIGIN', 1) \
             ON CONFLICT (stanox) DO UPDATE SET crs = EXCLUDED.crs",
        )
        .execute(&pool)
        .await
        .expect("seed stanox_crs origin");
        sqlx::query(
            "INSERT INTO stanox_crs (stanox, crs, tiploc, station_name, source_sequence) \
             VALUES ('TEST-BSKDEST-STANOX', 'BSK', 'BSKDEST', 'BASINGSTOKE', 1) \
             ON CONFLICT (stanox) DO UPDATE SET crs = EXCLUDED.crs",
        )
        .execute(&pool)
        .await
        .expect("seed stanox_crs destination");

        let service_date: chrono::NaiveDate = "2026-09-24".parse().unwrap();
        let population = serde_json::json!([{
            "uid": "TEST-REALBSK",
            "calling_points": [
                {
                    "tiploc": "BSKORIG",
                    "kind": "Origin",
                    "booked_arrival": null,
                    "booked_departure": "12:00:00",
                    "is_half_minute_arrival": false,
                    "is_half_minute_departure": false,
                    "day_offset": 0
                },
                {
                    "tiploc": "BSKDEST",
                    "kind": "Terminate",
                    "booked_arrival": "12:30:00",
                    "booked_departure": null,
                    "is_half_minute_arrival": false,
                    "is_half_minute_departure": false,
                    "day_offset": 0
                }
            ]
        }]);
        sqlx::query(
            "INSERT INTO schedule_line_population (line_id, service_date, population) \
             VALUES ('test-real-bsk-line', $1, $2) \
             ON CONFLICT (line_id, service_date) DO UPDATE SET population = EXCLUDED.population",
        )
        .bind(service_date)
        .bind(&population)
        .execute(&pool)
        .await
        .expect("seed schedule_line_population");

        let scheduled_departure: chrono::DateTime<chrono::Utc> =
            "2026-09-24T12:00:00+01:00".parse().unwrap();
        let mut crs_line_index = HashMap::new();
        crs_line_index.insert("ZBS".to_string(), vec!["test-real-bsk-line".to_string()]);

        let matched = find_schedule_match(
            &pool,
            "ZBS",
            scheduled_departure,
            service_date,
            &crs_line_index,
            None,
        )
        .await
        .expect("find_schedule_match")
        .expect("should match the seeded TEST-REALBSK population entry");

        assert_eq!(matched.uid, "TEST-REALBSK");
        assert_eq!(
            matched.destination_crs,
            Some("BSK".to_string()),
            "a genuine, non-X-prefixed destination CRS must resolve normally"
        );

        sqlx::query(
            "DELETE FROM schedule_line_population WHERE line_id = 'test-real-bsk-line' AND service_date = $1",
        )
        .bind(service_date)
        .execute(&pool)
        .await
        .expect("cleanup population");
        sqlx::query(
            "DELETE FROM stanox_crs WHERE stanox IN ('TEST-BSKORIG-STANOX', 'TEST-BSKDEST-STANOX')",
        )
        .execute(&pool)
        .await
        .expect("cleanup stanox_crs");
    }

    fn fixture_line_with_no_toml_tiploc(id: &str, crs: &str) -> LineDefinition {
        LineDefinition {
            id: id.to_string(),
            name: id.to_string(),
            mode: "rail".to_string(),
            category: "national-rail".to_string(),
            operators: vec![],
            stations: vec![common::Station {
                crs: crs.to_string(),
                tiploc: None, // the exact scenario the 2026-09-09 fix covers
                role: "minor".to_string(),
                segment: None,
            }],
            sample_stations: vec![],
            match_keywords: vec![],
            excluded_keywords: vec![],
            severity_overrides: std::collections::HashMap::new(),
            exclusive_segments: vec![],
            destination_crs_filter: vec![],
            headcode_prefixes: vec![],
            full_coverage_enabled: false,
        }
    }

    /// The actual regression test for the tiploc-schedule-matching-gap bug
    /// (2026-09-09): a station whose TOML entry carries no `tiploc` at all
    /// -- exactly the ~83% CRS-code case the live-production investigation
    /// found -- must still schedule-match, because its `crs_line_index`
    /// entry now comes from `crs_to_line_ids` itself (not hand-built, unlike
    /// the sibling tests above) and the real TIPLOC is resolved separately
    /// from the CIF-derived `stanox_crs` table. Before this fix,
    /// `crs_to_line_ids` would have produced an EMPTY index for this line
    /// (no station has a TOML `tiploc`), so `find_schedule_match` would
    /// have returned `Ok(None)` immediately, without ever touching
    /// `stanox_crs` -- permanently stuck "Waiting to hear from Network
    /// Rail" for any pin at this CRS.
    #[tokio::test]
    #[ignore = "requires a live database; see this plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p api \
                attempt_schedule_match -- --ignored --test-threads=1`"]
    async fn attempt_schedule_match_matches_a_station_with_no_toml_tiploc_via_real_stanox_crs_data()
    {
        let pool = connect().await;
        let user_id = "TEST-SCHEDULE-MATCH-NO-TOML-TIPLOC";
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind("no-toml-tiploc@example.com")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");

        // Real, CIF-derived data -- entirely independent of the TOML
        // catalogue below, and the only place a real TIPLOC comes from now.
        sqlx::query(
            "INSERT INTO stanox_crs (stanox, crs, tiploc, station_name, source_sequence) \
             VALUES ('TEST-NTT-STANOX', 'ZNT', 'ZNOTIPLOC', 'TEST NO TIPLOC STATION', 1) \
             ON CONFLICT (stanox) DO NOTHING",
        )
        .execute(&pool)
        .await
        .expect("seed stanox_crs");

        let service_date: chrono::NaiveDate = "2026-09-09".parse().unwrap();
        sqlx::query(
            "INSERT INTO schedule_line_population (line_id, service_date, population) \
             VALUES ('test-no-toml-tiploc-line', $1, $2) \
             ON CONFLICT (line_id, service_date) DO UPDATE SET population = EXCLUDED.population",
        )
        .bind(service_date)
        .bind(population_json("C88888", "ZNOTIPLOC", "19:15"))
        .execute(&pool)
        .await
        .expect("seed schedule_line_population");

        let scheduled_departure: chrono::DateTime<chrono::Utc> =
            "2026-09-09T19:15:00+01:00".parse().unwrap();
        let (tracked_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions (user_id, service_date, pin_origin_crs, pin_scheduled_departure) \
             VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(user_id)
        .bind(service_date)
        .bind("ZNT")
        .bind(scheduled_departure)
        .fetch_one(&pool)
        .await
        .expect("seed fixture tracked_trains row");

        // The load-bearing bit: this line's ONLY station has no TOML
        // `tiploc` set, and the index is built via the real
        // `crs_to_line_ids` function under test -- not hand-constructed
        // like the sibling tests above -- so this genuinely exercises the
        // fixed indexing behavior end to end.
        let lines = vec![fixture_line_with_no_toml_tiploc(
            "test-no-toml-tiploc-line",
            "ZNT",
        )];
        let crs_line_index = crs_to_line_ids(&lines);
        assert_eq!(
            crs_line_index.get("ZNT"),
            Some(&vec!["test-no-toml-tiploc-line".to_string()]),
            "sanity check: the fixed crs_to_line_ids must index a no-toml-tiploc station"
        );

        let matched = attempt_schedule_match(
            &pool,
            tracked_train_id,
            "ZNT",
            scheduled_departure,
            service_date,
            &crs_line_index,
            &[],
            None,
            None,
        )
        .await
        .expect("attempt schedule match");
        assert!(
            matched,
            "a station with no TOML tiploc must still schedule-match via real stanox_crs data"
        );

        let state = train_tracking::get_by_tracking_id(&pool, tracked_train_id)
            .await
            .expect("read tracked train")
            .expect("tracked train exists");
        assert_eq!(state.resolution_status, "schedule_matched");
        assert_eq!(state.train_uid, Some("C88888".to_string()));

        sqlx::query("DELETE FROM schedule_line_population WHERE line_id = 'test-no-toml-tiploc-line' AND service_date = $1")
            .bind(service_date)
            .execute(&pool)
            .await
            .expect("cleanup population");
        sqlx::query("DELETE FROM stanox_crs WHERE stanox = 'TEST-NTT-STANOX'")
            .execute(&pool)
            .await
            .expect("cleanup stanox_crs");
        sqlx::query("DELETE FROM train_subscriptions WHERE user_id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("cleanup tracked_trains");
        sqlx::query("DELETE FROM trains WHERE train_uid = 'C88888' AND service_date = $1")
            .bind(service_date)
            .execute(&pool)
            .await
            .expect("cleanup trains");
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("cleanup user");
    }

    #[tokio::test]
    #[ignore = "requires a live database; see this plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p api \
                attempt_schedule_match -- --ignored --test-threads=1`"]
    async fn attempt_schedule_match_with_no_candidate_line_leaves_the_row_pending() {
        let pool = connect().await;
        let user_id = "TEST-SCHEDULE-MATCH-NO-CANDIDATE";
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind("no-candidate@example.com")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");

        let service_date: chrono::NaiveDate = "2026-09-05".parse().unwrap();
        let (tracked_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions (user_id, service_date, pin_origin_crs, pin_scheduled_departure) \
             VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(user_id)
        .bind(service_date)
        .bind("ZZZ")
        .bind("2026-09-05T19:15:00Z".parse::<chrono::DateTime<chrono::Utc>>().unwrap())
        .fetch_one(&pool)
        .await
        .expect("seed fixture tracked_trains row");

        let matched = attempt_schedule_match(
            &pool,
            tracked_train_id,
            "ZZZ",
            "2026-09-05T19:15:00Z".parse().unwrap(),
            service_date,
            &HashMap::new(), // no candidate lines at all
            &[],
            None,
            None,
        )
        .await
        .expect("attempt schedule match");
        assert!(!matched);

        let state = train_tracking::get_by_tracking_id(&pool, tracked_train_id)
            .await
            .expect("read tracked train")
            .expect("tracked train exists");
        assert_eq!(state.resolution_status, "pending");
        assert_eq!(state.train_uid, None);

        sqlx::query("DELETE FROM train_subscriptions WHERE user_id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("cleanup tracked_trains");
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("cleanup user");
    }

    #[tokio::test]
    #[ignore = "requires a live database; see this plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p api \
                attempt_schedule_match_also_dual_writes_the_shared_trains_row -- --ignored --test-threads=1`"]
    async fn attempt_schedule_match_also_dual_writes_the_shared_trains_row() {
        let pool = connect().await;
        let user_id = "TEST-SCHEDULE-MATCH-DUAL-WRITE";
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind("dual-write@example.com")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");

        sqlx::query(
            "INSERT INTO stanox_crs (stanox, crs, tiploc, station_name, source_sequence) \
             VALUES ('TEST-DW-STANOX', 'EUS', 'EUSTON', 'LONDON EUSTON', 1) \
             ON CONFLICT (stanox) DO NOTHING",
        )
        .execute(&pool)
        .await
        .expect("seed stanox_crs");

        let service_date: chrono::NaiveDate = "2026-09-06".parse().unwrap();
        sqlx::query(
            "INSERT INTO schedule_line_population (line_id, service_date, population) \
             VALUES ('west-coast-main-line', $1, $2) \
             ON CONFLICT (line_id, service_date) DO UPDATE SET population = EXCLUDED.population",
        )
        .bind(service_date)
        .bind(population_json("TEST-DW-UID", "EUSTON ", "19:15"))
        .execute(&pool)
        .await
        .expect("seed schedule_line_population");

        let scheduled_departure: chrono::DateTime<chrono::Utc> =
            "2026-09-06T19:15:00+01:00".parse().unwrap();
        let (tracked_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions (user_id, service_date, pin_origin_crs, pin_scheduled_departure) \
             VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(user_id)
        .bind(service_date)
        .bind("EUS")
        .bind(scheduled_departure)
        .fetch_one(&pool)
        .await
        .expect("seed fixture tracked_trains row");

        let mut crs_line_index = HashMap::new();
        crs_line_index.insert("EUS".to_string(), vec!["west-coast-main-line".to_string()]);

        let matched = attempt_schedule_match(
            &pool,
            tracked_train_id,
            "EUS",
            scheduled_departure,
            service_date,
            &crs_line_index,
            &[],
            None,
            None,
        )
        .await
        .expect("attempt schedule match");
        assert!(matched);

        let (trains_id,): (Option<i64>,) =
            sqlx::query_as("SELECT trains_id FROM train_subscriptions WHERE id = $1")
                .bind(tracked_train_id)
                .fetch_one(&pool)
                .await
                .expect("read back trains_id");
        let trains_id = trains_id.expect("a successful schedule match must set trains_id");

        let (train_uid, matched_line_id): (String, Option<String>) =
            sqlx::query_as("SELECT train_uid, matched_line_id FROM trains WHERE id = $1")
                .bind(trains_id)
                .fetch_one(&pool)
                .await
                .expect("read back the shared trains row");
        assert_eq!(train_uid, "TEST-DW-UID");
        assert_eq!(matched_line_id, Some("west-coast-main-line".to_string()));

        sqlx::query("DELETE FROM schedule_line_population WHERE line_id = 'west-coast-main-line' AND service_date = $1")
            .bind(service_date)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM stanox_crs WHERE stanox = 'TEST-DW-STANOX'")
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM train_subscriptions WHERE user_id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trains WHERE train_uid = 'TEST-DW-UID'")
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .ok();
    }

    /// Regression test for the real production bug this closes
    /// (`https://ds.cursed.solutions/train/Y80908/2026-09-24` still showing
    /// no schedule at all, even after `enrich_public_train_schedule` -- see
    /// `find_schedule_match`'s own doc comment for the full account): a
    /// busy multi-line origin station (here, a stand-in for the real
    /// Birmingham New Street, which appears on a dozen `lines/*.toml`
    /// files) where the FIRST candidate line iterated for the origin CRS
    /// happens to have some OTHER, unrelated service within
    /// `common::MATCH_TOLERANCE` of the pin's own time, and only a LATER
    /// candidate line actually carries the train we already know the
    /// identity of.
    ///
    /// Before this fix, `find_schedule_match` returned on the FIRST
    /// candidate line's tolerance match regardless of uid, so
    /// `attempt_schedule_match_for_shared_train`'s own uid check discarded
    /// it and returned `Ok(false)` -- permanently, since nothing ever tried
    /// `real-line` at all. This reproduces exactly that shape (two
    /// candidate lines in `crs_line_index`, in an order where the WRONG one
    /// is tried first) and asserts the correct line's schedule is the one
    /// actually written.
    #[tokio::test]
    #[ignore = "requires a live database; see this plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p api \
                attempt_schedule_match_for_shared_train_keeps_trying_candidate_lines_past_a_wrong_uid_match \
                -- --ignored --test-threads=1`"]
    async fn attempt_schedule_match_for_shared_train_keeps_trying_candidate_lines_past_a_wrong_uid_match()
     {
        let pool = connect().await;
        let train_uid = "TEST-Y80908-SHAPE";
        let service_date: chrono::NaiveDate = "2026-09-24".parse().unwrap();

        // Simulates `trust-backlog-consumer`'s bare `find_or_create_train` +
        // `mark_train_resolved` -- real live TRUST identity, no
        // `train_subscriptions` row anywhere, exactly Y80908's own real
        // shape (a `trains` row created by broad ingestion, never tracked).
        let trains_id = crate::data::trains::find_or_create_train(&pool, train_uid, service_date)
            .await
            .expect("find_or_create_train for fixture");
        crate::data::trains::mark_train_resolved(&pool, trains_id, "TEST-Y80908-HC")
            .await
            .expect("mark_train_resolved for fixture");

        sqlx::query(
            "INSERT INTO stanox_crs (stanox, crs, tiploc, station_name, source_sequence) \
             VALUES ('TEST-BHM-STANOX', 'TBM', 'BHAMNWS', 'TEST BIRMINGHAM NEW STREET', 1) \
             ON CONFLICT (stanox) DO UPDATE SET crs = EXCLUDED.crs",
        )
        .execute(&pool)
        .await
        .expect("seed stanox_crs for the busy origin station");

        // The WRONG, earlier-iterated candidate line: a real, unrelated
        // service departing the same origin TIPLOC 10 minutes off Y80908's
        // own time -- well within the 20-minute `common::MATCH_TOLERANCE`,
        // exactly what happens for real at a busy terminus like Birmingham
        // New Street.
        sqlx::query(
            "INSERT INTO schedule_line_population (line_id, service_date, population) \
             VALUES ('test-cross-country-shape', $1, $2) \
             ON CONFLICT (line_id, service_date) DO UPDATE SET population = EXCLUDED.population",
        )
        .bind(service_date)
        .bind(population_json("TEST-UNRELATED-XC", "BHAMNWS", "12:10"))
        .execute(&pool)
        .await
        .expect("seed the wrong candidate line's population");

        // The CORRECT, later-iterated candidate line: Y80908's own real
        // service.
        sqlx::query(
            "INSERT INTO schedule_line_population (line_id, service_date, population) \
             VALUES ('test-lnwr-birmingham-crewe-shape', $1, $2) \
             ON CONFLICT (line_id, service_date) DO UPDATE SET population = EXCLUDED.population",
        )
        .bind(service_date)
        .bind(population_json(train_uid, "BHAMNWS", "12:00"))
        .execute(&pool)
        .await
        .expect("seed the correct candidate line's population");

        let scheduled_departure: chrono::DateTime<chrono::Utc> =
            "2026-09-24T12:00:00+01:00".parse().unwrap();
        let mut crs_line_index = HashMap::new();
        // Order matters: the wrong line MUST be iterated first to reproduce
        // the bug -- `Vec` insertion order here mirrors the real
        // `crs_to_line_ids` index, whose order follows `glob`'s alphabetical
        // directory listing of `lines/*.toml` (`cross-country.toml` sorts
        // before `lnwr-birmingham-crewe.toml`).
        crs_line_index.insert(
            "TBM".to_string(),
            vec![
                "test-cross-country-shape".to_string(),
                "test-lnwr-birmingham-crewe-shape".to_string(),
            ],
        );

        let matched = attempt_schedule_match_for_shared_train(
            &pool,
            train_uid,
            "TBM",
            scheduled_departure,
            service_date,
            &crs_line_index,
        )
        .await
        .expect("attempt_schedule_match_for_shared_train");
        assert!(
            matched,
            "must keep trying candidate lines past the wrong-uid match on the first one, not \
             give up"
        );

        let (matched_line_id,): (Option<String>,) =
            sqlx::query_as("SELECT matched_line_id FROM trains WHERE id = $1")
                .bind(trains_id)
                .fetch_one(&pool)
                .await
                .expect("read back the shared trains row");
        assert_eq!(
            matched_line_id,
            Some("test-lnwr-birmingham-crewe-shape".to_string()),
            "the shared row must carry the CORRECT line's match, never the wrong candidate \
             line's unrelated service"
        );

        sqlx::query("DELETE FROM trains WHERE train_uid = $1")
            .bind(train_uid)
            .execute(&pool)
            .await
            .ok();
        sqlx::query(
            "DELETE FROM schedule_line_population WHERE line_id IN \
             ('test-cross-country-shape', 'test-lnwr-birmingham-crewe-shape') AND service_date = $1",
        )
        .bind(service_date)
        .execute(&pool)
        .await
        .ok();
        sqlx::query("DELETE FROM stanox_crs WHERE stanox = 'TEST-BHM-STANOX'")
            .execute(&pool)
            .await
            .ok();
    }

    /// **The real `Y80908` regression test** (2026-09-24 round-3
    /// investigation) -- the shape the round-2 test directly above got
    /// wrong, and the reason deploying round 2 changed nothing in
    /// production.
    ///
    /// Measured live, not invented: on 2026-09-24 `Y80908` departs
    /// Birmingham New Street at 16:06 for London Euston, and `W75898`
    /// departs Birmingham New Street at 16:06 for Lichfield. Both call at
    /// `BHM`, so `schedules_touching` puts BOTH into the population of
    /// EVERY line that lists `BHM` -- the rival is not on some other
    /// candidate line, it is sitting right next to the correct schedule in
    /// the SAME one. `schedule_query::match_pin` picks the globally-closest
    /// entry with no regard for uid, and keeps the first on a tie, so the
    /// correct schedule (whose own delta is always exactly zero, and which
    /// therefore can only ever draw) loses -- on every candidate line
    /// identically, which is precisely why round 2's "try the next line"
    /// loop could not help.
    ///
    /// So this fixture deliberately uses ONE candidate line, with the rival
    /// FIRST in the population and the same booked departure minute. Before
    /// the round-3 fix this returned `Ok(false)` and wrote nothing; now the
    /// population is narrowed to `expected_uid` before matching, so there
    /// is no tie to lose.
    #[tokio::test]
    #[ignore = "requires a live database; see this plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p api \
                attempt_schedule_match_for_shared_train_wins_an_exact_minute_tie_inside_one_lines_population \
                -- --ignored --test-threads=1`"]
    async fn attempt_schedule_match_for_shared_train_wins_an_exact_minute_tie_inside_one_lines_population()
     {
        let pool = connect().await;
        let train_uid = "TEST-Y80908-TIE";
        let rival_uid = "TEST-W75898-TIE";
        let service_date: chrono::NaiveDate = "2026-09-24".parse().unwrap();

        let trains_id = crate::data::trains::find_or_create_train(&pool, train_uid, service_date)
            .await
            .expect("find_or_create_train for fixture");
        crate::data::trains::mark_train_resolved(&pool, trains_id, "TEST-Y80908-TIE-HC")
            .await
            .expect("mark_train_resolved for fixture");

        sqlx::query(
            "INSERT INTO stanox_crs (stanox, crs, tiploc, station_name, source_sequence) \
             VALUES ('TEST-TIE-STANOX', 'TTB', 'BHAMNWS', 'TEST BIRMINGHAM NEW STREET', 1) \
             ON CONFLICT (stanox) DO UPDATE SET crs = EXCLUDED.crs",
        )
        .execute(&pool)
        .await
        .expect("seed stanox_crs for the busy origin station");

        // ONE candidate line, both services in it, the RIVAL first -- so
        // `match_pin`'s first-wins tie-break hands back the wrong schedule
        // unless the population is narrowed by uid beforehand.
        sqlx::query(
            "INSERT INTO schedule_line_population (line_id, service_date, population) \
             VALUES ('test-bhm-peak-shape', $1, $2) \
             ON CONFLICT (line_id, service_date) DO UPDATE SET population = EXCLUDED.population",
        )
        .bind(service_date)
        .bind(population_json_multi(&[
            (rival_uid, "BHAMNWS", "16:06"),
            (train_uid, "BHAMNWS", "16:06"),
        ]))
        .execute(&pool)
        .await
        .expect("seed the busy line's population");

        let scheduled_departure: chrono::DateTime<chrono::Utc> =
            "2026-09-24T16:06:00+01:00".parse().unwrap();
        let mut crs_line_index = HashMap::new();
        crs_line_index.insert("TTB".to_string(), vec!["test-bhm-peak-shape".to_string()]);

        let matched = attempt_schedule_match_for_shared_train(
            &pool,
            train_uid,
            "TTB",
            scheduled_departure,
            service_date,
            &crs_line_index,
        )
        .await
        .expect("attempt_schedule_match_for_shared_train");
        assert!(
            matched,
            "a known-identity match must not be lost to another service departing the same \
             station in the same minute"
        );

        let (origin_crs, matched_line_id): (Option<String>, Option<String>) =
            sqlx::query_as("SELECT origin_crs, matched_line_id FROM trains WHERE id = $1")
                .bind(trains_id)
                .fetch_one(&pool)
                .await
                .expect("read back the shared trains row");
        assert_eq!(
            origin_crs,
            Some("TTB".to_string()),
            "the whole point of the fix: originCrs stops being NULL, so the public train page \
             stops rendering \"Unknown station\""
        );
        assert_eq!(matched_line_id, Some("test-bhm-peak-shape".to_string()));

        sqlx::query("DELETE FROM trains WHERE train_uid = $1")
            .bind(train_uid)
            .execute(&pool)
            .await
            .ok();
        sqlx::query(
            "DELETE FROM schedule_line_population WHERE line_id = 'test-bhm-peak-shape' \
             AND service_date = $1",
        )
        .bind(service_date)
        .execute(&pool)
        .await
        .ok();
        sqlx::query("DELETE FROM stanox_crs WHERE stanox = 'TEST-TIE-STANOX'")
            .execute(&pool)
            .await
            .ok();
    }

    /// The guard on the round-3 fix's blast radius: the UNTARGETED
    /// (`expected_uid: None`) legacy pin path must be completely unaffected
    /// by the uid narrowing. It has no identity to narrow BY -- discovering
    /// one is the entire point -- so it must still take the closest entry
    /// in the whole population, whatever uid that turns out to be.
    #[tokio::test]
    #[ignore = "requires a live database; see this plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p api \
                attempt_schedule_match_still_takes_the_closest_entry_of_the_whole_population \
                -- --ignored --test-threads=1`"]
    async fn attempt_schedule_match_still_takes_the_closest_entry_of_the_whole_population() {
        let pool = connect().await;
        let user_id = "TEST-SCHEDULE-MATCH-UNTARGETED";
        let service_date: chrono::NaiveDate = "2026-09-24".parse().unwrap();
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind("untargeted@example.com")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");

        sqlx::query(
            "INSERT INTO stanox_crs (stanox, crs, tiploc, station_name, source_sequence) \
             VALUES ('TEST-UNT-STANOX', 'TTU', 'BHAMNWS', 'TEST BIRMINGHAM NEW STREET', 1) \
             ON CONFLICT (stanox) DO UPDATE SET crs = EXCLUDED.crs",
        )
        .execute(&pool)
        .await
        .expect("seed stanox_crs");

        sqlx::query(
            "INSERT INTO schedule_line_population (line_id, service_date, population) \
             VALUES ('test-untargeted-shape', $1, $2) \
             ON CONFLICT (line_id, service_date) DO UPDATE SET population = EXCLUDED.population",
        )
        .bind(service_date)
        .bind(population_json_multi(&[
            ("TEST-UNT-FAR", "BHAMNWS", "16:16"),
            ("TEST-UNT-NEAR", "BHAMNWS", "16:06"),
        ]))
        .execute(&pool)
        .await
        .expect("seed population");

        let scheduled_departure: chrono::DateTime<chrono::Utc> =
            "2026-09-24T16:06:00+01:00".parse().unwrap();
        let (tracked_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions (user_id, service_date, pin_origin_crs, pin_scheduled_departure) \
             VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(user_id)
        .bind(service_date)
        .bind("TTU")
        .bind(scheduled_departure)
        .fetch_one(&pool)
        .await
        .expect("seed fixture subscription");

        let mut crs_line_index = HashMap::new();
        crs_line_index.insert("TTU".to_string(), vec!["test-untargeted-shape".to_string()]);

        let matched = attempt_schedule_match(
            &pool,
            tracked_train_id,
            "TTU",
            scheduled_departure,
            service_date,
            &crs_line_index,
            &[],
            None,
            None,
        )
        .await
        .expect("attempt_schedule_match");
        assert!(matched);

        let (trains_id,): (Option<i64>,) =
            sqlx::query_as("SELECT trains_id FROM train_subscriptions WHERE id = $1")
                .bind(tracked_train_id)
                .fetch_one(&pool)
                .await
                .expect("read back trains_id");
        let (train_uid,): (String,) = sqlx::query_as("SELECT train_uid FROM trains WHERE id = $1")
            .bind(trains_id.expect("a successful match sets trains_id"))
            .fetch_one(&pool)
            .await
            .expect("read back the shared trains row");
        assert_eq!(
            train_uid, "TEST-UNT-NEAR",
            "the untargeted path still discovers the CLOSEST schedule's identity, unnarrowed"
        );

        sqlx::query("DELETE FROM train_subscriptions WHERE user_id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trains WHERE train_uid IN ('TEST-UNT-NEAR', 'TEST-UNT-FAR')")
            .execute(&pool)
            .await
            .ok();
        sqlx::query(
            "DELETE FROM schedule_line_population WHERE line_id = 'test-untargeted-shape' \
             AND service_date = $1",
        )
        .bind(service_date)
        .execute(&pool)
        .await
        .ok();
        sqlx::query("DELETE FROM stanox_crs WHERE stanox = 'TEST-UNT-STANOX'")
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
