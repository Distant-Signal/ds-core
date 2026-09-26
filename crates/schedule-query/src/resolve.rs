//! STP-overlay resolution and the two read-only queries this crate exists
//! to answer: [`ScheduleIndex::schedule_for_uid`] (the direct
//! `train_uid` -> booked-schedule bridge
//! `crates/trust-consumer/src/matching.rs`'s own module doc names as
//! missing) and [`schedules_touching`] (the line-population query a
//! future full-coverage consumer would need).

use std::collections::HashMap;

use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveTime, Utc};

use crate::records::{CallingPoint, LinePopulationEntry, RawSchedule, StpIndicator};
use crate::tiploc::normalize_tiploc;

/// A schedule resolved for one specific `(UID, date)`, after STP-overlay
/// preference has already been applied.
///
/// Distinguishes two real cases this plan's own Task 3 requires kept
/// separate, not collapsed to the same `None`: "no schedule at all for
/// this UID/date" (see [`resolve_for_date`]'s `None` return) versus "a
/// schedule exists and says cancelled" (`cancelled: true`, empty
/// `calling_points`, per the real `C`-indicator "no body" property
/// [`StpIndicator::Cancellation`] documents).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSchedule {
    pub uid: String,
    pub stp_indicator: StpIndicator,
    pub cancelled: bool,
    pub calling_points: Vec<CallingPoint>,
    pub operator_atoc: Option<String>,
}

/// Assigns [`CallingPoint::day_offset`] over `calling_points`, IN PLACE, by
/// walking them in schedule order and incrementing a running offset every
/// time a calling point's own time regresses against the previous one --
/// the standard CIF convention for a bare `HH:MM` time with no day marker
/// of its own (this module's own doc comment; see
/// [`CallingPoint::day_offset`] for the real live-confirmed example this
/// fixes).
///
/// Per calling point, the EARLIEST time it carries (`booked_arrival` if
/// present, else `booked_departure`) is compared against the LATEST time
/// the previous calling point carried (`booked_departure` if present, else
/// `booked_arrival`) -- so a same-stop arrival/departure pair is only ever
/// compared against its own NEIGHBORS, never against itself. A regression
/// increments the running offset by exactly one and every subsequent
/// calling point inherits it (compounding again on a further regression,
/// e.g. a schedule that crosses two midnights) -- it never decrements,
/// since a real schedule's calling points are always chronological once
/// day-of-week is fixed.
///
/// **Known, accepted limitation** (same posture as this crate's other
/// documented rare-edge-case gaps, e.g. `match_pin`'s tie-break): a single
/// calling point that itself dwells across midnight (arrival 23:59,
/// departure 00:05 at the SAME stop) still gets only one `day_offset` for
/// both fields, so its `booked_departure` would be mis-dated by this
/// scheme alone. Not fixed here -- no real CIF calling point this deep
/// dive found actually does this (a stop dwelling into the next calendar
/// day), and modeling two independent day offsets per calling point would
/// double `CallingPoint`'s footprint for a case with no known real
/// instance.
fn assign_day_offsets(calling_points: &mut [CallingPoint]) {
    let mut offset: u8 = 0;
    let mut last_time: Option<NaiveTime> = None;

    for cp in calling_points.iter_mut() {
        let first_time = cp.booked_arrival.or(cp.booked_departure);
        if let (Some(last), Some(first)) = (last_time, first_time)
            && first < last
        {
            offset += 1;
        }
        cp.day_offset = offset;

        if let Some(latest) = cp.booked_departure.or(cp.booked_arrival) {
            last_time = Some(latest);
        }
    }
}

/// Resolves `uid`'s schedule for `date` out of `raw`: filters to records
/// matching `uid` whose date range (`date_from..=date_to`) and
/// days-of-week bitmask cover `date`, then picks the one with the lowest
/// (best-precedence) [`StpIndicator`] -- `C` beats `N` beats `O` beats `P`,
/// via `StpIndicator`'s own `Ord` impl, so this reads as a plain
/// `min_by_key` rather than hand-rolled comparison logic.
///
/// Returns `None` if no record in `raw` covers `uid`/`date` at all.
/// Returns `Some` with `cancelled: true` and empty `calling_points` when
/// the winning record's indicator is [`StpIndicator::Cancellation`].
///
/// The non-cancelled `calling_points` returned here always have
/// [`CallingPoint::day_offset`] freshly computed via [`assign_day_offsets`],
/// regardless of whatever placeholder value `winner.calling_points` carried
/// (the parser always writes `0` -- see that field's own doc comment) --
/// this is the one place that computation happens, exactly once per
/// resolved `(uid, date)`.
pub fn resolve_for_date(
    raw: &[RawSchedule],
    uid: &str,
    date: NaiveDate,
) -> Option<ResolvedSchedule> {
    let weekday_index = date.weekday().num_days_from_monday() as usize;

    let winner = raw
        .iter()
        .filter(|schedule| {
            schedule.basic.uid == uid
                && schedule.basic.date_from <= date
                && date <= schedule.basic.date_to
                && schedule.basic.days_of_week[weekday_index]
        })
        .min_by_key(|schedule| schedule.basic.stp_indicator)?;

    let cancelled = winner.basic.stp_indicator == StpIndicator::Cancellation;
    let calling_points = if cancelled {
        Vec::new()
    } else {
        let mut calling_points = winner.calling_points.clone();
        assign_day_offsets(&mut calling_points);
        calling_points
    };
    Some(ResolvedSchedule {
        uid: winner.basic.uid.clone(),
        stp_indicator: winner.basic.stp_indicator,
        cancelled,
        calling_points,
        operator_atoc: winner.basic.operator_atoc.clone(),
    })
}

/// Resolves every UID in `index` for `date` (via [`resolve_for_date`]),
/// keeping only the resolved, non-cancelled results whose `calling_points`
/// include at least one of `tiplocs` -- comparing with
/// [`normalize_tiploc`] so the fixed 7-character schedule-body padding
/// doesn't silently defeat the match. This is the line-population query.
pub fn schedules_touching(
    index: &ScheduleIndex,
    tiplocs: &[&str],
    date: NaiveDate,
) -> Vec<ResolvedSchedule> {
    let normalized_targets: Vec<&str> = tiplocs.iter().map(|t| normalize_tiploc(t)).collect();

    index
        .by_uid
        .iter()
        .filter_map(|(uid, raw)| resolve_for_date(raw, uid, date))
        .filter(|resolved| !resolved.cancelled)
        .filter(|resolved| {
            resolved
                .calling_points
                .iter()
                .any(|cp| normalized_targets.contains(&normalize_tiploc(&cp.tiploc)))
        })
        .collect()
}

/// Finds the best schedule match for a tracked-train pin (Decision 3 of
/// docs/superpowers/specs/2026-09-05-schedule-first-train-tracking-design.md):
/// among every `population` entry with a calling point whose TIPLOC
/// (compared via [`normalize_tiploc`]) is one of `crs_tiplocs` and whose
/// `booked_departure` resolves -- via caller-supplied `to_utc`, so this
/// pure crate never grows a `chrono-tz` dependency of its own; the real
/// caller passes a closure wrapping `crates/api/src/data/eta_blend.rs`'s
/// existing DST-aware `london_to_utc` -- to within `tolerance` of
/// `scheduled`, returns the entry whose matching calling point is
/// CLOSEST in time to `scheduled`.
///
/// Tie-break (the plan's Open Question 4): on an exact equal delta
/// between two candidates, the one encountered FIRST in `population`'s
/// own order wins -- the scan below only replaces `best` on a strictly
/// smaller delta, never an equal one. This is deterministic for one call
/// but not guaranteed stable across a `schedule-reference` republish that
/// reorders the underlying JSONB array; accepted as a rare-edge-case
/// limitation, not fixed here (see the plan's own writeup).
///
/// `None` if nothing in `population` has any calling point at any of
/// `crs_tiplocs` within `tolerance` of `scheduled`.
///
/// `to_utc`'s second parameter is the candidate calling point's own
/// [`CallingPoint::day_offset`] -- REQUIRED, not optional, because
/// `population`'s entries span every calling point of every schedule, not
/// just each schedule's origin, and a real overnight service's later
/// calling points fall on the calendar day AFTER the schedule's own
/// `service_date` (see that field's own doc comment for the live-confirmed
/// example). A caller building `to_utc` around
/// `eta_blend::london_to_utc(service_date.and_time(t))` must add
/// `day_offset` days to `service_date` first -- ignoring this parameter
/// reproduces exactly the bug this signature exists to prevent.
pub fn match_pin<'a>(
    population: &'a [LinePopulationEntry],
    crs_tiplocs: &[&str],
    scheduled: DateTime<Utc>,
    tolerance: Duration,
    to_utc: impl Fn(NaiveTime, u8) -> Option<DateTime<Utc>>,
) -> Option<&'a LinePopulationEntry> {
    match_pin_with_delta(population, crs_tiplocs, scheduled, tolerance, to_utc)
        .map(|(entry, _)| entry)
}

/// [`match_pin`]'s delta-reporting sibling -- identical scan, identical
/// tie-break, but it also hands back HOW FAR the winning entry's matching
/// calling point actually was from `scheduled`. [`match_pin`] is a thin
/// wrapper over this, so there is exactly one implementation of the scan.
///
/// The delta exists for callers that need to distinguish "this candidate
/// TIED for closest" from "this candidate merely matched within tolerance".
/// `crates/api`'s `schedule_matching::find_schedule_match` uses it for
/// exactly that: on the untargeted (identity-unknown) pin path, an exact
/// tie on departure time between two real services at a busy station is
/// resolved by preferring the one whose DESTINATION also matches the pin's
/// own -- a comparison that is only sound while both candidates are equally
/// close in time, which is what this delta lets the caller check rather
/// than assume.
pub fn match_pin_with_delta<'a>(
    population: &'a [LinePopulationEntry],
    crs_tiplocs: &[&str],
    scheduled: DateTime<Utc>,
    tolerance: Duration,
    to_utc: impl Fn(NaiveTime, u8) -> Option<DateTime<Utc>>,
) -> Option<(&'a LinePopulationEntry, Duration)> {
    let normalized_targets: Vec<&str> = crs_tiplocs.iter().map(|t| normalize_tiploc(t)).collect();

    let mut best: Option<(&'a LinePopulationEntry, Duration)> = None;
    for entry in population {
        for cp in &entry.calling_points {
            if !normalized_targets.contains(&normalize_tiploc(&cp.tiploc)) {
                continue;
            }
            let Some(booked) = cp.booked_departure else {
                continue;
            };
            let Some(candidate_utc) = to_utc(booked, cp.day_offset) else {
                continue;
            };
            let delta = (scheduled - candidate_utc).abs();
            if delta > tolerance {
                continue;
            }
            match &best {
                Some((_, best_delta)) if *best_delta <= delta => {}
                _ => best = Some((entry, delta)),
            }
        }
    }
    best
}

/// Every non-cancelled, resolved schedule's departure-bearing calling
/// points (`Origin`/`Intermediate`, i.e. `booked_departure.is_some()` --
/// `Terminate` never has one, see [`crate::records::CallingPointKind::Terminate`]'s
/// own doc), bucketed by CRS via `tiploc_to_crs` (normalized-TIPLOC keyed,
/// built by the caller from the SAME cycle's already-resolved crosswalk
/// rows -- no second lookup table, no new parse; that is the TIPLOC-primary
/// `tiploc_crs` set as of Task 4 of
/// docs/superpowers/plans/2026-09-24-tiploc-crs-crosswalk-plan.md, not the
/// STANOX-keyed `stanox_crs` one this doc comment originally named, see
/// `crates/schedule-reference/src/main.rs`'s
/// `publish_schedule_network_departures`). A calling
/// point whose TIPLOC has no `tiploc_to_crs` entry is dropped, not guessed
/// at -- a real, if rare, honest gap (see the design doc's Open Question
/// 4), not a silent one: the caller simply never sees that departure
/// rather than seeing it filed under a wrong or fabricated CRS. A calling
/// point that IS kept but whose *destination* TIPLOC has no
/// `tiploc_to_crs` entry gets `destination_crs: None`, not dropped -- see
/// the design doc's Decision 1 wire-type doc comment.
///
/// `now`: only calling points with `booked_departure >= now` are kept --
/// this is what keeps a station's bucket naturally small AND naturally
/// forward-looking without an arbitrary unbounded "whole day" list (see
/// the design doc's Decision 4). One O(all UIDs) resolve pass + O(total
/// calling points) bucketing -- the same complexity class
/// [`schedules_touching`] already pays per line, done once for the whole
/// network instead of once per line.
/// Is a calling point at `day_offset`/`time` genuinely before `now`, where
/// `now` is a bare clock time on the schedule's own `service_date` (i.e.
/// `day_offset` 0)?
///
/// **This exists because comparing the bare clock times was wrong in both
/// directions.** [`departures_by_crs`] and [`departures_by_destination_crs`]
/// both filtered with `departure < now`, ignoring
/// [`crate::records::CallingPoint::day_offset`] entirely -- the very field
/// this crate computes precisely because CIF times are bare `HH:MM` with no
/// day marker. So for a real overnight service (the live-confirmed c2c UID
/// `F49687`: `23:48` Liverpool Street, `00:07` Barking the NEXT day) at, say,
/// `now = 23:00`:
///
/// * the genuine FUTURE `00:07` departure (`day_offset: 1`, really 67 minutes
///   away) was dropped from the station board as though it had already gone,
///   and
/// * had `now` been `00:30` instead, that same `00:07` entry would have been
///   dropped while a `day_offset: 0` entry at `00:40` -- which really is
///   yesterday's, long past -- would have been kept.
///
/// Comparing `(day_offset, time)` against `(0, now)` as a tuple is the whole
/// fix: a calling point on a later calendar day is never in the past, and one
/// on the service date itself compares by clock time as before.
fn is_before(day_offset: u8, time: NaiveTime, now: NaiveTime) -> bool {
    (day_offset, time) < (0, now)
}

pub fn departures_by_crs(
    index: &ScheduleIndex,
    date: NaiveDate,
    now: NaiveTime,
    tiploc_to_crs: &HashMap<String, String>,
) -> HashMap<String, Vec<crate::records::ScheduleDeparture>> {
    let mut by_crs: HashMap<String, Vec<crate::records::ScheduleDeparture>> = HashMap::new();

    for uid in index.uids() {
        if let Some(resolved) = index.schedule_for_uid(uid, date) {
            collect_crs_departures(&resolved, 0, now, tiploc_to_crs, &mut by_crs);
        }
    }

    // A sleeper or late Thameslink/c2c-class overnight service is booked
    // under YESTERDAY's date (`resolve_for_date`/`schedule_for_uid` pick the
    // schedule instance whose own days-of-week bitmask/date range cover the
    // date it's booked FOR, not the date its later calling points land on),
    // so the loop above -- which only ever calls `schedule_for_uid(uid,
    // date)` -- never resolves that instance at all when building `date`'s
    // own board, and its post-midnight `day_offset >= 1` calling points
    // (already, correctly, kept forward-looking by `is_before` when
    // YESTERDAY's own board was published) are silently absent from today's
    // board too. A delivery processed at 01:30 is exactly this case: the
    // real sleeper/Thameslink/c2c calling points still to come are booked
    // under yesterday's UID instance, not today's.
    //
    // Fixed the same way `is_before`'s own doc comment fixed the sibling
    // bug: resolve yesterday's instance too, keep only the calling points
    // that land on `date` (`day_offset >= 1` there -- `day_offset == 0`
    // really is yesterday's own departure, already covered when yesterday's
    // board was published), and REBASE each kept calling point's day_offset
    // by -1 before storing it, so a `ScheduleDeparture::day_offset` stored
    // under `date`'s board always means what its own doc comment says:
    // days past THIS board's `date`, never the originating instance's.
    if let Some(yesterday) = date.pred_opt() {
        for uid in index.uids() {
            if let Some(resolved) = index.schedule_for_uid(uid, yesterday) {
                collect_crs_departures(&resolved, 1, now, tiploc_to_crs, &mut by_crs);
            }
        }
    }

    by_crs
}

/// The shared per-schedule body [`departures_by_crs`]'s two passes (`date`'s
/// own instance, and the previous calendar day's overnight-carryover
/// instance) both run: every departure-bearing calling point of `resolved`
/// with `day_offset >= min_day_offset` is bucketed by CRS, same
/// public-pickup/tiploc-resolution rules either pass, with `day_offset -
/// min_day_offset` stored rather than the raw `day_offset` -- a no-op
/// rebasing for the `date`-own pass (`min_day_offset: 0`), and exactly what
/// turns "1 day past the schedule instance booked for yesterday" into "0
/// days past `date`" for the carryover pass (`min_day_offset: 1`).
/// Cancelled schedules contribute nothing, from either pass.
fn collect_crs_departures(
    resolved: &ResolvedSchedule,
    min_day_offset: u8,
    now: NaiveTime,
    tiploc_to_crs: &HashMap<String, String>,
    by_crs: &mut HashMap<String, Vec<crate::records::ScheduleDeparture>>,
) {
    if resolved.cancelled {
        return;
    }
    for cp in &resolved.calling_points {
        if cp.day_offset < min_day_offset {
            continue;
        }
        let day_offset = cp.day_offset - min_day_offset;
        let Some(departure) = cp.booked_departure else {
            continue;
        };
        if is_before(day_offset, departure, now) {
            continue;
        }
        // A booked departure is not the same thing as a place a passenger
        // may board. Until 2026-09-25 this bucket -- which backs
        // `GET /public/stations/{crs}/schedule-departures` -- published
        // set-down-only (`D`), operational (`OP`) and
        // not-advertised-to-the-public (`N`) stops as boardable
        // departures, because the CIF Activity field was never decoded at
        // all. See `CallingPoint::is_public_pickup`.
        if !cp.is_public_pickup() {
            continue;
        }
        let Some(crs) = tiploc_to_crs.get(normalize_tiploc(&cp.tiploc)) else {
            continue;
        };
        let destination_crs = resolved
            .calling_points
            .last()
            .and_then(|last| tiploc_to_crs.get(normalize_tiploc(&last.tiploc)))
            .cloned();
        by_crs
            .entry(crs.clone())
            .or_default()
            .push(crate::records::ScheduleDeparture {
                uid: resolved.uid.clone(),
                scheduled: departure,
                day_offset,
                destination_crs,
            });
    }
}

/// The destination-keyed sibling of [`departures_by_crs`]: every
/// non-cancelled, resolved schedule's `now`-forward, departure-bearing
/// calling points, bucketed by the CRS of that schedule's own TERMINATING
/// calling point rather than by each calling point's own CRS. Backs the
/// calling-point-first whole-network train search (this function itself is
/// unchanged and still literally buckets by destination; the
/// calling-point-first framing is what the read side built on top of its
/// output does with the result)
/// (docs/superpowers/specs/2026-09-07-train-listing-page-design.md,
/// Approach B).
///
/// Runs against the SAME already-built, transient, per-cycle
/// [`ScheduleIndex`] as [`departures_by_crs`] -- one extra O(all UIDs)
/// resolve pass plus O(total calling points) bucketing per cycle, no second
/// parse and no resident index (that constraint is restated verbatim in the
/// design doc's §6).
///
/// **(2026-09-23, revised)** A schedule whose terminating TIPLOC has no
/// `tiploc_to_crs` entry is no longer dropped entirely. It used to be --
/// see [`unresolved_destination_key`]'s own doc comment for why that was a
/// real, live completeness bug (not a hypothetical one: STANOX `89428`,
/// `52215` and `89530` in `reference-data/stanox-crs.md`'s own documented
/// ambiguity list are Ashford International, Stratford International and
/// Ebbsfleet International -- real major stations, real genuine schedule
/// termini, permanently unresolvable via this map) rather than a justified
/// tradeoff, and was fixed the same way the analogous
/// `crates/api/src/data/journey.rs` pre-tracking calling-point gap was
/// fixed: don't require CRS resolution to succeed just to know the
/// schedule exists. The bucket key is now the resolved CRS when one
/// exists, or [`unresolved_destination_key`]'s always-distinguishable
/// fallback (built from the terminus's own TIPLOC, which is always known)
/// when it doesn't -- so the schedule's other, resolvable calling points
/// still surface under `station=<their own CRS>` search instead of the
/// entire train vanishing from the whole-network search everywhere it
/// calls, just because its OWN terminus happens to sit on one of these
/// STANOX-ambiguity stations.
///
/// One asymmetry with [`departures_by_crs`] remains, both about the
/// "drop, never fabricate a CRS" rule:
///
/// * A calling point whose OWN TIPLOC has no `tiploc_to_crs` entry still
///   drops just that entry, leaving the schedule's other entries in the
///   bucket -- identical to `departures_by_crs`'s own per-calling-point
///   drop. This is unchanged: it is a genuinely different situation from
///   the bucket-key case above, because that calling point's own
///   `origin_crs` is a required, non-optional field on
///   [`crate::records::DestinationDeparture`] (unlike the bucket key,
///   there is no fallback string to fall back to that would not misrepresent
///   a real search filter as matching a station it cannot honestly confirm).
///
/// **There is no cap, here or anywhere downstream.** This function returns
/// every matching calling point, unsorted, exactly like
/// `departures_by_crs`, and its caller
/// (`crates/schedule-reference/src/main.rs`'s
/// `schedule_destination_departures_rows`) merely flattens the result --
/// it does not sort, truncate, or bucket it. An earlier design capped each
/// bucket at a constant; that was measured and rejected, because the
/// busiest destination holds ~9,634 entries for one day and the next
/// several busiest are within the same order of magnitude, so no cap value
/// truncates honestly. See
/// docs/superpowers/specs/2026-09-07-train-listing-destination-search-sizing-design.md
/// (§1 for the measurement, §3 Approach C for what replaced it): the whole
/// day is published uncapped and the `now`-forward filter and pagination
/// happen at READ time instead, as an indexed range scan with a keyset
/// cursor. Do not reintroduce a cap in this function's caller.
pub fn departures_by_destination_crs(
    index: &ScheduleIndex,
    date: NaiveDate,
    now: NaiveTime,
    tiploc_to_crs: &HashMap<String, String>,
) -> HashMap<String, Vec<crate::records::DestinationDeparture>> {
    let mut by_destination: HashMap<String, Vec<crate::records::DestinationDeparture>> =
        HashMap::new();

    for uid in index.uids() {
        let Some(resolved) = index.schedule_for_uid(uid, date) else {
            continue;
        };
        if resolved.cancelled {
            continue;
        }
        // A schedule with no calling points at all has no terminus TIPLOC
        // to fall back to either -- still `continue`, same as before. Every
        // real, non-cancelled `ResolvedSchedule` has at least one calling
        // point, so this is a defensive no-op in practice, not a live case.
        let Some(last) = resolved.calling_points.last() else {
            continue;
        };
        let terminus_tiploc = normalize_tiploc(&last.tiploc);
        // `.cloned().unwrap_or_else(...)`, NOT the old `let Some(...) else
        // { continue }`: see this function's own doc comment (2026-09-23
        // revision) for why a schedule is never dropped just because its
        // terminus's own TIPLOC fails to resolve to a CRS any more.
        let destination_crs = tiploc_to_crs
            .get(terminus_tiploc)
            .cloned()
            .unwrap_or_else(|| unresolved_destination_key(terminus_tiploc));
        // Computed once per schedule, exactly like destination_crs above,
        // and attached unchanged to every entry this schedule contributes
        // -- NOT recomputed per calling point, which is what would make it
        // just a duplicate of `origin_crs` instead of the schedule's own
        // true first stop.
        let true_origin_crs = resolved
            .calling_points
            .first()
            .and_then(|first| tiploc_to_crs.get(normalize_tiploc(&first.tiploc)))
            .cloned();
        // The mirror of true_origin_crs directly above, but from the
        // LAST calling point's booked_arrival (Terminate: arrival only,
        // no departure) instead of the FIRST's booked_departure.
        // Computed once per schedule, attached unchanged to every entry.
        let destination_arrival = resolved
            .calling_points
            .last()
            .and_then(|last| last.booked_arrival);
        // The terminating calling point's OWN day_offset -- already
        // computed for every calling point (including the last one) by
        // `assign_day_offsets` inside `resolve_for_date`, just not
        // previously read for this purpose. Deliberately NOT this row's
        // own `cp.day_offset` below, which describes the DEPARTING calling
        // point -- a real overnight schedule's departure and terminus can
        // genuinely be on two different calendar days (see
        // `DestinationDeparture::destination_arrival_day_offset`'s own doc
        // comment).
        let destination_arrival_day_offset = resolved
            .calling_points
            .last()
            .map(|last| last.day_offset)
            .unwrap_or(0);
        // Computed once per schedule, exactly like true_origin_crs above,
        // and attached unchanged to every entry this schedule contributes.
        let operator_atoc = resolved.operator_atoc.clone();
        for cp in &resolved.calling_points {
            let Some(departure) = cp.booked_departure else {
                continue;
            };
            if is_before(cp.day_offset, departure, now) {
                continue;
            }
            // Same "a booked departure is not a boardable departure" filter as
            // `departures_by_crs` above -- this product backs
            // `GET /public/trains/search`, where every row is offered to a
            // user as a train they can catch FROM `origin_crs`.
            if !cp.is_public_pickup() {
                continue;
            }
            let Some(origin_crs) = tiploc_to_crs.get(normalize_tiploc(&cp.tiploc)) else {
                continue;
            };
            by_destination
                .entry(destination_crs.clone())
                .or_default()
                .push(crate::records::DestinationDeparture {
                    uid: resolved.uid.clone(),
                    origin_crs: origin_crs.clone(),
                    scheduled: departure,
                    day_offset: cp.day_offset,
                    true_origin_crs: true_origin_crs.clone(),
                    // THIS calling point's own arrival -- recomputed per
                    // entry, unlike destination_arrival below, which is
                    // computed once per schedule and copied onto every
                    // entry. `None` for the schedule's true origin (an
                    // Origin calling point never has a booked_arrival).
                    calling_point_arrival: cp.booked_arrival,
                    destination_arrival,
                    destination_arrival_day_offset,
                    operator_atoc: operator_atoc.clone(),
                });
        }
    }

    by_destination
}

/// The bucket key [`departures_by_destination_crs`] uses in place of a real
/// CRS when the terminating calling point's own TIPLOC has no
/// `tiploc_to_crs` entry.
///
/// **This is a real, live gap, confirmed against this repo's own checked-in
/// reference data, not a hypothetical one.** `reference-data/stanox-crs.md`
/// documents 5 real STANOX values that are permanently excluded from CRS
/// resolution because two genuinely distinct, non-`X`-prefixed CRS codes
/// share one physical STANOX with no principled tiebreaker (see that file's
/// "Extraction and exclusion policy" section, and this crate's own
/// `crates/schedule-reference/src/parser.rs::resolve_tests`, which pins the
/// exact same 5 exclusions in code). Three of those five are not junctions
/// or sidings -- they are the domestic/international platform split at
/// three major, real, high-frequency HS1 stations, every one of which is a
/// genuine, regular schedule terminus:
///
/// * STANOX `89428`: TIPLOC `ASHFKI`/CRS `ASI` vs TIPLOC `ASHFKY`/CRS `AFK`
///   -- Ashford International / Ashford (Kent).
/// * STANOX `52215`: TIPLOC `STFORDI`/CRS `SDI` vs TIPLOC `STFODOM`/CRS
///   `SFA` -- Stratford International (domestic vs international
///   platforms).
/// * STANOX `89530`: TIPLOC `EBSFLTI`/CRS `EBF` vs TIPLOC `EBSFDOM`/CRS
///   `EBD` -- Ebbsfleet International (same split).
///
/// Before this function existed, ANY schedule terminating at one of these
/// stations (e.g. a Southeastern "Javelin" domestic service from London St
/// Pancras terminating at Ashford International) was dropped ENTIRELY from
/// `departures_by_destination_crs`'s output -- not just its own terminating
/// calling point, every departure-bearing calling point of the whole
/// schedule, including ones at ordinary, perfectly resolvable stations
/// earlier in its route. That made the train invisible to
/// `GET /public/trains/search?station=<any other calling point on its own
/// route>`, which is precisely the search this table exists to serve, for a
/// reason that has nothing to do with whether that OTHER station is
/// findable.
///
/// The fix mirrors `crates/api/src/data/journey.rs`'s pre-tracking
/// calling-point fallback fix (2026-09-23, same session): don't require CRS
/// resolution to succeed just to know the schedule -- and its OTHER,
/// resolvable calling points -- exist. The bucket key becomes this
/// fallback, built from the one thing always genuinely known (the
/// terminus's own TIPLOC), rather than a value invented out of nothing.
///
/// **Status update (2026-09-24): those three specific HS1 examples no
/// longer reach this fallback, but this fallback is still load-bearing.**
/// Task 4 of
/// docs/superpowers/plans/2026-09-24-tiploc-crs-crosswalk-plan.md switched
/// the real caller's `tiploc_to_crs` map
/// (`crates/schedule-reference/src/main.rs`'s
/// `publish_schedule_destination_departures`) from the STANOX-keyed
/// `stanox_crs` set to the TIPLOC-primary `tiploc_crs` one, and
/// `parser::resolve_tiploc_crs` applies NO STANOX-level exclusion at all --
/// so `ASHFKI`/`ASHFKY`, `STFORDI`/`STFODOM` and `EBSFLTI`/`EBSFDOM` each
/// now carry their own row and resolve to their own real CRS (see that
/// function's own
/// `ambiguous_stanox_with_two_genuine_non_x_candidates_now_resolves_both_instead_of_neither`
/// test). What still reaches this fallback is the residual case: a schedule
/// whose terminating TIPLOC has no resolvable CRS ANYWHERE -- no `TI` CRS
/// and no matching `MSN` `A` record, i.e. a genuine non-station terminus
/// such as a carriage siding or depot (the `WATRLWC` shape
/// `resolve_tiploc_crs`'s own
/// `a_junction_tiploc_sharing_a_stations_stanox_with_no_own_crs_anywhere_still_does_not_resolve`
/// test pins). Do not read the now-resolved HS1 examples above as evidence
/// that this fallback can be removed; read them as the history of why it
/// was added.
///
/// **Never collides with a real CRS.** Every real CRS (including
/// `X`-prefixed pseudo-codes) is exactly 3 uppercase ASCII letters by
/// Network Rail's own convention -- see `reference-data/stanox-crs.md`.
/// `~` is not a valid CRS byte, so prefixing with it makes this key
/// unambiguously distinguishable from a real CRS regardless of the
/// TIPLOC's own length (some real TIPLOCs, e.g. `ASH`/`LEE`/`ORE` in
/// `reference-data/crs-tiploc.csv`, are themselves exactly 3 letters and so
/// would otherwise risk looking like a real CRS on the wire).
///
/// A caller-supplied `stops_at=<CRS>`/`destination` filter can never match
/// this key (a real caller never types a `~`-prefixed value), which is the
/// correct degrade: the code correctly declines to claim a station this
/// schedule's terminus cannot be honestly named as. `GET
/// /public/trains/search`'s own `destinationCrs` response field passes this
/// value straight through as-is -- `render::calling_point_departure_json`
/// does now look up a display name for `destinationCrs` (added after this
/// paragraph was first written; see that function's own doc comment), but
/// a `~`-prefixed key is never a real CRS and so never has a `stations`
/// row to resolve a name from -- so it degrades to a visibly-not-a-
/// station-code string with a `null` name on the wire rather than silently
/// pretending to be a real one.
fn unresolved_destination_key(terminus_tiploc: &str) -> String {
    format!("~{terminus_tiploc}")
}

/// The observability counterpart to [`departures_by_crs`]/
/// [`departures_by_destination_crs`]'s own "drop, never fabricate" posture:
/// both of those silently drop exactly the calling points this function
/// surfaces (a calling point's own TIPLOC, or a schedule's terminating
/// TIPLOC, absent from `tiploc_to_crs` entirely -- see each function's own
/// doc comment), which is correct for what THEY produce (a bucket needs a
/// real CRS key/field, not a guess), but on its own leaves no visibility
/// into which TIPLOCs are actually being dropped. This crate is
/// deliberately I/O- and `tracing`-free (see this module's own doc
/// comment), so it hands back plain data for a caller with a logging
/// dependency to act on -- see `crates/schedule-reference/src/main.rs`'s
/// `log_new_unresolved_booked_tiplocs` and
/// `crates/api/src/data/journey.rs`'s `log_if_unresolved_booked_stop` for
/// the two real callers.
///
/// Returns every distinct, normalized (see [`normalize_tiploc`]) TIPLOC
/// across `index`'s resolved, non-cancelled schedules for `date` that BOTH:
///
/// - carries at least one booked time of its own (`booked_arrival` or
///   `booked_departure` -- i.e. this schedule's own working timetable
///   records a genuine timed stop here, not a bare pass-through with
///   neither field set), and
/// - has NO entry in `tiploc_to_crs` at all.
///
/// The second condition is the load-bearing "looks like it could be a real
/// station" signal, not merely "didn't resolve": every real caller builds
/// `tiploc_to_crs` directly from the FULL resolved CRS crosswalk,
/// X-prefixed rows included (see e.g. `main.rs`'s own `tiploc_to_crs`
/// construction, shared verbatim by `departures_by_crs`/
/// `departures_by_destination_crs`'s own call sites) -- so a TIPLOC that
/// resolves to an X-prefixed Network Rail pseudo-CRS (a junction, siding,
/// or depot; see `crates/api/src/data/journey.rs::is_bookable_crs`'s own
/// doc comment for the convention and real examples) is present as a KEY
/// here and never returned by this function. Only a TIPLOC with no
/// crosswalk row of any kind at all -- not even an X-prefixed one --
/// reaches this function's result, exactly inverting `is_bookable_crs`'s
/// own "resolved but not bookable" case to get at "not resolved, therefore
/// possibly a real, unmapped station" instead.
///
/// "The crosswalk" here is the TIPLOC-primary `tiploc_crs` set as of Task 4
/// of docs/superpowers/plans/2026-09-24-tiploc-crs-crosswalk-plan.md, not
/// the STANOX-keyed `stanox_crs` one this doc comment originally named:
/// `crates/schedule-reference/src/main.rs`'s
/// `log_new_unresolved_booked_tiplocs` now builds its map from
/// `tiploc_crs_records`, so it sees the same superset
/// `departures_by_crs`/`departures_by_destination_crs` do. That makes this
/// function's "no row of any kind" bar STRICTER than before (a TIPLOC that
/// `stanox_crs`'s one-row-per-STANOX schema dropped, but that carries its
/// own CRS, is now a key and so is correctly no longer warned about), which
/// is exactly the intent -- fewer false "possibly a real, unmapped station"
/// warnings, not fewer real ones.
///
/// Sorted (a `BTreeSet` collected to `Vec`, not insertion order) purely so
/// this function's own output -- and any test asserting on it -- is
/// deterministic regardless of `index`'s internal `HashMap` iteration
/// order.
pub fn unresolved_booked_tiplocs(
    index: &ScheduleIndex,
    date: NaiveDate,
    tiploc_to_crs: &HashMap<String, String>,
) -> Vec<String> {
    let mut unresolved: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();

    for uid in index.uids() {
        let Some(resolved) = index.schedule_for_uid(uid, date) else {
            continue;
        };
        if resolved.cancelled {
            continue;
        }
        for cp in &resolved.calling_points {
            if cp.booked_arrival.is_none() && cp.booked_departure.is_none() {
                continue;
            }
            let key = normalize_tiploc(&cp.tiploc);
            if tiploc_to_crs.contains_key(key) {
                continue;
            }
            unresolved.insert(key.to_string());
        }
    }

    unresolved.into_iter().collect()
}

/// A thin wrapper grouping `Vec<RawSchedule>` by `uid`, built once, so
/// [`ScheduleIndex::schedule_for_uid`]/[`schedules_touching`] aren't
/// re-scanning a flat `Vec` on every call.
#[derive(Debug, Clone, Default)]
pub struct ScheduleIndex {
    by_uid: HashMap<String, Vec<RawSchedule>>,
}

impl ScheduleIndex {
    /// Groups already-parsed `raw` schedules by `uid`.
    pub fn build(raw: Vec<RawSchedule>) -> Self {
        let mut by_uid: HashMap<String, Vec<RawSchedule>> = HashMap::new();
        for schedule in raw {
            by_uid
                .entry(schedule.basic.uid.clone())
                .or_default()
                .push(schedule);
        }
        Self { by_uid }
    }

    /// Composes [`crate::parse::parse_schedule_records`] with [`Self::build`]
    /// as the one convenience entry point most callers will actually use.
    pub fn from_text(text: &str) -> Self {
        Self::build(crate::parse::parse_schedule_records(text))
    }

    /// The direct `train_uid` -> booked-schedule bridge
    /// `crates/trust-consumer/src/matching.rs`'s own module doc names as
    /// missing. A thin, `ScheduleIndex`-scoped convenience over
    /// [`resolve_for_date`].
    pub fn schedule_for_uid(&self, uid: &str, date: NaiveDate) -> Option<ResolvedSchedule> {
        let raw = self.by_uid.get(uid).map(Vec::as_slice).unwrap_or(&[]);
        resolve_for_date(raw, uid, date)
    }

    /// Distinct UIDs currently indexed. Exposed for [`schedules_touching`]
    /// and any future caller that needs to enumerate the index rather than
    /// query it by UID.
    pub fn uids(&self) -> impl Iterator<Item = &str> {
        self.by_uid.keys().map(String::as_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::records::{BasicSchedule, CallingPointKind};

    fn basic(uid: &str, stp: StpIndicator, from: &str, to: &str, days: [bool; 7]) -> BasicSchedule {
        BasicSchedule {
            uid: uid.to_string(),
            stp_indicator: stp,
            date_from: NaiveDate::parse_from_str(from, "%Y-%m-%d").unwrap(),
            date_to: NaiveDate::parse_from_str(to, "%Y-%m-%d").unwrap(),
            days_of_week: days,
            operator_atoc: None,
        }
    }

    fn calling_point(tiploc: &str, kind: CallingPointKind) -> CallingPoint {
        CallingPoint {
            tiploc: tiploc.to_string(),
            kind,
            booked_arrival: None,
            booked_departure: None,
            is_half_minute_arrival: false,
            is_half_minute_departure: false,
            day_offset: 0,
            activity: String::new(),
            public_arrival: None,
            public_departure: None,
            platform: None,
        }
    }

    fn calling_point_with_departure(
        tiploc: &str,
        kind: CallingPointKind,
        departure: &str,
    ) -> CallingPoint {
        CallingPoint {
            tiploc: tiploc.to_string(),
            kind,
            booked_arrival: None,
            booked_departure: Some(NaiveTime::parse_from_str(departure, "%H:%M").unwrap()),
            is_half_minute_arrival: false,
            is_half_minute_departure: false,
            day_offset: 0,
            activity: String::new(),
            public_arrival: None,
            public_departure: None,
            platform: None,
        }
    }

    fn calling_point_with_arrival(
        tiploc: &str,
        kind: CallingPointKind,
        arrival: &str,
    ) -> CallingPoint {
        CallingPoint {
            tiploc: tiploc.to_string(),
            kind,
            booked_arrival: Some(NaiveTime::parse_from_str(arrival, "%H:%M").unwrap()),
            booked_departure: None,
            is_half_minute_arrival: false,
            is_half_minute_departure: false,
            day_offset: 0,
            activity: String::new(),
            public_arrival: None,
            public_departure: None,
            platform: None,
        }
    }

    fn calling_point_with_both(
        tiploc: &str,
        kind: CallingPointKind,
        arrival: &str,
        departure: &str,
    ) -> CallingPoint {
        CallingPoint {
            tiploc: tiploc.to_string(),
            kind,
            booked_arrival: Some(NaiveTime::parse_from_str(arrival, "%H:%M").unwrap()),
            booked_departure: Some(NaiveTime::parse_from_str(departure, "%H:%M").unwrap()),
            is_half_minute_arrival: false,
            is_half_minute_departure: false,
            day_offset: 0,
            activity: String::new(),
            public_arrival: None,
            public_departure: None,
            platform: None,
        }
    }

    /// A calling point with a real booked departure AND a CIF Activity field --
    /// the pairing that distinguishes "this train departs from here" from
    /// "a passenger may board here", which until 2026-09-25 this crate could
    /// not tell apart at all. See `CallingPoint::is_public_pickup`.
    fn calling_point_with_departure_and_activity(
        tiploc: &str,
        kind: CallingPointKind,
        departure: &str,
        activity: &str,
    ) -> CallingPoint {
        CallingPoint {
            activity: activity.to_string(),
            ..calling_point_with_departure(tiploc, kind, departure)
        }
    }

    fn tiploc_map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(tiploc, crs)| (tiploc.to_string(), crs.to_string()))
            .collect()
    }

    const WEEKDAYS: [bool; 7] = [true, true, true, true, true, false, false];
    const MONDAY_ONLY: [bool; 7] = [true, false, false, false, false, false, false];
    const ALL_DAYS: [bool; 7] = [true, true, true, true, true, true, true];

    // Real UID/STP/date-range/days values, transcribed from the findings
    // doc's own (paraphrased, not raw-byte) real Bank Holiday cross-check
    // quote (2026-08-31/09-01 section):
    //   UID=C11052 stp=P from=260518 to=261211 days=1111100 [base pattern]
    //   UID=C11052 stp=C from=260831 to=260831 days=1000000 [cancelled today]
    // 2026-08-31 is independently confirmed a Monday in the same section
    // ("2026-08-31 is a Monday, and turned out to be the UK August Bank
    // Holiday"), which is what pins down days=1000000 meaning "Monday
    // only" and therefore this crate's index-0-is-Monday convention.
    fn c11052_raw() -> Vec<RawSchedule> {
        vec![
            RawSchedule {
                basic: basic(
                    "C11052",
                    StpIndicator::Permanent,
                    "2026-05-18",
                    "2026-12-11",
                    WEEKDAYS,
                ),
                calling_points: vec![calling_point("EUSTON ", CallingPointKind::Origin)],
            },
            RawSchedule {
                basic: basic(
                    "C11052",
                    StpIndicator::Cancellation,
                    "2026-08-31",
                    "2026-08-31",
                    MONDAY_ONLY,
                ),
                calling_points: Vec::new(),
            },
        ]
    }

    #[test]
    fn resolve_for_date_picks_the_base_pattern_on_an_ordinary_tuesday() {
        // 2026-09-01 is the Tuesday immediately after the real 2026-08-31
        // Bank Holiday Monday cited above -- an ordinary weekday the
        // STP=C override does not cover.
        let date = NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let resolved = resolve_for_date(&c11052_raw(), "C11052", date).unwrap();
        assert_eq!(resolved.stp_indicator, StpIndicator::Permanent);
        assert!(!resolved.cancelled);
        assert_eq!(resolved.calling_points.len(), 1);
    }

    #[test]
    fn resolve_for_date_picks_the_real_cancellation_override_on_260831() {
        let date = NaiveDate::from_ymd_opt(2026, 8, 31).unwrap();
        let resolved = resolve_for_date(&c11052_raw(), "C11052", date).unwrap();
        assert_eq!(resolved.stp_indicator, StpIndicator::Cancellation);
        assert!(resolved.cancelled);
        assert!(resolved.calling_points.is_empty());
    }

    #[test]
    fn schedule_for_uid_on_a_uid_not_in_the_index_returns_none_not_a_panic() {
        let index = ScheduleIndex::build(c11052_raw());
        let date = NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        assert_eq!(index.schedule_for_uid("Z99999", date), None);
    }

    #[test]
    fn schedule_for_uid_on_a_date_outside_every_records_range_returns_none() {
        let index = ScheduleIndex::build(c11052_raw());
        let date = NaiveDate::from_ymd_opt(2027, 1, 1).unwrap();
        assert_eq!(index.schedule_for_uid("C11052", date), None);
    }

    #[test]
    fn schedule_for_uid_matches_resolve_for_date_via_the_index() {
        let index = ScheduleIndex::build(c11052_raw());
        let date = NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let via_index = index.schedule_for_uid("C11052", date).unwrap();
        let via_free_fn = resolve_for_date(&c11052_raw(), "C11052", date).unwrap();
        assert_eq!(via_index, via_free_fn);
    }

    #[test]
    fn departures_by_crs_buckets_an_origin_departure_under_its_crs_with_destination_resolved() {
        let raw = vec![RawSchedule {
            basic: basic(
                "C11052",
                StpIndicator::Permanent,
                "2026-05-18",
                "2026-12-11",
                WEEKDAYS,
            ),
            calling_points: vec![
                calling_point_with_departure("EUSTON ", CallingPointKind::Origin, "08:22"),
                calling_point("CREWE  ", CallingPointKind::Terminate), // no booked_departure
            ],
        }];
        let index = ScheduleIndex::build(raw);
        let date = NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let now = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
        let tiploc_to_crs = tiploc_map(&[("EUSTON", "EUS"), ("CREWE", "CRE")]);

        let by_crs = departures_by_crs(&index, date, now, &tiploc_to_crs);

        assert_eq!(
            by_crs.len(),
            1,
            "only EUS gets a bucket -- CREWE's Terminate has no booked_departure"
        );
        let euston = &by_crs["EUS"];
        assert_eq!(euston.len(), 1);
        assert_eq!(euston[0].uid, "C11052");
        assert_eq!(
            euston[0].scheduled,
            NaiveTime::from_hms_opt(8, 22, 0).unwrap()
        );
        assert_eq!(euston[0].destination_crs, Some("CRE".to_string()));
        assert!(!by_crs.contains_key("CRE"));
    }

    #[test]
    fn departures_by_crs_excludes_a_departure_already_before_now() {
        let raw = vec![RawSchedule {
            basic: basic(
                "C11052",
                StpIndicator::Permanent,
                "2026-05-18",
                "2026-12-11",
                WEEKDAYS,
            ),
            calling_points: vec![calling_point_with_departure(
                "EUSTON ",
                CallingPointKind::Origin,
                "08:22",
            )],
        }];
        let index = ScheduleIndex::build(raw);
        let date = NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let now = NaiveTime::from_hms_opt(9, 0, 0).unwrap(); // after 08:22
        let tiploc_to_crs = tiploc_map(&[("EUSTON", "EUS")]);

        let by_crs = departures_by_crs(&index, date, now, &tiploc_to_crs);
        assert!(by_crs.is_empty());
    }

    /// **Regression test for the 2026-09-25 Activity-code fix.** A set-down-only
    /// (`D`) calling point has a real booked departure, so before the Activity
    /// field was decoded at all it was published as a boardable departure --
    /// `GET /public/stations/{crs}/schedule-departures` offered it as a train a
    /// user could catch from a station where, in reality, nobody may board.
    #[test]
    fn departures_by_crs_excludes_a_set_down_only_calling_point() {
        let raw = vec![RawSchedule {
            basic: basic(
                "C11052",
                StpIndicator::Permanent,
                "2026-05-18",
                "2026-12-11",
                WEEKDAYS,
            ),
            calling_points: vec![
                calling_point_with_departure_and_activity(
                    "EUSTON ",
                    CallingPointKind::Origin,
                    "08:22",
                    "TB",
                ),
                // Real booked departure, but passengers may only get OFF here.
                calling_point_with_departure_and_activity(
                    "CARLILE",
                    CallingPointKind::Intermediate,
                    "12:13",
                    "D",
                ),
                calling_point_with_arrival("CREWE  ", CallingPointKind::Terminate, "13:00"),
            ],
        }];
        let index = ScheduleIndex::build(raw);
        let date = NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let now = NaiveTime::MIN;
        let tiploc_to_crs = tiploc_map(&[("EUSTON", "EUS"), ("CARLILE", "CAR"), ("CREWE", "CRE")]);

        let by_crs = departures_by_crs(&index, date, now, &tiploc_to_crs);

        assert!(
            by_crs.contains_key("EUS"),
            "the origin (`TB`, train begins) is a genuine boardable departure"
        );
        assert!(
            !by_crs.contains_key("CAR"),
            "a set-down-only stop must not be published as a departure a user can board"
        );
    }

    /// The same filter on the whole-network search product, which backs
    /// `GET /public/trains/search` -- every row there is offered to a user as a
    /// train they can catch FROM `origin_crs`.
    #[test]
    fn departures_by_destination_crs_excludes_a_set_down_only_calling_point() {
        let raw = vec![RawSchedule {
            basic: basic(
                "C11052",
                StpIndicator::Permanent,
                "2026-05-18",
                "2026-12-11",
                WEEKDAYS,
            ),
            calling_points: vec![
                calling_point_with_departure_and_activity(
                    "EUSTON ",
                    CallingPointKind::Origin,
                    "08:22",
                    "TB",
                ),
                calling_point_with_departure_and_activity(
                    "CARLILE",
                    CallingPointKind::Intermediate,
                    "12:13",
                    "D",
                ),
                calling_point_with_arrival("CREWE  ", CallingPointKind::Terminate, "13:00"),
            ],
        }];
        let index = ScheduleIndex::build(raw);
        let date = NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let tiploc_to_crs = tiploc_map(&[("EUSTON", "EUS"), ("CARLILE", "CAR"), ("CREWE", "CRE")]);

        let by_destination =
            departures_by_destination_crs(&index, date, NaiveTime::MIN, &tiploc_to_crs);

        let origins: Vec<&str> = by_destination["CRE"]
            .iter()
            .map(|d| d.origin_crs.as_str())
            .collect();
        assert_eq!(
            origins,
            vec!["EUS"],
            "only the genuinely boardable calling point may be offered as an origin"
        );
    }

    /// The fail-open property at the consumer level: a calling point with NO
    /// Activity field (every pre-existing fixture in this file, and every
    /// `schedule_line_population` blob published before the field existed) must
    /// still publish. Without this the fix would silently empty station boards
    /// on any decode gap.
    #[test]
    fn departures_by_crs_still_publishes_a_calling_point_with_no_activity_field_at_all() {
        let raw = vec![RawSchedule {
            basic: basic(
                "C11052",
                StpIndicator::Permanent,
                "2026-05-18",
                "2026-12-11",
                WEEKDAYS,
            ),
            calling_points: vec![calling_point_with_departure(
                "EUSTON ",
                CallingPointKind::Origin,
                "08:22",
            )],
        }];
        let index = ScheduleIndex::build(raw);
        let date = NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let tiploc_to_crs = tiploc_map(&[("EUSTON", "EUS")]);

        let by_crs = departures_by_crs(&index, date, NaiveTime::MIN, &tiploc_to_crs);
        assert!(by_crs.contains_key("EUS"));
    }

    /// **Regression test for the 2026-09-25 overnight `now`-filter fix.** The
    /// `now`-forward filter compared bare clock times and ignored
    /// `day_offset`, so at `now = 23:00` on the service date this real
    /// live-confirmed overnight working's Barking departure -- `00:07`,
    /// `day_offset: 1`, genuinely 67 minutes in the FUTURE -- was dropped from
    /// the station board as though it had already gone. Liverpool Street's
    /// `23:48` (same day, 48 minutes away) survived, which is what made the
    /// bug look like correct behavior at a glance.
    #[test]
    fn departures_by_crs_keeps_a_genuine_post_midnight_departure_that_is_still_in_the_future() {
        let index = ScheduleIndex::build(f49687_raw());
        let date = NaiveDate::from_ymd_opt(2026, 9, 5).unwrap();
        let now = NaiveTime::from_hms_opt(23, 0, 0).unwrap();
        let tiploc_to_crs = tiploc_map(&[
            ("LIVST", "LST"),
            ("STFD", "SRA"),
            ("BARKING", "BKG"),
            ("SHENFLD", "SNF"),
        ]);

        let by_crs = departures_by_crs(&index, date, now, &tiploc_to_crs);

        assert_eq!(
            by_crs["LST"][0].scheduled,
            NaiveTime::from_hms_opt(23, 48, 0).unwrap(),
            "the same-day 23:48 departure is 48 minutes away and must be kept"
        );
        let barking = by_crs.get("BKG").unwrap_or_else(|| {
            panic!(
                "Barking's 00:07 departure is day_offset 1 -- 67 minutes in the FUTURE at 23:00 \
                 -- and must not be dropped as though its bare clock time made it past"
            )
        });
        assert_eq!(
            barking[0].scheduled,
            NaiveTime::from_hms_opt(0, 7, 0).unwrap()
        );
        assert_eq!(barking[0].day_offset, 1);
    }

    /// **Regression test for the L2 overnight-carryover fix.** `F49687`
    /// (ALL_DAYS) is booked under `2026-09-04`'s instance too, and that
    /// instance's Barking (`00:07`, `day_offset: 1` relative to
    /// `2026-09-04`) genuinely lands on `2026-09-05` -- a delivery processed
    /// at `00:05` on `2026-09-05` (a real post-midnight cycle time, e.g. a
    /// 01:30 delivery) must still surface it on `2026-09-05`'s own board,
    /// rebased to `day_offset: 0` (it IS today's departure, not tomorrow's),
    /// not only on `2026-09-04`'s now-superseded board.
    #[test]
    fn departures_by_crs_includes_yesterdays_still_future_post_midnight_calling_point() {
        let index = ScheduleIndex::build(f49687_raw());
        let date = NaiveDate::from_ymd_opt(2026, 9, 5).unwrap();
        let now = NaiveTime::from_hms_opt(0, 5, 0).unwrap();
        let tiploc_to_crs = tiploc_map(&[
            ("LIVST", "LST"),
            ("STFD", "SRA"),
            ("BARKING", "BKG"),
            ("SHENFLD", "SNF"),
        ]);

        let by_crs = departures_by_crs(&index, date, now, &tiploc_to_crs);

        let barking = by_crs.get("BKG").unwrap_or_else(|| {
            panic!(
                "yesterday's (2026-09-04) F49687 instance's 00:07 Barking calling point is still \
                 12 minutes in the future at 00:05 on 2026-09-05 and must not be dropped just \
                 because it was booked under yesterday's schedule instance"
            )
        });
        let carried_over = barking
            .iter()
            .find(|d| d.day_offset == 0)
            .unwrap_or_else(|| {
                panic!(
                    "the carried-over calling point must be rebased to day_offset 0 -- it lands \
                     on 2026-09-05 itself, not the day after"
                )
            });
        assert_eq!(
            carried_over.scheduled,
            NaiveTime::from_hms_opt(0, 7, 0).unwrap()
        );
        // 2026-09-05's OWN F49687 instance also legitimately contributes a
        // day_offset: 1 Barking entry (tonight's 23:48 departure's own
        // 00:07-the-day-after continuation) -- both are real, distinct
        // physical departures and neither should suppress the other.
        assert!(
            barking.iter().any(|d| d.day_offset == 1),
            "2026-09-05's own instance must still independently contribute its own tomorrow-\
             bound continuation"
        );
    }

    /// The other direction of the same bug, and the reason the fix is a tuple
    /// comparison rather than "always keep `day_offset >= 1`": at
    /// `now = 00:30` a `day_offset: 0` departure at `00:40` is genuinely
    /// today's and still in the future, while the SAME clock time at
    /// `day_offset: 1` belongs to tomorrow and is also in the future. What
    /// must be excluded is only what is really past -- a `day_offset: 0`
    /// departure before `now`.
    #[test]
    fn departures_by_crs_excludes_only_what_is_really_past_once_day_offset_is_considered() {
        let raw = vec![RawSchedule {
            basic: basic(
                "F49687",
                StpIndicator::Permanent,
                "2026-05-18",
                "2026-12-11",
                ALL_DAYS,
            ),
            calling_points: vec![
                // day_offset 0, 00:10 -- really past at 00:30.
                calling_point_with_departure("LIVST  ", CallingPointKind::Origin, "00:10"),
                // day_offset 0, 00:40 -- still to come at 00:30.
                calling_point_with_both(
                    "STFD   ",
                    CallingPointKind::Intermediate,
                    "00:35",
                    "00:40",
                ),
                calling_point_with_arrival("SHENFLD", CallingPointKind::Terminate, "01:01"),
            ],
        }];
        let index = ScheduleIndex::build(raw);
        let date = NaiveDate::from_ymd_opt(2026, 9, 5).unwrap();
        let now = NaiveTime::from_hms_opt(0, 30, 0).unwrap();
        let tiploc_to_crs = tiploc_map(&[("LIVST", "LST"), ("STFD", "SRA"), ("SHENFLD", "SNF")]);

        let by_crs = departures_by_crs(&index, date, now, &tiploc_to_crs);

        assert!(
            !by_crs.contains_key("LST"),
            "a same-day 00:10 departure really is past at 00:30 and must still be excluded"
        );
        assert_eq!(
            by_crs["SRA"][0].scheduled,
            NaiveTime::from_hms_opt(0, 40, 0).unwrap(),
            "a same-day 00:40 departure is still to come at 00:30"
        );
    }

    #[test]
    fn departures_by_crs_carries_each_calling_points_own_day_offset_through() {
        // The exact real live-confirmed overnight working
        // (`f49687_raw`, see its own doc comment) this whole day_offset
        // fix targets: Liverpool Street 23:48 (day_offset 0) and Barking
        // 00:06/00:07 (day_offset 1, the real next calendar day) both flow
        // through `departures_by_crs` -- this is the exact bucket that
        // backs `GET /public/stations/{crs}/schedule-departures`, the CIF
        // fallback picker `TrackTrainForm.tsx::pickCifDeparture` reads.
        // Before this fix, `ScheduleDeparture` had no `day_offset` field at
        // all, so Barking's entry silently claimed "same day as
        // Liverpool Street" on the wire.
        let index = ScheduleIndex::build(f49687_raw());
        let date = NaiveDate::from_ymd_opt(2026, 9, 5).unwrap();
        let now = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
        let tiploc_to_crs = tiploc_map(&[
            ("LIVST", "LST"),
            ("STFD", "SRA"),
            ("BARKING", "BKG"),
            ("SHENFLD", "SNF"),
        ]);

        let by_crs = departures_by_crs(&index, date, now, &tiploc_to_crs);

        assert_eq!(
            by_crs["LST"][0].day_offset, 0,
            "Liverpool Street 23:48 is still 2026-09-05"
        );
        assert_eq!(
            by_crs["BKG"][0].day_offset, 1,
            "Barking 00:06/00:07 is really 2026-09-06 -- the exact live-confirmed regression"
        );
    }

    #[test]
    fn departures_by_crs_excludes_a_cancelled_schedule_even_though_its_time_has_not_passed() {
        // Same real UID/date/days shape as this file's own `c11052_raw`
        // fixture (a base P pattern plus a real STP=C override on 2026-08-31).
        let raw = c11052_with_departures();
        let index = ScheduleIndex::build(raw);
        let date = NaiveDate::from_ymd_opt(2026, 8, 31).unwrap(); // the cancelled date
        let now = NaiveTime::from_hms_opt(0, 0, 0).unwrap(); // well before any booked time
        let tiploc_to_crs = tiploc_map(&[("EUSTON", "EUS")]);

        let by_crs = departures_by_crs(&index, date, now, &tiploc_to_crs);
        assert!(
            by_crs.is_empty(),
            "the STP=C override must suppress this date's bucket entirely"
        );
    }

    #[test]
    fn departures_by_crs_drops_a_calling_point_whose_own_tiploc_is_unresolved() {
        let raw = vec![RawSchedule {
            basic: basic(
                "C11052",
                StpIndicator::Permanent,
                "2026-05-18",
                "2026-12-11",
                WEEKDAYS,
            ),
            calling_points: vec![calling_point_with_departure(
                "EUSTON ",
                CallingPointKind::Origin,
                "08:22",
            )],
        }];
        let index = ScheduleIndex::build(raw);
        let date = NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let now = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
        let tiploc_to_crs = HashMap::new(); // EUSTON not resolved at all

        let by_crs = departures_by_crs(&index, date, now, &tiploc_to_crs);
        assert!(
            by_crs.is_empty(),
            "an unresolved origin TIPLOC drops the whole calling point, never a fabricated CRS"
        );
    }

    #[test]
    fn departures_by_crs_keeps_a_calling_point_with_an_unresolved_destination_as_none() {
        let raw = vec![RawSchedule {
            basic: basic(
                "C11052",
                StpIndicator::Permanent,
                "2026-05-18",
                "2026-12-11",
                WEEKDAYS,
            ),
            calling_points: vec![
                calling_point_with_departure("EUSTON ", CallingPointKind::Origin, "08:22"),
                calling_point("CREWE  ", CallingPointKind::Terminate),
            ],
        }];
        let index = ScheduleIndex::build(raw);
        let date = NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let now = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
        let tiploc_to_crs = tiploc_map(&[("EUSTON", "EUS")]); // CREWE deliberately absent

        let by_crs = departures_by_crs(&index, date, now, &tiploc_to_crs);
        assert_eq!(by_crs["EUS"][0].destination_crs, None);
    }

    #[test]
    fn departures_by_crs_buckets_an_intermediate_calling_point_departure_under_its_own_crs() {
        let raw = vec![RawSchedule {
            basic: basic(
                "C11052",
                StpIndicator::Permanent,
                "2026-05-18",
                "2026-12-11",
                WEEKDAYS,
            ),
            calling_points: vec![
                calling_point_with_departure("EUSTON ", CallingPointKind::Origin, "08:22"),
                calling_point_with_departure("CREWE  ", CallingPointKind::Intermediate, "10:05"),
                calling_point("MNCRPIC", CallingPointKind::Terminate),
            ],
        }];
        let index = ScheduleIndex::build(raw);
        let date = NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let now = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
        let tiploc_to_crs = tiploc_map(&[("EUSTON", "EUS"), ("CREWE", "CRE"), ("MNCRPIC", "MAN")]);

        let by_crs = departures_by_crs(&index, date, now, &tiploc_to_crs);
        assert_eq!(
            by_crs.len(),
            2,
            "both EUSTON (Origin) and CREWE (Intermediate) get their own bucket entry"
        );
        assert_eq!(
            by_crs["EUS"][0].scheduled,
            NaiveTime::from_hms_opt(8, 22, 0).unwrap()
        );
        assert_eq!(
            by_crs["CRE"][0].scheduled,
            NaiveTime::from_hms_opt(10, 5, 0).unwrap()
        );
        assert_eq!(by_crs["CRE"][0].destination_crs, Some("MAN".to_string()));
    }

    #[test]
    fn departures_by_destination_crs_buckets_an_origin_departure_under_the_schedules_destination() {
        let raw = vec![RawSchedule {
            basic: basic(
                "C11052",
                StpIndicator::Permanent,
                "2026-05-18",
                "2026-12-11",
                WEEKDAYS,
            ),
            calling_points: vec![
                calling_point_with_departure("EUSTON ", CallingPointKind::Origin, "08:22"),
                calling_point("CREWE  ", CallingPointKind::Terminate),
            ],
        }];
        let index = ScheduleIndex::build(raw);
        let date = NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let now = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
        let tiploc_to_crs = tiploc_map(&[("EUSTON", "EUS"), ("CREWE", "CRE")]);

        let by_destination = departures_by_destination_crs(&index, date, now, &tiploc_to_crs);

        assert_eq!(
            by_destination.len(),
            1,
            "the ONLY bucket key is the schedule's destination (CRE), never its origin"
        );
        assert!(
            !by_destination.contains_key("EUS"),
            "this function must not also bucket by origin -- that is departures_by_crs's job"
        );
        let crewe = &by_destination["CRE"];
        assert_eq!(crewe.len(), 1);
        assert_eq!(crewe[0].uid, "C11052");
        assert_eq!(crewe[0].origin_crs, "EUS");
        assert_eq!(
            crewe[0].scheduled,
            NaiveTime::from_hms_opt(8, 22, 0).unwrap()
        );
    }

    #[test]
    fn departures_by_destination_crs_buckets_every_departure_bearing_calling_point_under_one_destination()
     {
        // The load-bearing difference from departures_by_crs: a train from
        // EUSTON to MNCRPIC calling at CREWE contributes TWO entries to the
        // SAME (MAN) bucket -- "next train to Manchester from anywhere"
        // must find it whether the searcher is at Euston or at Crewe. This
        // is also exactly why this bucket's cardinality needed its own
        // sizing pass (Task 1) rather than reusing the origin-keyed cap.
        let raw = vec![RawSchedule {
            basic: basic(
                "C11052",
                StpIndicator::Permanent,
                "2026-05-18",
                "2026-12-11",
                WEEKDAYS,
            ),
            calling_points: vec![
                calling_point_with_departure("EUSTON ", CallingPointKind::Origin, "08:22"),
                calling_point_with_departure("CREWE  ", CallingPointKind::Intermediate, "10:05"),
                calling_point("MNCRPIC", CallingPointKind::Terminate),
            ],
        }];
        let index = ScheduleIndex::build(raw);
        let date = NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let now = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
        let tiploc_to_crs = tiploc_map(&[("EUSTON", "EUS"), ("CREWE", "CRE"), ("MNCRPIC", "MAN")]);

        let by_destination = departures_by_destination_crs(&index, date, now, &tiploc_to_crs);

        assert_eq!(by_destination.len(), 1, "one destination, one bucket");
        let manchester = &by_destination["MAN"];
        assert_eq!(manchester.len(), 2);
        let mut origins: Vec<&str> = manchester.iter().map(|d| d.origin_crs.as_str()).collect();
        origins.sort();
        assert_eq!(origins, vec!["CRE", "EUS"]);
    }

    #[test]
    fn departures_by_destination_crs_attaches_the_schedules_true_origin_to_every_one_of_its_entries()
     {
        // The load-bearing distinction from `origin_crs`: EVERY entry of
        // this schedule carries the SAME true_origin_crs (EUS), even the
        // entry whose own origin_crs (the calling point it represents) is
        // CRE, not EUS.
        let raw = vec![RawSchedule {
            basic: basic(
                "C11052",
                StpIndicator::Permanent,
                "2026-05-18",
                "2026-12-11",
                WEEKDAYS,
            ),
            calling_points: vec![
                calling_point_with_departure("EUSTON ", CallingPointKind::Origin, "08:22"),
                calling_point_with_departure("CREWE  ", CallingPointKind::Intermediate, "10:05"),
                calling_point("MNCRPIC", CallingPointKind::Terminate),
            ],
        }];
        let index = ScheduleIndex::build(raw);
        let date = NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let now = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
        let tiploc_to_crs = tiploc_map(&[("EUSTON", "EUS"), ("CREWE", "CRE"), ("MNCRPIC", "MAN")]);

        let by_destination = departures_by_destination_crs(&index, date, now, &tiploc_to_crs);

        let manchester = &by_destination["MAN"];
        assert_eq!(manchester.len(), 2);
        for entry in manchester {
            assert_eq!(entry.true_origin_crs, Some("EUS".to_string()));
        }
        let mut origins: Vec<&str> = manchester.iter().map(|d| d.origin_crs.as_str()).collect();
        origins.sort();
        assert_eq!(origins, vec!["CRE", "EUS"]);
    }

    #[test]
    fn departures_by_destination_crs_attaches_each_entrys_own_calling_point_arrival_not_the_schedules()
     {
        // The load-bearing distinction from destination_arrival/
        // true_origin_crs: calling_point_arrival varies PER ENTRY. EUSTON
        // is the schedule's Origin (no booked_arrival at all, by
        // definition), so its entry's calling_point_arrival is None even
        // though the schedule DOES have a real destination_arrival at
        // MNCRPIC. CREWE is a genuine Intermediate stop with its own
        // booked_arrival (09:58), distinct from both its own departure
        // (10:05) and the schedule's destination_arrival (11:30).
        let raw = vec![RawSchedule {
            basic: basic(
                "C11052",
                StpIndicator::Permanent,
                "2026-05-18",
                "2026-12-11",
                WEEKDAYS,
            ),
            calling_points: vec![
                calling_point_with_departure("EUSTON ", CallingPointKind::Origin, "08:22"),
                calling_point_with_both(
                    "CREWE  ",
                    CallingPointKind::Intermediate,
                    "09:58",
                    "10:05",
                ),
                calling_point_with_arrival("MNCRPIC", CallingPointKind::Terminate, "11:30"),
            ],
        }];
        let index = ScheduleIndex::build(raw);
        let date = NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let now = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
        let tiploc_to_crs = tiploc_map(&[("EUSTON", "EUS"), ("CREWE", "CRE"), ("MNCRPIC", "MAN")]);

        let by_destination = departures_by_destination_crs(&index, date, now, &tiploc_to_crs);

        let manchester = &by_destination["MAN"];
        let eus_entry = manchester
            .iter()
            .find(|d| d.origin_crs == "EUS")
            .expect("EUS entry present");
        assert_eq!(
            eus_entry.calling_point_arrival, None,
            "the schedule's true origin has no booked_arrival of its own"
        );
        assert_eq!(
            eus_entry.destination_arrival,
            Some(NaiveTime::from_hms_opt(11, 30, 0).unwrap()),
            "destination_arrival is still the SCHEDULE's true-destination arrival, unaffected"
        );

        let cre_entry = manchester
            .iter()
            .find(|d| d.origin_crs == "CRE")
            .expect("CRE entry present");
        assert_eq!(
            cre_entry.calling_point_arrival,
            Some(NaiveTime::from_hms_opt(9, 58, 0).unwrap()),
            "an intermediate calling point's own booked_arrival, NOT its departure (10:05) and \
             NOT the schedule's destination_arrival (11:30)"
        );
    }

    #[test]
    fn departures_by_destination_crs_keeps_a_row_with_true_origin_crs_none_when_the_schedules_first_calling_point_is_unresolved()
     {
        // Contrast with departures_by_destination_crs_drops_a_schedule_whose_destination_tiploc_is_unresolved
        // (a bucket-KEY unresolved -> drop the whole schedule). true_origin_crs
        // is a plain FILTER field, so it follows departures_by_crs's own
        // softer "degrade to None, keep the row" precedent instead.
        let raw = vec![RawSchedule {
            basic: basic(
                "C11052",
                StpIndicator::Permanent,
                "2026-05-18",
                "2026-12-11",
                WEEKDAYS,
            ),
            calling_points: vec![
                calling_point_with_departure("EUSTON ", CallingPointKind::Origin, "08:22"),
                calling_point_with_departure("CREWE  ", CallingPointKind::Intermediate, "10:05"),
                calling_point("MNCRPIC", CallingPointKind::Terminate),
            ],
        }];
        let index = ScheduleIndex::build(raw);
        let date = NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let now = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
        // EUSTON (the schedule's true origin) deliberately absent; CREWE
        // and MNCRPIC both resolve.
        let tiploc_to_crs = tiploc_map(&[("CREWE", "CRE"), ("MNCRPIC", "MAN")]);

        let by_destination = departures_by_destination_crs(&index, date, now, &tiploc_to_crs);

        assert_eq!(
            by_destination["MAN"].len(),
            1,
            "EUSTON's own row is still dropped -- its origin_crs can't resolve either, same as \
             departures_by_crs_drops_a_calling_point_whose_own_tiploc_is_unresolved"
        );
        assert_eq!(by_destination["MAN"][0].origin_crs, "CRE");
        assert_eq!(
            by_destination["MAN"][0].true_origin_crs, None,
            "the schedule's true origin TIPLOC never resolved, so this filter field degrades to \
             None -- it does NOT drop the row the way an unresolved destination_crs would"
        );
    }

    #[test]
    fn departures_by_destination_crs_excludes_a_departure_already_before_now() {
        // Same `now`-forward posture as departures_by_crs (resolve.rs:193):
        // the 08:22 EUSTON departure is gone by 10:00, but the 10:05 CREWE
        // one is still ahead -- the bucket keeps only the latter.
        let raw = vec![RawSchedule {
            basic: basic(
                "C11052",
                StpIndicator::Permanent,
                "2026-05-18",
                "2026-12-11",
                WEEKDAYS,
            ),
            calling_points: vec![
                calling_point_with_departure("EUSTON ", CallingPointKind::Origin, "08:22"),
                calling_point_with_departure("CREWE  ", CallingPointKind::Intermediate, "10:05"),
                calling_point("MNCRPIC", CallingPointKind::Terminate),
            ],
        }];
        let index = ScheduleIndex::build(raw);
        let date = NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let now = NaiveTime::from_hms_opt(10, 0, 0).unwrap();
        let tiploc_to_crs = tiploc_map(&[("EUSTON", "EUS"), ("CREWE", "CRE"), ("MNCRPIC", "MAN")]);

        let by_destination = departures_by_destination_crs(&index, date, now, &tiploc_to_crs);

        assert_eq!(by_destination["MAN"].len(), 1);
        assert_eq!(by_destination["MAN"][0].origin_crs, "CRE");
    }

    #[test]
    fn departures_by_destination_crs_excludes_a_cancelled_schedule_even_though_its_time_has_not_passed()
     {
        // Real UID/STP/date-range/days values (a base P pattern plus a real
        // STP=C override on 2026-08-31), reusing this module's own
        // c11052_with_departures fixture and its Bank Holiday cross-check.
        let index = ScheduleIndex::build(c11052_with_departures());
        let date = NaiveDate::from_ymd_opt(2026, 8, 31).unwrap(); // the cancelled date
        let now = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
        let tiploc_to_crs = tiploc_map(&[("EUSTON", "EUS")]);

        let by_destination = departures_by_destination_crs(&index, date, now, &tiploc_to_crs);
        assert!(
            by_destination.is_empty(),
            "the STP=C override must suppress this date's bucket entirely"
        );
    }

    #[test]
    fn departures_by_destination_crs_falls_back_to_a_tiploc_keyed_bucket_when_the_destination_is_unresolved()
     {
        // (2026-09-23, revised) This used to assert the OPPOSITE: that an
        // unresolved destination TIPLOC dropped the whole schedule, because
        // "there is no honest bucket key to file it under". That was a
        // real, live completeness bug, not a justified tradeoff -- see
        // `unresolved_destination_key`'s own doc comment for the real,
        // checked-in-reference-data evidence (Ashford International,
        // Stratford International, Ebbsfleet International are all
        // permanently unresolvable via `tiploc_to_crs` for exactly this
        // reason). The schedule is no longer dropped: its EUSTON entry
        // (which resolves fine) must still surface, bucketed under a
        // TIPLOC-derived fallback key instead of vanishing.
        let raw = vec![RawSchedule {
            basic: basic(
                "C11052",
                StpIndicator::Permanent,
                "2026-05-18",
                "2026-12-11",
                WEEKDAYS,
            ),
            calling_points: vec![
                calling_point_with_departure("EUSTON ", CallingPointKind::Origin, "08:22"),
                calling_point("CREWE  ", CallingPointKind::Terminate),
            ],
        }];
        let index = ScheduleIndex::build(raw);
        let date = NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let now = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
        let tiploc_to_crs = tiploc_map(&[("EUSTON", "EUS")]); // CREWE deliberately absent

        let by_destination = departures_by_destination_crs(&index, date, now, &tiploc_to_crs);
        assert!(
            !by_destination.contains_key("CREWE") && !by_destination.contains_key("crewe"),
            "the fallback key must be visibly distinct from a bare TIPLOC, never the raw string"
        );
        let bucket = by_destination
            .get("~CREWE")
            .expect("EUSTON's resolvable entry must surface under the CREWE fallback key");
        assert_eq!(bucket.len(), 1);
        assert_eq!(bucket[0].origin_crs, "EUS");
        assert_eq!(bucket[0].uid, "C11052");
    }

    #[test]
    fn departures_by_destination_crs_falls_back_for_the_real_ashford_international_ambiguity() {
        // Grounded in real, checked-in reference data, not a synthetic
        // worst case: reference-data/stanox-crs.md documents STANOX 89428
        // as genuinely irresolvable -- TIPLOC ASHFKI/CRS ASI (Ashford
        // International) and TIPLOC ASHFKY/CRS AFK (Ashford (Kent)) share
        // one physical STANOX with no principled tiebreaker between two
        // real, non-X-prefixed CRS codes, so NEITHER TIPLOC ever appears in
        // a real `tiploc_to_crs` map built from this data (see
        // crates/schedule-reference/src/parser.rs::resolve_tests's own
        // `ambiguous_stanox_with_two_non_x_candidates_is_excluded_entirely`
        // and `all_14_real_ambiguous_stanox_values_...` for the same
        // exclusion pinned at the source). A real Southeastern "Javelin"
        // service terminating at Ashford International after calling at
        // Tonbridge (a perfectly ordinary, resolvable intermediate station)
        // must still be findable by searching from Tonbridge.
        let raw = vec![RawSchedule {
            basic: basic(
                "Z12345",
                StpIndicator::Permanent,
                "2026-05-18",
                "2026-12-11",
                WEEKDAYS,
            ),
            calling_points: vec![
                calling_point_with_departure("TONBRDG", CallingPointKind::Origin, "18:04"),
                calling_point("ASHFKI ", CallingPointKind::Terminate),
            ],
        }];
        let index = ScheduleIndex::build(raw);
        let date = NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let now = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
        // ASHFKI deliberately absent -- exactly like a real tiploc_to_crs
        // map built from stanox-crs.md's own documented exclusion.
        let tiploc_to_crs = tiploc_map(&[("TONBRDG", "TON")]);

        let by_destination = departures_by_destination_crs(&index, date, now, &tiploc_to_crs);
        let bucket = by_destination
            .get("~ASHFKI")
            .expect("Tonbridge's resolvable entry must not vanish just because Ashford International's own TIPLOC is unresolvable");
        assert_eq!(bucket.len(), 1);
        assert_eq!(bucket[0].origin_crs, "TON");
        assert_eq!(bucket[0].uid, "Z12345");
    }

    #[test]
    fn departures_by_destination_crs_drops_only_the_calling_point_whose_own_tiploc_is_unresolved() {
        // Complementary to the test above: an unresolved INTERMEDIATE
        // tiploc drops just that one entry, not the schedule -- the
        // destination bucket still exists and still holds the resolvable
        // calling points. Same "drop, never fabricate" rule as
        // departures_by_crs (resolve.rs:196-198), applied per entry.
        let raw = vec![RawSchedule {
            basic: basic(
                "C11052",
                StpIndicator::Permanent,
                "2026-05-18",
                "2026-12-11",
                WEEKDAYS,
            ),
            calling_points: vec![
                calling_point_with_departure("EUSTON ", CallingPointKind::Origin, "08:22"),
                calling_point_with_departure("CREWE  ", CallingPointKind::Intermediate, "10:05"),
                calling_point("MNCRPIC", CallingPointKind::Terminate),
            ],
        }];
        let index = ScheduleIndex::build(raw);
        let date = NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let now = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
        // CREWE deliberately absent; EUSTON and MNCRPIC both resolve.
        let tiploc_to_crs = tiploc_map(&[("EUSTON", "EUS"), ("MNCRPIC", "MAN")]);

        let by_destination = departures_by_destination_crs(&index, date, now, &tiploc_to_crs);

        assert_eq!(by_destination["MAN"].len(), 1);
        assert_eq!(by_destination["MAN"][0].origin_crs, "EUS");
    }

    #[test]
    fn departures_by_destination_crs_never_buckets_the_terminating_calling_point_itself() {
        // A Terminate calling point has no booked_departure by
        // construction (CallingPointKind::Terminate's own doc), so a train
        // must never appear as "departing from X" in X's own arrivals
        // bucket. Guards against a future refactor that starts reading
        // booked_arrival here.
        let raw = vec![RawSchedule {
            basic: basic(
                "C11052",
                StpIndicator::Permanent,
                "2026-05-18",
                "2026-12-11",
                WEEKDAYS,
            ),
            calling_points: vec![
                calling_point_with_departure("EUSTON ", CallingPointKind::Origin, "08:22"),
                calling_point("CREWE  ", CallingPointKind::Terminate),
            ],
        }];
        let index = ScheduleIndex::build(raw);
        let date = NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let now = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
        let tiploc_to_crs = tiploc_map(&[("EUSTON", "EUS"), ("CREWE", "CRE")]);

        let by_destination = departures_by_destination_crs(&index, date, now, &tiploc_to_crs);
        assert_eq!(by_destination["CRE"].len(), 1);
        assert_eq!(
            by_destination["CRE"][0].origin_crs, "EUS",
            "CRE must not appear as its own bucket's origin"
        );
    }

    #[test]
    fn departures_by_destination_crs_attaches_the_terminating_calling_points_arrival_to_every_entry()
     {
        // The load-bearing mirror of
        // departures_by_destination_crs_attaches_the_schedules_true_origin_to_every_one_of_its_entries,
        // but for the LAST calling point's booked_arrival instead of the
        // FIRST's booked_departure.
        let raw = vec![RawSchedule {
            basic: basic(
                "C11052",
                StpIndicator::Permanent,
                "2026-05-18",
                "2026-12-11",
                WEEKDAYS,
            ),
            calling_points: vec![
                calling_point_with_departure("EUSTON ", CallingPointKind::Origin, "08:22"),
                calling_point_with_departure("CREWE  ", CallingPointKind::Intermediate, "10:05"),
                calling_point_with_arrival("MNCRPIC", CallingPointKind::Terminate, "11:30"),
            ],
        }];
        let index = ScheduleIndex::build(raw);
        let date = NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let now = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
        let tiploc_to_crs = tiploc_map(&[("EUSTON", "EUS"), ("CREWE", "CRE"), ("MNCRPIC", "MAN")]);

        let by_destination = departures_by_destination_crs(&index, date, now, &tiploc_to_crs);

        let manchester = &by_destination["MAN"];
        assert_eq!(manchester.len(), 2);
        for entry in manchester {
            assert_eq!(
                entry.destination_arrival,
                Some(NaiveTime::from_hms_opt(11, 30, 0).unwrap()),
                "every entry for this schedule must carry the SAME terminating arrival time"
            );
        }
    }

    #[test]
    fn departures_by_destination_crs_attaches_the_terminating_calling_points_own_day_offset_distinct_from_the_departures()
     {
        // The exact real live-confirmed overnight working (`f49687_raw`,
        // see its own doc comment) this fix targets: Liverpool Street
        // departs on `service_date` itself (day_offset 0), but the
        // terminating calling point (Shenfield) is really the day AFTER
        // `service_date` (day_offset 1) -- `destination_arrival_day_offset`
        // must reflect the TERMINATING calling point's own day_offset, not
        // be copied from the departing calling point's, and the two must
        // genuinely differ on this row. Before this fix,
        // `DestinationDeparture` had no `destination_arrival_day_offset`
        // field at all, so a real overnight schedule's `destination_arrival`
        // silently had no day of its own on the wire.
        let index = ScheduleIndex::build(f49687_raw());
        let date = NaiveDate::from_ymd_opt(2026, 9, 5).unwrap();
        let now = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
        let tiploc_to_crs = tiploc_map(&[
            ("LIVST", "LST"),
            ("STFD", "SRA"),
            ("BARKING", "BKG"),
            ("SHENFLD", "SNF"),
        ]);

        let by_destination = departures_by_destination_crs(&index, date, now, &tiploc_to_crs);

        let snf = &by_destination["SNF"];
        let livst_entry = snf
            .iter()
            .find(|e| e.origin_crs == "LST")
            .expect("Liverpool Street's own departure entry");
        assert_eq!(
            livst_entry.day_offset, 0,
            "Liverpool Street 23:48 is still 2026-09-05"
        );
        assert_eq!(
            livst_entry.destination_arrival_day_offset, 1,
            "Shenfield's terminating calling point is really 2026-09-06"
        );
        assert_ne!(
            livst_entry.day_offset, livst_entry.destination_arrival_day_offset,
            "the departure's own day_offset and the terminus's day_offset are two DIFFERENT \
             calling points on a genuine overnight schedule -- they must not collapse to the \
             same value"
        );

        let barking_entry = snf
            .iter()
            .find(|e| e.origin_crs == "BKG")
            .expect("Barking's own departure entry");
        assert_eq!(
            barking_entry.destination_arrival_day_offset, 1,
            "every entry for this schedule carries the SAME terminating day_offset, regardless \
             of which calling point it represents"
        );
    }

    #[test]
    fn departures_by_destination_crs_degrades_destination_arrival_to_none_when_the_terminating_calling_point_has_no_booked_arrival()
     {
        // A Terminate calling point built with the plain `calling_point`
        // helper (no booked_arrival) -- a real-world gap in the CIF data,
        // not a test bug. Must degrade this filter field to None, not
        // drop the row or panic.
        let raw = vec![RawSchedule {
            basic: basic(
                "C11052",
                StpIndicator::Permanent,
                "2026-05-18",
                "2026-12-11",
                WEEKDAYS,
            ),
            calling_points: vec![
                calling_point_with_departure("EUSTON ", CallingPointKind::Origin, "08:22"),
                calling_point("CREWE  ", CallingPointKind::Terminate),
            ],
        }];
        let index = ScheduleIndex::build(raw);
        let date = NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let now = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
        let tiploc_to_crs = tiploc_map(&[("EUSTON", "EUS"), ("CREWE", "CRE")]);

        let by_destination = departures_by_destination_crs(&index, date, now, &tiploc_to_crs);

        assert_eq!(by_destination["CRE"].len(), 1);
        assert_eq!(by_destination["CRE"][0].destination_arrival, None);
    }

    /// Same real UID/STP/date-range/days values as this file's own `c11052_raw`
    /// (a real Bank Holiday cross-check, see that fixture's own comment), but
    /// with a real `booked_departure` added to the base pattern's Origin
    /// calling point so `departures_by_crs` has something to (correctly) NOT
    /// return on the cancelled date.
    fn c11052_with_departures() -> Vec<RawSchedule> {
        vec![
            RawSchedule {
                basic: basic(
                    "C11052",
                    StpIndicator::Permanent,
                    "2026-05-18",
                    "2026-12-11",
                    WEEKDAYS,
                ),
                calling_points: vec![calling_point_with_departure(
                    "EUSTON ",
                    CallingPointKind::Origin,
                    "08:22",
                )],
            },
            RawSchedule {
                basic: basic(
                    "C11052",
                    StpIndicator::Cancellation,
                    "2026-08-31",
                    "2026-08-31",
                    MONDAY_ONLY,
                ),
                calling_points: Vec::new(),
            },
        ]
    }

    fn population_entry(uid: &str, calling_points: Vec<CallingPoint>) -> LinePopulationEntry {
        LinePopulationEntry {
            uid: uid.to_string(),
            calling_points,
        }
    }

    // Identity closure: every test below constructs `booked_departure` values
    // already meant to be read as UTC instants directly, so `to_utc` just
    // pairs a bare NaiveTime (shifted by `day_offset` days past the fixed
    // date) -- exercising `match_pin`'s arithmetic without pulling in a real
    // Europe/London conversion (that's `eta_blend::london_to_utc`'s own,
    // separately-tested job). Takes `day_offset` as its second parameter,
    // exactly like the real `to_utc` closures `schedule_matching::find_schedule_match`
    // and `eta_blend`/`journey` build around `london_to_utc`.
    fn utc_on(date: &str) -> impl Fn(NaiveTime, u8) -> Option<DateTime<Utc>> {
        let date = NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap();
        move |t, day_offset| {
            Some(DateTime::<Utc>::from_naive_utc_and_offset(
                (date + Duration::days(day_offset as i64)).and_time(t),
                Utc,
            ))
        }
    }

    #[test]
    fn match_pin_matches_a_departure_within_tolerance() {
        let population = vec![population_entry(
            "C11052",
            vec![calling_point_with_departure(
                "EUSTON ",
                CallingPointKind::Origin,
                "19:15",
            )],
        )];
        let scheduled: DateTime<Utc> = "2026-09-05T19:15:00Z".parse().unwrap();
        let matched = match_pin(
            &population,
            &["EUSTON"],
            scheduled,
            Duration::minutes(20),
            utc_on("2026-09-05"),
        );
        assert_eq!(matched.map(|e| e.uid.as_str()), Some("C11052"));
    }

    #[test]
    fn match_pin_rejects_a_departure_outside_tolerance() {
        let population = vec![population_entry(
            "C11052",
            vec![calling_point_with_departure(
                "EUSTON ",
                CallingPointKind::Origin,
                "19:15",
            )],
        )];
        let scheduled: DateTime<Utc> = "2026-09-05T20:00:00Z".parse().unwrap(); // 45m away
        assert_eq!(
            match_pin(
                &population,
                &["EUSTON"],
                scheduled,
                Duration::minutes(20),
                utc_on("2026-09-05")
            ),
            None
        );
    }

    #[test]
    fn match_pin_rejects_a_tiploc_not_in_crs_tiplocs() {
        let population = vec![population_entry(
            "C11052",
            vec![calling_point_with_departure(
                "CREWE  ",
                CallingPointKind::Origin,
                "19:15",
            )],
        )];
        let scheduled: DateTime<Utc> = "2026-09-05T19:15:00Z".parse().unwrap();
        assert_eq!(
            match_pin(
                &population,
                &["EUSTON"],
                scheduled,
                Duration::minutes(20),
                utc_on("2026-09-05")
            ),
            None
        );
    }

    #[test]
    fn match_pin_ignores_a_calling_point_with_no_booked_departure() {
        let population = vec![population_entry(
            "C11052",
            vec![calling_point("EUSTON ", CallingPointKind::Terminate)], // no booked_departure
        )];
        let scheduled: DateTime<Utc> = "2026-09-05T19:15:00Z".parse().unwrap();
        assert_eq!(
            match_pin(
                &population,
                &["EUSTON"],
                scheduled,
                Duration::minutes(20),
                utc_on("2026-09-05")
            ),
            None
        );
    }

    #[test]
    fn match_pin_nearest_time_wins_between_two_in_tolerance_candidates() {
        let population = vec![
            population_entry(
                "FAR",
                vec![calling_point_with_departure(
                    "EUSTON ",
                    CallingPointKind::Origin,
                    "19:05",
                )],
            ), // 10m away
            population_entry(
                "NEAR",
                vec![calling_point_with_departure(
                    "EUSTON ",
                    CallingPointKind::Origin,
                    "19:12",
                )],
            ), // 3m away
        ];
        let scheduled: DateTime<Utc> = "2026-09-05T19:15:00Z".parse().unwrap();
        let matched = match_pin(
            &population,
            &["EUSTON"],
            scheduled,
            Duration::minutes(20),
            utc_on("2026-09-05"),
        );
        assert_eq!(matched.map(|e| e.uid.as_str()), Some("NEAR"));
    }

    #[test]
    fn match_pin_on_an_exact_tie_the_first_in_population_order_wins() {
        let population = vec![
            population_entry(
                "FIRST",
                vec![calling_point_with_departure(
                    "EUSTON ",
                    CallingPointKind::Origin,
                    "19:10",
                )],
            ),
            population_entry(
                "SECOND",
                vec![calling_point_with_departure(
                    "EUSTON ",
                    CallingPointKind::Origin,
                    "19:20",
                )],
            ),
        ];
        let scheduled: DateTime<Utc> = "2026-09-05T19:15:00Z".parse().unwrap(); // exactly 5m from both
        let matched = match_pin(
            &population,
            &["EUSTON"],
            scheduled,
            Duration::minutes(20),
            utc_on("2026-09-05"),
        );
        assert_eq!(matched.map(|e| e.uid.as_str()), Some("FIRST"));
    }

    #[test]
    fn match_pin_skips_a_candidate_whose_to_utc_conversion_fails() {
        // Simulates a nonexistent-local-time DST edge case: to_utc returns
        // None for every candidate, so nothing can match even though the
        // TIPLOC/tolerance checks would otherwise pass.
        let population = vec![population_entry(
            "C11052",
            vec![calling_point_with_departure(
                "EUSTON ",
                CallingPointKind::Origin,
                "19:15",
            )],
        )];
        let scheduled: DateTime<Utc> = "2026-09-05T19:15:00Z".parse().unwrap();
        let matched = match_pin(
            &population,
            &["EUSTON"],
            scheduled,
            Duration::minutes(20),
            |_, _| None,
        );
        assert_eq!(matched, None);
    }

    #[test]
    fn match_pin_passes_each_calling_points_own_day_offset_to_to_utc() {
        // The exact shape of the live-confirmed midnight-crossing bug
        // (2026-09-09 investigation, c2c UID F49687): a calling point whose
        // `day_offset` is 1 (i.e. really the day AFTER the schedule's own
        // `service_date`) must have that offset handed to `to_utc`, not
        // silently dropped. `to_utc` here (`utc_on`) adds `day_offset` days
        // to its fixed base date before pairing it with the time -- so a
        // pin timed for the REAL next-calendar-day instant only matches
        // when this plumbing is correct.
        let population = vec![population_entry(
            "F49687",
            vec![CallingPoint {
                tiploc: "BARKING".to_string(),
                kind: CallingPointKind::Intermediate,
                booked_arrival: NaiveTime::from_hms_opt(0, 6, 0),
                booked_departure: NaiveTime::from_hms_opt(0, 7, 0),
                is_half_minute_arrival: false,
                is_half_minute_departure: false,
                day_offset: 1,
                activity: String::new(),
                public_arrival: None,
                public_departure: None,
                platform: None,
            }],
        )];
        // service_date is 2026-09-05, but Barking's real booked_departure
        // (00:07, day_offset 1) is really 2026-09-06 00:07.
        let scheduled: DateTime<Utc> = "2026-09-06T00:07:00Z".parse().unwrap();
        let matched = match_pin(
            &population,
            &["BARKING"],
            scheduled,
            Duration::minutes(1),
            utc_on("2026-09-05"),
        );
        assert_eq!(
            matched.map(|e| e.uid.as_str()),
            Some("F49687"),
            "day_offset must shift the base date forward, or this pin (dated the REAL next \
             calendar day) can never match a candidate still stamped with the schedule's own \
             service_date"
        );

        // The old, buggy behavior: ignoring day_offset and treating Barking's
        // 00:07 as if it were still 2026-09-05T00:07Z leaves no candidate
        // within tolerance of the real 2026-09-06T00:07Z pin.
        let stuck_on_service_date = |t: NaiveTime, _day_offset: u8| {
            Some(DateTime::<Utc>::from_naive_utc_and_offset(
                NaiveDate::from_ymd_opt(2026, 9, 5).unwrap().and_time(t),
                Utc,
            ))
        };
        assert_eq!(
            match_pin(
                &population,
                &["BARKING"],
                scheduled,
                Duration::minutes(1),
                stuck_on_service_date,
            ),
            None,
            "sanity check: ignoring day_offset really does reproduce the reported bug"
        );
    }

    // ---- assign_day_offsets / resolve_for_date day-offset tests ----

    /// The real live-confirmed c2c overnight working (2026-09-09
    /// investigation), UID F49687, service_date 2026-09-05: Liverpool
    /// Street 23:48 -> Stratford 23:54/23:55 -> Barking 00:06/00:07 ->
    /// Shoeburyness 01:01. Every stop from Barking onward is really
    /// 2026-09-06 wall-clock.
    fn f49687_raw() -> Vec<RawSchedule> {
        vec![RawSchedule {
            basic: basic(
                "F49687",
                StpIndicator::Permanent,
                "2026-05-18",
                "2026-12-11",
                ALL_DAYS, // 2026-09-05, this test's own date, is a Saturday
            ),
            calling_points: vec![
                calling_point_with_departure("LIVST  ", CallingPointKind::Origin, "23:48"),
                calling_point_with_both(
                    "STFD   ",
                    CallingPointKind::Intermediate,
                    "23:54",
                    "23:55",
                ),
                calling_point_with_both(
                    "BARKING",
                    CallingPointKind::Intermediate,
                    "00:06",
                    "00:07",
                ),
                calling_point_with_arrival("SHENFLD", CallingPointKind::Terminate, "01:01"),
            ],
        }]
    }

    #[test]
    fn assign_day_offsets_leaves_a_same_day_schedule_entirely_at_zero() {
        let index = ScheduleIndex::build(c11052_with_departures());
        let date = NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
        let resolved = index.schedule_for_uid("C11052", date).unwrap();
        assert!(resolved.calling_points.iter().all(|cp| cp.day_offset == 0));
    }

    #[test]
    fn resolve_for_date_assigns_day_offset_zero_before_and_one_at_and_after_the_midnight_crossing()
    {
        let index = ScheduleIndex::build(f49687_raw());
        let date = NaiveDate::from_ymd_opt(2026, 9, 5).unwrap();
        let resolved = index.schedule_for_uid("F49687", date).unwrap();

        assert_eq!(resolved.calling_points.len(), 4);
        assert_eq!(
            resolved.calling_points[0].day_offset, 0,
            "Liverpool Street 23:48 is still 2026-09-05"
        );
        assert_eq!(
            resolved.calling_points[1].day_offset, 0,
            "Stratford 23:54/23:55 is still 2026-09-05"
        );
        assert_eq!(
            resolved.calling_points[2].day_offset, 1,
            "Barking 00:06/00:07 is really 2026-09-06 -- the exact live-confirmed regression"
        );
        assert_eq!(
            resolved.calling_points[3].day_offset, 1,
            "Shenfield 01:01 stays on the crossed-into day, not a second crossing"
        );
    }

    #[test]
    fn resolve_for_date_assigns_a_second_day_offset_increment_on_a_second_midnight_crossing() {
        // Synthetic (no real 2-midnight CIF service confirmed live), but a
        // schedule can in principle cross midnight twice; the algorithm
        // must not cap at 1.
        let raw = vec![RawSchedule {
            basic: basic(
                "Z00000",
                StpIndicator::Permanent,
                "2026-05-18",
                "2026-12-11",
                ALL_DAYS, // 2026-09-05, this test's own date, is a Saturday
            ),
            calling_points: vec![
                calling_point_with_departure("AAA    ", CallingPointKind::Origin, "23:00"),
                calling_point_with_both(
                    "BBB    ",
                    CallingPointKind::Intermediate,
                    "01:00",
                    "23:30",
                ),
                calling_point_with_arrival("CCC    ", CallingPointKind::Terminate, "00:30"),
            ],
        }];
        let index = ScheduleIndex::build(raw);
        let date = NaiveDate::from_ymd_opt(2026, 9, 5).unwrap();
        let resolved = index.schedule_for_uid("Z00000", date).unwrap();

        assert_eq!(resolved.calling_points[0].day_offset, 0);
        assert_eq!(
            resolved.calling_points[1].day_offset, 1,
            "01:00 < 23:00 -- first crossing"
        );
        assert_eq!(
            resolved.calling_points[2].day_offset, 2,
            "00:30 < 23:30 -- second crossing"
        );
    }

    #[test]
    fn schedules_touching_carries_the_computed_day_offset_through_to_the_published_line_population()
    {
        // Line-population publishing (schedule-reference's own
        // publish_schedule_line_population) goes through schedules_touching,
        // not schedule_for_uid directly -- this proves that path also
        // carries day_offset, since LinePopulationEntry::from(ResolvedSchedule)
        // just moves calling_points verbatim.
        let index = ScheduleIndex::build(f49687_raw());
        let date = NaiveDate::from_ymd_opt(2026, 9, 5).unwrap();
        let touching = schedules_touching(&index, &["BARKING"], date);
        assert_eq!(touching.len(), 1);
        let barking = touching[0]
            .calling_points
            .iter()
            .find(|cp| normalize_tiploc(&cp.tiploc) == "BARKING")
            .unwrap();
        assert_eq!(barking.day_offset, 1);
    }

    // `unresolved_booked_tiplocs`'s own tests. Real TIPLOC/CRS values reused
    // from this crate's own doc comments and the checked-in 2026-09-23
    // Northampton/Hanslope Junction incident this function exists for
    // (`NMPTN`/no CRS at all is the real genuine-gap case; `HANSLPJ`/`XHN`
    // is the real legitimate-non-station case -- see `is_bookable_crs`'s own
    // doc comment in `crates/api/src/data/journey.rs`).
    mod unresolved_booked_tiplocs_tests {
        use super::*;

        fn schedule_with_one_calling_point(uid: &str, cp: CallingPoint) -> RawSchedule {
            RawSchedule {
                basic: basic(
                    uid,
                    StpIndicator::Permanent,
                    "2026-05-18",
                    "2026-12-11",
                    ALL_DAYS,
                ),
                calling_points: vec![cp],
            }
        }

        #[test]
        fn a_tiploc_that_resolves_to_a_real_crs_is_not_returned() {
            let index = ScheduleIndex::build(vec![schedule_with_one_calling_point(
                "T00001",
                calling_point_with_departure("EUSTON ", CallingPointKind::Origin, "08:22"),
            )]);
            let date = NaiveDate::from_ymd_opt(2026, 9, 23).unwrap();
            let tiploc_to_crs = tiploc_map(&[("EUSTON", "EUS")]);

            let unresolved = unresolved_booked_tiplocs(&index, date, &tiploc_to_crs);
            assert!(unresolved.is_empty());
        }

        #[test]
        fn a_tiploc_resolved_to_an_x_prefixed_pseudo_crs_is_not_returned() {
            // HANSLPJ (Hanslope Junction) resolving to XHN is present as a
            // KEY in tiploc_to_crs -- exactly like the real production case
            // -- so this is the legitimate-non-station branch, not a gap.
            let index = ScheduleIndex::build(vec![schedule_with_one_calling_point(
                "T00002",
                calling_point_with_departure("HANSLPJ", CallingPointKind::Intermediate, "10:05"),
            )]);
            let date = NaiveDate::from_ymd_opt(2026, 9, 23).unwrap();
            let tiploc_to_crs = tiploc_map(&[("HANSLPJ", "XHN")]);

            let unresolved = unresolved_booked_tiplocs(&index, date, &tiploc_to_crs);
            assert!(
                unresolved.is_empty(),
                "an X-prefixed pseudo-CRS is still a resolved entry -- not this function's job"
            );
        }

        #[test]
        fn a_tiploc_with_no_crs_row_at_all_but_a_booked_time_is_returned() {
            // The real Northampton case: NMPTN has a genuine booked time but
            // no stanox_crs row of any kind.
            let index = ScheduleIndex::build(vec![schedule_with_one_calling_point(
                "T00003",
                calling_point_with_departure("NMPTN  ", CallingPointKind::Intermediate, "09:15"),
            )]);
            let date = NaiveDate::from_ymd_opt(2026, 9, 23).unwrap();
            let tiploc_to_crs = HashMap::new(); // NMPTN entirely absent

            let unresolved = unresolved_booked_tiplocs(&index, date, &tiploc_to_crs);
            assert_eq!(unresolved, vec!["NMPTN".to_string()]);
        }

        #[test]
        fn a_pure_pass_with_no_booked_time_and_no_crs_row_is_not_returned() {
            // Neither booked_arrival nor booked_departure set at all -- a
            // bare pass-through location, not a genuine timed stop, even
            // though it also has no stanox_crs row. Must not be mistaken
            // for a "looks like a station" gap.
            let index = ScheduleIndex::build(vec![schedule_with_one_calling_point(
                "T00004",
                calling_point("PUREPSJ", CallingPointKind::Intermediate),
            )]);
            let date = NaiveDate::from_ymd_opt(2026, 9, 23).unwrap();
            let tiploc_to_crs = HashMap::new();

            let unresolved = unresolved_booked_tiplocs(&index, date, &tiploc_to_crs);
            assert!(
                unresolved.is_empty(),
                "no booked time at all means this is not a genuine timed stop"
            );
        }

        #[test]
        fn the_same_unresolved_tiploc_across_two_schedules_is_returned_only_once() {
            let index = ScheduleIndex::build(vec![
                schedule_with_one_calling_point(
                    "T00005",
                    calling_point_with_departure(
                        "NMPTN  ",
                        CallingPointKind::Intermediate,
                        "09:15",
                    ),
                ),
                schedule_with_one_calling_point(
                    "T00006",
                    calling_point_with_departure(
                        "NMPTN  ",
                        CallingPointKind::Intermediate,
                        "14:40",
                    ),
                ),
            ]);
            let date = NaiveDate::from_ymd_opt(2026, 9, 23).unwrap();
            let tiploc_to_crs = HashMap::new();

            let unresolved = unresolved_booked_tiplocs(&index, date, &tiploc_to_crs);
            assert_eq!(unresolved, vec!["NMPTN".to_string()]);
        }

        #[test]
        fn a_cancelled_schedules_calling_points_are_not_considered() {
            let raw = vec![RawSchedule {
                basic: basic(
                    "T00007",
                    StpIndicator::Cancellation,
                    "2026-09-23",
                    "2026-09-23",
                    ALL_DAYS,
                ),
                calling_points: Vec::new(),
            }];
            let index = ScheduleIndex::build(raw);
            let date = NaiveDate::from_ymd_opt(2026, 9, 23).unwrap();
            let tiploc_to_crs = HashMap::new();

            let unresolved = unresolved_booked_tiplocs(&index, date, &tiploc_to_crs);
            assert!(unresolved.is_empty());
        }

        #[test]
        fn results_are_sorted_regardless_of_hashmap_iteration_order() {
            let index = ScheduleIndex::build(vec![
                schedule_with_one_calling_point(
                    "T00008",
                    calling_point_with_departure(
                        "ZULU   ",
                        CallingPointKind::Intermediate,
                        "09:15",
                    ),
                ),
                schedule_with_one_calling_point(
                    "T00009",
                    calling_point_with_departure(
                        "ALPHA  ",
                        CallingPointKind::Intermediate,
                        "09:16",
                    ),
                ),
            ]);
            let date = NaiveDate::from_ymd_opt(2026, 9, 23).unwrap();
            let tiploc_to_crs = HashMap::new();

            let unresolved = unresolved_booked_tiplocs(&index, date, &tiploc_to_crs);
            assert_eq!(unresolved, vec!["ALPHA".to_string(), "ZULU".to_string()]);
        }
    }
}
