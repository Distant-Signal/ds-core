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
    Some(ResolvedSchedule {
        uid: winner.basic.uid.clone(),
        stp_indicator: winner.basic.stp_indicator,
        cancelled,
        calling_points: if cancelled {
            Vec::new()
        } else {
            winner.calling_points.clone()
        },
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
pub fn match_pin<'a>(
    population: &'a [LinePopulationEntry],
    crs_tiplocs: &[&str],
    scheduled: DateTime<Utc>,
    tolerance: Duration,
    to_utc: impl Fn(NaiveTime) -> Option<DateTime<Utc>>,
) -> Option<&'a LinePopulationEntry> {
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
            let Some(candidate_utc) = to_utc(booked) else {
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
    best.map(|(entry, _)| entry)
}

/// Every non-cancelled, resolved schedule's departure-bearing calling
/// points (`Origin`/`Intermediate`, i.e. `booked_departure.is_some()` --
/// `Terminate` never has one, see [`crate::records::CallingPointKind::Terminate`]'s
/// own doc), bucketed by CRS via `tiploc_to_crs` (normalized-TIPLOC keyed,
/// built by the caller from the SAME cycle's already-resolved
/// `stanox_crs` rows -- no second lookup table, no new parse). A calling
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
pub fn departures_by_crs(
    index: &ScheduleIndex,
    date: NaiveDate,
    now: NaiveTime,
    tiploc_to_crs: &HashMap<String, String>,
) -> HashMap<String, Vec<crate::records::ScheduleDeparture>> {
    let mut by_crs: HashMap<String, Vec<crate::records::ScheduleDeparture>> = HashMap::new();

    for uid in index.uids() {
        let Some(resolved) = index.schedule_for_uid(uid, date) else {
            continue;
        };
        if resolved.cancelled {
            continue;
        }
        for cp in &resolved.calling_points {
            let Some(departure) = cp.booked_departure else {
                continue;
            };
            if departure < now {
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
                    destination_crs,
                });
        }
    }

    by_crs
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
/// Two deliberate asymmetries with [`departures_by_crs`], both about the
/// "drop, never fabricate" rule applied to a value that is now a bucket
/// KEY rather than a field:
///
/// * A schedule whose terminating TIPLOC has no `tiploc_to_crs` entry is
///   dropped **entirely** -- there is no honest bucket to file it under.
///   `departures_by_crs` can degrade the same case to
///   `destination_crs: None` because there the destination is only a
///   field; here it is the key.
/// * A calling point whose OWN TIPLOC has no `tiploc_to_crs` entry drops
///   just that entry, leaving the schedule's other entries in the bucket --
///   identical to `departures_by_crs`'s own per-calling-point drop.
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
        let Some(destination_crs) = resolved
            .calling_points
            .last()
            .and_then(|last| tiploc_to_crs.get(normalize_tiploc(&last.tiploc)))
        else {
            continue;
        };
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
        for cp in &resolved.calling_points {
            let Some(departure) = cp.booked_departure else {
                continue;
            };
            if departure < now {
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
                    true_origin_crs: true_origin_crs.clone(),
                    destination_arrival,
                });
        }
    }

    by_destination
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
    fn departures_by_destination_crs_buckets_every_departure_bearing_calling_point_under_one_destination() {
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
    fn departures_by_destination_crs_excludes_a_cancelled_schedule_even_though_its_time_has_not_passed() {
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
    fn departures_by_destination_crs_drops_a_schedule_whose_destination_tiploc_is_unresolved() {
        // The asymmetry with departures_by_crs, and it is deliberate: THERE,
        // an unresolved destination degrades to `destination_crs: None` and
        // the row is still returned under its own origin. HERE the
        // destination IS the bucket key, so there is no honest bucket to
        // file this schedule under -- it is dropped entirely rather than
        // guessed at or filed under a fabricated key.
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
            by_destination.is_empty(),
            "an unresolved DESTINATION tiploc drops the whole schedule -- there is no bucket key"
        );
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
    // pairs a bare NaiveTime with a fixed date -- exercising `match_pin`'s
    // arithmetic without pulling in a real Europe/London conversion (that's
    // `eta_blend::london_to_utc`'s own, separately-tested job).
    fn utc_on(date: &str) -> impl Fn(NaiveTime) -> Option<DateTime<Utc>> {
        let date = NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap();
        move |t| {
            Some(DateTime::<Utc>::from_naive_utc_and_offset(
                date.and_time(t),
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
            |_| None,
        );
        assert_eq!(matched, None);
    }
}
