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
