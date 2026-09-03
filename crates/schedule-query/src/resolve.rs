//! STP-overlay resolution and the two read-only queries this crate exists
//! to answer: [`ScheduleIndex::schedule_for_uid`] (the direct
//! `train_uid` -> booked-schedule bridge
//! `crates/trust-consumer/src/matching.rs`'s own module doc names as
//! missing) and [`schedules_touching`] (the line-population query a
//! future full-coverage consumer would need).

use std::collections::HashMap;

use chrono::{Datelike, NaiveDate};

use crate::records::{CallingPoint, RawSchedule, StpIndicator};
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
}
