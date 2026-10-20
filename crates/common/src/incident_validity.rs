//! Where an incident's own validity windows put it relative to now: still
//! to come, in effect, or over. The one rule behind the incident archive's
//! state (`api::data::queries::IncidentState`, its SQL twin
//! `INCIDENT_STATE_SQL`, and the badge the frontend draws from it) and the
//! aggregator's "does this incident's RDM validity cover now" test, so an
//! incident can't be "Active" on its own page while the line it names shows
//! nothing because its window hasn't started
//! (docs/superpowers/specs/2026-10-20-incident-validity-state-design.md).
//!
//! The windows are the Knowledgebase feed's structured `ValidityPeriod`s
//! (`StartTime`/`EndTime`, which carry a UTC offset, so each bound is an
//! exact instant). Comparing instants is the same in every time zone: a
//! window that ends at 01:59 BST has ended at 01:30 GMT the same night,
//! even though "01:30" reads earlier on a London clock. Europe/London
//! matters only when a bound is shown, which the frontend does.
//!
//! LLM-extracted periods (`incidents.extracted_periods`) are deliberately
//! not read here: see the design spec's "Which periods to trust".

use chrono::{DateTime, Utc};

use crate::ValidityPeriod;

/// Whether `period` covers the instant `now`: started (`from_date <= now`)
/// and not yet finished (no `to_date`, or `to_date > now`). A window is
/// half-open, `[from, to)`.
pub fn period_covers(period: &ValidityPeriod, now: DateTime<Utc>) -> bool {
    period.from_date <= now && period.to_date.is_none_or(|to| to > now)
}

/// An incident's validity windows, relative to now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidityPhase {
    /// Some window covers now. Also an incident with no windows at all:
    /// RDM's `ValidityPeriod` is mandatory, so none means "not stated",
    /// and the aggregator has always read that as open-ended from now.
    Active,
    /// No window covers now and at least one starts later: planned work
    /// that hasn't started yet, or the gap between two windows.
    Upcoming,
    /// Every window has finished.
    Ended,
}

impl ValidityPhase {
    /// Where `periods` stand at `now`.
    pub fn of(periods: &[ValidityPeriod], now: DateTime<Utc>) -> Self {
        if periods.is_empty() || periods.iter().any(|p| period_covers(p, now)) {
            Self::Active
        } else if periods.iter().any(|p| p.from_date > now) {
            Self::Upcoming
        } else {
            Self::Ended
        }
    }
}

/// When the next window starts: the earliest `from_date` after `now`.
/// Only meaningful for [`ValidityPhase::Upcoming`] (an active incident may
/// also have a later window, but it is already in effect).
pub fn next_start(periods: &[ValidityPeriod], now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    periods
        .iter()
        .map(|p| p.from_date)
        .filter(|from| *from > now)
        .min()
}

/// When the last window finished, if every window has ([`ValidityPhase::Ended`]).
pub fn ended_at(periods: &[ValidityPeriod], now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    if ValidityPhase::of(periods, now) != ValidityPhase::Ended {
        return None;
    }
    periods.iter().filter_map(|p| p.to_date).max()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, NaiveDate, TimeZone};
    use chrono_tz::Europe::London;

    fn window(from: DateTime<Utc>, to: Option<DateTime<Utc>>) -> ValidityPeriod {
        ValidityPeriod {
            from_date: from,
            to_date: to,
            is_now: to.is_none(),
        }
    }

    /// A London wall-clock time, picking the first reading of an ambiguous
    /// (fall-back) hour and the second one when `later` is set.
    fn london(y: i32, m: u32, d: u32, h: u32, min: u32, later: bool) -> DateTime<Utc> {
        let naive = NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(h, min, 0)
            .unwrap();
        let local = London.from_local_datetime(&naive);
        let chosen = if later {
            local.latest().unwrap()
        } else {
            local.earliest().unwrap()
        };
        chosen.with_timezone(&Utc)
    }

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 20, 12, 0, 0).unwrap()
    }

    #[test]
    fn a_window_wholly_in_the_future_is_upcoming() {
        // 2D8C8B610B654F93BEC3A7B377A9B2A0 on 2026-10-20: listed, never
        // cleared, and shown "Active" for a closure on Sunday 25 October.
        let periods = [window(
            Utc.with_ymd_and_hms(2026, 10, 24, 23, 0, 0).unwrap(),
            Some(Utc.with_ymd_and_hms(2026, 10, 25, 23, 59, 0).unwrap()),
        )];
        assert_eq!(ValidityPhase::of(&periods, now()), ValidityPhase::Upcoming);
        assert_eq!(next_start(&periods, now()), Some(periods[0].from_date));
        assert_eq!(ended_at(&periods, now()), None);
    }

    #[test]
    fn a_window_covering_now_is_active() {
        let periods = [window(
            now() - Duration::hours(1),
            Some(now() + Duration::hours(1)),
        )];
        assert_eq!(ValidityPhase::of(&periods, now()), ValidityPhase::Active);
    }

    #[test]
    fn a_window_wholly_in_the_past_is_ended() {
        let end = now() - Duration::hours(1);
        let periods = [window(now() - Duration::days(1), Some(end))];
        assert_eq!(ValidityPhase::of(&periods, now()), ValidityPhase::Ended);
        assert_eq!(ended_at(&periods, now()), Some(end));
        assert_eq!(next_start(&periods, now()), None);
    }

    #[test]
    fn the_bounds_are_half_open() {
        let periods = [window(now(), Some(now() + Duration::hours(1)))];
        assert_eq!(ValidityPhase::of(&periods, now()), ValidityPhase::Active);
        let periods = [window(now() - Duration::hours(1), Some(now()))];
        assert_eq!(ValidityPhase::of(&periods, now()), ValidityPhase::Ended);
    }

    #[test]
    fn several_windows_straddling_now() {
        let past = window(now() - Duration::days(2), Some(now() - Duration::days(1)));
        let current = window(now() - Duration::hours(1), Some(now() + Duration::hours(1)));
        let future = window(now() + Duration::days(1), Some(now() + Duration::days(2)));
        let later = window(now() + Duration::days(3), Some(now() + Duration::days(4)));

        // Any window in effect makes the incident active, whatever the others say.
        let all = [past.clone(), current, future.clone(), later.clone()];
        assert_eq!(ValidityPhase::of(&all, now()), ValidityPhase::Active);

        // Between two windows: the next one is still to come.
        let gap = [later.clone(), past.clone(), future.clone()];
        assert_eq!(ValidityPhase::of(&gap, now()), ValidityPhase::Upcoming);
        assert_eq!(next_start(&gap, now()), Some(future.from_date));

        // Every window over: ended at the latest end, not the first listed.
        let over = [
            past.clone(),
            window(now() - Duration::days(5), Some(now() - Duration::hours(3))),
        ];
        assert_eq!(ValidityPhase::of(&over, now()), ValidityPhase::Ended);
        assert_eq!(ended_at(&over, now()), Some(now() - Duration::hours(3)));
    }

    #[test]
    fn an_open_ended_window_is_active_once_started_and_upcoming_before() {
        let started = [window(now() - Duration::days(30), None)];
        assert_eq!(ValidityPhase::of(&started, now()), ValidityPhase::Active);
        // `is_now` is set on every open-ended window (poller-incidents), but
        // it means "no end", not "now": a future start still wins.
        let future = [window(now() + Duration::days(1), None)];
        assert!(future[0].is_now);
        assert_eq!(ValidityPhase::of(&future, now()), ValidityPhase::Upcoming);
    }

    #[test]
    fn no_windows_is_active() {
        assert_eq!(ValidityPhase::of(&[], now()), ValidityPhase::Active);
        assert_eq!(ended_at(&[], now()), None);
    }

    #[test]
    fn across_the_october_clock_change() {
        // Sunday 25 October 2026: 02:00 BST falls back to 01:00 GMT, so
        // 01:00-01:59 happens twice. A closure from 00:00 BST to 23:59 GMT
        // is the example incident's real window.
        let from = london(2026, 10, 25, 0, 0, false);
        let to = london(2026, 10, 25, 23, 59, false);
        assert_eq!(from, Utc.with_ymd_and_hms(2026, 10, 24, 23, 0, 0).unwrap());
        assert_eq!(to, Utc.with_ymd_and_hms(2026, 10, 25, 23, 59, 0).unwrap());
        let closure = [window(from, Some(to))];
        let just_before = london(2026, 10, 24, 23, 59, false);
        assert_eq!(
            ValidityPhase::of(&closure, just_before),
            ValidityPhase::Upcoming
        );
        // The second 01:30 (GMT) is inside it.
        let second_half_past_one = london(2026, 10, 25, 1, 30, true);
        assert_eq!(
            ValidityPhase::of(&closure, second_half_past_one),
            ValidityPhase::Active
        );

        // A window ending at 01:45 BST has ended by 01:30 GMT, the
        // following hour, although 01:30 reads earlier on the clock.
        let short = [window(
            london(2026, 10, 25, 0, 0, false),
            Some(london(2026, 10, 25, 1, 45, false)),
        )];
        assert_eq!(
            ValidityPhase::of(&short, london(2026, 10, 25, 1, 30, false)),
            ValidityPhase::Active
        );
        assert_eq!(
            ValidityPhase::of(&short, second_half_past_one),
            ValidityPhase::Ended
        );
    }

    #[test]
    fn across_the_march_clock_change() {
        // Sunday 28 March 2027: 01:00 GMT jumps to 02:00 BST. A window
        // starting at 02:30 BST starts at 01:30 UTC, half an hour after the
        // jump.
        let start = london(2027, 3, 28, 2, 30, false);
        assert_eq!(start, Utc.with_ymd_and_hms(2027, 3, 28, 1, 30, 0).unwrap());
        let periods = [window(start, Some(london(2027, 3, 28, 6, 0, false)))];
        let at_0115_utc = Utc.with_ymd_and_hms(2027, 3, 28, 1, 15, 0).unwrap();
        assert_eq!(
            ValidityPhase::of(&periods, at_0115_utc),
            ValidityPhase::Upcoming
        );
        assert_eq!(
            ValidityPhase::of(&periods, start + Duration::minutes(1)),
            ValidityPhase::Active
        );
    }
}
