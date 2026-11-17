//! The rail-day boundary this app already uses for incident staleness
//! (`crates/aggregator/src/aggregation.rs`'s original site) and, as of
//! docs/superpowers/specs/2026-09-04-option-b-live-consumer-design.md
//! Decision 2e, for full-coverage Resolved/Pending gating too. Extracted
//! here (rather than left `aggregator`-private) specifically so a second
//! crate can share it without duplicating the Europe/London 02:00-cutoff
//! DST-transition-safe logic -- pure code motion, no behavior change; see
//! docs/superpowers/plans/2026-09-04-option-b-live-consumer-plan.md Task 2.

use chrono::{DateTime, Duration, NaiveTime, TimeZone, Utc};

/// The next rail-day boundary (02:00 Europe/London) strictly after
/// `at`. If `at`'s local time is already past 02:00, that's the current
/// day's 02:00; otherwise it's the next calendar day's 02:00.
///
/// UK clocks change exactly at the 01:00/02:00 boundary in both directions,
/// so local 02:00 itself is never ambiguous or missing on a transition day.
#[expect(
    clippy::expect_used,
    reason = "a constant or range-checked time is always valid"
)]
pub fn next_rail_day_boundary(at: DateTime<Utc>) -> DateTime<Utc> {
    let local = at.with_timezone(&chrono_tz::Europe::London);
    let boundary_time = NaiveTime::from_hms_opt(2, 0, 0).expect("2:00:00 is a valid time");

    let boundary_date = if local.time() < boundary_time {
        local.date_naive()
    } else {
        local.date_naive() + Duration::days(1)
    };
    let boundary_naive = boundary_date.and_time(boundary_time);

    match chrono_tz::Europe::London.from_local_datetime(&boundary_naive) {
        chrono::LocalResult::Single(dt) => dt.with_timezone(&Utc),
        other => panic!(
            "unexpected {other:?} resolving rail-day boundary {boundary_naive} in Europe/London; \
             02:00 local should never be ambiguous or missing"
        ),
    }
}

/// Which rail day `at` currently falls in -- the inverse of
/// [`next_rail_day_boundary`], using the same 02:00 Europe/London cutoff
/// and therefore the same DST-transition safety.
///
/// Hoisted here from `trust-backlog-consumer`'s own crate-local copy (whose
/// doc comment already named this move as the right follow-up) because a
/// second and third caller now need it: `trust-consumer` has to date an
/// Activation ("which day's running is this Activation actually for",
/// finding #2 of the 2026-09-25 review) and to age parked Activations out
/// against the CURRENT rail day rather than a permanent CIF schedule's
/// months-away `schedule_end_date` (finding #5).
///
/// A bare `Utc::now().date_naive()` is NOT an acceptable substitute:
/// it ignores both the Europe/London offset and this codebase's own
/// 02:00-cutoff convention, so an event at 00:30 local would be filed under
/// the wrong (next) rail day -- which is exactly the class of date bug
/// `trust-backlog-consumer`'s own `service_date` handling was already burned
/// by in production.
#[expect(
    clippy::expect_used,
    reason = "a constant or range-checked time is always valid"
)]
pub fn current_rail_day(at: DateTime<Utc>) -> chrono::NaiveDate {
    let local = at.with_timezone(&chrono_tz::Europe::London);
    let cutoff = NaiveTime::from_hms_opt(2, 0, 0).expect("2:00:00 is a valid time");
    if local.time() < cutoff {
        local.date_naive() - Duration::days(1)
    } else {
        local.date_naive()
    }
}

/// A Europe/London wall-clock time on `date` (a CIF booked time, say) as a
/// UTC instant. On the autumn change the repeated hour resolves to its
/// first (BST) occurrence; on the spring change a time inside the skipped
/// hour is read as the instant one hour later (the clock would already
/// show BST), so no booked time is ever lost.
pub fn london_to_utc(date: chrono::NaiveDate, time: NaiveTime) -> DateTime<Utc> {
    let naive = date.and_time(time);
    match chrono_tz::Europe::London.from_local_datetime(&naive) {
        chrono::LocalResult::Single(dt) => dt.with_timezone(&Utc),
        chrono::LocalResult::Ambiguous(earliest, _) => earliest.with_timezone(&Utc),
        chrono::LocalResult::None => chrono_tz::Europe::London
            .from_local_datetime(&(naive + Duration::hours(1)))
            .earliest()
            .map_or_else(|| naive.and_utc(), |dt| dt.with_timezone(&Utc)),
    }
}

#[cfg(test)]
mod london_to_utc_tests {
    use super::*;

    fn at(date: &str, time: &str) -> DateTime<Utc> {
        london_to_utc(date.parse().unwrap(), time.parse().unwrap())
    }

    #[test]
    fn bst_gmt_and_both_transitions() {
        assert_eq!(
            at("2026-07-15", "08:22:00").to_rfc3339(),
            "2026-07-15T07:22:00+00:00"
        );
        assert_eq!(
            at("2026-01-15", "08:22:00").to_rfc3339(),
            "2026-01-15T08:22:00+00:00"
        );
        // 2026-10-25: 01:30 happens twice; the first is BST.
        assert_eq!(
            at("2026-10-25", "01:30:00").to_rfc3339(),
            "2026-10-25T00:30:00+00:00"
        );
        // 2026-03-29: 01:30 does not exist; read as 02:30 BST.
        assert_eq!(
            at("2026-03-29", "01:30:00").to_rfc3339(),
            "2026-03-29T01:30:00+00:00"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_rail_day_well_after_the_0200_cutoff_is_that_calendar_day() {
        let at: DateTime<Utc> = "2026-09-05T13:00:00Z".parse().unwrap();
        assert_eq!(
            current_rail_day(at),
            "2026-09-05".parse::<chrono::NaiveDate>().unwrap()
        );
    }

    #[test]
    fn current_rail_day_just_before_the_0200_cutoff_is_the_previous_calendar_day() {
        // 00:30 UTC = 01:30 BST (September is daylight saving), clearly
        // before the 02:00 Europe/London cutoff -- so this instant still
        // belongs to the rail day that started the previous calendar day.
        let at: DateTime<Utc> = "2026-09-05T00:30:00Z".parse().unwrap();
        assert_eq!(
            current_rail_day(at),
            "2026-09-04".parse::<chrono::NaiveDate>().unwrap()
        );
    }

    /// The two halves of this module must agree: the rail day `at` falls in
    /// is the one whose boundary is the NEXT one after `at`.
    #[test]
    fn current_rail_day_and_next_rail_day_boundary_agree() {
        for raw in [
            "2026-09-05T13:00:00Z",
            "2026-09-05T00:30:00Z",
            "2026-03-29T00:30:00Z",
            "2026-10-25T00:30:00Z",
        ] {
            let at: DateTime<Utc> = raw.parse().unwrap();
            let boundary = next_rail_day_boundary(at);
            assert_eq!(
                current_rail_day(at) + Duration::days(1),
                current_rail_day(boundary),
                "the rail day {at} falls in must end at its next boundary {boundary}"
            );
        }
    }

    #[test]
    fn next_rail_day_boundary_on_a_plain_midweek_day() {
        // 2026-07-15 13:00 UTC is 14:00 BST (July is daylight saving) --
        // still well before that rail day's 02:00-the-next-day end, so the
        // boundary is 2026-07-16 02:00 BST = 2026-07-16 01:00 UTC.
        let first_seen_at: DateTime<Utc> = "2026-07-15T13:00:00Z".parse().unwrap();
        let boundary = next_rail_day_boundary(first_seen_at);
        assert_eq!(
            boundary,
            "2026-07-16T01:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    #[test]
    fn next_rail_day_boundary_just_before_local_0200_stays_in_the_earlier_rail_day() {
        // 2026-07-16 00:30 UTC is 01:30 BST -- still inside the rail day
        // that started 2026-07-15 02:00 BST, so the boundary is only 30
        // local minutes away: 2026-07-16 02:00 BST = 2026-07-16 01:00 UTC.
        let first_seen_at: DateTime<Utc> = "2026-07-16T00:30:00Z".parse().unwrap();
        let boundary = next_rail_day_boundary(first_seen_at);
        assert_eq!(
            boundary,
            "2026-07-16T01:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    #[test]
    fn next_rail_day_boundary_just_after_local_0200_rolls_to_the_next_rail_day() {
        // 2026-07-16 01:05 UTC is 02:05 BST -- just past that day's 02:00,
        // so it belongs to the rail day that just started, and the next
        // boundary is a full rail day away: 2026-07-17 02:00 BST = 01:00 UTC.
        let first_seen_at: DateTime<Utc> = "2026-07-16T01:05:00Z".parse().unwrap();
        let boundary = next_rail_day_boundary(first_seen_at);
        assert_eq!(
            boundary,
            "2026-07-17T01:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    #[test]
    fn next_rail_day_boundary_across_the_spring_forward_transition() {
        // UK clocks spring forward at 01:00 UTC on the last Sunday in March
        // (2026-03-29), jumping local time from 01:00 GMT straight to 02:00
        // BST. 2026-03-29 00:30 UTC is *before* that jump, so local time is
        // still 00:30 GMT -- before that day's local 02:00, so the boundary
        // is that same day's 02:00, which (having just jumped) is already
        // BST: 2026-03-29 02:00 BST = 2026-03-29 01:00 UTC.
        let first_seen_at: DateTime<Utc> = "2026-03-29T00:30:00Z".parse().unwrap();
        let boundary = next_rail_day_boundary(first_seen_at);
        assert_eq!(
            boundary,
            "2026-03-29T01:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    #[test]
    fn next_rail_day_boundary_across_the_autumn_fallback_transition() {
        // UK clocks fall back at 02:00 BST -> 01:00 GMT on the last Sunday
        // in October (2026-10-25). 2026-10-25 00:30 UTC is 01:30 BST --
        // before that day's local 02:00, which (after the fallback
        // completes) resolves as GMT -- so the boundary is 2026-10-25
        // 02:00 GMT = 2026-10-25 02:00 UTC.
        let first_seen_at: DateTime<Utc> = "2026-10-25T00:30:00Z".parse().unwrap();
        let boundary = next_rail_day_boundary(first_seen_at);
        assert_eq!(
            boundary,
            "2026-10-25T02:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }
}

/// Pins the Europe/London behaviour of this module around both clock changes
/// of 2025 and 2026 and on plain winter and summer days. The expected values
/// were worked out independently of the code under test (from the IANA rules
/// with an unrelated implementation), so the tables stay valid if the time
/// zone implementation behind the functions changes.
#[cfg(test)]
mod dst_pinning_tests {
    use super::*;

    /// `(instant, London calendar date, rail day, next rail-day boundary)`.
    #[rustfmt::skip]
    const INSTANTS: &[(&str, &str, &str, &str)] = &[
        ("2025-03-29T22:00:00Z", "2025-03-29", "2025-03-29", "2025-03-30T01:00:00Z"),
        ("2025-03-29T23:00:00Z", "2025-03-29", "2025-03-29", "2025-03-30T01:00:00Z"),
        ("2025-03-29T23:30:00Z", "2025-03-29", "2025-03-29", "2025-03-30T01:00:00Z"),
        ("2025-03-30T00:00:00Z", "2025-03-30", "2025-03-29", "2025-03-30T01:00:00Z"),
        ("2025-03-30T00:30:00Z", "2025-03-30", "2025-03-29", "2025-03-30T01:00:00Z"),
        ("2025-03-30T00:59:00Z", "2025-03-30", "2025-03-29", "2025-03-30T01:00:00Z"),
        ("2025-03-30T01:00:00Z", "2025-03-30", "2025-03-30", "2025-03-31T01:00:00Z"),
        ("2025-03-30T01:01:00Z", "2025-03-30", "2025-03-30", "2025-03-31T01:00:00Z"),
        ("2025-03-30T01:30:00Z", "2025-03-30", "2025-03-30", "2025-03-31T01:00:00Z"),
        ("2025-03-30T01:59:00Z", "2025-03-30", "2025-03-30", "2025-03-31T01:00:00Z"),
        ("2025-03-30T02:00:00Z", "2025-03-30", "2025-03-30", "2025-03-31T01:00:00Z"),
        ("2025-03-30T02:30:00Z", "2025-03-30", "2025-03-30", "2025-03-31T01:00:00Z"),
        ("2025-03-30T03:00:00Z", "2025-03-30", "2025-03-30", "2025-03-31T01:00:00Z"),
        ("2025-03-30T12:00:00Z", "2025-03-30", "2025-03-30", "2025-03-31T01:00:00Z"),
        ("2025-03-30T22:59:00Z", "2025-03-30", "2025-03-30", "2025-03-31T01:00:00Z"),
        ("2025-03-30T23:00:00Z", "2025-03-31", "2025-03-30", "2025-03-31T01:00:00Z"),
        ("2025-03-30T23:30:00Z", "2025-03-31", "2025-03-30", "2025-03-31T01:00:00Z"),
        ("2025-03-31T00:30:00Z", "2025-03-31", "2025-03-30", "2025-03-31T01:00:00Z"),
        ("2025-10-25T22:00:00Z", "2025-10-25", "2025-10-25", "2025-10-26T02:00:00Z"),
        ("2025-10-25T23:00:00Z", "2025-10-26", "2025-10-25", "2025-10-26T02:00:00Z"),
        ("2025-10-25T23:30:00Z", "2025-10-26", "2025-10-25", "2025-10-26T02:00:00Z"),
        ("2025-10-26T00:00:00Z", "2025-10-26", "2025-10-25", "2025-10-26T02:00:00Z"),
        ("2025-10-26T00:30:00Z", "2025-10-26", "2025-10-25", "2025-10-26T02:00:00Z"),
        ("2025-10-26T00:59:00Z", "2025-10-26", "2025-10-25", "2025-10-26T02:00:00Z"),
        ("2025-10-26T01:00:00Z", "2025-10-26", "2025-10-25", "2025-10-26T02:00:00Z"),
        ("2025-10-26T01:01:00Z", "2025-10-26", "2025-10-25", "2025-10-26T02:00:00Z"),
        ("2025-10-26T01:30:00Z", "2025-10-26", "2025-10-25", "2025-10-26T02:00:00Z"),
        ("2025-10-26T01:59:00Z", "2025-10-26", "2025-10-25", "2025-10-26T02:00:00Z"),
        ("2025-10-26T02:00:00Z", "2025-10-26", "2025-10-26", "2025-10-27T02:00:00Z"),
        ("2025-10-26T02:30:00Z", "2025-10-26", "2025-10-26", "2025-10-27T02:00:00Z"),
        ("2025-10-26T03:00:00Z", "2025-10-26", "2025-10-26", "2025-10-27T02:00:00Z"),
        ("2025-10-26T12:00:00Z", "2025-10-26", "2025-10-26", "2025-10-27T02:00:00Z"),
        ("2025-10-26T22:59:00Z", "2025-10-26", "2025-10-26", "2025-10-27T02:00:00Z"),
        ("2025-10-26T23:00:00Z", "2025-10-26", "2025-10-26", "2025-10-27T02:00:00Z"),
        ("2025-10-26T23:30:00Z", "2025-10-26", "2025-10-26", "2025-10-27T02:00:00Z"),
        ("2025-10-27T00:30:00Z", "2025-10-27", "2025-10-26", "2025-10-27T02:00:00Z"),
        ("2025-12-31T22:00:00Z", "2025-12-31", "2025-12-31", "2026-01-01T02:00:00Z"),
        ("2025-12-31T22:59:00Z", "2025-12-31", "2025-12-31", "2026-01-01T02:00:00Z"),
        ("2025-12-31T23:00:00Z", "2025-12-31", "2025-12-31", "2026-01-01T02:00:00Z"),
        ("2025-12-31T23:30:00Z", "2025-12-31", "2025-12-31", "2026-01-01T02:00:00Z"),
        ("2025-12-31T23:59:00Z", "2025-12-31", "2025-12-31", "2026-01-01T02:00:00Z"),
        ("2026-01-01T00:00:00Z", "2026-01-01", "2025-12-31", "2026-01-01T02:00:00Z"),
        ("2026-01-01T00:30:00Z", "2026-01-01", "2025-12-31", "2026-01-01T02:00:00Z"),
        ("2026-01-01T00:59:00Z", "2026-01-01", "2025-12-31", "2026-01-01T02:00:00Z"),
        ("2026-01-01T01:00:00Z", "2026-01-01", "2025-12-31", "2026-01-01T02:00:00Z"),
        ("2026-01-01T01:30:00Z", "2026-01-01", "2025-12-31", "2026-01-01T02:00:00Z"),
        ("2026-01-01T01:59:00Z", "2026-01-01", "2025-12-31", "2026-01-01T02:00:00Z"),
        ("2026-01-01T02:00:00Z", "2026-01-01", "2026-01-01", "2026-01-02T02:00:00Z"),
        ("2026-01-01T02:30:00Z", "2026-01-01", "2026-01-01", "2026-01-02T02:00:00Z"),
        ("2026-01-15T22:00:00Z", "2026-01-15", "2026-01-15", "2026-01-16T02:00:00Z"),
        ("2026-01-15T22:59:00Z", "2026-01-15", "2026-01-15", "2026-01-16T02:00:00Z"),
        ("2026-01-15T23:00:00Z", "2026-01-15", "2026-01-15", "2026-01-16T02:00:00Z"),
        ("2026-01-15T23:30:00Z", "2026-01-15", "2026-01-15", "2026-01-16T02:00:00Z"),
        ("2026-01-15T23:59:00Z", "2026-01-15", "2026-01-15", "2026-01-16T02:00:00Z"),
        ("2026-01-16T00:00:00Z", "2026-01-16", "2026-01-15", "2026-01-16T02:00:00Z"),
        ("2026-01-16T00:30:00Z", "2026-01-16", "2026-01-15", "2026-01-16T02:00:00Z"),
        ("2026-01-16T00:59:00Z", "2026-01-16", "2026-01-15", "2026-01-16T02:00:00Z"),
        ("2026-01-16T01:00:00Z", "2026-01-16", "2026-01-15", "2026-01-16T02:00:00Z"),
        ("2026-01-16T01:30:00Z", "2026-01-16", "2026-01-15", "2026-01-16T02:00:00Z"),
        ("2026-01-16T01:59:00Z", "2026-01-16", "2026-01-15", "2026-01-16T02:00:00Z"),
        ("2026-01-16T02:00:00Z", "2026-01-16", "2026-01-16", "2026-01-17T02:00:00Z"),
        ("2026-01-16T02:30:00Z", "2026-01-16", "2026-01-16", "2026-01-17T02:00:00Z"),
        ("2026-03-28T22:00:00Z", "2026-03-28", "2026-03-28", "2026-03-29T01:00:00Z"),
        ("2026-03-28T23:00:00Z", "2026-03-28", "2026-03-28", "2026-03-29T01:00:00Z"),
        ("2026-03-28T23:30:00Z", "2026-03-28", "2026-03-28", "2026-03-29T01:00:00Z"),
        ("2026-03-29T00:00:00Z", "2026-03-29", "2026-03-28", "2026-03-29T01:00:00Z"),
        ("2026-03-29T00:30:00Z", "2026-03-29", "2026-03-28", "2026-03-29T01:00:00Z"),
        ("2026-03-29T00:59:00Z", "2026-03-29", "2026-03-28", "2026-03-29T01:00:00Z"),
        ("2026-03-29T01:00:00Z", "2026-03-29", "2026-03-29", "2026-03-30T01:00:00Z"),
        ("2026-03-29T01:01:00Z", "2026-03-29", "2026-03-29", "2026-03-30T01:00:00Z"),
        ("2026-03-29T01:30:00Z", "2026-03-29", "2026-03-29", "2026-03-30T01:00:00Z"),
        ("2026-03-29T01:59:00Z", "2026-03-29", "2026-03-29", "2026-03-30T01:00:00Z"),
        ("2026-03-29T02:00:00Z", "2026-03-29", "2026-03-29", "2026-03-30T01:00:00Z"),
        ("2026-03-29T02:30:00Z", "2026-03-29", "2026-03-29", "2026-03-30T01:00:00Z"),
        ("2026-03-29T03:00:00Z", "2026-03-29", "2026-03-29", "2026-03-30T01:00:00Z"),
        ("2026-03-29T12:00:00Z", "2026-03-29", "2026-03-29", "2026-03-30T01:00:00Z"),
        ("2026-03-29T22:59:00Z", "2026-03-29", "2026-03-29", "2026-03-30T01:00:00Z"),
        ("2026-03-29T23:00:00Z", "2026-03-30", "2026-03-29", "2026-03-30T01:00:00Z"),
        ("2026-03-29T23:30:00Z", "2026-03-30", "2026-03-29", "2026-03-30T01:00:00Z"),
        ("2026-03-30T00:30:00Z", "2026-03-30", "2026-03-29", "2026-03-30T01:00:00Z"),
        ("2026-06-30T22:00:00Z", "2026-06-30", "2026-06-30", "2026-07-01T01:00:00Z"),
        ("2026-06-30T22:59:00Z", "2026-06-30", "2026-06-30", "2026-07-01T01:00:00Z"),
        ("2026-06-30T23:00:00Z", "2026-07-01", "2026-06-30", "2026-07-01T01:00:00Z"),
        ("2026-06-30T23:30:00Z", "2026-07-01", "2026-06-30", "2026-07-01T01:00:00Z"),
        ("2026-06-30T23:59:00Z", "2026-07-01", "2026-06-30", "2026-07-01T01:00:00Z"),
        ("2026-07-01T00:00:00Z", "2026-07-01", "2026-06-30", "2026-07-01T01:00:00Z"),
        ("2026-07-01T00:30:00Z", "2026-07-01", "2026-06-30", "2026-07-01T01:00:00Z"),
        ("2026-07-01T00:59:00Z", "2026-07-01", "2026-06-30", "2026-07-01T01:00:00Z"),
        ("2026-07-01T01:00:00Z", "2026-07-01", "2026-07-01", "2026-07-02T01:00:00Z"),
        ("2026-07-01T01:30:00Z", "2026-07-01", "2026-07-01", "2026-07-02T01:00:00Z"),
        ("2026-07-01T01:59:00Z", "2026-07-01", "2026-07-01", "2026-07-02T01:00:00Z"),
        ("2026-07-01T02:00:00Z", "2026-07-01", "2026-07-01", "2026-07-02T01:00:00Z"),
        ("2026-07-01T02:30:00Z", "2026-07-01", "2026-07-01", "2026-07-02T01:00:00Z"),
        ("2026-07-15T22:00:00Z", "2026-07-15", "2026-07-15", "2026-07-16T01:00:00Z"),
        ("2026-07-15T22:59:00Z", "2026-07-15", "2026-07-15", "2026-07-16T01:00:00Z"),
        ("2026-07-15T23:00:00Z", "2026-07-16", "2026-07-15", "2026-07-16T01:00:00Z"),
        ("2026-07-15T23:30:00Z", "2026-07-16", "2026-07-15", "2026-07-16T01:00:00Z"),
        ("2026-07-15T23:59:00Z", "2026-07-16", "2026-07-15", "2026-07-16T01:00:00Z"),
        ("2026-07-16T00:00:00Z", "2026-07-16", "2026-07-15", "2026-07-16T01:00:00Z"),
        ("2026-07-16T00:30:00Z", "2026-07-16", "2026-07-15", "2026-07-16T01:00:00Z"),
        ("2026-07-16T00:59:00Z", "2026-07-16", "2026-07-15", "2026-07-16T01:00:00Z"),
        ("2026-07-16T01:00:00Z", "2026-07-16", "2026-07-16", "2026-07-17T01:00:00Z"),
        ("2026-07-16T01:30:00Z", "2026-07-16", "2026-07-16", "2026-07-17T01:00:00Z"),
        ("2026-07-16T01:59:00Z", "2026-07-16", "2026-07-16", "2026-07-17T01:00:00Z"),
        ("2026-07-16T02:00:00Z", "2026-07-16", "2026-07-16", "2026-07-17T01:00:00Z"),
        ("2026-07-16T02:30:00Z", "2026-07-16", "2026-07-16", "2026-07-17T01:00:00Z"),
        ("2026-10-24T22:00:00Z", "2026-10-24", "2026-10-24", "2026-10-25T02:00:00Z"),
        ("2026-10-24T23:00:00Z", "2026-10-25", "2026-10-24", "2026-10-25T02:00:00Z"),
        ("2026-10-24T23:30:00Z", "2026-10-25", "2026-10-24", "2026-10-25T02:00:00Z"),
        ("2026-10-25T00:00:00Z", "2026-10-25", "2026-10-24", "2026-10-25T02:00:00Z"),
        ("2026-10-25T00:30:00Z", "2026-10-25", "2026-10-24", "2026-10-25T02:00:00Z"),
        ("2026-10-25T00:59:00Z", "2026-10-25", "2026-10-24", "2026-10-25T02:00:00Z"),
        ("2026-10-25T01:00:00Z", "2026-10-25", "2026-10-24", "2026-10-25T02:00:00Z"),
        ("2026-10-25T01:01:00Z", "2026-10-25", "2026-10-24", "2026-10-25T02:00:00Z"),
        ("2026-10-25T01:30:00Z", "2026-10-25", "2026-10-24", "2026-10-25T02:00:00Z"),
        ("2026-10-25T01:59:00Z", "2026-10-25", "2026-10-24", "2026-10-25T02:00:00Z"),
        ("2026-10-25T02:00:00Z", "2026-10-25", "2026-10-25", "2026-10-26T02:00:00Z"),
        ("2026-10-25T02:30:00Z", "2026-10-25", "2026-10-25", "2026-10-26T02:00:00Z"),
        ("2026-10-25T03:00:00Z", "2026-10-25", "2026-10-25", "2026-10-26T02:00:00Z"),
        ("2026-10-25T12:00:00Z", "2026-10-25", "2026-10-25", "2026-10-26T02:00:00Z"),
        ("2026-10-25T22:59:00Z", "2026-10-25", "2026-10-25", "2026-10-26T02:00:00Z"),
        ("2026-10-25T23:00:00Z", "2026-10-25", "2026-10-25", "2026-10-26T02:00:00Z"),
        ("2026-10-25T23:30:00Z", "2026-10-25", "2026-10-25", "2026-10-26T02:00:00Z"),
        ("2026-10-26T00:30:00Z", "2026-10-26", "2026-10-25", "2026-10-26T02:00:00Z"),
    ];

    /// `(London date, London wall-clock time, UTC instant)` under
    /// [`london_to_utc`]'s rule: the first reading of a repeated time, and the
    /// reading one hour later for a time that does not exist.
    #[rustfmt::skip]
    const WALL_CLOCK: &[(&str, &str, &str)] = &[
        ("2025-03-30", "00:00:00", "2025-03-30T00:00:00Z"),
        ("2025-03-30", "00:30:00", "2025-03-30T00:30:00Z"),
        ("2025-03-30", "00:59:00", "2025-03-30T00:59:00Z"),
        ("2025-03-30", "01:00:00", "2025-03-30T01:00:00Z"),
        ("2025-03-30", "01:01:00", "2025-03-30T01:01:00Z"),
        ("2025-03-30", "01:30:00", "2025-03-30T01:30:00Z"),
        ("2025-03-30", "01:59:00", "2025-03-30T01:59:00Z"),
        ("2025-03-30", "02:00:00", "2025-03-30T01:00:00Z"),
        ("2025-03-30", "02:01:00", "2025-03-30T01:01:00Z"),
        ("2025-03-30", "02:30:00", "2025-03-30T01:30:00Z"),
        ("2025-03-30", "03:00:00", "2025-03-30T02:00:00Z"),
        ("2025-03-29", "23:30:00", "2025-03-29T23:30:00Z"),
        ("2025-03-29", "23:59:00", "2025-03-29T23:59:00Z"),
        ("2025-10-26", "00:00:00", "2025-10-25T23:00:00Z"),
        ("2025-10-26", "00:30:00", "2025-10-25T23:30:00Z"),
        ("2025-10-26", "00:59:00", "2025-10-25T23:59:00Z"),
        ("2025-10-26", "01:00:00", "2025-10-26T00:00:00Z"),
        ("2025-10-26", "01:01:00", "2025-10-26T00:01:00Z"),
        ("2025-10-26", "01:30:00", "2025-10-26T00:30:00Z"),
        ("2025-10-26", "01:59:00", "2025-10-26T00:59:00Z"),
        ("2025-10-26", "02:00:00", "2025-10-26T02:00:00Z"),
        ("2025-10-26", "02:01:00", "2025-10-26T02:01:00Z"),
        ("2025-10-26", "02:30:00", "2025-10-26T02:30:00Z"),
        ("2025-10-26", "03:00:00", "2025-10-26T03:00:00Z"),
        ("2025-10-25", "23:30:00", "2025-10-25T22:30:00Z"),
        ("2025-10-25", "23:59:00", "2025-10-25T22:59:00Z"),
        ("2026-03-29", "00:00:00", "2026-03-29T00:00:00Z"),
        ("2026-03-29", "00:30:00", "2026-03-29T00:30:00Z"),
        ("2026-03-29", "00:59:00", "2026-03-29T00:59:00Z"),
        ("2026-03-29", "01:00:00", "2026-03-29T01:00:00Z"),
        ("2026-03-29", "01:01:00", "2026-03-29T01:01:00Z"),
        ("2026-03-29", "01:30:00", "2026-03-29T01:30:00Z"),
        ("2026-03-29", "01:59:00", "2026-03-29T01:59:00Z"),
        ("2026-03-29", "02:00:00", "2026-03-29T01:00:00Z"),
        ("2026-03-29", "02:01:00", "2026-03-29T01:01:00Z"),
        ("2026-03-29", "02:30:00", "2026-03-29T01:30:00Z"),
        ("2026-03-29", "03:00:00", "2026-03-29T02:00:00Z"),
        ("2026-03-28", "23:30:00", "2026-03-28T23:30:00Z"),
        ("2026-03-28", "23:59:00", "2026-03-28T23:59:00Z"),
        ("2026-10-25", "00:00:00", "2026-10-24T23:00:00Z"),
        ("2026-10-25", "00:30:00", "2026-10-24T23:30:00Z"),
        ("2026-10-25", "00:59:00", "2026-10-24T23:59:00Z"),
        ("2026-10-25", "01:00:00", "2026-10-25T00:00:00Z"),
        ("2026-10-25", "01:01:00", "2026-10-25T00:01:00Z"),
        ("2026-10-25", "01:30:00", "2026-10-25T00:30:00Z"),
        ("2026-10-25", "01:59:00", "2026-10-25T00:59:00Z"),
        ("2026-10-25", "02:00:00", "2026-10-25T02:00:00Z"),
        ("2026-10-25", "02:01:00", "2026-10-25T02:01:00Z"),
        ("2026-10-25", "02:30:00", "2026-10-25T02:30:00Z"),
        ("2026-10-25", "03:00:00", "2026-10-25T03:00:00Z"),
        ("2026-10-24", "23:30:00", "2026-10-24T22:30:00Z"),
        ("2026-10-24", "23:59:00", "2026-10-24T22:59:00Z"),
        ("2026-01-15", "00:00:00", "2026-01-15T00:00:00Z"),
        ("2026-01-15", "00:30:00", "2026-01-15T00:30:00Z"),
        ("2026-01-15", "01:30:00", "2026-01-15T01:30:00Z"),
        ("2026-01-15", "02:30:00", "2026-01-15T02:30:00Z"),
        ("2026-01-15", "08:22:00", "2026-01-15T08:22:00Z"),
        ("2026-01-15", "23:59:00", "2026-01-15T23:59:00Z"),
        ("2026-07-15", "00:00:00", "2026-07-14T23:00:00Z"),
        ("2026-07-15", "00:30:00", "2026-07-14T23:30:00Z"),
        ("2026-07-15", "01:30:00", "2026-07-15T00:30:00Z"),
        ("2026-07-15", "02:30:00", "2026-07-15T01:30:00Z"),
        ("2026-07-15", "08:22:00", "2026-07-15T07:22:00Z"),
        ("2026-07-15", "23:59:00", "2026-07-15T22:59:00Z"),
    ];

    fn instant(raw: &str) -> DateTime<Utc> {
        raw.parse().unwrap()
    }

    fn date(raw: &str) -> chrono::NaiveDate {
        raw.parse().unwrap()
    }

    #[test]
    fn current_rail_day_matches_the_table() {
        for &(at, _, rail_day, _) in INSTANTS {
            assert_eq!(
                current_rail_day(instant(at)),
                date(rail_day),
                "rail day at {at}"
            );
        }
    }

    #[test]
    fn next_rail_day_boundary_matches_the_table() {
        for &(at, _, _, boundary) in INSTANTS {
            assert_eq!(
                next_rail_day_boundary(instant(at)),
                instant(boundary),
                "boundary after {at}"
            );
        }
    }

    #[test]
    fn the_boundary_is_always_strictly_after_the_instant_and_within_a_day() {
        for &(at, _, _, _) in INSTANTS {
            let at = instant(at);
            let boundary = next_rail_day_boundary(at);
            assert!(boundary > at, "boundary {boundary} is not after {at}");
            assert!(
                boundary - at <= Duration::hours(25),
                "boundary {boundary} is more than a rail day after {at}"
            );
        }
    }

    #[test]
    fn london_to_utc_matches_the_table() {
        for &(day, wall, expected) in WALL_CLOCK {
            assert_eq!(
                london_to_utc(date(day), wall.parse().unwrap()),
                instant(expected),
                "{day} {wall} London"
            );
        }
    }

    #[test]
    fn london_to_utc_keeps_subsecond_and_second_precision() {
        let at = london_to_utc(date("2026-07-15"), "08:22:30.250".parse().unwrap());
        assert_eq!(at, instant("2026-07-15T07:22:30.250Z"));
        let skipped = london_to_utc(date("2026-03-29"), "01:30:15".parse().unwrap());
        assert_eq!(skipped, instant("2026-03-29T01:30:15Z"));
        let repeated = london_to_utc(date("2026-10-25"), "01:30:15".parse().unwrap());
        assert_eq!(repeated, instant("2026-10-25T00:30:15Z"));
    }
}
