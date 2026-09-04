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

#[cfg(test)]
mod tests {
    use super::*;

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
