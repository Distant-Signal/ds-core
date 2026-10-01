//! Each journey stop's LDBWS-style live status, served as `status` (and
//! `lateMinutes`) on every `JourneyStop`. This sits beside the existing
//! finer-grained fields (actual/estimated times, `stopStatus`,
//! `variationStatus`, `delayMinutes`), which stay unchanged. It exists so a
//! client moving off LDBWS `GetServiceDetails` has one field that reads
//! like LDBWS's `at`/`et`/`isCancelled`.
//!
//! Values, first match wins:
//!
//! | `status`    | When | LDBWS equivalent |
//! |-------------|------|------------------|
//! | `Cancelled` | Darwin or TRUST says this call is skipped (`stopStatus: Skipped`), or the train is cancelled and has not reached this stop | `isCancelled: true` |
//! | `Departed`  | A TRUST departure was reported here | `at`/`atd` = the actual time |
//! | `Arrived`   | A TRUST arrival, but no departure yet (or the terminus) | `ata` = the actual time |
//! | `NoReport`  | Not reported, but a LATER stop has been, so the train has passed it | `at: "No report"` |
//! | `Late`      | Not yet reached, and the live estimate is at least one minute after the booked time; `lateMinutes` is by how much | `et: "HH:MM"` (later than scheduled) or `"Delayed"` |
//! | `OnTime`    | Not yet reached, and the live estimate is not after the booked time | `et: "On time"` |
//! | `Scheduled` | Not yet reached, with no live data for the train or no estimate for this stop | none (LDBWS shows `"On time"` here; DS does not claim it) |
//!
//! Differences from Darwin:
//! - The estimates are TRUST's current delay carried forward to every
//!   later stop (`journey::apply_delay_estimates`), not Darwin's own
//!   forecasts, so recovery time in the schedule is not modelled.
//! - A train cancelled en route marks every stop it has not reached as
//!   `Cancelled`, including a stop it passed without TRUST reporting it
//!   (unless a later stop was reported, which gives `NoReport`).
//! - There is no `Delayed` (late, with no estimate): a stop with no
//!   estimate is `Scheduled`.

use serde::Serialize;

use crate::data::journey::{JourneyStop, StopStatus};

/// See the module doc for each value's meaning. `PascalCase` on the wire,
/// like `StopStatus` beside it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum LiveStopStatus {
    OnTime,
    Late,
    Cancelled,
    NoReport,
    Arrived,
    Departed,
    Scheduled,
}

/// Whether the train has reported anything: its TRUST status is past
/// activation, or any stop has an actual time.
pub fn train_has_live_data(status: Option<&str>, stops: &[JourneyStop]) -> bool {
    matches!(status, Some("en_route" | "completed" | "cancelled"))
        || stops
            .iter()
            .any(|s| s.actual_arrival.is_some() || s.actual_departure.is_some())
}

/// Sets `live_status` and `late_minutes` on every stop.
pub fn apply(stops: &mut [JourneyStop], cancelled: bool, live: bool) {
    let last_reached = stops
        .iter()
        .rposition(|s| s.actual_arrival.is_some() || s.actual_departure.is_some());
    for (index, stop) in stops.iter_mut().enumerate() {
        let (status, late) = if stop.stop_status == StopStatus::Skipped {
            (LiveStopStatus::Cancelled, None)
        } else if stop.actual_departure.is_some() {
            (LiveStopStatus::Departed, None)
        } else if stop.actual_arrival.is_some() {
            (LiveStopStatus::Arrived, None)
        } else if last_reached.is_some_and(|reached| reached > index) {
            (LiveStopStatus::NoReport, None)
        } else if cancelled {
            (LiveStopStatus::Cancelled, None)
        } else if !live {
            (LiveStopStatus::Scheduled, None)
        } else {
            match expected_lateness_minutes(stop) {
                Some(minutes) if minutes >= 1 => (LiveStopStatus::Late, Some(minutes)),
                Some(_) => (LiveStopStatus::OnTime, None),
                None => (LiveStopStatus::Scheduled, None),
            }
        };
        stop.live_status = Some(status);
        stop.late_minutes = late;
    }
}

/// Estimated minus booked, in whole minutes: departure where the stop has
/// one, else arrival.
#[expect(
    clippy::cast_possible_truncation,
    reason = "lateness in minutes is far below i32::MAX"
)]
fn expected_lateness_minutes(stop: &JourneyStop) -> Option<i32> {
    let pair = match (stop.scheduled_departure, stop.estimated_departure) {
        (Some(scheduled), Some(estimated)) => Some((scheduled, estimated)),
        _ => stop.scheduled_arrival.zip(stop.estimated_arrival),
    };
    pair.map(|(scheduled, estimated)| (estimated - scheduled).num_minutes() as i32)
}

#[cfg(test)]
#[expect(
    clippy::unnecessary_wraps,
    reason = "test code: fakes mirror the signatures they stand in for"
)]
mod tests {
    use super::*;
    use crate::data::journey::StopTimetable;
    use chrono::{DateTime, Utc};

    fn at(hhmm: &str) -> Option<DateTime<Utc>> {
        Some(format!("2026-09-27T{hhmm}:00Z").parse().unwrap())
    }

    fn stop(scheduled: &str) -> JourneyStop {
        JourneyStop {
            crs: None,
            name: None,
            tiploc: None,
            kind: None,
            scheduled_arrival: at(scheduled),
            scheduled_departure: at(scheduled),
            actual_arrival: None,
            actual_departure: None,
            estimated_arrival: None,
            estimated_departure: None,
            last_event_type: None,
            variation_status: None,
            delay_minutes: None,
            stop_status: StopStatus::Scheduled,
            skip_source: None,
            platform: None,
            planned_platform: None,
            platform_changed: false,
            platform_status: None,
            booked_platform: None,
            live_status: None,
            late_minutes: None,
            board: None,
            timetable: StopTimetable::default(),
        }
    }

    fn statuses(stops: &[JourneyStop]) -> Vec<(Option<LiveStopStatus>, Option<i32>)> {
        stops
            .iter()
            .map(|s| (s.live_status, s.late_minutes))
            .collect()
    }

    #[test]
    fn every_class_on_a_running_train() {
        let mut stops = vec![
            stop("08:00"),
            stop("08:10"),
            stop("08:20"),
            stop("08:30"),
            stop("08:40"),
            stop("08:50"),
        ];
        stops[0].actual_departure = at("08:01"); // Departed
        // stops[1]: never reported, but a later stop was -> NoReport
        stops[2].actual_arrival = at("08:22"); // Arrived (not yet departed)
        stops[3].estimated_departure = at("08:35"); // Late by 5
        stops[4].estimated_departure = at("08:40"); // OnTime
        stops[5].stop_status = StopStatus::Skipped; // Cancelled
        apply(&mut stops, false, true);
        assert_eq!(
            statuses(&stops),
            vec![
                (Some(LiveStopStatus::Departed), None),
                (Some(LiveStopStatus::NoReport), None),
                (Some(LiveStopStatus::Arrived), None),
                (Some(LiveStopStatus::Late), Some(5)),
                (Some(LiveStopStatus::OnTime), None),
                (Some(LiveStopStatus::Cancelled), None),
            ]
        );
    }

    #[test]
    fn a_cancelled_train_cancels_only_the_stops_it_has_not_reached() {
        let mut stops = vec![stop("08:00"), stop("08:10"), stop("08:20")];
        stops[0].actual_departure = at("08:00");
        apply(&mut stops, true, true);
        assert_eq!(
            statuses(&stops),
            vec![
                (Some(LiveStopStatus::Departed), None),
                (Some(LiveStopStatus::Cancelled), None),
                (Some(LiveStopStatus::Cancelled), None),
            ]
        );
    }

    #[test]
    fn no_live_data_or_no_estimate_is_scheduled_not_on_time() {
        let mut stops = vec![stop("08:00")];
        apply(&mut stops, false, false);
        assert_eq!(
            statuses(&stops),
            vec![(Some(LiveStopStatus::Scheduled), None)]
        );
        let mut stops = vec![stop("08:00")];
        apply(&mut stops, false, true);
        assert_eq!(
            statuses(&stops),
            vec![(Some(LiveStopStatus::Scheduled), None)]
        );
    }

    #[test]
    fn a_terminus_uses_its_arrival_estimate() {
        let mut stops = vec![stop("08:00")];
        stops[0].scheduled_departure = None;
        stops[0].estimated_arrival = at("08:03");
        apply(&mut stops, false, true);
        assert_eq!(
            statuses(&stops),
            vec![(Some(LiveStopStatus::Late), Some(3))]
        );
    }

    #[test]
    fn live_data_is_status_or_any_actual() {
        let mut stops = vec![stop("08:00")];
        assert!(!train_has_live_data(None, &stops));
        assert!(!train_has_live_data(Some("awaiting_activation"), &stops));
        assert!(train_has_live_data(Some("en_route"), &stops));
        stops[0].actual_arrival = at("08:00");
        assert!(train_has_live_data(None, &stops));
    }
}
