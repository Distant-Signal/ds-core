//! `TfL`'s Tube, DLR and tram route sequences (DS-151;
//! docs/superpowers/specs/2026-10-23-trips-plan-tfl-stop-ends-design.md §3):
//! the `tfl-line-routes/1` row poller-tfl publishes on `ds:ingest:tfl-stops`
//! and the ingest-writer applies to `tfl_line_routes`. Each row is one of a
//! line's routes end to end (`Uxbridge ↔ Aldgate`), its stops in running
//! order, from `GET /Line/{id}/Route/Sequence/{direction}`'s
//! `orderedLineRoutes`, and (DS-206) the timetabled running time between
//! each two of its stops, from `GET /Line/{id}/Timetable/{from}/to/{to}`.
//! The api's Tube access legs time a ride along them, and charge a change
//! when no one route runs from one stop to the other.

use serde::{Deserialize, Serialize};

/// The stream schema's name (`tfl-line-routes/1`).
pub const SCHEMA: &str = "tfl-line-routes";

/// `ingest_freshness.source` of the routes snapshot.
pub const FRESHNESS_SOURCE: &str = "tfl-line-routes";

/// The directions `TfL` names a line's sequences by.
pub const DIRECTIONS: [&str; 2] = ["inbound", "outbound"];

/// The longest route name kept (`tfl_line_routes.name`'s CHECK).
pub const NAME_MAX_CHARS: usize = 200;

/// The most stops one route may have (`tfl_line_routes.stop_ids`' CHECK):
/// the longest real one (the Circle's spiral) has about 36.
pub const MAX_STOPS: usize = 200;

/// The longest running time kept between two stops of a route
/// (`tfl_line_routes.step_seconds`' CHECK): an hour. The longest real one
/// (the Metropolitan's Chalfont to Chesham) is under ten minutes.
pub const MAX_STEP_SECONDS: u32 = 3_600;

/// One route of a `tfl-line-routes/1` snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TflLineRoute {
    /// `TfL`'s line id (`metropolitan`, `dlr`, `tram`).
    pub line_id: String,
    /// `inbound` or `outbound`, as `TfL` names the sequence it came from.
    pub direction: String,
    /// `TfL`'s name for it (`Uxbridge ↔ Aldgate`), whitespace collapsed.
    pub name: String,
    /// The `NaPTAN` stop-area ids (`tfl_stops.stop_id`) in running order. A
    /// stop may come twice (the Circle passes Edgware Road twice).
    pub stop_ids: Vec<String>,
    /// DS-206: the timetabled running time from each stop to the next, in
    /// seconds (`step_seconds[i]` is `stop_ids[i]` to `stop_ids[i + 1]`,
    /// the wait at `stop_ids[i]` included), `None` where `TfL`'s timetable
    /// has no train calling at both in turn. Absent when the route's
    /// timetable could not be fetched (and from a producer older than
    /// DS-206); see [`step_seconds_fit`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_seconds: Option<Vec<Option<u32>>>,
}

/// Whether `steps` fit a route of `stop_count` stops: one per pair of
/// consecutive stops, each at most [`MAX_STEP_SECONDS`].
pub fn step_seconds_fit(steps: &[Option<u32>], stop_count: usize) -> bool {
    steps.len() + 1 == stop_count
        && steps
            .iter()
            .flatten()
            .all(|seconds| *seconds <= MAX_STEP_SECONDS)
}

/// Whether `id` looks like a `TfL` line id: lower-case letters, digits and
/// hyphens (`hammersmith-city`, `waterloo-city`).
pub fn is_line_id(id: &str) -> bool {
    (1..=40).contains(&id.len())
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn line_ids_are_lower_case_slugs() {
        assert!(is_line_id("metropolitan"));
        assert!(is_line_id("hammersmith-city"));
        assert!(!is_line_id("Metropolitan"));
        assert!(!is_line_id(""));
        assert!(!is_line_id("a b"));
    }

    #[test]
    fn a_row_round_trips() {
        let row = TflLineRoute {
            line_id: "testline".into(),
            direction: "inbound".into(),
            name: "Testbury ↔ Testmere".into(),
            stop_ids: vec!["940GZZLUXAA".into(), "940GZZLUXAB".into()],
            step_seconds: None,
        };
        let json = serde_json::to_string(&row).unwrap();
        assert!(!json.contains("step_seconds"), "{json}");
        assert_eq!(serde_json::from_str::<TflLineRoute>(&json).unwrap(), row);
        let timed = TflLineRoute {
            step_seconds: Some(vec![Some(90)]),
            ..row
        };
        let json = serde_json::to_string(&timed).unwrap();
        assert_eq!(serde_json::from_str::<TflLineRoute>(&json).unwrap(), timed);
    }

    #[test]
    fn step_seconds_fit_one_per_pair_of_stops() {
        assert!(step_seconds_fit(&[Some(60), None], 3));
        assert!(step_seconds_fit(&[], 1));
        assert!(!step_seconds_fit(&[Some(60)], 3));
        assert!(!step_seconds_fit(&[Some(MAX_STEP_SECONDS + 1)], 2));
    }
}
