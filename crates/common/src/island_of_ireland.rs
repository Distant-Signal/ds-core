//! Shared data model for both Irish-jurisdiction rail networks -- Iarnród
//! Éireann (Republic of Ireland) and, once
//! docs/superpowers/specs/2026-09-05-ireland-rail-support-design.md's open
//! question #1 (NIR's OpenDataNI stations-CSV schema) is resolved, Northern
//! Ireland Railways/Translink. See that design doc's §3 for the full
//! reasoning behind one generic, network-tagged type rather than two
//! parallel network-specific ones.
//!
//! **Only `RepublicOfIreland`-tagged rows are ever constructed anywhere in
//! this codebase today.** `IslandOfIrelandNetwork::NorthernIreland` exists
//! because the enum is inherently two-sided (a station's authoritative
//! network has to be nameable even when only one value is real yet), not
//! because any NIR ingestion exists -- see
//! docs/superpowers/plans/2026-09-05-ireland-rail-support-plan.md's Global
//! Constraints.
//!
//! `id` is the *sourcing* network's own station code/slug -- for Iarnród
//! Éireann, GTFS's own `stops.txt` `stop_id` (Tier A) is a DIFFERENT
//! identifier scheme from `api.irishrail.ie`'s own `StationCode` (Tier B) --
//! see the plan's Judgment Call #1. Do not assume the two match.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Which network's own feed is authoritative for this row -- "which feed do
/// we source this from," not "which jurisdiction is this station
/// physically in" (design spec §3: the Belfast-area border stations are
/// tagged `RepublicOfIreland` despite being physically in Northern
/// Ireland, per design spec §4's single-authoritative-source policy).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IslandOfIrelandNetwork {
    NorthernIreland,
    RepublicOfIreland,
}

/// A station on either Irish-jurisdiction network. Deliberately NOT
/// `common::Station` (`crates/common/src/lib.rs:443-451`): that type's
/// `crs: String` field is required and has no Irish-network-shaped value
/// (design spec §3, carried from the superseded NI spec's own §2 finding).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IslandOfIrelandStation {
    pub id: String,
    pub name: String,
    pub network: IslandOfIrelandNetwork,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
}

/// A named line/route on either Irish-jurisdiction network. Deliberately
/// NOT `common::LineDefinition` (`crates/common/src/lib.rs:461-500`): that
/// type's `stations: Vec<Station>` embeds CRS-keyed rows, `operators` is
/// ATOC-coded, and its `severity_overrides`/`sample_stations`/`exclusive_segments`
/// fields all exist to support this app's own GB severity-inference
/// pipeline, which this type does not participate in (see this plan's
/// Judgment Call #3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IslandOfIrelandLineDefinition {
    pub id: String,
    pub name: String,
    pub network: IslandOfIrelandNetwork,
    /// Ordered `IslandOfIrelandStation.id` values, same-network only. For a
    /// GTFS-sourced row (Tier A), this is the stop sequence of that
    /// route's longest trip -- see Task A4's own reasoning for why
    /// "longest trip" is this plan's chosen representative stopping
    /// pattern.
    pub stations: Vec<String>,
}

/// One service from an Iarnród Éireann live departure board
/// (`api.irishrail.ie/realtime/realtime.asmx/getStationDataByCodeXML`).
/// Field names/types below are the confirmed live schema, per the friction
/// doc (docs/superpowers/specs/2026-09-05-ireland-vs-northern-ireland-friction-research.md
/// section 1): "Origin, Destination, Scharrival/Schdepart, Exparrival/Expdepart,
/// Late (minutes), Status, Duein -- a live per-service departure-board
/// record with an explicit delay-minutes field already computed."
/// Deliberately NOT `common::StationDeparture` -- that type's
/// `destination_crs: String` has no Irish-network-shaped value, mirroring
/// why `IslandOfIrelandStation` isn't `common::Station`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IslandOfIrelandDeparture {
    /// Iarnrod Eireann's own train identifier, e.g. "A101" -- the friction
    /// doc's own confirmed example (section 4, the Enterprise service).
    pub train_code: String,
    pub origin: String,
    pub destination: String,
    /// HH:MM scheduled times, carried as the upstream API's own string
    /// representation -- same posture `common::StationDeparture.scheduled`
    /// already takes for GB LDBWS times, not parsed into a `NaiveTime`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduled_arrival: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduled_departure: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_arrival: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_departure: Option<String>,
    /// Minutes late -- the upstream API's own `Late` field, already
    /// computed server-side (unlike GB LDBWS, no client-side delay
    /// derivation is needed here). Confirmed signed (a real live poll
    /// during this crate's implementation observed `Late = -2` for an
    /// early-running Enterprise service), not just non-negative.
    pub late_minutes: i32,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub due_in_minutes: Option<i32>,
}

/// One live poll of one station's departure board.
/// `station_id` is `api.irishrail.ie`'s own `StationCode` (e.g. `"BFSTC"`)
/// -- NOT necessarily the same identifier as
/// `IslandOfIrelandStation.id` for the same physical station when that
/// station's `id` came from GTFS (Tier A). See
/// docs/superpowers/plans/2026-09-05-ireland-rail-support-plan.md's
/// Judgment Call #1 -- this is a real, unreconciled gap, not an oversight.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IslandOfIrelandStationSample {
    pub station_id: String,
    pub network: IslandOfIrelandNetwork,
    pub polled_at: DateTime<Utc>,
    pub departures: Vec<IslandOfIrelandDeparture>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn network_serializes_as_kebab_case() {
        assert_eq!(
            serde_json::to_value(IslandOfIrelandNetwork::RepublicOfIreland).unwrap(),
            serde_json::json!("republic-of-ireland")
        );
        assert_eq!(
            serde_json::to_value(IslandOfIrelandNetwork::NorthernIreland).unwrap(),
            serde_json::json!("northern-ireland")
        );
    }

    #[test]
    fn station_round_trips_through_json() {
        let station = IslandOfIrelandStation {
            id: "8350IR0001".to_string(),
            name: "Dublin Connolly".to_string(),
            network: IslandOfIrelandNetwork::RepublicOfIreland,
            latitude: Some(53.3556),
            longitude: Some(-6.2497),
        };
        let json = serde_json::to_value(&station).unwrap();
        let back: IslandOfIrelandStation = serde_json::from_value(json).unwrap();
        assert_eq!(back.id, station.id);
        assert_eq!(back.network, station.network);
    }

    #[test]
    fn line_definition_round_trips_through_json() {
        let line = IslandOfIrelandLineDefinition {
            id: "DUB-BFT-I".to_string(),
            name: "Belfast - Dublin".to_string(),
            network: IslandOfIrelandNetwork::RepublicOfIreland,
            stations: vec!["8350IR0001".to_string(), "BFSTC".to_string()],
        };
        let json = serde_json::to_value(&line).unwrap();
        let back: IslandOfIrelandLineDefinition = serde_json::from_value(json).unwrap();
        assert_eq!(back.stations, line.stations);
    }
}

#[cfg(test)]
mod sample_tests {
    use super::*;

    #[test]
    fn station_sample_round_trips_through_json() {
        let sample = IslandOfIrelandStationSample {
            station_id: "BFSTC".to_string(),
            network: IslandOfIrelandNetwork::RepublicOfIreland,
            polled_at: "2026-09-05T06:00:00Z".parse().unwrap(),
            departures: vec![IslandOfIrelandDeparture {
                train_code: "A101".to_string(),
                origin: "Belfast".to_string(),
                destination: "Dublin Connolly".to_string(),
                scheduled_arrival: None,
                scheduled_departure: Some("06:00".to_string()),
                expected_arrival: None,
                expected_departure: Some("06:00".to_string()),
                late_minutes: 0,
                status: "On Time".to_string(),
                due_in_minutes: Some(5),
            }],
        };
        let json = serde_json::to_value(&sample).unwrap();
        let back: IslandOfIrelandStationSample = serde_json::from_value(json).unwrap();
        assert_eq!(back.departures[0].train_code, "A101");
    }

    #[test]
    fn departure_late_minutes_can_be_negative() {
        // A real live poll against api.irishrail.ie during this crate's
        // implementation observed `Late = -2` for an Enterprise service
        // running early -- confirms `i32`, not `u32`, is the right type.
        let departure = IslandOfIrelandDeparture {
            train_code: "A119".to_string(),
            origin: "Belfast".to_string(),
            destination: "Dublin Connolly".to_string(),
            scheduled_arrival: Some("17:15".to_string()),
            scheduled_departure: None,
            expected_arrival: Some("17:13".to_string()),
            expected_departure: None,
            late_minutes: -2,
            status: "En Route".to_string(),
            due_in_minutes: Some(14),
        };
        let json = serde_json::to_value(&departure).unwrap();
        let back: IslandOfIrelandDeparture = serde_json::from_value(json).unwrap();
        assert_eq!(back.late_minutes, -2);
    }
}
