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
