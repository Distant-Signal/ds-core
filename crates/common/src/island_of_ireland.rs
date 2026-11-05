//! Shared data model for both Irish-jurisdiction rail networks -- Iarnród
//! Éireann (Republic of Ireland) and, once
//! docs/superpowers/specs/2026-09-05-ireland-rail-support-design.md's open
//! question #1 (NIR's `OpenDataNI` stations-CSV schema) is resolved, Northern
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

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

pub mod live_codes;
pub mod tracking;
pub mod train_state;

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
    /// The source's own name when `name` is a tidied form of it, e.g. NIR's
    /// raw `OpenDataNI` CSV value `"HOLLYWOOD RAIL HALT"` behind the display
    /// name `"Holywood"`. `None` when the source's name is shown as is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_name: Option<String>,
}

/// A named line/route on either Irish-jurisdiction network. Deliberately
/// NOT `common::LineDefinition` (`crates/common/src/lib.rs:461-500`): that
/// type's `stations: Vec<Station>` embeds CRS-keyed rows, `operators` is
/// ATOC-coded, and its `severity_overrides`/`sample_stations` fields both
/// exist to support this app's own GB severity-inference pipeline, which
/// this type does not participate in (see this plan's Judgment Call #3).
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
    /// The feed's short name (GTFS `route_short_name`), when it has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub short_name: Option<String>,
    /// The operator's name (GTFS `agency.txt`'s `agency_name`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agency: Option<String>,
    /// The line's colour as `#RRGGBB` (GTFS `route_color`), when the feed
    /// gives one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
    /// The text colour to draw on [`Self::color`], as `#RRGGBB` (GTFS
    /// `route_text_color`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text_color: Option<String>,
    /// Ids of the same network's earlier lines this one replaces: the
    /// directional GTFS routes (`DUB-BFT-I`, `DUB-BFT-O`) merged into one
    /// corridor (`DUB-BFT`). The writer deletes those rows when it applies
    /// this line, so a merge leaves no stale directional rows behind.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replaces: Vec<String>,
}

/// One station as Iarnród Éireann's live API lists it (`getAllStationsXML`):
/// the `StationCode` its departure boards are keyed by, and the name,
/// alias and position used to match it to a catalogue station
/// ([`live_codes`]). The list also holds non-passenger points (junctions),
/// which simply match nothing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IslandOfIrelandLiveStation {
    /// `StationCode`, e.g. `"BFSTC"`, trimmed.
    pub code: String,
    /// `StationDesc`, e.g. `"Belfast"`.
    pub name: String,
    /// `StationAlias`, e.g. `"Connolly"`, when not blank.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
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
    /// `Traindate`: the day the train started, which stays the same past
    /// midnight. With `train_code` it names one train (train codes repeat
    /// every day). `None` from a producer built before it was parsed, or
    /// when the feed's value doesn't parse.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub train_date: Option<NaiveDate>,
    /// `Origintime`, `HH:MM` Irish local time, as sent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_time: Option<String>,
    /// `Destinationtime`, `HH:MM` Irish local time, as sent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_time: Option<String>,
    /// `Lastlocation`, e.g. `"Departed Dundalk"`, trimmed; `None` when
    /// blank (a train with no report yet). [`train_state`] reads it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_location: Option<String>,
}

/// One live poll of one station's departure board.
/// `station_id` is `api.irishrail.ie`'s own `StationCode` (e.g. `"BFSTC"`)
/// -- NOT the same identifier as `IslandOfIrelandStation.id` for the same
/// physical station when that station's `id` came from GTFS (Tier A). The
/// two are joined by [`live_codes`], which the writer stores as
/// `island_of_ireland_stations.live_code`.
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
            raw_name: None,
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
            short_name: None,
            agency: Some("Iarnród Éireann / Irish Rail".to_string()),
            color: Some("#00A651".to_string()),
            text_color: None,
            replaces: vec!["DUB-BFT-I".to_string()],
        };
        let json = serde_json::to_value(&line).unwrap();
        let back: IslandOfIrelandLineDefinition = serde_json::from_value(json).unwrap();
        assert_eq!(back.stations, line.stations);
        assert_eq!(back.color, line.color);
        assert_eq!(back.replaces, line.replaces);
    }

    /// A body from a producer built before the optional fields existed
    /// still decodes (the writer may run ahead of the pollers), and an
    /// unset optional field is left out of the wire form.
    #[test]
    fn bodies_without_the_optional_fields_decode() {
        let station: IslandOfIrelandStation = serde_json::from_value(serde_json::json!({
            "id": "nir-bangor", "name": "BANGOR RAIL STATION",
            "network": "northern-ireland", "latitude": 54.6, "longitude": -5.6
        }))
        .unwrap();
        assert_eq!(station.raw_name, None);
        let line: IslandOfIrelandLineDefinition = serde_json::from_value(serde_json::json!({
            "id": "DUB-BFT-I", "name": "Belfast - Dublin",
            "network": "republic-of-ireland", "stations": ["A", "B"]
        }))
        .unwrap();
        assert_eq!((line.color, line.agency), (None, None));
        assert!(line.replaces.is_empty());
        let json = serde_json::to_value(&station).unwrap();
        assert!(json.get("raw_name").is_none());
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
                train_date: Some(NaiveDate::from_ymd_opt(2026, 9, 5).unwrap()),
                origin_time: Some("06:00".to_string()),
                destination_time: Some("08:15".to_string()),
                last_location: None,
            }],
        };
        let json = serde_json::to_value(&sample).unwrap();
        assert_eq!(json["departures"][0]["train_date"], "2026-09-05");
        let back: IslandOfIrelandStationSample = serde_json::from_value(json).unwrap();
        assert_eq!(back, sample);
    }

    /// A sample from a poller built before `train_date` and the other
    /// optional fields were parsed still decodes, with them `None`; and an
    /// unset field is left out of the wire form, so an older writer reads
    /// a newer poller's body too.
    #[test]
    fn departures_without_the_board_fields_decode() {
        let departure: IslandOfIrelandDeparture = serde_json::from_value(serde_json::json!({
            "train_code": "A101", "origin": "Belfast", "destination": "Dublin Connolly",
            "late_minutes": 0, "status": "No Information"
        }))
        .unwrap();
        assert_eq!(departure.train_date, None);
        assert_eq!(departure.origin_time, None);
        assert_eq!(departure.last_location, None);
        let json = serde_json::to_value(&departure).unwrap();
        assert!(json.get("train_date").is_none());
        assert!(json.get("last_location").is_none());
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
            train_date: None,
            origin_time: None,
            destination_time: None,
            last_location: Some("Departed Portmarnock".to_string()),
        };
        let json = serde_json::to_value(&departure).unwrap();
        let back: IslandOfIrelandDeparture = serde_json::from_value(json).unwrap();
        assert_eq!(back.late_minutes, -2);
    }
}
