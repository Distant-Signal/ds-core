use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::Result;
use chrono::{DateTime, Utc};
use glob::glob;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

pub mod ingest;

/// Status severity scale. Mirrors TfL's `statusSeverity` codes 0–14 where the
/// meanings carry over, with NR-specific extensions above 14. Lower is worse,
/// except 0 (Special Service) and 10 (Good Service) which are canonical "fine"
/// states. Sort ascending for disrupted-lines-first UI ordering.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize_repr, Deserialize_repr,
)]
#[repr(u8)]
pub enum Severity {
    SpecialService = 0,
    Closed = 1,
    Suspended = 2,
    PartSuspended = 3,
    PlannedClosure = 4,
    PartClosure = 5,
    SevereDelays = 6,
    ReducedService = 7,
    /// Rail replacement bus service.
    BusService = 8,
    MinorDelays = 9,
    GoodService = 10,
    PartClosed = 11,
    ExitOnly = 12,
    NoStepFree = 13,
    ChangeOfFrequency = 14,
    /// Post-incident catch-up (NR extension).
    Recovering = 20,
    /// Services running on an alternative route (NR extension).
    Diverted = 21,
}

impl Severity {
    pub fn description(self) -> &'static str {
        match self {
            Self::SpecialService => "Special Service",
            Self::Closed => "Closed",
            Self::Suspended => "Suspended",
            Self::PartSuspended => "Part Suspended",
            Self::PlannedClosure => "Planned Closure",
            Self::PartClosure => "Part Closure",
            Self::SevereDelays => "Severe Delays",
            Self::ReducedService => "Reduced Service",
            Self::BusService => "Rail Replacement",
            Self::MinorDelays => "Minor Delays",
            Self::GoodService => "Good Service",
            Self::PartClosed => "Part Closed",
            Self::ExitOnly => "Exit Only",
            Self::NoStepFree => "No Step Free Access",
            Self::ChangeOfFrequency => "Change of Frequency",
            Self::Recovering => "Recovering",
            Self::Diverted => "Diverted",
        }
    }
}

// --- dataclasses.rs ---

/// How confident are we in this status?
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DataQuality {
    #[default]
    Knowledgebase,
    LdbwsInferred,
    TrustInferred,
    Planned,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidityPeriod {
    pub from_date: DateTime<Utc>,
    pub to_date: Option<DateTime<Utc>>,
    pub is_now: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AffectedRoute {
    pub from_crs: String,
    pub to_crs: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Disruption {
    /// `"RealTime"` | `"PlannedWork"` | `"Information"`
    pub category: String,
    pub description: String,
    #[serde(default)]
    pub affected_stops: Vec<String>,
    #[serde(default)]
    pub affected_routes: Vec<AffectedRoute>,
    /// e.g. `"knowledgebase-incident-12345"`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

/// One status entry on a line. A line may have several simultaneously.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineStatus {
    pub severity: Severity,
    pub reason: String,
    pub validity: ValidityPeriod,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disruption: Option<Disruption>,
    #[serde(default)]
    pub data_quality: DataQuality,
}

/// Top-level object returned by the API for one line.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineStatusReport {
    pub id: String,
    pub name: String,
    pub mode_name: String,
    pub operators: Vec<String>,
    pub statuses: Vec<LineStatus>,
}

impl LineStatusReport {
    /// Lowest numeric severity is the most disruptive.
    pub fn worst_severity(&self) -> Severity {
        self.statuses
            .iter()
            .map(|s| s.severity)
            .min()
            .unwrap_or(Severity::GoodService)
    }
}

// --- Inputs the aggregator consumes ---

/// One service from an LDBWS departure board.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StationDeparture {
    pub service_id: String,
    pub operator: String,
    pub destination_crs: String,
    /// `std` field.
    pub scheduled: String,
    /// `etd` — may be `"On time"`, `"Cancelled"`, or `"HH:MM"`.
    pub estimated: String,
    pub is_cancelled: bool,
    pub delay_minutes: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delay_reason: Option<String>,
    /// e.g. `"1P23"`, from Darwin's `trainid`/`rid`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headcode: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthStatus {
    pub message: String,
}

// --- data/lines.rs ---

/// A station as it appears on one specific line.
///
/// `segment` groups consecutive stations into a named section of track.
/// Segments shared between lines represent shared trunks; segments unique to a
/// line are that line's exclusive sections.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Station {
    pub crs: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tiploc: Option<String>,
    #[serde(default = "Station::default_role")]
    pub role: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub segment: Option<String>,
}

impl Station {
    fn default_role() -> String {
        "minor".to_string()
    }
}

/// A user-facing "line" the aggregator reports status for.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineDefinition {
    pub id: String,
    pub name: String,
    pub mode: String,
    pub category: String,
    pub operators: Vec<String>,
    pub stations: Vec<Station>,
    #[serde(default)]
    pub sample_stations: Vec<String>,
    #[serde(default)]
    pub match_keywords: Vec<String>,
    #[serde(default)]
    pub excluded_keywords: Vec<String>,
    #[serde(default)]
    pub severity_overrides: HashMap<String, f64>,
    /// Segments this line considers exclusive (not shared with other lines).
    /// If empty, the matcher derives exclusivity by comparing segment usage
    /// across all loaded lines.
    #[serde(default)]
    pub exclusive_segments: Vec<String>,
    /// Destination CRS filters used during LDBWS inference.
    #[serde(default)]
    pub destination_crs_filter: Vec<String>,
    /// Headcode prefix filters used during LDBWS inference.
    #[serde(default)]
    pub headcode_prefixes: Vec<String>,
}

impl LineDefinition {
    pub fn from_file(path: &Path) -> Result<Self> {
        Ok(toml::from_str(&std::fs::read_to_string(path)?)?)
    }

    pub fn from_dir(dir_path: &Path) -> Result<Vec<Self>> {
        let paths = glob(&format!("{}/*.toml", dir_path.display()))?;
        paths.map(|path| { Self::from_file(&path?) }).collect()
    }

    pub fn has_station(&self, crs: &str) -> bool {
        self.stations.iter().any(|s| s.crs == crs)
    }

    pub fn segment_for(&self, crs: &str) -> Option<&str> {
        self.stations
            .iter()
            .find(|s| s.crs == crs)
            .and_then(|s| s.segment.as_deref())
    }

    pub fn segments(&self) -> HashSet<&str> {
        self.stations
            .iter()
            .filter_map(|s| s.segment.as_deref())
            .collect()
    }

    /// Returns CRS codes between two stations inclusive, in order.
    pub fn stations_between(&self, from_crs: &str, to_crs: &str) -> Vec<&str> {
        let crs_list: Vec<&str> = self.stations.iter().map(|s| s.crs.as_str()).collect();
        let Some(i) = crs_list.iter().position(|&c| c == from_crs) else {
            return vec![];
        };
        let Some(j) = crs_list.iter().position(|&c| c == to_crs) else {
            return vec![];
        };
        let (lo, hi) = if i <= j { (i, j) } else { (j, i) };
        crs_list[lo..=hi].to_vec()
    }
}

// --- data/database.rs ---
//
// The old shape here was never wired into the crate (data/mod.rs never
// declared `pub mod database;`), so this is a clean rename to match the
// actual Knowledgebase/LDBWS schemas, not a breaking migration.

/// A parsed Knowledgebase incident.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncidentMessage {
    pub incident_id: String, // maps IncidentNumber
    pub summary: String,
    pub description: String,
    pub operators: Vec<String>, // ATOC codes, flattened from Affects.Operators.AffectedOperator[].OperatorRef
    pub affected_stations: Vec<String>, // left empty by pollers — no CRS field exists in the Incidents schema, only free-text RoutesAffected
    pub priority: i32, // raw IncidentPriority integer — no documented enum, do not re-invent "major"/"minor"
    pub validity: Vec<ValidityPeriod>, // schema allows repeated ValidityPeriod, not a single from/to pair
    pub is_planned: bool,              // maps Planned
    pub is_cleared: bool, // maps ClearedIncident (spec: feed retains cleared incidents for a time)
}

/// An LDBWS poll result for one station along a line.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StationSample {
    pub crs: String,
    pub polled_at: DateTime<Utc>,
    pub departures: Vec<StationDeparture>,
}

/// Reference data for a station, as published by the station-reference feed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StationReference {
    pub crs: String,
    pub name: String,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub station_operator: Option<String>,
    /// JSONB passthrough — schema not modeled further here.
    pub accessibility: serde_json::Value,
}

/// Reference data for a Train Operating Company, as published by the
/// TOC-reference feed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TocReference {
    pub atoc_code: String,
    pub name: String,
    pub legal_name: String,
    pub atoc_member: Option<bool>,
    pub station_operator: Option<bool>,
}
