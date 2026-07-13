use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::Result;
use chrono::{DateTime, Utc};
use glob::glob;
use serde::{Deserialize, Serialize};
use serde_inline_default::serde_inline_default;
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_stats: Option<SampleStats>,
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
    /// CRS codes of scheduled calling points this specific service is
    /// skipping today (Darwin's per-calling-point `isCancelled`, not the
    /// same signal as the whole-service `is_cancelled`). Empty when the
    /// service reports no skipped calls.
    #[serde(default)]
    pub skipped_stations: Vec<String>,
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

/// Sample-derived delay/cancellation/skipped-stop stats for a line, computed
/// from LDBWS `StationSample`s independently of whether the line also has an
/// incident-derived status. Informational only — never used to change a
/// `LineStatus.severity` that came from an incident. `avg_delay_minutes`
/// is averaged over non-cancelled ("running") sampled departures only.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SampleStats {
    pub total: usize,
    pub delayed: usize,
    pub cancelled: usize,
    pub skipped: usize,
    pub avg_delay_minutes: f64,
}

/// A user-defined line (see the `custom_lines` table in the `api` crate).
/// Deliberately a much smaller shape than `LineDefinition` — no segments,
/// match keywords, or severity overrides; those encode official-line route
/// topology and threshold tuning that doesn't apply to an arbitrary
/// user-picked station set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomLine {
    pub id: String,
    pub name: String,
    pub operators: Vec<String>,
    /// Ordered CRS codes. Every station here is also used as an LDBWS
    /// sample station — a custom line has no separate concept of "route
    /// station" vs "station to poll for delay data."
    pub stations: Vec<String>,
    #[serde(default)]
    pub headcode_prefixes: Vec<String>,
    #[serde(default)]
    pub destination_crs_filter: Vec<String>,
}

impl From<CustomLine> for LineDefinition {
    fn from(c: CustomLine) -> Self {
        LineDefinition {
            id: c.id,
            name: c.name,
            mode: "national-rail".to_string(),
            category: "custom".to_string(),
            operators: c.operators,
            stations: c
                .stations
                .iter()
                .map(|crs| Station {
                    crs: crs.clone(),
                    tiploc: None,
                    role: Station::default_role(),
                    segment: None,
                })
                .collect(),
            sample_stations: c.stations,
            match_keywords: vec![],
            excluded_keywords: vec![],
            severity_overrides: HashMap::new(),
            exclusive_segments: vec![],
            destination_crs_filter: c.destination_crs_filter,
            headcode_prefixes: c.headcode_prefixes,
        }
    }
}

// --- Aggregator thresholds (ported from Python config.py DEFAULTS) ---

/// Default thresholds for status derivation. Lines override any subset via
/// `LineDefinition.severity_overrides`. Field names match the keys used in
/// `severity_overrides` TOML tables (e.g. `minor_delays_pct = 0.20`).
#[serde_inline_default]
#[derive(Clone, Deserialize, Debug, PartialEq)]
pub struct Defaults {
    /// A service is "delayed" once its delay exceeds this many minutes.
    #[serde_inline_default(5)]
    pub delay_threshold_minutes: i64,
    /// >25% of sampled services delayed -> Minor Delays.
    #[serde_inline_default(0.25)]
    pub minor_delays_pct: f64,
    /// >50% of sampled services delayed -> Severe Delays.
    #[serde_inline_default(0.50)]
    pub severe_delays_pct: f64,
    /// >25% of sampled services cancelled -> Reduced Service.
    #[serde_inline_default(0.25)]
    pub reduced_service_pct: f64,
    /// >60% of sampled services cancelled -> Part Suspended.
    #[serde_inline_default(0.60)]
    pub part_suspended_pct: f64,
    /// >25% of sampled services skipping a scheduled stop -> Minor Delays.
    /// Independent of `minor_delays_pct` (which only looks at lateness).
    #[serde_inline_default(0.25)]
    pub minor_delays_skip_pct: f64,
    /// >50% of sampled services skipping a scheduled stop -> Severe Delays.
    /// Independent of `severe_delays_pct` (which only looks at lateness).
    #[serde_inline_default(0.50)]
    pub severe_delays_skip_pct: f64,
    /// Unused by the current keyword-only severity classifier; kept for
    /// parity with the Python prototype's `DEFAULTS` dict and any future
    /// use once `IncidentMessage.priority`'s meaning is confirmed.
    #[serde_inline_default(0)]
    pub knowledgebase_severity_floor: i8,
    /// Below this many sampled services, don't infer a status from LDBWS
    /// samples alone.
    #[serde_inline_default(3)]
    pub min_sample_size: i64,
}

impl Default for Defaults {
    fn default() -> Self {
        toml::from_str("").expect("Defaults must deserialize from an empty TOML table via serde_inline_default")
    }
}

/// Merges a line's `severity_overrides` on top of shared `Defaults`,
/// returning a new `Defaults` with any recognized keys overridden. Unknown
/// keys are ignored (there's no field for them to override). Ported from
/// Python's `config.thresholds_for`.
pub fn thresholds_for(defaults: &Defaults, overrides: &HashMap<String, f64>) -> Defaults {
    let mut merged = defaults.clone();
    for (key, value) in overrides {
        match key.as_str() {
            "delay_threshold_minutes" => merged.delay_threshold_minutes = *value as i64,
            "minor_delays_pct" => merged.minor_delays_pct = *value,
            "severe_delays_pct" => merged.severe_delays_pct = *value,
            "minor_delays_skip_pct" => merged.minor_delays_skip_pct = *value,
            "severe_delays_skip_pct" => merged.severe_delays_skip_pct = *value,
            "reduced_service_pct" => merged.reduced_service_pct = *value,
            "part_suspended_pct" => merged.part_suspended_pct = *value,
            "knowledgebase_severity_floor" => merged.knowledgebase_severity_floor = *value as i8,
            "min_sample_size" => merged.min_sample_size = *value as i64,
            _ => {}
        }
    }
    merged
}

#[cfg(test)]
mod defaults_tests {
    use super::*;

    #[test]
    fn no_overrides_returns_defaults_unchanged() {
        let defaults = Defaults::default();
        let merged = thresholds_for(&defaults, &HashMap::new());
        assert_eq!(merged, defaults);
    }

    #[test]
    fn partial_override_changes_only_named_fields() {
        let defaults = Defaults::default();
        let mut overrides = HashMap::new();
        overrides.insert("minor_delays_pct".to_string(), 0.20);
        overrides.insert("delay_threshold_minutes".to_string(), 4.0);
        let merged = thresholds_for(&defaults, &overrides);
        assert_eq!(merged.minor_delays_pct, 0.20);
        assert_eq!(merged.delay_threshold_minutes, 4);
        assert_eq!(merged.severe_delays_pct, defaults.severe_delays_pct);
        assert_eq!(merged.min_sample_size, defaults.min_sample_size);
    }

    #[test]
    fn every_field_can_be_overridden() {
        let defaults = Defaults::default();
        let mut overrides = HashMap::new();
        overrides.insert("delay_threshold_minutes".to_string(), 10.0);
        overrides.insert("minor_delays_pct".to_string(), 0.30);
        overrides.insert("severe_delays_pct".to_string(), 0.60);
        overrides.insert("minor_delays_skip_pct".to_string(), 0.35);
        overrides.insert("severe_delays_skip_pct".to_string(), 0.65);
        overrides.insert("reduced_service_pct".to_string(), 0.40);
        overrides.insert("part_suspended_pct".to_string(), 0.70);
        overrides.insert("knowledgebase_severity_floor".to_string(), 1.0);
        overrides.insert("min_sample_size".to_string(), 5.0);
        let merged = thresholds_for(&defaults, &overrides);
        assert_eq!(merged.delay_threshold_minutes, 10);
        assert_eq!(merged.minor_delays_pct, 0.30);
        assert_eq!(merged.severe_delays_pct, 0.60);
        assert_eq!(merged.minor_delays_skip_pct, 0.35);
        assert_eq!(merged.severe_delays_skip_pct, 0.65);
        assert_eq!(merged.reduced_service_pct, 0.40);
        assert_eq!(merged.part_suspended_pct, 0.70);
        assert_eq!(merged.knowledgebase_severity_floor, 1);
        assert_eq!(merged.min_sample_size, 5);
    }

    #[test]
    fn unknown_key_is_ignored() {
        let defaults = Defaults::default();
        let mut overrides = HashMap::new();
        overrides.insert("not_a_real_field".to_string(), 42.0);
        let merged = thresholds_for(&defaults, &overrides);
        assert_eq!(merged, defaults);
    }
}

#[cfg(test)]
mod custom_line_tests {
    use super::*;

    #[test]
    fn custom_line_converts_to_line_definition_with_no_segments_or_keywords() {
        let custom = CustomLine {
            id: "custom-my-commute".to_string(),
            name: "My Commute".to_string(),
            operators: vec!["SW".to_string()],
            stations: vec!["WOK".to_string(), "AON".to_string()],
            headcode_prefixes: vec!["1P".to_string()],
            destination_crs_filter: vec!["AON".to_string()],
        };
        let line: LineDefinition = custom.into();
        assert_eq!(line.id, "custom-my-commute");
        assert_eq!(line.name, "My Commute");
        assert_eq!(line.mode, "national-rail");
        assert_eq!(line.category, "custom");
        assert_eq!(line.operators, vec!["SW".to_string()]);
        assert_eq!(line.stations.len(), 2);
        assert_eq!(line.stations[0].crs, "WOK");
        assert!(line.stations[0].segment.is_none());
        assert_eq!(line.sample_stations, vec!["WOK".to_string(), "AON".to_string()]);
        assert!(line.match_keywords.is_empty());
        assert!(line.severity_overrides.is_empty());
        assert_eq!(line.headcode_prefixes, vec!["1P".to_string()]);
        assert_eq!(line.destination_crs_filter, vec!["AON".to_string()]);
    }
}
