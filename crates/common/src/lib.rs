use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::Result;
use chrono::{DateTime, Utc};
use glob::glob;
use serde::{Deserialize, Serialize};
use serde_inline_default::serde_inline_default;
use serde_repr::{Deserialize_repr, Serialize_repr};

pub mod config;
pub mod ingest;
pub mod island_of_ireland;
pub mod metrics;
pub mod oauth_client;
pub mod poller_loop;
pub mod rail_day;
pub mod service_args;

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
    /// TfL code 20. The line is shut for the night (or has not started for
    /// the day) — the ordinary overnight state of the Underground, not a
    /// fault. Deliberately NOT discriminant 20: that is already the NR
    /// extension `Recovering`, and renumbering would change the meaning of
    /// every `statusSeverity` already stored in `line_status.statuses` and
    /// rendered by `frontend/lib/severity.ts`.
    ServiceClosed = 22,
    /// TfL code 16. Unlike `ServiceClosed`, this is a service that should
    /// be running and is not.
    NotRunning = 23,
    /// TfL code 17.
    IssuesReported = 24,
    /// TfL code 18. TfL's own "everything is fine" wording for modes that
    /// don't use `Good Service`.
    NoIssues = 25,
    /// TfL code 19, and this crate's landing place for any future TfL code
    /// it has never heard of (see `severity_from_tfl_code`).
    Information = 26,
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
            Self::ServiceClosed => "Service Closed",
            Self::NotRunning => "Not Running",
            Self::IssuesReported => "Issues Reported",
            Self::NoIssues => "No Issues",
            Self::Information => "Information",
        }
    }
}

/// True severity rank for a `Severity`. **Higher is worse**, the opposite
/// direction from the discriminant.
///
/// `Severity`'s derived `Ord` sorts by declaration order / discriminant
/// value, and TfL's `statusSeverity` codes are **not** monotonic with actual
/// severity: `Diverted = 21` and `PartClosed = 11` are numerically high (so
/// they compare as "mild") but are genuinely severe, while `GoodService = 10`
/// sits in the middle of the numeric range. Anywhere the real question is
/// "which of these is worse", rank through this function rather than
/// comparing discriminants.
///
/// The groups are the exact mirror of `frontend/lib/severity.ts`'s
/// `SEVERITY_TABLE` + `GROUP_RANK` (good=0, informational=1, planned=2,
/// mild=3, severe=4), so the two ends of the stack agree on ordering. The
/// `match` is exhaustive on purpose: a new `Severity` variant must not
/// silently acquire a default rank.
pub fn severity_rank(severity: Severity) -> u8 {
    match severity {
        Severity::GoodService | Severity::NoIssues => 0,
        Severity::SpecialService
        | Severity::ExitOnly
        | Severity::NoStepFree
        | Severity::ServiceClosed
        | Severity::Information => 1,
        Severity::PlannedClosure | Severity::PartClosure => 2,
        Severity::ReducedService
        | Severity::MinorDelays
        | Severity::ChangeOfFrequency
        | Severity::Recovering
        | Severity::IssuesReported => 3,
        Severity::Closed
        | Severity::Suspended
        | Severity::PartSuspended
        | Severity::SevereDelays
        | Severity::BusService
        | Severity::PartClosed
        | Severity::Diverted
        | Severity::NotRunning => 4,
    }
}

#[cfg(test)]
mod severity_rank_tests {
    use super::*;

    #[test]
    fn rank_matches_the_frontends_group_table() {
        // One assertion per row of frontend/lib/severity.ts's SEVERITY_TABLE,
        // in its numeric order, so drift between the two is a test failure
        // rather than a silently divergent passenger-facing ordering.
        for (severity, expected) in [
            (Severity::SpecialService, 1),
            (Severity::Closed, 4),
            (Severity::Suspended, 4),
            (Severity::PartSuspended, 4),
            (Severity::PlannedClosure, 2),
            (Severity::PartClosure, 2),
            (Severity::SevereDelays, 4),
            (Severity::ReducedService, 3),
            (Severity::BusService, 4),
            (Severity::MinorDelays, 3),
            (Severity::GoodService, 0),
            (Severity::PartClosed, 4),
            (Severity::ExitOnly, 1),
            (Severity::NoStepFree, 1),
            (Severity::ChangeOfFrequency, 3),
            (Severity::Recovering, 3),
            (Severity::Diverted, 4),
            (Severity::ServiceClosed, 1),
            (Severity::NotRunning, 4),
            (Severity::IssuesReported, 3),
            (Severity::NoIssues, 0),
            (Severity::Information, 1),
        ] {
            assert_eq!(severity_rank(severity), expected, "{severity:?}");
        }
    }

    #[test]
    fn rank_disagrees_with_the_discriminant_where_the_codes_are_non_monotonic() {
        // The whole reason this function exists. By discriminant, Diverted
        // (21) and PartClosed (11) sort as milder than MinorDelays (9); by
        // rank they are correctly more severe.
        assert!(Severity::Diverted > Severity::MinorDelays);
        assert!(Severity::PartClosed > Severity::MinorDelays);
        assert!(severity_rank(Severity::Diverted) > severity_rank(Severity::MinorDelays));
        assert!(severity_rank(Severity::PartClosed) > severity_rank(Severity::MinorDelays));
        // GoodService is the mildest thing there is despite sitting mid-range.
        assert_eq!(severity_rank(Severity::GoodService), 0);
    }
}

/// Maps a TfL Unified API `statusSeverity` code to this app's `Severity`.
///
/// Codes 0–14 are the same scale in both systems (ours was modelled on
/// TfL's). 15–20 are not: TfL 15 is its own `Diverted` where ours is 21,
/// and TfL 20 is `Service Closed` where our 20 is the NR extension
/// `Recovering` — so a raw numeric passthrough would have mislabelled the
/// ordinary overnight closure of every Underground line as "Recovering".
///
/// `None` means TfL has published a code this table has never seen. Callers
/// must not drop the status (a line with no statuses renders as Good
/// Service) and must not guess a severity: `crates/poller-tfl` records it
/// as `Severity::Information` and carries TfL's own description through in
/// the reason text.
pub fn severity_from_tfl_code(code: u8) -> Option<Severity> {
    Some(match code {
        0 => Severity::SpecialService,
        1 => Severity::Closed,
        2 => Severity::Suspended,
        3 => Severity::PartSuspended,
        4 => Severity::PlannedClosure,
        5 => Severity::PartClosure,
        6 => Severity::SevereDelays,
        7 => Severity::ReducedService,
        8 => Severity::BusService,
        9 => Severity::MinorDelays,
        10 => Severity::GoodService,
        11 => Severity::PartClosed,
        12 => Severity::ExitOnly,
        13 => Severity::NoStepFree,
        14 => Severity::ChangeOfFrequency,
        15 => Severity::Diverted,
        16 => Severity::NotRunning,
        17 => Severity::IssuesReported,
        18 => Severity::NoIssues,
        19 => Severity::Information,
        20 => Severity::ServiceClosed,
        _ => return None,
    })
}

/// The `operators` entry every TfL-sourced line carries. TfL has no
/// per-line ATOC-style operator code the way National Rail does — tube,
/// DLR, Overground, Elizabeth line and tram are all "TfL" — so this is a
/// constant rather than anything derived from the feed.
pub const TFL_OPERATOR: &str = "TfL";

/// Prefix on every TfL line id. `line_status.line_id` is a primary key and
/// TfL's tube line id is `northern`, which is also the id in
/// `lines/northern.toml`; without this prefix the two railways would fight
/// over one row. Applied once, in `crates/poller-tfl`.
pub const TFL_LINE_ID_PREFIX: &str = "tfl-";

/// Maps a TfL line id (already `TFL_LINE_ID_PREFIX`-namespaced) to the NR
/// catalogue line id covering the same railway, for the small set of lines
/// where a TfL-sourced `line_status` row and an NR/Darwin-sourced one exist
/// independently for what is, to a passenger, one railway. See
/// `docs/superpowers/specs/2026-08-22-tfl-service-metrics-v2-design.md`
/// Areas 1 and 2. Elizabeth line is Area 1; the six London Overground lines
/// (Area 2) were added once `lines/overground-*.toml` existed for them --
/// their TfL `id`s (`liberty`, `lioness`, `mildmay`, `suffragette`,
/// `weaver`, `windrush`) were verified against a live
/// `GET /Line/Mode/overground/Status` call rather than guessed.
/// `nr_line_id_for_tfl`/`tfl_line_id_for_nr` are written generically over
/// this table so extending it needs no code change beyond a new row.
const TFL_TO_NR_LINE_ID: &[(&str, &str)] = &[
    ("tfl-elizabeth", "elizabeth-line"),
    ("tfl-liberty", "overground-liberty"),
    ("tfl-lioness", "overground-lioness"),
    ("tfl-mildmay", "overground-mildmay"),
    ("tfl-suffragette", "overground-suffragette"),
    ("tfl-weaver", "overground-weaver"),
    ("tfl-windrush", "overground-windrush"),
];

/// The NR catalogue line id a TfL line's status should be merged into for
/// display, or `None` if this TfL line has no NR counterpart (true for
/// every TfL line except the ones in `TFL_TO_NR_LINE_ID`).
pub fn nr_line_id_for_tfl(tfl_line_id: &str) -> Option<&'static str> {
    TFL_TO_NR_LINE_ID
        .iter()
        .find(|(tfl, _)| *tfl == tfl_line_id)
        .map(|(_, nr)| *nr)
}

/// The TfL line id whose status should be overlaid onto this NR catalogue
/// line id's detail view, or `None` if this NR line has no TfL counterpart.
pub fn tfl_line_id_for_nr(nr_line_id: &str) -> Option<&'static str> {
    TFL_TO_NR_LINE_ID
        .iter()
        .find(|(_, nr)| *nr == nr_line_id)
        .map(|(tfl, _)| *tfl)
}

// --- dataclasses.rs ---

/// How confident are we in this status?
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DataQuality {
    #[default]
    Knowledgebase,
    LdbwsInferred,
    /// Reserved for a future full-coverage TRUST-vs-schedule consumer
    /// ("Option B" — see
    /// `docs/superpowers/specs/2026-08-29-trust-schedule-delay-inference-design.md`
    /// and
    /// `docs/superpowers/specs/2026-09-03-full-coverage-metrics-transition-design.md`).
    /// **Not constructed anywhere in this codebase today** — Option B has
    /// not been built, and its own go/no-go decision gate
    /// (`docs/superpowers/specs/2026-08-29-trust-schedule-delay-validation-findings.md`,
    /// Task 8) has been re-run three times and has not reached "go" as of
    /// this comment. When it does, this would be set by the same narrow
    /// rule `LdbwsInferred` already follows: only when a full-coverage
    /// status determines a line's severity with no active Knowledgebase
    /// incident present, never when full-coverage data merely escalates an
    /// incident-derived status's severity (which keeps its original
    /// `Knowledgebase`/`Planned` provenance, mirroring
    /// `escalate_from_sample_stats`'s existing behavior for LDBWS).
    TrustInferred,
    Planned,
    /// Published by TfL as line status, not inferred by this app from
    /// incidents or departure boards. The most authoritative quality there
    /// is for a TfL line, and deliberately not folded into
    /// `Knowledgebase` — that name means the National Rail RDM
    /// Knowledgebase feed specifically.
    Tfl,
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
    /// `rail_replacement_bus` | `no_scheduled_service` | `diversion` | `null`
    /// -- the currently-governing period's `impact_type`, per
    /// `aggregator::governing_impact_type`. `None` for a disruption with no
    /// currently-Active period stating one of these facts (the overwhelming
    /// majority), and unconditionally `None` for a TfL- or LDBWS-derived
    /// disruption, which never runs the enricher's extraction pipeline at
    /// all. `#[serde(default, ...)]` matches `source`'s own attribute --
    /// load-bearing for deserializing `line_status_history` rows written
    /// before this field existed (see `crates/api/src/data/queries.rs`'s
    /// `line_status_history_for_range`, which reads this struct back from
    /// stored JSONB).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub impact_type: Option<String>,
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
    #[serde(default = "SampleAvailability::no_coverage_default")]
    pub sample_availability: SampleAvailability,
    /// Full-coverage analog of `sample_stats` -- see `FullCoverageAvailability`'s
    /// own doc comment and
    /// docs/superpowers/specs/2026-09-03-full-coverage-metrics-transition-design.md
    /// Decision 1. `None` for every line today (nothing produces this yet);
    /// permanent, additive scaffolding, not a replacement for `sample_stats`
    /// -- see that design doc's Decision 3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub full_coverage_stats: Option<SampleStats>,
    #[serde(default = "FullCoverageAvailability::not_enabled_default")]
    pub full_coverage_availability: FullCoverageAvailability,
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
    /// Opt-in per line, catalogue-authored -- mirrors `severity_overrides`'s
    /// existing per-line-TOML-field precedent. Gates whether a future
    /// full-coverage (TRUST-vs-schedule) consumer even attempts this line,
    /// not merely whether its result is shown once resolved -- so a
    /// line's `LineStatus.full_coverage_availability` genuinely stays
    /// `NotEnabled` (not `Pending` forever) until this flag is set,
    /// distinguishing "not rolled out to yet" from "rolled out, still
    /// resolving." See
    /// docs/superpowers/specs/2026-09-03-full-coverage-metrics-transition-design.md
    /// Decision 3. `false` for every line in this repo's catalogue today
    /// -- nothing consumes this yet (see `crates/aggregator::merge_full_coverage`).
    #[serde(default)]
    pub full_coverage_enabled: bool,
}

impl LineDefinition {
    pub fn from_file(path: &Path) -> Result<Self> {
        Ok(toml::from_str(&std::fs::read_to_string(path)?)?)
    }

    pub fn from_dir(dir_path: &Path) -> Result<Vec<Self>> {
        let paths = glob(&format!("{}/*.toml", dir_path.display()))?;
        paths.map(|path| Self::from_file(&path?)).collect()
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

/// One resolved `(crs, operator)` full-coverage row, mirroring
/// `StationSample`'s own per-station shape one level finer. Written
/// directly by a future `full-coverage-consumer` to `POST
/// /private/station-full-coverage-samples` (not built by this plan — see
/// docs/superpowers/specs/2026-09-04-option-b-live-consumer-design.md
/// Decision 2h/3, converged on
/// docs/superpowers/specs/2026-09-04-per-station-full-coverage-stats-design.md
/// Decision 2's own guess verbatim), read at request time by
/// `queries::latest_station_full_coverage_samples`. `stats` stores
/// serialized JSONB in `station_full_coverage_samples.stats` -- same
/// storage posture as `StationSample.departures`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StationFullCoverageSample {
    pub crs: String,
    pub operator: String,
    pub resolved_at: DateTime<Utc>,
    pub stats: SampleStats,
}

/// Pin-creation payload for `POST /Train/track` (`crates/api/src/routes/train.rs`).
/// Deliberately does NOT include `train_uid` -- per the design doc's
/// Tracking semantics, the pinned service is only ever known by what a
/// departure-board view already has (RDM's ephemeral `serviceID`-adjacent
/// fields), never by a durable train identity at pin time. Resolution to
/// `(train_uid, service_date)` happens later, out of band, once
/// trust-consumer observes a matching TRUST Activation (see
/// docs/superpowers/plans/2026-08-28-train-tracking.md Task 10).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackPinRequest {
    pub service_date: chrono::NaiveDate,
    pub origin_crs: String,
    pub scheduled_departure: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_crs: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operator: Option<String>,
}

/// Manual ticket-entry payload for `POST /Train/{trackingId}/tickets`
/// (`crates/api/src/routes/train.rs`) -- the durable v1 backbone every
/// ingestion tier ultimately funnels through (see
/// docs/superpowers/plans/2026-08-29-journey-ticket-tracking.md's
/// Architecture section). `source` defaults to "manual"; a `.pkpass`/PDF
/// upload preview (Tasks 6-9) is turned into a saved row by the client
/// re-submitting this same request shape with `source` set to whichever
/// tier produced the reviewed data ("pkpass-semantics" / "pkpass-heuristic"
/// / "pdf-heuristic") -- there is no separate "confirm upload" endpoint;
/// this is the only write path, deliberately.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TicketEntryRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operator: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ticket_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_crs: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_crs: Option<String>,
    #[serde(default = "default_ticket_source")]
    pub source: String,
}

fn default_ticket_source() -> String {
    "manual".to_string()
}

/// Shared bound for a tracked train's or ticket's `custom_name` (see
/// `docs/superpowers/specs/2026-09-05-custom-tracking-names-design.md`'s
/// Decision 1) -- one source of truth so `crates/api`'s validator
/// (`train_tracking::validate_custom_name`) never drifts from whatever a
/// future frontend character-counter would also need to know, the same
/// reasoning `TFL_OPERATOR`/`TFL_LINE_ID_PREFIX` above already establish
/// for a cross-cutting constant living here rather than as a private
/// literal in one crate. Counts Unicode scalar values (`str::chars().count()`),
/// not bytes -- "100 characters" is the user-facing wording
/// (`validate_custom_name`'s own error message), so the bound should match
/// what a human would count, not UTF-8 byte length. A reasonable-sounding,
/// not researched or load-tested figure, same posture
/// `crates/api/src/data/train_tracking.rs`'s own `MAX_PIN_AGE`/
/// `MINE_LIST_LIMIT` are flagged with -- revisit once real usage exists.
pub const CUSTOM_NAME_MAX_LENGTH: usize = 100;

/// One TRUST-derived event for a tracked train, as `trust-consumer` posts
/// it to `POST /private/train-events`. Carries both the raw event (for the
/// immutable log, `train_movement_events`) and trust-consumer's own
/// derived current-state fields (for `train_current_state`) in the same
/// message -- denormalize-on-write, per this plan's Global Constraints.
///
/// `resolved_train_uid`/`resolved_train_id` are only `Some` on the one
/// message that resolves a pending pin (i.e. the Activation-derived
/// event); every subsequent event for the same tracked train carries them
/// as `None`, since the binding doesn't change again.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrainMovementEventMessage {
    pub tracked_train_id: i64,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_train_uid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_train_id: Option<String>,

    pub dedup_key: String,
    pub msg_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loc_stanox: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loc_crs: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub planned_timestamp: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_timestamp: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variation_status: Option<String>,
    pub raw_body: serde_json::Value,

    // Derived current-state fields, computed by trust-consumer (Tasks
    // 11-12) and written straight through to train_current_state.
    pub status: String, // "awaiting_activation" | "en_route" | "cancelled" | "completed"
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_reported_location: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_event_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delay_minutes: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_calling_point: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eta_next: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eta_source: Option<String>, // "trust-propagated", set by trust-consumer.
                                    // "darwin-estimated" is only ever
                                    // produced at read time (Task 6), never
                                    // written back by trust-consumer.
}

/// What `trust-consumer` needs to know about each active tracked train:
/// pending pins to attempt resolving, and already-resolved ones to
/// recognize incoming TRUST messages against, after a restart or on its
/// periodic reload (see Task 14). "Active" excludes `completed`/`cancelled`
/// rows in `train_current_state` and `unresolved` rows in `tracked_trains`
/// -- there is nothing further for trust-consumer to do with either.
///
/// Lives here rather than in `crates/api/src/data/train_tracking.rs`
/// (where the brief's ingest-route return type is otherwise defined)
/// because `trust-consumer` (Task 14), which has no direct DB access,
/// deserializes this exact struct from the JSON `GET
/// /private/tracked-trains` returns — the same snake_case,
/// `Serialize + Deserialize`, no-`sqlx::FromRow` wire-type convention
/// already used by `TrainMovementEventMessage` above. `crates/api`'s
/// `list_active_tracked_trains` still queries Postgres directly; it maps
/// its result rows into this type rather than deriving `sqlx::FromRow` on
/// it, since `crates/common` has no `sqlx` dependency.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackedTrainRef {
    pub id: i64,
    pub service_date: chrono::NaiveDate,
    pub pin_origin_crs: String,
    pub pin_scheduled_departure: DateTime<Utc>,
    pub resolution_status: String,
    pub train_uid: Option<String>,
    pub train_id: Option<String>,
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

/// One resolved STANOX->CRS row, as `crates/schedule-reference` derives it
/// from a CIF SCHEDULE delivery's `TI`/`A` records and POSTs it to
/// `api`'s `/private/stanox-crs`, and as `trust-consumer` GETs the full
/// current table back. See
/// docs/superpowers/specs/2026-09-01-schedule-ingest-stanox-crs-table-design.md
/// Decision 2 for the schema and the disambiguation policy that produced
/// each row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StanoxCrsRecord {
    pub stanox: String,
    pub crs: String,
    pub tiploc: String,
    pub station_name: String,
    /// Which `schedule_feed_ingests.sequence` this row was last derived
    /// from -- provenance a live source benefits from that the static CSV
    /// never needed.
    pub source_sequence: i32,
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

/// Why a line's `sample_stats` is (or isn't) populated this cycle -- see
/// docs/superpowers/specs/2026-09-01-line-status-sample-coverage-design.md
/// Decision 2. Answers only the single-cycle "did any configured sample
/// station have live departure data to look at at all" question --
/// deliberately does NOT distinguish "genuinely quiet" from "structurally
/// under-sampled" (both collapse into `BelowThreshold`; that distinction
/// needs a pattern over many cycles, which is `line_status_daily_stats`'s
/// `sample_cycles`'s job, not a single cycle's -- see this plan's Not in
/// this plan section), and does NOT distinguish "no row" from "row present
/// but stale" (that is 2026-08-31-sample-data-availability-design.md's
/// still-open, separate `stationSamples`-on-`/public/freshness` proposal).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub enum SampleAvailability {
    /// None of this line's `sample_stations` had a `StationSample` row in
    /// this cycle's load at all (`samples.get(crs).is_none()` for every
    /// configured station). A real, currently-invisible signal -- a
    /// polling gap, a brand-new custom line never yet polled, or a
    /// widespread LDBWS outage.
    NoCoverage,
    /// At least one configured station had a row, but the operator-filtered
    /// relevant-departure count fell below `min_sample_size`.
    BelowThreshold { observed: usize, required: i64 },
    /// At or above threshold; the real `SampleStats` this cycle produced is
    /// also carried, unchanged, on `LineStatus.sample_stats` -- not
    /// duplicated a second time on the wire (see `crates/api/src/render.rs`,
    /// Task 4).
    Available(SampleStats),
}

impl SampleAvailability {
    /// `#[serde(default = ...)]` shim for deserializing `line_status_history`
    /// rows written before this field existed. `write_line_status` itself
    /// always writes a freshly computed value -- this default is a
    /// read-compat fallback, not a real "unknown" state.
    pub fn no_coverage_default() -> Self {
        SampleAvailability::NoCoverage
    }

    /// Extracts the `Available` payload, if any -- kept for the two call
    /// sites below that still want the old `Option<SampleStats>` shape
    /// internally, so `relevant_departures`/`stats_from_departures` don't
    /// need a second return channel added just for this.
    pub fn sample_stats(&self) -> Option<SampleStats> {
        match self {
            SampleAvailability::Available(stats) => Some(stats.clone()),
            _ => None,
        }
    }
}

/// Why `full_coverage_stats` is (or isn't) populated -- the full-coverage
/// analog of `SampleAvailability`, deliberately a SIBLING type, not a
/// reuse of it. `SampleAvailability::BelowThreshold` encodes an
/// LDBWS-specific station-count threshold with no honest full-coverage
/// analog: a dedicated TRUST/schedule consumer either has resolved this
/// line's service population for the current window, or it hasn't --
/// there is no "too few observed, raise the threshold" state once every
/// scheduled service is structurally in view. See
/// docs/superpowers/specs/2026-09-03-full-coverage-metrics-transition-design.md
/// Decision 1 for the full reasoning (this repo already forced one
/// structurally different producer -- the DLR pilot's per-trip resolution
/// warm-up -- into `SampleAvailability::BelowThreshold` once; this type
/// exists so a second, wider-blast-radius producer doesn't repeat that
/// compromise).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub enum FullCoverageAvailability {
    /// No full-coverage producer exists for this line at all -- either
    /// Option B has never shipped, or it has shipped but this specific
    /// line has not been enabled for it yet (see `LineDefinition::full_coverage_enabled`).
    /// The default, and the ONLY value this type can take until both are
    /// true.
    NotEnabled,
    /// Enabled for this line, but the consumer has not yet resolved a
    /// population for the current window this cycle -- a fresh
    /// deployment, a consumer outage, or (structurally different from
    /// LDBWS's per-cycle immediacy) TRUST resolution lag: a scheduled
    /// service with no Activation/Movement event seen yet, which this
    /// cycle cannot distinguish from "hasn't run yet."
    Pending,
    /// Resolved: every scheduled service on this line for the current
    /// window has been matched against real TRUST movement data. The
    /// payload is NOT duplicated a second time on the wire -- see
    /// `crates/api/src/render.rs`, mirroring `SampleAvailability::Available`'s
    /// own established precedent.
    Available(SampleStats),
}

impl FullCoverageAvailability {
    /// `#[serde(default = ...)]` shim for deserializing `LineStatus` rows
    /// written before this field existed -- the exact same read-compat
    /// role `SampleAvailability::no_coverage_default` plays for its
    /// sibling field. Every real writer computes a fresh value; this
    /// default is a read-compat fallback, not a real "unknown" state.
    pub fn not_enabled_default() -> Self {
        FullCoverageAvailability::NotEnabled
    }

    /// Extracts the `Available` payload, if any -- mirrors
    /// `SampleAvailability::sample_stats`'s own accessor shape.
    pub fn full_coverage_stats(&self) -> Option<SampleStats> {
        match self {
            FullCoverageAvailability::Available(stats) => Some(stats.clone()),
            _ => None,
        }
    }
}

/// One `full_coverage_line_stats` row -- the per-line counterpart of
/// `common::StationFullCoverageSample` (owned by a different plan). Posted
/// by `full-coverage-consumer` to `POST /private/full-coverage-stats`;
/// read by `aggregator` via a direct SQL query
/// (`crates/aggregator/src/queries.rs::load_full_coverage_line_stats`,
/// NOT over HTTP -- see
/// docs/superpowers/plans/2026-09-04-option-b-live-consumer-plan.md's
/// Correction 1). snake_case field names -- a private producer payload
/// between this app's own crates, not a public wire type (see that plan's
/// Global Constraints).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FullCoverageLineStatsRow {
    pub line_id: String,
    pub service_date: chrono::NaiveDate,
    pub availability: String, // "pending" | "available"
    pub stats: SampleStats,
}

#[cfg(test)]
mod full_coverage_availability_tests {
    use super::*;

    #[test]
    fn wire_tags_match_the_design_spec() {
        assert_eq!(
            serde_json::to_value(FullCoverageAvailability::NotEnabled).unwrap(),
            serde_json::json!({"state": "not-enabled"})
        );
        assert_eq!(
            serde_json::to_value(FullCoverageAvailability::Pending).unwrap(),
            serde_json::json!({"state": "pending"})
        );
    }

    #[test]
    fn available_case_tags_as_available() {
        // Unlike render.rs's hand-built wire JSON (which deliberately omits
        // the SampleStats payload -- see full_coverage_availability_json's
        // own test), this crate's raw internally-tagged serde
        // representation DOES flatten SampleStats's fields into the same
        // object alongside "state": "available" -- that's how serde's
        // `#[serde(tag = ...)]` works for a newtype variant, and matches
        // `SampleAvailability::Available`'s identical raw shape. The
        // no-duplication guarantee is a render.rs wire-shape property, not
        // a raw-serialization one -- asserted there instead.
        let stats = SampleStats {
            total: 5,
            delayed: 1,
            cancelled: 0,
            skipped: 0,
            avg_delay_minutes: 2.0,
        };
        let value = serde_json::to_value(FullCoverageAvailability::Available(stats)).unwrap();
        assert_eq!(value["state"], "available");
        assert_eq!(value["total"], 5);
    }

    #[test]
    fn not_enabled_default_returns_not_enabled() {
        assert_eq!(
            FullCoverageAvailability::not_enabled_default(),
            FullCoverageAvailability::NotEnabled
        );
    }

    #[test]
    fn full_coverage_stats_accessor_extracts_only_the_available_variant() {
        assert_eq!(
            FullCoverageAvailability::NotEnabled.full_coverage_stats(),
            None
        );
        assert_eq!(
            FullCoverageAvailability::Pending.full_coverage_stats(),
            None
        );
        let stats = SampleStats {
            total: 5,
            delayed: 1,
            cancelled: 0,
            skipped: 0,
            avg_delay_minutes: 2.0,
        };
        assert_eq!(
            FullCoverageAvailability::Available(stats.clone()).full_coverage_stats(),
            Some(stats)
        );
    }

    #[test]
    fn a_pre_change_line_status_json_with_no_full_coverage_keys_deserializes_with_defaults() {
        // Read-compat: a LineStatus JSON blob recorded before these two
        // fields existed (line_status_history rows, most concretely) must
        // still deserialize, with full_coverage_availability defaulting to
        // NotEnabled and full_coverage_stats to None -- the same
        // read-compat contract SampleAvailability::no_coverage_default
        // documents for its sibling field, exercised here for real against
        // the whole LineStatus struct.
        let json = serde_json::json!({
            "severity": 10,
            "reason": "Good Service",
            "validity": {"from_date": "2026-09-03T00:00:00Z", "to_date": null, "is_now": true},
            "data_quality": "knowledgebase",
            "sample_availability": {"state": "no-coverage"}
        });
        let status: LineStatus =
            serde_json::from_value(json).expect("pre-change LineStatus row must still parse");
        assert_eq!(status.full_coverage_stats, None);
        assert_eq!(
            status.full_coverage_availability,
            FullCoverageAvailability::NotEnabled
        );
    }
}

#[cfg(test)]
mod sample_availability_tests {
    use super::*;

    #[test]
    fn wire_tags_match_the_design_spec() {
        assert_eq!(
            serde_json::to_value(SampleAvailability::NoCoverage).unwrap(),
            serde_json::json!({"state": "no-coverage"})
        );
        assert_eq!(
            serde_json::to_value(SampleAvailability::BelowThreshold {
                observed: 2,
                required: 3
            })
            .unwrap(),
            serde_json::json!({"state": "below-threshold", "observed": 2, "required": 3})
        );
    }

    #[test]
    fn sample_stats_accessor_extracts_only_the_available_variant() {
        assert_eq!(SampleAvailability::NoCoverage.sample_stats(), None);
        assert_eq!(
            SampleAvailability::BelowThreshold {
                observed: 0,
                required: 3
            }
            .sample_stats(),
            None
        );
        let stats = SampleStats {
            total: 5,
            delayed: 1,
            cancelled: 0,
            skipped: 0,
            avg_delay_minutes: 2.0,
        };
        assert_eq!(
            SampleAvailability::Available(stats.clone()).sample_stats(),
            Some(stats)
        );
    }
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
            // A user-defined line is never a full-coverage rollout
            // candidate -- that catalogue is curated by this repo, not by
            // an end user picking arbitrary stations.
            full_coverage_enabled: false,
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
        toml::from_str("")
            .expect("Defaults must deserialize from an empty TOML table via serde_inline_default")
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

/// Shared delayed/cancelled/skipped/avg-delay arithmetic underlying every
/// `SampleStats` computation in this app. `is_skip` is a caller-supplied
/// predicate rather than a fixed membership check, because "skip" means
/// two different, both legitimate things depending on the caller: the
/// line-level caller means "skips a stop somewhere on the line's route"
/// (`line.stations`); the per-(station, operator) caller
/// (docs/superpowers/specs/2026-09-03-per-station-stats-design.md
/// Decision 4) means "skips calling at this specific station"
/// (`skipped_stations.contains(this_crs)`). Only ever evaluated for a
/// non-cancelled departure, matching every existing caller.
pub fn compute_sample_stats(
    departures: &[&StationDeparture],
    delay_threshold_minutes: i64,
    is_skip: impl Fn(&StationDeparture) -> bool,
) -> SampleStats {
    let total = departures.len();
    let cancelled = departures.iter().filter(|d| d.is_cancelled).count();
    let delayed = departures
        .iter()
        .filter(|d| !d.is_cancelled && d.delay_minutes as i64 >= delay_threshold_minutes)
        .count();
    let skipped = departures
        .iter()
        .filter(|d| !d.is_cancelled && is_skip(d))
        .count();
    let running: Vec<&&StationDeparture> = departures.iter().filter(|d| !d.is_cancelled).collect();
    let avg_delay_minutes = if running.is_empty() {
        0.0
    } else {
        running.iter().map(|d| d.delay_minutes as f64).sum::<f64>() / running.len() as f64
    };

    SampleStats {
        total,
        delayed,
        cancelled,
        skipped,
        avg_delay_minutes,
    }
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
mod compute_sample_stats_tests {
    use super::*;

    fn departure(
        delay_minutes: i32,
        is_cancelled: bool,
        skipped_stations: Vec<&str>,
    ) -> StationDeparture {
        StationDeparture {
            service_id: "svc".to_string(),
            operator: "SW".to_string(),
            destination_crs: "WAT".to_string(),
            scheduled: "10:00".to_string(),
            estimated: "10:00".to_string(),
            is_cancelled,
            delay_minutes,
            cancel_reason: if is_cancelled {
                Some("fault".to_string())
            } else {
                None
            },
            delay_reason: None,
            headcode: None,
            skipped_stations: skipped_stations.into_iter().map(str::to_string).collect(),
        }
    }

    #[test]
    fn empty_input_gives_zeroed_stats_not_nan() {
        let departures: Vec<&StationDeparture> = vec![];
        let stats = compute_sample_stats(&departures, 5, |_| false);
        assert_eq!(stats.total, 0);
        assert_eq!(stats.delayed, 0);
        assert_eq!(stats.cancelled, 0);
        assert_eq!(stats.skipped, 0);
        assert_eq!(stats.avg_delay_minutes, 0.0);
    }

    #[test]
    fn average_delay_excludes_cancelled_departures() {
        // A cancelled departure with a huge delay_minutes value; if it were
        // included in the average, the result would be much higher than 5.0.
        let on_time = departure(0, false, vec![]);
        let ten_late = departure(10, false, vec![]);
        let cancelled_with_huge_delay = departure(500, true, vec![]);
        let departures = vec![&on_time, &ten_late, &cancelled_with_huge_delay];
        let stats = compute_sample_stats(&departures, 5, |_| false);

        assert_eq!(stats.total, 3);
        assert_eq!(stats.cancelled, 1);
        assert_eq!(stats.delayed, 1); // only ten_late clears the threshold
        // average over the two running (non-cancelled) departures: (0 + 10) / 2
        assert_eq!(stats.avg_delay_minutes, 5.0);
    }

    #[test]
    fn cancelled_departure_matching_is_skip_does_not_count_as_skipped() {
        let cancelled_and_skipping = departure(0, true, vec!["EDB"]);
        let departures = vec![&cancelled_and_skipping];
        let stats = compute_sample_stats(&departures, 5, |d| {
            d.skipped_stations.iter().any(|c| c == "EDB")
        });

        assert_eq!(stats.cancelled, 1);
        assert_eq!(stats.skipped, 0);
    }

    #[test]
    fn non_cancelled_departure_matching_is_skip_counts_as_skipped() {
        let skipping = departure(0, false, vec!["EDB"]);
        let departures = vec![&skipping];
        let stats = compute_sample_stats(&departures, 5, |d| {
            d.skipped_stations.iter().any(|c| c == "EDB")
        });

        assert_eq!(stats.skipped, 1);
    }

    #[test]
    fn delay_exactly_at_threshold_counts_as_delayed() {
        let exactly_at_threshold = departure(5, false, vec![]);
        let departures = vec![&exactly_at_threshold];
        let stats = compute_sample_stats(&departures, 5, |_| false);

        assert_eq!(stats.delayed, 1);
    }

    #[test]
    fn delay_below_threshold_does_not_count_as_delayed() {
        let below_threshold = departure(4, false, vec![]);
        let departures = vec![&below_threshold];
        let stats = compute_sample_stats(&departures, 5, |_| false);

        assert_eq!(stats.delayed, 0);
    }
}

#[cfg(test)]
mod disruption_impact_type_tests {
    use super::*;

    #[test]
    fn a_pre_change_disruption_json_with_no_impact_type_key_deserializes_to_none() {
        let json = serde_json::json!({
            "category": "RealTime",
            "description": "Signal failure",
            "affected_stops": [],
            "affected_routes": [],
            "source": null
        });
        let disruption: Disruption =
            serde_json::from_value(json).expect("pre-change row must still parse");
        assert_eq!(disruption.impact_type, None);
    }

    #[test]
    fn impact_type_is_omitted_from_serialized_json_when_none() {
        let disruption = Disruption {
            category: "RealTime".to_string(),
            description: "Signal failure".to_string(),
            affected_stops: vec![],
            affected_routes: vec![],
            source: None,
            impact_type: None,
        };
        let value = serde_json::to_value(&disruption).unwrap();
        assert!(value.get("impact_type").is_none());
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
        assert_eq!(
            line.sample_stations,
            vec!["WOK".to_string(), "AON".to_string()]
        );
        assert!(line.match_keywords.is_empty());
        assert!(line.severity_overrides.is_empty());
        assert_eq!(line.headcode_prefixes, vec!["1P".to_string()]);
        assert_eq!(line.destination_crs_filter, vec!["AON".to_string()]);
        assert!(!line.full_coverage_enabled);
    }
}

#[cfg(test)]
mod tfl_severity_tests {
    use super::*;

    /// TfL's own `GET /Line/Meta/Severity` table, transcribed verbatim from
    /// a live fetch on 2026-08-22. The descriptions were identical for
    /// every mode this app ingests (tube, dlr, overground, elizabeth-line,
    /// tram) — checked all five, zero differences — which is why the
    /// mapping is a compile-time table here instead of a per-cycle request
    /// to that endpoint. If TfL extends or renumbers the scale, this test
    /// is what fails.
    const TFL_SEVERITY_TABLE: [(u8, &str); 21] = [
        (0, "Special Service"),
        (1, "Closed"),
        (2, "Suspended"),
        (3, "Part Suspended"),
        (4, "Planned Closure"),
        (5, "Part Closure"),
        (6, "Severe Delays"),
        (7, "Reduced Service"),
        (8, "Bus Service"),
        (9, "Minor Delays"),
        (10, "Good Service"),
        (11, "Part Closed"),
        (12, "Exit Only"),
        (13, "No Step Free Access"),
        (14, "Change of frequency"),
        (15, "Diverted"),
        (16, "Not Running"),
        (17, "Issues Reported"),
        (18, "No Issues"),
        (19, "Information"),
        (20, "Service Closed"),
    ];

    #[test]
    fn every_published_tfl_code_maps_to_a_severity() {
        for (code, description) in TFL_SEVERITY_TABLE {
            assert!(
                severity_from_tfl_code(code).is_some(),
                "TfL code {code} ({description}) has no mapping"
            );
        }
    }

    #[test]
    fn our_wording_matches_tfls_except_two_deliberate_rewordings() {
        for (code, tfl_description) in TFL_SEVERITY_TABLE {
            let ours = severity_from_tfl_code(code).unwrap().description();
            match code {
                // Pre-existing NR wording, unchanged by this feature: the
                // NR feed's equivalent is a rail replacement bus.
                8 => assert_eq!(ours, "Rail Replacement"),
                // Same words, our capitalisation.
                14 => assert_eq!(ours, "Change of Frequency"),
                _ => assert_eq!(ours, tfl_description, "code {code}"),
            }
        }
    }

    #[test]
    fn tfl_codes_above_14_do_not_collide_with_the_nr_extensions() {
        // The whole reason the new variants exist. Our 20 is the NR
        // extension `Recovering` and our 21 is `Diverted`; TfL's 20 is
        // "Service Closed" (which 13 of 20 lines were reporting at the time
        // of capture) and TfL's 15 is its Diverted. Mapping by raw number
        // would have shown "Recovering" all night, every night.
        assert_eq!(severity_from_tfl_code(20), Some(Severity::ServiceClosed));
        assert_ne!(severity_from_tfl_code(20), Some(Severity::Recovering));
        assert_eq!(severity_from_tfl_code(15), Some(Severity::Diverted));
        assert_eq!(Severity::Recovering as u8, 20);
        assert_eq!(Severity::Diverted as u8, 21);
    }

    #[test]
    fn an_unpublished_code_has_no_mapping() {
        // 21 is deliberately included: it is a valid discriminant on OUR
        // scale but not on TfL's, so a naive round-trip would "succeed".
        assert_eq!(severity_from_tfl_code(21), None);
        assert_eq!(severity_from_tfl_code(99), None);
    }

    #[test]
    fn service_closed_is_informational_and_not_running_is_severe() {
        // An overnight closure is the normal state of the Underground and
        // must not paint the network red; a service that is unexpectedly
        // absent must.
        assert_eq!(severity_rank(Severity::ServiceClosed), 1);
        assert_eq!(severity_rank(Severity::NotRunning), 4);
        assert_eq!(severity_rank(Severity::NoIssues), 0);
    }
}

#[cfg(test)]
mod tfl_nr_merge_tests {
    use super::*;

    #[test]
    fn elizabeth_line_tfl_id_maps_to_its_nr_counterpart() {
        assert_eq!(nr_line_id_for_tfl("tfl-elizabeth"), Some("elizabeth-line"));
    }

    #[test]
    fn elizabeth_line_nr_id_maps_back_to_its_tfl_counterpart() {
        assert_eq!(tfl_line_id_for_nr("elizabeth-line"), Some("tfl-elizabeth"));
    }

    #[test]
    fn overground_mildmay_tfl_id_maps_to_its_nr_counterpart() {
        assert_eq!(
            nr_line_id_for_tfl("tfl-mildmay"),
            Some("overground-mildmay")
        );
    }

    #[test]
    fn overground_mildmay_nr_id_maps_back_to_its_tfl_counterpart() {
        assert_eq!(
            tfl_line_id_for_nr("overground-mildmay"),
            Some("tfl-mildmay")
        );
    }

    #[test]
    fn every_overground_line_has_a_two_way_mapping() {
        // One assertion per line rather than per-line dedicated tests like
        // Elizabeth/Mildmay above -- this loop is what actually proves the
        // table is complete and the lookup functions are generic over all
        // six rows, matching this repo's existing test-coverage density for
        // table-driven mappings (see `defaults_tests`/`custom_line_tests`
        // above).
        for (tfl_id, nr_id) in [
            ("tfl-liberty", "overground-liberty"),
            ("tfl-lioness", "overground-lioness"),
            ("tfl-mildmay", "overground-mildmay"),
            ("tfl-suffragette", "overground-suffragette"),
            ("tfl-weaver", "overground-weaver"),
            ("tfl-windrush", "overground-windrush"),
        ] {
            assert_eq!(nr_line_id_for_tfl(tfl_id), Some(nr_id), "{tfl_id}");
            assert_eq!(tfl_line_id_for_nr(nr_id), Some(tfl_id), "{nr_id}");
        }
    }

    #[test]
    fn a_tfl_line_with_no_nr_counterpart_has_no_mapping() {
        // The overwhelming majority of TfL lines -- e.g. the Northern line,
        // which collides in *name* with an NR catalogue line but has no
        // shared-infrastructure NR counterpart the way Elizabeth line does.
        assert_eq!(nr_line_id_for_tfl("tfl-northern"), None);
    }

    #[test]
    fn an_nr_line_with_no_tfl_counterpart_has_no_mapping() {
        assert_eq!(tfl_line_id_for_nr("waterloo-main-line"), None);
    }
}
