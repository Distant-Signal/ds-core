//! Transit networks that are neither National Rail nor `TfL` (Manchester
//! Metrolink first): the ids, the stream payloads and the network list the
//! producer (`poller-transit-schedule`), the ingest-writer and the api share.
//! Design: docs/superpowers/specs/2026-10-09-transit-networks-design.md
//! (§7.2 the schemas, §8 the tables they land in).
//!
//! **Ids.** A network id and a line's local id are `[a-z0-9-]+`
//! ([`is_id`]); a line's public id is network-qualified, `metrolink:blue`
//! ([`QualifiedLineId`]). Routes carry the local id, bodies, pins and
//! notifications the qualified one (spec §11). Stop ids are `NaPTAN` ATCO
//! codes (`9400ZZMAPIC`, `9400ZZMAPIC1`), upper-case letters and digits
//! ([`is_stop_id`]).
//!
//! **Schemas** (one stream per network, `ds:ingest:transit-<network>`):
//!
//! | Schema | Body |
//! |---|---|
//! | `transit-timetable/1` | [`TimetablePart`]: one kind of rows of one feed version |
//! | `transit-timetable-commit/1` | [`TimetableCommit`]: the version's manifest, last in the item |
//! | `transit-situations/1` | [`TransitSituations`]: the network's whole current set (batch 4) |
//!
//! Golden fixtures of each body are in `crates/common/tests/fixtures/transit/`.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub mod status;

/// The networks compiled into this build (spec §5: adding a network is a
/// code change). The producer refuses any other `TRANSIT_NETWORK`, and the
/// writer pins each `ds:ingest:transit-<network>` stream to its network.
pub const NETWORKS: &[&str] = &["metrolink"];

/// The schema names (all at version 1).
pub mod schema {
    /// [`super::TimetablePart`].
    pub const TIMETABLE: &str = "transit-timetable";
    /// [`super::TimetableCommit`].
    pub const TIMETABLE_COMMIT: &str = "transit-timetable-commit";
    /// [`super::TransitSituations`].
    pub const SITUATIONS: &str = "transit-situations";
}

/// The longest network or local line id.
pub const MAX_ID_LEN: usize = 32;

/// The longest stop id (an ATCO code is at most 12 characters).
pub const MAX_STOP_ID_LEN: usize = 16;

/// Whether `id` is a valid network or local line id: 1 to [`MAX_ID_LEN`]
/// characters of `[a-z0-9-]`.
pub fn is_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= MAX_ID_LEN
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// Whether `id` is a valid stop id: 1 to [`MAX_STOP_ID_LEN`] upper-case
/// ASCII letters and digits (a `NaPTAN` ATCO code).
pub fn is_stop_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= MAX_STOP_ID_LEN
        && id
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
}

/// The `ingest_freshness` source of a network's timetable
/// (`transit-metrolink-timetable`).
pub fn timetable_freshness_source(network: &NetworkId) -> String {
    format!("transit-{network}-timetable")
}

/// The `ingest_freshness` source of a network's situations
/// (`transit-metrolink-situations`).
pub fn situations_freshness_source(network: &NetworkId) -> String {
    format!("transit-{network}-situations")
}

/// The service date `at` falls on: its London date. Every network compiled
/// in is in Great Britain (`Europe/London`, spec §6.2).
pub fn service_date(at: DateTime<Utc>) -> NaiveDate {
    at.with_timezone(&chrono_tz::Europe::London).date_naive()
}

/// An id that does not follow the grammar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidId(pub String);

impl fmt::Display for InvalidId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid transit id {:?}", self.0)
    }
}

impl std::error::Error for InvalidId {}

/// A network id (`metrolink`). See [`is_id`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NetworkId(String);

impl NetworkId {
    pub fn parse(id: &str) -> Result<Self, InvalidId> {
        if is_id(id) {
            Ok(Self(id.to_owned()))
        } else {
            Err(InvalidId(id.to_owned()))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether this build knows the network ([`NETWORKS`]).
    pub fn is_known(&self) -> bool {
        NETWORKS.contains(&self.0.as_str())
    }
}

impl fmt::Display for NetworkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for NetworkId {
    type Err = InvalidId;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl Serialize for NetworkId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for NetworkId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        Self::parse(&text).map_err(serde::de::Error::custom)
    }
}

/// A line's public id, `<network>:<line>` (`metrolink:blue`). Both halves
/// follow [`is_id`], so there is exactly one colon.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QualifiedLineId {
    network: NetworkId,
    line: String,
}

impl QualifiedLineId {
    /// From a network and a local line id.
    pub fn new(network: &NetworkId, line: &str) -> Result<Self, InvalidId> {
        if is_id(line) {
            Ok(Self {
                network: network.clone(),
                line: line.to_owned(),
            })
        } else {
            Err(InvalidId(line.to_owned()))
        }
    }

    /// From `<network>:<line>`.
    pub fn parse(id: &str) -> Result<Self, InvalidId> {
        let invalid = || InvalidId(id.to_owned());
        let (network, line) = id.split_once(':').ok_or_else(invalid)?;
        let network = NetworkId::parse(network).map_err(|_| invalid())?;
        Self::new(&network, line).map_err(|_| invalid())
    }

    pub fn network(&self) -> &NetworkId {
        &self.network
    }

    /// The local id (`blue`).
    pub fn line(&self) -> &str {
        &self.line
    }
}

impl fmt::Display for QualifiedLineId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.network, self.line)
    }
}

impl FromStr for QualifiedLineId {
    type Err = InvalidId;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl Serialize for QualifiedLineId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for QualifiedLineId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        Self::parse(&text).map_err(serde::de::Error::custom)
    }
}

// --- transit-timetable/1 ---------------------------------------------------

/// The kind of rows a [`TimetablePart`] carries, and the key of
/// [`TimetableCommit::parts`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimetablePartKind {
    Network,
    Lines,
    Stops,
    Patterns,
    Trips,
    ServiceDays,
    RailLinks,
}

impl TimetablePartKind {
    pub const ALL: [Self; 7] = [
        Self::Network,
        Self::Lines,
        Self::Stops,
        Self::Patterns,
        Self::Trips,
        Self::ServiceDays,
        Self::RailLinks,
    ];

    /// The wire name (`service_days`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Network => "network",
            Self::Lines => "lines",
            Self::Stops => "stops",
            Self::Patterns => "patterns",
            Self::Trips => "trips",
            Self::ServiceDays => "service_days",
            Self::RailLinks => "rail_links",
        }
    }
}

impl fmt::Display for TimetablePartKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One `transit-timetable/1` entry: rows of one kind of one feed version.
/// The envelope's batch part (`<produced_at>/<kind>`, part `i` of `n`)
/// numbers it within its kind; the commit lists `n` per kind.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimetablePart {
    pub network: NetworkId,
    /// `<source feed_version>-<first 12 hex of the content SHA-256>` (spec
    /// §7.2), at most [`MAX_FEED_VERSION_LEN`] characters.
    pub feed_version: String,
    #[serde(flatten)]
    pub rows: TimetableRows,
}

/// The longest `feed_version`.
pub const MAX_FEED_VERSION_LEN: usize = 64;

/// The rows of a [`TimetablePart`], tagged by kind:
/// `{"kind": "trips", "rows": [...]}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "rows", rename_all = "snake_case")]
pub enum TimetableRows {
    /// Exactly one row.
    Network(Vec<TransitNetworkRow>),
    Lines(Vec<TransitLine>),
    Stops(Vec<TransitStop>),
    Patterns(Vec<TransitPattern>),
    Trips(Vec<TransitTrip>),
    ServiceDays(Vec<TransitServiceDays>),
    RailLinks(Vec<TransitRailLink>),
}

impl TimetableRows {
    pub fn kind(&self) -> TimetablePartKind {
        match self {
            Self::Network(_) => TimetablePartKind::Network,
            Self::Lines(_) => TimetablePartKind::Lines,
            Self::Stops(_) => TimetablePartKind::Stops,
            Self::Patterns(_) => TimetablePartKind::Patterns,
            Self::Trips(_) => TimetablePartKind::Trips,
            Self::ServiceDays(_) => TimetablePartKind::ServiceDays,
            Self::RailLinks(_) => TimetablePartKind::RailLinks,
        }
    }

    /// The number of rows (for the writer's row counts).
    pub fn len(&self) -> usize {
        match self {
            Self::Network(rows) => rows.len(),
            Self::Lines(rows) => rows.len(),
            Self::Stops(rows) => rows.len(),
            Self::Patterns(rows) => rows.len(),
            Self::Trips(rows) => rows.len(),
            Self::ServiceDays(rows) => rows.len(),
            Self::RailLinks(rows) => rows.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The network's own row: what `/public/transit/networks` shows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitNetworkRow {
    pub name: String,
    /// Display only (`tram`); NOT a `SUPPORTED_MODES` value (D4).
    pub mode: String,
    /// IANA zone of the timetable's times (`Europe/London`).
    pub timezone: String,
    pub attribution: TransitAttribution,
}

/// The licence notice a network's data needs (spec §12.3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitAttribution {
    /// SPDX-style id (`OGL-UK-3.0`).
    pub licence: String,
    pub text: String,
}

/// A GTFS direction (`direction_id`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    /// `direction_id` 0.
    Outbound,
    /// `direction_id` 1.
    Inbound,
}

impl Direction {
    /// The GTFS `direction_id`, as stored.
    pub fn id(self) -> i16 {
        match self {
            Self::Outbound => 0,
            Self::Inbound => 1,
        }
    }

    pub fn from_id(id: i16) -> Option<Self> {
        match id {
            0 => Some(Self::Outbound),
            1 => Some(Self::Inbound),
            _ => None,
        }
    }
}

/// A line (spec §8.2 `transit_lines`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitLine {
    /// The local id ([`is_id`]): `blue`.
    pub line_id: String,
    pub name: String,
    /// `#rrggbb`.
    pub colour: Option<String>,
    pub sort_order: i32,
    /// Station order per direction (spec §6.2 step 10).
    pub directions: Vec<TransitLineDirection>,
}

/// One direction of a line: its stations in order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitLineDirection {
    pub direction: Direction,
    /// The most common headsign, if any.
    pub headsign: Option<String>,
    /// Station ids in calling order.
    pub stations: Vec<String>,
}

/// A stop's kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopKind {
    /// The `NaPTAN` `MET` access node (`9400ZZMAPIC`): what a board is for.
    Station,
    /// A `NaPTAN` `PLT` node (`9400ZZMAPIC1`): what a pattern calls at.
    Platform,
}

impl StopKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Station => "station",
            Self::Platform => "platform",
        }
    }
}

/// A station or platform (spec §8.2 `transit_stops`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransitStop {
    pub stop_id: String,
    pub kind: StopKind,
    /// A platform's station.
    pub parent_stop_id: Option<String>,
    pub name: String,
    /// A platform's `NaPTAN` indicator (`To Ashton`).
    pub indicator: Option<String>,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    /// GTFS `wheelchair_boarding`: `None` when unknown.
    pub wheelchair: Option<bool>,
}

/// A stop pattern: a line, a direction and the ordered calls relative to
/// the first departure (spec §6.2 step 9).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitPattern {
    pub pattern_id: String,
    /// `None` for a replacement bus's pattern.
    pub line_id: Option<String>,
    pub direction: Direction,
    pub headsign: Option<String>,
    pub stops: Vec<TransitPatternStop>,
}

/// One call of a [`TransitPattern`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitPatternStop {
    /// The platform.
    pub stop_id: String,
    /// Seconds after the trip's first departure.
    pub arr_offset_s: i32,
    pub dep_offset_s: i32,
    /// GTFS `pickup_type` (0 regular, 1 none, 2 phone, 3 ask the driver).
    pub pickup: u8,
    /// GTFS `drop_off_type`, as `pickup`.
    pub drop_off: u8,
}

/// The largest GTFS pickup or drop-off type.
pub const MAX_PICKUP_DROP_OFF: u8 = 3;

/// A trip (spec §8.2 `transit_trips`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitTrip {
    pub trip_id: String,
    /// `None` for a replacement bus (Q3: on boards as a bus, not in line
    /// timetables).
    pub line_id: Option<String>,
    pub pattern_id: String,
    pub service_id: String,
    /// Seconds after the service day's local midnight (GTFS noon minus 12
    /// h); may exceed 86,400.
    pub start_s: i32,
    pub headsign: Option<String>,
    pub replacement_bus: bool,
}

/// A service's dates within the stored horizon (spec §8.2
/// `transit_service_days`, one row per date).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitServiceDays {
    pub service_id: String,
    pub dates: Vec<NaiveDate>,
}

/// How a transit stop relates to a National Rail station.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RailLinkKind {
    /// The same complex.
    Interchange,
    /// A short walk (no minutes, Q10).
    Nearby,
}

impl RailLinkKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Interchange => "interchange",
            Self::Nearby => "nearby",
        }
    }
}

/// A link between a transit station and a National Rail station (spec §8.4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitRailLink {
    pub stop_id: String,
    pub crs: String,
    pub tiploc: String,
    pub kind: RailLinkKind,
    pub note: Option<String>,
}

// --- transit-timetable-commit/1 -------------------------------------------

/// The last entry of a timetable item: the feed version's manifest (spec
/// §7.2). The writer activates the version only if it staged exactly
/// `parts` (spec §9.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimetableCommit {
    pub network: NetworkId,
    pub feed_version: String,
    /// SHA-256 of the canonical model, lower-case hex.
    pub content_sha256: String,
    /// Parts per kind.
    pub parts: BTreeMap<TimetablePartKind, u32>,
    pub counts: TimetableCounts,
    pub horizon: Horizon,
    pub source: TimetableSource,
}

/// Row counts of a feed version (the producer's feed guard compares them
/// with the last commit's).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TimetableCounts {
    pub lines: u32,
    pub stops: u32,
    pub patterns: u32,
    pub trips: u32,
    /// Service dates, summed over services.
    pub service_days: u32,
}

/// The stored service dates, inclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Horizon {
    pub from: NaiveDate,
    pub to: NaiveDate,
}

/// Where a feed version came from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimetableSource {
    /// `bods-gtfs` or `tfgm-gtfs` (the fallback).
    pub url_kind: String,
    /// The source's own version (`feed_info.txt`), if it has one.
    pub feed_version: Option<String>,
    pub published_at: Option<DateTime<Utc>>,
    /// The operator's refresh request this commit answers
    /// (`TRANSIT_TIMETABLE_REFRESH`, chart
    /// `pollerTransitSchedule.networks.<n>.timetableRefresh`), if it was
    /// forced. The producer reads it back from the stream at start, so a
    /// request is answered once. Absent from every other commit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh: Option<String>,
}

/// The longest `TRANSIT_TIMETABLE_REFRESH` token.
pub const MAX_REFRESH_TOKEN_LEN: usize = 64;

/// Whether `token` is a valid refresh request: 1 to
/// [`MAX_REFRESH_TOKEN_LEN`] of `[A-Za-z0-9._:-]` (a date, a counter).
pub fn is_refresh_token(token: &str) -> bool {
    !token.is_empty()
        && token.len() <= MAX_REFRESH_TOKEN_LEN
        && token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'))
}

// --- transit-situations/1 (batch 4) ----------------------------------------

/// A network's whole current set of situations (spec §7.2, §10.1): a
/// situation missing from it has ended.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransitSituations {
    pub network: NetworkId,
    /// Sorted by `situation_id`.
    pub situations: Vec<TransitSituation>,
}

/// A normalised SIRI-SX `PtSituationElement` (spec §8.3, §10.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransitSituation {
    /// SIRI `SituationNumber`.
    pub situation_id: String,
    pub version: Option<i32>,
    /// SIRI `Progress` (`open`, `closed`).
    pub progress: String,
    pub planned: bool,
    /// `equipment:liftFailure`, `miscellaneous:…`.
    pub reason: Option<String>,
    pub summary: String,
    pub description: Option<String>,
    pub info_url: Option<String>,
    pub validity: Vec<Period>,
    pub publication: Vec<Period>,
    pub consequences: Vec<TransitConsequence>,
    /// Mapped local line ids over every consequence.
    pub line_ids: Vec<String>,
    /// Station ids over every consequence (platform codes folded).
    pub stop_ids: Vec<String>,
    pub scope: SituationScope,
}

/// A time window; `end: None` is open-ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Period {
    pub start: DateTime<Utc>,
    pub end: Option<DateTime<Utc>>,
}

/// One SIRI `Consequence`, normalised.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitConsequence {
    /// SIRI `Condition` (`suspended`, `delayed`, …).
    pub condition: Option<String>,
    /// SIRI `Severity` (`slight`, `severe`, …).
    pub severity: Option<String>,
    /// `AllLines`.
    pub all_lines: bool,
    /// Mapped local line ids.
    pub line_ids: Vec<String>,
    /// `LineRef`s with no alias in the network config, kept raw.
    pub unmapped_line_refs: Vec<String>,
    pub stop_ids: Vec<String>,
    pub advice: Option<String>,
}

/// What a situation affects (spec §10.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SituationScope {
    Line,
    Stop,
    Network,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metrolink() -> NetworkId {
        NetworkId::parse("metrolink").unwrap()
    }

    #[test]
    fn ids_follow_the_grammar() {
        assert!(is_id("metrolink"));
        assert!(is_id("net-2"));
        assert!(!is_id(""));
        assert!(!is_id("Metrolink"));
        assert!(!is_id("metro_link"));
        assert!(!is_id(&"a".repeat(MAX_ID_LEN + 1)));
        assert!(is_stop_id("9400ZZMAPIC1"));
        assert!(!is_stop_id("9400zzmapic"));
        assert!(!is_stop_id(""));
        assert!(!is_stop_id("9400 ZZ"));
        assert!(metrolink().is_known());
        assert!(!NetworkId::parse("net").unwrap().is_known());
    }

    #[test]
    fn qualified_line_ids_parse_and_print() {
        let id = QualifiedLineId::parse("metrolink:blue").unwrap();
        assert_eq!(id.network(), &metrolink());
        assert_eq!(id.line(), "blue");
        assert_eq!(id.to_string(), "metrolink:blue");
        assert_eq!(QualifiedLineId::new(&metrolink(), "blue").unwrap(), id);
        for bad in [
            "Metrolink:blue",
            "metrolink",
            "a:b:c",
            ":blue",
            "metrolink:",
            "metrolink:Blue",
        ] {
            assert!(QualifiedLineId::parse(bad).is_err(), "{bad}");
        }
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, r#""metrolink:blue""#);
        assert_eq!(serde_json::from_str::<QualifiedLineId>(&json).unwrap(), id);
        assert!(serde_json::from_str::<QualifiedLineId>(r#""a:b:c""#).is_err());
        assert!(serde_json::from_str::<NetworkId>(r#""Metrolink""#).is_err());
    }

    #[test]
    fn freshness_sources_are_per_network() {
        assert_eq!(
            timetable_freshness_source(&metrolink()),
            "transit-metrolink-timetable"
        );
        assert_eq!(
            situations_freshness_source(&metrolink()),
            "transit-metrolink-situations"
        );
    }

    #[test]
    fn a_part_is_tagged_by_kind() {
        let part = TimetablePart {
            network: metrolink(),
            feed_version: "v-1".into(),
            rows: TimetableRows::ServiceDays(vec![TransitServiceDays {
                service_id: "s1".into(),
                dates: vec![NaiveDate::from_ymd_opt(2026, 10, 14).unwrap()],
            }]),
        };
        let json = serde_json::to_value(&part).unwrap();
        assert_eq!(json["kind"], "service_days");
        assert_eq!(json["rows"][0]["dates"][0], "2026-10-14");
        let back: TimetablePart = serde_json::from_value(json).unwrap();
        assert_eq!(back, part);
        assert_eq!(back.rows.kind(), TimetablePartKind::ServiceDays);
        assert_eq!(back.rows.len(), 1);
        for kind in TimetablePartKind::ALL {
            assert_eq!(
                serde_json::to_value(kind).unwrap(),
                serde_json::Value::String(kind.as_str().to_owned())
            );
        }
    }

    #[test]
    fn a_commit_may_name_the_refresh_it_answers() {
        // A commit from before the field (the fixture) reads as no refresh.
        let mut commit: TimetableCommit = serde_json::from_str(include_str!(
            "../tests/fixtures/transit/timetable-commit.json"
        ))
        .unwrap();
        assert_eq!(commit.source.refresh, None);
        assert!(
            serde_json::to_value(&commit).unwrap()["source"]
                .get("refresh")
                .is_none()
        );
        commit.source.refresh = Some("2026-10-15".to_owned());
        let json = serde_json::to_value(&commit).unwrap();
        assert_eq!(json["source"]["refresh"], "2026-10-15");
        assert_eq!(
            serde_json::from_value::<TimetableCommit>(json).unwrap(),
            commit
        );
        for ok in ["2026-10-15", "1", "a.b_c:d-e"] {
            assert!(is_refresh_token(ok), "{ok}");
        }
        for bad in [
            "",
            "a b",
            "x/y",
            "é",
            &"a".repeat(MAX_REFRESH_TOKEN_LEN + 1),
        ] {
            assert!(!is_refresh_token(bad), "{bad}");
        }
    }

    #[test]
    fn directions_round_trip_their_gtfs_ids() {
        for d in [Direction::Outbound, Direction::Inbound] {
            assert_eq!(Direction::from_id(d.id()), Some(d));
        }
        assert_eq!(Direction::from_id(2), None);
    }
}
