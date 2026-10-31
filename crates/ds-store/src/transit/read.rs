//! The read side of the transit tables (transit spec §11, plan 3.1): what
//! the api's `/public/transit/...` routes serve. Every query reads the
//! network's active feed version (`transit_networks.active_feed_version`),
//! so a reader never sees half a feed, and a version's activation switches
//! every result at once.
//!
//! Times: a trip's `start_s` plus a call's offset is seconds after its
//! service day's local midnight (past 86,400 for a trip running past
//! midnight). A board for date D lists the departures that fall on D: D's
//! own before 24:00 and D-1's from 24:00. Clock times are London's (every
//! network compiled in is in Great Britain); the hour a clock change adds
//! or removes is not corrected for.
//!
//! The result types serialize in the api's JSON shape (camelCase).

use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, Result};
use chrono::{DateTime, Days, NaiveDate, TimeZone, Utc};
use chrono_tz::Europe::London;
use serde::Serialize;
use sqlx::PgPool;

const DAY_S: i32 = 86_400;

/// `list_networks`' row.
type NetworkRow = (
    String,
    String,
    String,
    serde_json::Value,
    String,
    Option<NaiveDate>,
    Option<NaiveDate>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
);
/// A rail link row: stop, CRS, TIPLOC, kind, note, the station's name.
type RailLinkRow = (
    String,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
);
/// `stations`' row: id, name, lat, lon, feed version, member stops (the
/// station and its platforms).
type StationRow = (
    String,
    String,
    Option<f64>,
    Option<f64>,
    String,
    Vec<String>,
);
/// `stop_departures`' row: seconds after midnight, trip, platform, indicator, line id, line
/// name, colour, replacement bus, headsign, last stop's name, line mode.
type BoardSqlRow = (
    i32,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    bool,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// A network as `/public/transit/networks` lists it.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkSummary {
    pub id: String,
    pub name: String,
    pub mode: String,
    pub attribution: serde_json::Value,
    pub feed_version: String,
    pub horizon_from: Option<NaiveDate>,
    pub horizon_to: Option<NaiveDate>,
    /// The timetable's freshness marker (its last applied commit).
    pub timetable_as_of: Option<DateTime<Utc>>,
    /// The situations' marker (batch 4; `None` until then).
    pub situations_as_of: Option<DateTime<Utc>>,
}

/// A line.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LineSummary {
    /// The public id, `<network>:<line>`.
    pub id: String,
    /// The local id, the route's path segment.
    pub line_id: String,
    pub name: String,
    pub colour: Option<String>,
    /// The line's own mode (`tube`, `dlr`, `tram`), else its network's
    /// (the `TfL` design §6).
    pub mode: String,
    /// The `line_status` id its status is read from, and that a pin of it
    /// stores: its own public id, or for a network whose status is `TfL`'s
    /// (`common::transit::status_from_tfl`) `TfL`'s line (`tfl-victoria`).
    pub status_line_id: String,
}

/// A station, as a list entry or a reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StationRef {
    pub id: String,
    pub name: String,
}

/// One direction of a line, its stations in order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LineDirection {
    /// `outbound` or `inbound`.
    pub direction: String,
    pub headsign: Option<String>,
    pub stations: Vec<StationRef>,
}

/// `/public/transit/{network}/lines/{line}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LineDetail {
    #[serde(flatten)]
    pub line: LineSummary,
    pub directions: Vec<LineDirection>,
    /// The line's branches (the `TfL` design §8.3): the station runs its
    /// trips make, each direction's in order of their share of its trips.
    pub branches: Vec<LineBranch>,
}

/// One branch of a line: a run of stations that at least
/// [`BRANCH_MIN_SHARE_PERCENT`] of a direction's trips make (a trip making
/// part of a longer run counts for the longer one).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LineBranch {
    /// `<direction>-<n>`, stable for a feed version: the timetable's
    /// `branch`.
    pub id: String,
    /// `outbound` or `inbound`.
    pub direction: String,
    /// `Edgware to Morden`, with `via <station>` when two branches share
    /// their ends.
    pub name: String,
    pub stations: Vec<StationRef>,
    /// Its share of the direction's trips, whole percent.
    pub share_percent: u32,
}

/// A branch carrying a smaller share of its direction's trips is left out
/// (an odd depot run, not a branch).
const BRANCH_MIN_SHARE_PERCENT: u32 = 5;

/// One pattern of a line: its direction, its stations in order (a
/// platform as its station, repeats in a row once) and its trips.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LinePattern {
    pattern_id: String,
    direction: i16,
    stations: Vec<String>,
    trips: i64,
}

/// Whether `needle` is a run of `hay`'s stations, in order.
fn contains_run(hay: &[String], needle: &[String]) -> bool {
    needle.is_empty() || hay.windows(needle.len()).any(|window| window == needle)
}

/// A line's patterns (the trips' stopping patterns), with their trips
/// (replacement buses left out).
async fn line_patterns(
    pool: &PgPool,
    network: &str,
    feed_version: &str,
    line: &str,
) -> Result<Vec<LinePattern>> {
    let rows: Vec<(String, i16, Vec<String>, i64)> = sqlx::query_as(
        "SELECT p.pattern_id, p.direction, \
                array_agg(COALESCE(pl.parent_stop_id, pl.stop_id) ORDER BY ps.seq), \
                (SELECT count(*) FROM transit_trips t WHERE t.network = p.network \
                   AND t.feed_version = p.feed_version AND t.pattern_id = p.pattern_id \
                   AND NOT t.replacement_bus) \
         FROM transit_patterns p \
         JOIN transit_pattern_stops ps ON ps.network = p.network AND ps.feed_version = p.feed_version \
              AND ps.pattern_id = p.pattern_id \
         JOIN transit_stops pl ON pl.network = ps.network AND pl.feed_version = ps.feed_version \
              AND pl.stop_id = ps.stop_id \
         WHERE p.network = $1 AND p.feed_version = $2 AND p.line_id = $3 \
         GROUP BY p.network, p.feed_version, p.pattern_id, p.direction",
    )
    .bind(network)
    .bind(feed_version)
    .bind(line)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(pattern_id, direction, mut stations, trips)| {
            stations.dedup();
            LinePattern {
                pattern_id,
                direction,
                stations,
                trips,
            }
        })
        .collect())
}

/// The branches of `patterns` (see [`LineBranch`]): per direction, the
/// station runs longest first, a run inside a longer one counted for that
/// one, those under [`BRANCH_MIN_SHARE_PERCENT`] left out, the rest by
/// share. Named by their ends, with `via` the first station that tells two
/// with the same ends apart.
fn branches_of(patterns: &[LinePattern], names: &HashMap<String, String>) -> Vec<LineBranch> {
    let name_of = |id: &String| names.get(id).cloned().unwrap_or_else(|| id.clone());
    let mut out = Vec::new();
    for direction in [0_i16, 1] {
        let mut runs: BTreeMap<&[String], i64> = BTreeMap::new();
        for pattern in patterns.iter().filter(|p| p.direction == direction) {
            *runs.entry(pattern.stations.as_slice()).or_default() += pattern.trips;
        }
        let total: i64 = runs.values().sum();
        if total == 0 {
            continue;
        }
        let mut ordered: Vec<(&[String], i64)> = runs.into_iter().collect();
        // Longest first; then the busier; then by content, for a stable order.
        ordered.sort_by(|a, b| {
            b.0.len()
                .cmp(&a.0.len())
                .then(b.1.cmp(&a.1))
                .then(a.0.cmp(b.0))
        });
        let mut kept: Vec<(&[String], i64)> = Vec::new();
        for (run, trips) in ordered {
            match kept
                .iter_mut()
                .find(|(longer, _)| contains_run(longer, run))
            {
                Some(longer) => longer.1 += trips,
                None => kept.push((run, trips)),
            }
        }
        let share = |trips: i64| u32::try_from(trips * 100 / total).unwrap_or(0);
        kept.retain(|(_, trips)| share(*trips) >= BRANCH_MIN_SHARE_PERCENT);
        kept.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
        let ends: Vec<String> = kept
            .iter()
            .map(|(run, _)| match (run.first(), run.last()) {
                (Some(first), Some(last)) => format!("{} to {}", name_of(first), name_of(last)),
                _ => String::new(),
            })
            .collect();
        for (n, (run, trips)) in kept.iter().enumerate() {
            let mut name = ends[n].clone();
            let twin = kept
                .iter()
                .enumerate()
                .find(|(m, _)| *m != n && ends[*m] == ends[n]);
            if let Some((_, (other, _))) = twin
                && let Some(via) = run.iter().find(|s| !other.contains(s))
            {
                name = format!("{name} via {}", name_of(via));
            }
            let direction_name = if direction == 1 {
                "inbound"
            } else {
                "outbound"
            };
            out.push(LineBranch {
                id: format!("{direction_name}-{}", n + 1),
                direction: direction_name.to_owned(),
                name,
                stations: run
                    .iter()
                    .map(|id| StationRef {
                        id: id.clone(),
                        name: name_of(id),
                    })
                    .collect(),
                share_percent: share(*trips),
            });
        }
    }
    out
}

/// A link to a National Rail station.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RailLink {
    pub crs: String,
    pub tiploc: String,
    /// `interchange` or `nearby`.
    pub kind: String,
    pub note: Option<String>,
    /// The National Rail station's name (`Manchester Piccadilly`), when
    /// the station list has the CRS.
    pub station_name: Option<String>,
}

/// A transit stop linked to a National Rail station
/// (`/public/transit/rail-links?crs=`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StationTransitLink {
    pub network: String,
    pub network_name: String,
    /// The transit station's id and name.
    pub stop_id: String,
    pub stop_name: String,
    /// `interchange` or `nearby`.
    pub kind: String,
    pub note: Option<String>,
}

/// The transit stops of `networks` linked to the National Rail station
/// `crs` (spec §12.1), from each network's active timetable, by network
/// in `networks` order then stop name.
pub async fn rail_links_for_crs(
    pool: &PgPool,
    networks: &[String],
    crs: &str,
) -> Result<Vec<StationTransitLink>> {
    let rows: Vec<(String, String, String, String, String, Option<String>)> = sqlx::query_as(
        "SELECT l.network, n.name, l.stop_id, s.name, l.kind, l.note \
         FROM transit_rail_links l \
         JOIN transit_networks n ON n.network = l.network \
         JOIN transit_stops s ON s.network = l.network AND s.feed_version = n.active_feed_version \
              AND s.stop_id = l.stop_id \
         WHERE l.crs = $1 AND l.network = ANY($2) \
         ORDER BY array_position($2, l.network), s.name, l.stop_id",
    )
    .bind(crs)
    .bind(networks)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(network, network_name, stop_id, stop_name, kind, note)| StationTransitLink {
                network,
                network_name,
                stop_id,
                stop_name,
                kind,
                note,
            },
        )
        .collect())
}

/// A station with the lines calling there and its rail links.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StopSummary {
    pub id: String,
    pub name: String,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    /// Public line ids, in line order.
    pub lines: Vec<String>,
    pub rail_links: Vec<RailLink>,
}

/// A platform of a station.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Platform {
    pub id: String,
    pub indicator: Option<String>,
}

/// `/public/transit/{network}/stops/{stop}`.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StopDetail {
    #[serde(flatten)]
    pub stop: StopSummary,
    pub platforms: Vec<Platform>,
}

/// One scheduled departure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BoardRow {
    /// London local time, RFC 3339 with its offset.
    pub scheduled_departure: String,
    pub trip_id: String,
    /// `None` for a replacement bus.
    pub line: Option<LineSummary>,
    pub destination: String,
    pub platform_id: String,
    pub platform: Option<String>,
    pub replacement_bus: bool,
}

/// A scheduled board page (spec §11: never live).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Board {
    pub date: NaiveDate,
    pub departures: Vec<BoardRow>,
    /// Pass back as `cursor` for the next page.
    pub next_cursor: Option<String>,
    /// The date is outside the stored horizon: no rows, not "no service".
    pub outside_horizon: bool,
}

/// One trip of a line timetable: its time at each station (`None` where it
/// does not call).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TimetableTrip {
    pub trip_id: String,
    pub headsign: Option<String>,
    /// `HH:MM` London time, one per station.
    pub times: Vec<Option<String>>,
}

/// `/public/transit/{network}/lines/{line}/timetable`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LineTimetable {
    pub date: NaiveDate,
    pub direction: String,
    /// The branch shown (`LineBranch::id`), when one was asked for and is
    /// the direction's: its stations are the rows, and only trips that
    /// stay on it are listed.
    pub branch: Option<String>,
    pub stations: Vec<StationRef>,
    pub trips: Vec<TimetableTrip>,
    pub outside_horizon: bool,
}

/// The timetable's trips from its calls (trip, start, headsign, station,
/// time; in trip and call order), one column per station of `stations`:
/// each trip's first time at each.
fn timetable_grid(
    rows: Vec<(String, i32, Option<String>, String, i32)>,
    stations: &[StationRef],
) -> Vec<TimetableTrip> {
    let column: HashMap<&str, usize> = stations
        .iter()
        .enumerate()
        .map(|(i, s)| (s.id.as_str(), i))
        .collect();
    let mut trips: Vec<TimetableTrip> = Vec::new();
    for (trip_id, _, headsign, station, at) in rows {
        if trips.last().is_none_or(|t| t.trip_id != trip_id) {
            trips.push(TimetableTrip {
                trip_id: trip_id.clone(),
                headsign,
                times: vec![None; stations.len()],
            });
        }
        if let (Some(trip), Some(&i)) = (trips.last_mut(), column.get(station.as_str()))
            && trip.times[i].is_none()
        {
            trip.times[i] = Some(clock(at));
        }
    }
    trips
}

/// What a line timetable shows: one direction (and branch) of one date,
/// trips starting in `[from_s, to_s)`.
#[derive(Debug, Clone, Copy)]
pub struct TimetableWindow<'a> {
    pub date: NaiveDate,
    /// `outbound` or `inbound`.
    pub direction: &'a str,
    pub from_s: i32,
    pub to_s: i32,
    /// A `LineBranch::id`; an unknown one or another direction's shows the
    /// whole direction.
    pub branch: Option<&'a str>,
}

/// A stop's first and last departure of the day on one line, one way
/// (the `TfL` design §8.4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StopFirstLast {
    pub line: LineSummary,
    /// `outbound` or `inbound`.
    pub direction: String,
    /// Where that direction goes: its headsign, else its last station.
    pub towards: Option<String>,
    /// `HH:MM` London time.
    pub first: String,
    pub last: String,
}

/// `station`'s first and last departures on `date` per line and
/// direction (trips that start there or call there, not those ending
/// there; replacement buses left out), in the network's line order. `None`
/// for an unknown station; empty outside the horizon.
pub async fn stop_first_last(
    pool: &PgPool,
    network: &str,
    station: &str,
    date: NaiveDate,
) -> Result<Option<Vec<StopFirstLast>>> {
    let Some(act) = active(pool, network).await? else {
        return Ok(None);
    };
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM transit_stops WHERE network = $1 AND feed_version = $2 \
         AND stop_id = $3 AND kind = 'station')",
    )
    .bind(network)
    .bind(&act.feed_version)
    .bind(station)
    .fetch_one(pool)
    .await?;
    if !exists {
        return Ok(None);
    }
    if !act.covers(date) {
        return Ok(Some(Vec::new()));
    }
    let rows: Vec<(String, i16, i32, i32)> = sqlx::query_as(
        "SELECT t.line_id, p.direction, min(t.start_s + ps.dep_offset_s), \
                max(t.start_s + ps.dep_offset_s) \
         FROM transit_stops pl \
         JOIN transit_pattern_stops ps ON ps.network = pl.network AND ps.feed_version = pl.feed_version \
              AND ps.stop_id = pl.stop_id \
         JOIN transit_patterns p ON p.network = ps.network AND p.feed_version = ps.feed_version \
              AND p.pattern_id = ps.pattern_id \
         JOIN transit_trips t ON t.network = ps.network AND t.feed_version = ps.feed_version \
              AND t.pattern_id = ps.pattern_id \
         JOIN transit_service_days d ON d.network = t.network AND d.feed_version = t.feed_version \
              AND d.service_id = t.service_id AND d.service_date = $4 \
         WHERE pl.network = $1 AND pl.feed_version = $2 AND (pl.stop_id = $3 OR pl.parent_stop_id = $3) \
           AND ps.pickup <> 1 AND t.line_id IS NOT NULL AND NOT t.replacement_bus \
           AND ps.seq < (SELECT max(x.seq) FROM transit_pattern_stops x WHERE x.network = ps.network \
                AND x.feed_version = ps.feed_version AND x.pattern_id = ps.pattern_id) \
         GROUP BY t.line_id, p.direction",
    )
    .bind(network)
    .bind(&act.feed_version)
    .bind(station)
    .bind(date)
    .fetch_all(pool)
    .await?;
    let lines = list_lines(pool, network).await?;
    let names = station_names(pool, network, &act.feed_version).await?;
    let directions: Vec<(String, serde_json::Value)> = sqlx::query_as(
        "SELECT line_id, directions FROM transit_lines WHERE network = $1 AND feed_version = $2",
    )
    .bind(network)
    .bind(&act.feed_version)
    .fetch_all(pool)
    .await?;
    let mut towards: HashMap<(String, String), String> = HashMap::new();
    for (line_id, json) in directions {
        let stored: Vec<StoredDirection> =
            serde_json::from_value(json).context("transit_lines.directions")?;
        for d in stored {
            let name = d
                .headsign
                .or_else(|| d.stations.last().and_then(|s| names.get(s).cloned()));
            if let Some(name) = name {
                towards.insert((line_id.clone(), d.direction), name);
            }
        }
    }
    let order: HashMap<&str, usize> = lines
        .iter()
        .enumerate()
        .map(|(i, l)| (l.line_id.as_str(), i))
        .collect();
    let mut out: Vec<(usize, i16, StopFirstLast)> = rows
        .into_iter()
        .filter_map(|(line_id, direction, first, last)| {
            let index = *order.get(line_id.as_str())?;
            let direction_name = if direction == 1 {
                "inbound"
            } else {
                "outbound"
            };
            Some((
                index,
                direction,
                StopFirstLast {
                    line: lines[index].clone(),
                    direction: direction_name.to_owned(),
                    towards: towards.get(&(line_id, direction_name.to_owned())).cloned(),
                    first: clock(first),
                    last: clock(last),
                },
            ))
        })
        .collect();
    out.sort_by_key(|(index, direction, _)| (*index, *direction));
    Ok(Some(out.into_iter().map(|(_, _, row)| row).collect()))
}

/// A band of the service day, for a line's frequency summary (the `TfL`
/// design §8.3): `[from_s, to_s)` seconds after midnight.
const FREQUENCY_BANDS: &[(&str, i32, i32)] = &[
    ("early", 0, 7 * 3600),
    ("morning-peak", 7 * 3600, 10 * 3600),
    ("daytime", 10 * 3600, 16 * 3600),
    ("evening-peak", 16 * 3600, 19 * 3600),
    ("evening", 19 * 3600, 22 * 3600),
    ("late", 22 * 3600, 2 * DAY_S),
];

/// Gaps shorter than this are trains of two branches meeting, not a
/// frequency: left out of a band's shortest gap.
const MIN_GAP_S: i32 = 60;

/// One band of a direction's frequency summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FrequencyBand {
    /// `early`, `morning-peak`, `daytime`, `evening-peak`, `evening`,
    /// `late`.
    pub band: String,
    /// `HH:MM`, its start and end (`24:00` style ends are `00:00`).
    pub from: String,
    pub to: String,
    /// Departures in the band from the station.
    pub departures: u32,
    /// The shortest and longest gap in whole minutes between one departure
    /// in the band and the next; `None` with fewer than two.
    pub min_gap_minutes: Option<u32>,
    pub max_gap_minutes: Option<u32>,
}

/// One direction's frequency summary, at its busiest station.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DirectionFrequency {
    /// `outbound` or `inbound`.
    pub direction: String,
    /// The station measured: the direction's with the most departures.
    pub station: StationRef,
    /// `HH:MM` London time: the first and last departure from it.
    pub first: String,
    pub last: String,
    pub bands: Vec<FrequencyBand>,
}

/// A line's frequency summary for a date.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LineFrequency {
    pub date: NaiveDate,
    pub directions: Vec<DirectionFrequency>,
    /// The date is outside the timetable's horizon (no directions).
    pub outside_horizon: bool,
}

/// The frequency summary of one direction's sorted departure times.
fn bands_of(times: &[i32]) -> Vec<FrequencyBand> {
    FREQUENCY_BANDS
        .iter()
        .filter_map(|&(band, from, to)| {
            let inside: Vec<i32> = times
                .iter()
                .copied()
                .filter(|t| (from..to).contains(t))
                .collect();
            if inside.is_empty() {
                return None;
            }
            let gaps: Vec<i32> = inside
                .windows(2)
                .map(|w| w[1] - w[0])
                .filter(|g| *g >= MIN_GAP_S)
                .collect();
            let minutes = |s: i32| u32::try_from((s + 30) / 60).unwrap_or(0);
            Some(FrequencyBand {
                band: band.to_owned(),
                from: clock(from),
                to: clock(to.min(DAY_S)),
                departures: u32::try_from(inside.len()).unwrap_or(u32::MAX),
                min_gap_minutes: gaps.iter().copied().min().map(minutes),
                max_gap_minutes: gaps.iter().copied().max().map(minutes),
            })
        })
        .collect()
}

/// `line`'s frequency summary on `date` (the `TfL` design §8.3): for each
/// direction, the departures of its trips (replacement buses left out)
/// from its busiest station (of equals, the one trips reach first), their
/// first and last, and the gaps between them by band of the day. `None`
/// for an unknown line.
pub async fn line_frequency(
    pool: &PgPool,
    network: &str,
    line: &str,
    date: NaiveDate,
) -> Result<Option<LineFrequency>> {
    let Some(detail) = get_line(pool, network, line).await? else {
        return Ok(None);
    };
    let Some(act) = active(pool, network).await? else {
        return Ok(None);
    };
    if !act.covers(date) {
        return Ok(Some(LineFrequency {
            date,
            directions: vec![],
            outside_horizon: true,
        }));
    }
    let names: HashMap<String, String> = detail
        .directions
        .iter()
        .flat_map(|d| d.stations.iter().map(|s| (s.id.clone(), s.name.clone())))
        .collect();
    let rows: Vec<(i16, String, i32)> = sqlx::query_as(
        "WITH calls AS ( \
           SELECT p.direction, COALESCE(pl.parent_stop_id, pl.stop_id) AS station, \
                  t.start_s + ps.dep_offset_s AS at_s, ps.seq \
           FROM transit_trips t \
           JOIN transit_patterns p ON p.network = t.network AND p.feed_version = t.feed_version \
                AND p.pattern_id = t.pattern_id \
           JOIN transit_service_days d ON d.network = t.network AND d.feed_version = t.feed_version \
                AND d.service_id = t.service_id AND d.service_date = $4 \
           JOIN transit_pattern_stops ps ON ps.network = t.network AND ps.feed_version = t.feed_version \
                AND ps.pattern_id = t.pattern_id \
           JOIN transit_stops pl ON pl.network = ps.network AND pl.feed_version = ps.feed_version \
                AND pl.stop_id = ps.stop_id \
           WHERE t.network = $1 AND t.feed_version = $2 AND t.line_id = $3 \
             AND NOT t.replacement_bus AND ps.pickup <> 1 \
         ), busiest AS ( \
           SELECT DISTINCT ON (direction) direction, station FROM calls \
           GROUP BY direction, station ORDER BY direction, count(*) DESC, min(seq), station \
         ) \
         SELECT c.direction, c.station, c.at_s FROM calls c \
         JOIN busiest b ON b.direction = c.direction AND b.station = c.station \
         ORDER BY c.direction, c.at_s",
    )
    .bind(network)
    .bind(&act.feed_version)
    .bind(line)
    .bind(date)
    .fetch_all(pool)
    .await?;
    let mut by_direction: BTreeMap<i16, (String, Vec<i32>)> = BTreeMap::new();
    for (direction, station, at) in rows {
        by_direction
            .entry(direction)
            .or_insert_with(|| (station, Vec::new()))
            .1
            .push(at);
    }
    let directions = by_direction
        .into_iter()
        .filter_map(|(direction, (station, times))| {
            let (&first, &last) = (times.first()?, times.last()?);
            Some(DirectionFrequency {
                direction: if direction == 1 {
                    "inbound"
                } else {
                    "outbound"
                }
                .to_owned(),
                station: StationRef {
                    name: names
                        .get(&station)
                        .cloned()
                        .unwrap_or_else(|| station.clone()),
                    id: station,
                },
                first: clock(first),
                last: clock(last),
                bands: bands_of(&times),
            })
        })
        .collect();
    Ok(Some(LineFrequency {
        date,
        directions,
        outside_horizon: false,
    }))
}

/// The network's active version and its horizon.
struct Active {
    feed_version: String,
    horizon_from: Option<NaiveDate>,
    horizon_to: Option<NaiveDate>,
}

impl Active {
    fn covers(&self, date: NaiveDate) -> bool {
        self.horizon_from.is_none_or(|f| date >= f) && self.horizon_to.is_none_or(|t| date <= t)
    }
}

async fn active(pool: &PgPool, network: &str) -> Result<Option<Active>> {
    let row: Option<(String, Option<NaiveDate>, Option<NaiveDate>)> = sqlx::query_as(
        "SELECT n.active_feed_version, v.horizon_from, v.horizon_to \
         FROM transit_networks n JOIN transit_feed_versions v \
           ON v.network = n.network AND v.feed_version = n.active_feed_version \
         WHERE n.network = $1",
    )
    .bind(network)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(feed_version, horizon_from, horizon_to)| Active {
        feed_version,
        horizon_from,
        horizon_to,
    }))
}

/// The networks in `networks` that have an active timetable, in that order.
pub async fn list_networks(pool: &PgPool, networks: &[String]) -> Result<Vec<NetworkSummary>> {
    let rows: Vec<NetworkRow> = sqlx::query_as(
        "SELECT n.network, n.name, n.mode, n.attribution, n.active_feed_version, \
                v.horizon_from, v.horizon_to, \
                (SELECT fetched_at FROM ingest_freshness WHERE source = 'transit-' || n.network || '-timetable'), \
                (SELECT fetched_at FROM ingest_freshness WHERE source = 'transit-' || n.network || '-situations') \
         FROM transit_networks n JOIN transit_feed_versions v \
           ON v.network = n.network AND v.feed_version = n.active_feed_version \
         WHERE n.network = ANY($1)",
    )
    .bind(networks)
    .fetch_all(pool)
    .await?;
    let mut by_id: HashMap<String, NetworkSummary> = rows
        .into_iter()
        .map(|r| {
            (
                r.0.clone(),
                NetworkSummary {
                    id: r.0,
                    name: r.1,
                    mode: r.2,
                    attribution: r.3,
                    feed_version: r.4,
                    horizon_from: r.5,
                    horizon_to: r.6,
                    timetable_as_of: r.7,
                    situations_as_of: r.8,
                },
            )
        })
        .collect();
    Ok(networks.iter().filter_map(|n| by_id.remove(n)).collect())
}

/// The network, if it has an active timetable.
pub async fn get_network(pool: &PgPool, network: &str) -> Result<Option<NetworkSummary>> {
    Ok(list_networks(pool, &[network.to_owned()]).await?.pop())
}

fn line_summary(
    network: &str,
    line_id: String,
    name: String,
    colour: Option<String>,
    mode: String,
) -> LineSummary {
    let id = format!("{network}:{line_id}");
    let status_line_id = if common::transit::status_from_tfl(network) {
        format!("{}{line_id}", common::TFL_LINE_ID_PREFIX)
    } else {
        id.clone()
    };
    LineSummary {
        id,
        line_id,
        name,
        colour,
        mode,
        status_line_id,
    }
}

/// The network's lines, in their order.
pub async fn list_lines(pool: &PgPool, network: &str) -> Result<Vec<LineSummary>> {
    let rows: Vec<(String, String, Option<String>, String)> = sqlx::query_as(
        "SELECT l.line_id, l.name, l.colour, COALESCE(l.mode, n.mode) FROM transit_lines l \
         JOIN transit_networks n ON n.network = l.network AND n.active_feed_version = l.feed_version \
         WHERE l.network = $1 ORDER BY l.sort_order, l.line_id",
    )
    .bind(network)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(id, name, colour, mode)| line_summary(network, id, name, colour, mode))
        .collect())
}

/// Station names by id for the active version.
async fn station_names(pool: &PgPool, network: &str, fv: &str) -> Result<HashMap<String, String>> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT stop_id, name FROM transit_stops \
         WHERE network = $1 AND feed_version = $2 AND kind = 'station'",
    )
    .bind(network)
    .bind(fv)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().collect())
}

#[derive(serde::Deserialize)]
struct StoredDirection {
    direction: String,
    headsign: Option<String>,
    stations: Vec<String>,
}

/// A line and its stations per direction.
pub async fn get_line(pool: &PgPool, network: &str, line: &str) -> Result<Option<LineDetail>> {
    Ok(line_with_patterns(pool, network, line)
        .await?
        .map(|(detail, _)| detail))
}

/// [`get_line`] and the line's patterns, which its branches came from.
async fn line_with_patterns(
    pool: &PgPool,
    network: &str,
    line: &str,
) -> Result<Option<(LineDetail, Vec<LinePattern>)>> {
    let Some(act) = active(pool, network).await? else {
        return Ok(None);
    };
    let row: Option<(String, Option<String>, serde_json::Value, String)> = sqlx::query_as(
        "SELECT l.name, l.colour, l.directions, COALESCE(l.mode, n.mode) FROM transit_lines l \
         JOIN transit_networks n ON n.network = l.network \
         WHERE l.network = $1 AND l.feed_version = $2 AND l.line_id = $3",
    )
    .bind(network)
    .bind(&act.feed_version)
    .bind(line)
    .fetch_optional(pool)
    .await?;
    let Some((name, colour, directions, mode)) = row else {
        return Ok(None);
    };
    let names = station_names(pool, network, &act.feed_version).await?;
    let stored: Vec<StoredDirection> =
        serde_json::from_value(directions).context("transit_lines.directions")?;
    let directions = stored
        .into_iter()
        .map(|d| LineDirection {
            direction: d.direction,
            headsign: d.headsign,
            stations: d
                .stations
                .into_iter()
                .map(|id| StationRef {
                    name: names.get(&id).cloned().unwrap_or_else(|| id.clone()),
                    id,
                })
                .collect(),
        })
        .collect();
    let patterns = line_patterns(pool, network, &act.feed_version, line).await?;
    let detail = LineDetail {
        line: line_summary(network, line.to_owned(), name, colour, mode),
        directions,
        branches: branches_of(&patterns, &names),
    };
    Ok(Some((detail, patterns)))
}

/// Stations (all, or one) with their lines and rail links.
///
/// Two queries, each joined on plain equalities: the stations with their
/// member stops (the station and its platforms, where its calls are), from
/// `transit_stops` alone; then the lines calling at those stops, an index
/// lookup per member (`ps.stop_id = ANY($3)`). Not one join of stations to
/// pattern stops on "the station's or a platform's id": production's
/// Postgres 16 cannot use that `OR` as an index condition, and with the
/// stations underestimated it filtered every pattern stop of the network
/// per station and platform, so `TfL`'s list (1,143 stops, 216,766 pattern
/// stops) hit the 30 s statement timeout (DS-223). These take about 0.3 s
/// there.
async fn stations(pool: &PgPool, network: &str, only: Option<&str>) -> Result<Vec<StopSummary>> {
    let rows: Vec<StationRow> = sqlx::query_as(
        "SELECT s.stop_id, s.name, s.lat, s.lon, s.feed_version, array_agg(m.stop_id) \
         FROM transit_networks n \
         JOIN transit_stops s ON s.network = n.network AND s.feed_version = n.active_feed_version \
         JOIN transit_stops m ON m.network = s.network AND m.feed_version = s.feed_version \
              AND COALESCE(m.parent_stop_id, m.stop_id) = s.stop_id \
         WHERE n.network = $1 AND s.kind = 'station' AND ($2::text IS NULL OR s.stop_id = $2) \
         GROUP BY s.stop_id, s.name, s.lat, s.lon, s.feed_version \
         ORDER BY s.name, s.stop_id",
    )
    .bind(network)
    .bind(only)
    .fetch_all(pool)
    .await?;
    let Some(feed_version) = rows.first().map(|row| row.4.clone()) else {
        return Ok(Vec::new());
    };
    let members: Vec<&str> = rows
        .iter()
        .flat_map(|row| row.5.iter().map(String::as_str))
        .collect();
    let calls: Vec<(String, String)> = sqlx::query_as(
        "SELECT DISTINCT ps.stop_id, p.line_id \
         FROM transit_pattern_stops ps \
         JOIN transit_patterns p ON p.network = ps.network AND p.feed_version = ps.feed_version \
              AND p.pattern_id = ps.pattern_id \
         WHERE ps.network = $1 AND ps.feed_version = $2 AND ps.stop_id = ANY($3) \
           AND p.line_id IS NOT NULL",
    )
    .bind(network)
    .bind(&feed_version)
    .bind(&members)
    .fetch_all(pool)
    .await?;
    let mut lines_at: HashMap<String, Vec<String>> = HashMap::new();
    for (stop, line) in calls {
        lines_at.entry(stop).or_default().push(line);
    }
    let order: HashMap<String, usize> = list_lines(pool, network)
        .await?
        .into_iter()
        .enumerate()
        .map(|(i, l)| (l.line_id, i))
        .collect();
    let links: Vec<RailLinkRow> = sqlx::query_as(
        "SELECT l.stop_id, l.crs, l.tiploc, l.kind, l.note, st.name::text \
         FROM transit_rail_links l LEFT JOIN stations st ON st.crs = l.crs \
         WHERE l.network = $1 AND ($2::text IS NULL OR l.stop_id = $2) ORDER BY l.crs",
    )
    .bind(network)
    .bind(only)
    .fetch_all(pool)
    .await?;
    let mut links_by: BTreeMap<String, Vec<RailLink>> = BTreeMap::new();
    for (stop, crs, tiploc, kind, note, station_name) in links {
        links_by.entry(stop).or_default().push(RailLink {
            crs,
            tiploc,
            kind,
            note,
            station_name,
        });
    }
    Ok(rows
        .into_iter()
        .map(|(id, name, lat, lon, _, members)| {
            let mut called: Vec<String> = members
                .iter()
                .filter_map(|m| lines_at.get(m))
                .flatten()
                .cloned()
                .collect();
            called.sort_by_key(|l| (order.get(l).copied().unwrap_or(usize::MAX), l.clone()));
            called.dedup();
            StopSummary {
                rail_links: links_by.remove(&id).unwrap_or_default(),
                lines: called
                    .into_iter()
                    .map(|l| format!("{network}:{l}"))
                    .collect(),
                id,
                name,
                lat,
                lon,
            }
        })
        .collect())
}

/// The network's stations, by name.
pub async fn list_stops(pool: &PgPool, network: &str) -> Result<Vec<StopSummary>> {
    stations(pool, network, None).await
}

/// A station, its platforms, lines and rail links.
pub async fn get_stop(pool: &PgPool, network: &str, stop: &str) -> Result<Option<StopDetail>> {
    let Some(summary) = stations(pool, network, Some(stop)).await?.pop() else {
        return Ok(None);
    };
    let platforms: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT s.stop_id, s.indicator FROM transit_stops s \
         JOIN transit_networks n ON n.network = s.network AND n.active_feed_version = s.feed_version \
         WHERE s.network = $1 AND s.parent_stop_id = $2 ORDER BY s.stop_id",
    )
    .bind(network)
    .bind(stop)
    .fetch_all(pool)
    .await?;
    Ok(Some(StopDetail {
        stop: summary,
        platforms: platforms
            .into_iter()
            .map(|(id, indicator)| Platform { id, indicator })
            .collect(),
    }))
}

/// `date`'s local midnight plus `secs`, as RFC 3339 London time.
fn london_time(date: NaiveDate, secs: i32) -> String {
    let midnight = London
        .from_local_datetime(&date.and_hms_opt(0, 0, 0).unwrap_or_default())
        .earliest()
        .map_or_else(
            || date.and_hms_opt(0, 0, 0).unwrap_or_default().and_utc(),
            |t| t.with_timezone(&Utc),
        );
    (midnight + chrono::TimeDelta::seconds(i64::from(secs)))
        .with_timezone(&London)
        .to_rfc3339()
}

/// `HH:MM` of `secs` after midnight (wrapping past 24:00).
fn clock(secs: i32) -> String {
    let secs = secs.rem_euclid(DAY_S);
    format!("{:02}:{:02}", secs / 3600, secs % 3600 / 60)
}

/// A board cursor: `<seconds after midnight>.<trip id>.<platform id>`.
fn parse_cursor(cursor: &str) -> Option<(i32, String, String)> {
    let mut parts = cursor.splitn(3, '.');
    let secs = parts.next()?.parse().ok()?;
    Some((secs, parts.next()?.to_owned(), parts.next()?.to_owned()))
}

/// The scheduled departures from `station`'s platforms on `date`, from
/// `after_s` seconds after midnight (or after `cursor`), at most `limit`.
/// A call where pickup is not allowed, or a trip's last call, is not a
/// departure. `None` when the network or station is unknown.
#[expect(clippy::too_many_lines, reason = "one query and its row mapping")]
pub async fn stop_departures(
    pool: &PgPool,
    network: &str,
    station: &str,
    date: NaiveDate,
    after_s: i32,
    cursor: Option<&str>,
    limit: i64,
) -> Result<Option<Board>> {
    let Some(act) = active(pool, network).await? else {
        return Ok(None);
    };
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM transit_stops WHERE network = $1 AND feed_version = $2 \
         AND stop_id = $3 AND kind = 'station')",
    )
    .bind(network)
    .bind(&act.feed_version)
    .bind(station)
    .fetch_one(pool)
    .await?;
    if !exists {
        return Ok(None);
    }
    if !act.covers(date) {
        return Ok(Some(Board {
            date,
            departures: vec![],
            next_cursor: None,
            outside_horizon: true,
        }));
    }
    let (from_s, from_trip, from_stop) =
        cursor
            .and_then(parse_cursor)
            .unwrap_or((after_s, String::new(), String::new()));
    let yesterday = date.checked_sub_days(Days::new(1)).context("date")?;
    let rows: Vec<BoardSqlRow> = sqlx::query_as(
        "WITH calls AS ( \
           SELECT CASE WHEN d.service_date = $4 THEN t.start_s + ps.dep_offset_s \
                       ELSE t.start_s + ps.dep_offset_s - 86400 END AS at_s, \
                  t.trip_id, ps.stop_id, pl.indicator, t.line_id, t.headsign, t.replacement_bus, \
                  t.pattern_id \
           FROM transit_stops pl \
           JOIN transit_pattern_stops ps ON ps.network = pl.network AND ps.feed_version = pl.feed_version \
                AND ps.stop_id = pl.stop_id \
           JOIN transit_trips t ON t.network = ps.network AND t.feed_version = ps.feed_version \
                AND t.pattern_id = ps.pattern_id \
           JOIN transit_service_days d ON d.network = t.network AND d.feed_version = t.feed_version \
                AND d.service_id = t.service_id AND d.service_date IN ($4, $5) \
           WHERE pl.network = $1 AND pl.feed_version = $2 AND (pl.stop_id = $3 OR pl.parent_stop_id = $3) \
             AND ps.pickup <> 1 \
             AND ps.seq < (SELECT max(x.seq) FROM transit_pattern_stops x WHERE x.network = ps.network \
                  AND x.feed_version = ps.feed_version AND x.pattern_id = ps.pattern_id) \
         ) \
         SELECT c.at_s, c.trip_id, c.stop_id, c.indicator, c.line_id, l.name, l.colour, \
                c.replacement_bus, c.headsign, \
                (SELECT COALESCE(st.name, pf.name) FROM transit_pattern_stops z \
                   JOIN transit_stops pf ON pf.network = z.network AND pf.feed_version = z.feed_version \
                        AND pf.stop_id = z.stop_id \
                   LEFT JOIN transit_stops st ON st.network = pf.network AND st.feed_version = pf.feed_version \
                        AND st.stop_id = pf.parent_stop_id \
                   WHERE z.network = $1 AND z.feed_version = $2 AND z.pattern_id = c.pattern_id \
                   ORDER BY z.seq DESC LIMIT 1), \
                COALESCE(l.mode, (SELECT n.mode FROM transit_networks n WHERE n.network = $1)) \
         FROM calls c \
         LEFT JOIN transit_lines l ON l.network = $1 AND l.feed_version = $2 AND l.line_id = c.line_id \
         WHERE c.at_s >= 0 AND c.at_s < 86400 AND (c.at_s, c.trip_id, c.stop_id) > ($6, $7, $8) \
         ORDER BY c.at_s, c.trip_id, c.stop_id \
         LIMIT $9",
    )
    .bind(network)
    .bind(&act.feed_version)
    .bind(station)
    .bind(date)
    .bind(yesterday)
    .bind(from_s)
    .bind(&from_trip)
    .bind(&from_stop)
    .bind(limit + 1)
    .fetch_all(pool)
    .await?;
    let more = i64::try_from(rows.len()).unwrap_or(i64::MAX) > limit;
    let rows: Vec<_> = rows
        .into_iter()
        .take(usize::try_from(limit).unwrap_or(0))
        .collect();
    let next_cursor = if more {
        rows.last()
            .map(|last| format!("{}.{}.{}", last.0, last.1, last.2))
    } else {
        None
    };
    let departures: Vec<BoardRow> = rows
        .into_iter()
        .map(|r| BoardRow {
            scheduled_departure: london_time(date, r.0),
            trip_id: r.1,
            platform_id: r.2,
            platform: r.3,
            line: match (r.4, r.5, r.10) {
                (Some(id), Some(name), Some(line_mode)) => {
                    Some(line_summary(network, id, name, r.6, line_mode))
                }
                _ => None,
            },
            replacement_bus: r.7,
            destination: r.8.or(r.9).unwrap_or_default(),
        })
        .collect();
    Ok(Some(Board {
        date,
        departures,
        next_cursor,
        outside_horizon: false,
    }))
}

/// A line's timetable on `date` in `direction` (`outbound`/`inbound`):
/// its trips (not replacement buses) starting between `from_s` and `to_s`
/// seconds after midnight, with a time at each station. `None` when the
/// network, line or direction is unknown.
pub async fn line_timetable(
    pool: &PgPool,
    network: &str,
    line: &str,
    window: TimetableWindow<'_>,
) -> Result<Option<LineTimetable>> {
    let TimetableWindow {
        date,
        direction,
        from_s,
        to_s,
        branch,
    } = window;
    let Some((detail, patterns)) = line_with_patterns(pool, network, line).await? else {
        return Ok(None);
    };
    let Some(dir) = detail
        .directions
        .into_iter()
        .find(|d| d.direction == direction)
    else {
        return Ok(None);
    };
    let Some(act) = active(pool, network).await? else {
        return Ok(None);
    };
    let direction_id = i16::from(direction == "inbound");
    let branch = branch.and_then(|id| {
        detail
            .branches
            .into_iter()
            .find(|b| b.id == id && b.direction == direction)
    });
    // A branch: its stations, and the patterns that stay on them.
    let (stations, only_patterns) = match branch.as_ref() {
        Some(branch) => {
            let ids: Vec<String> = branch.stations.iter().map(|s| s.id.clone()).collect();
            let keep: Vec<String> = patterns
                .iter()
                .filter(|p| p.direction == direction_id && contains_run(&ids, &p.stations))
                .map(|p| p.pattern_id.clone())
                .collect();
            (branch.stations.clone(), Some(keep))
        }
        None => (dir.stations, None),
    };
    let branch = branch.map(|b| b.id);
    if !act.covers(date) {
        return Ok(Some(LineTimetable {
            date,
            direction: direction.to_owned(),
            branch,
            stations,
            trips: vec![],
            outside_horizon: true,
        }));
    }
    let rows: Vec<(String, i32, Option<String>, String, i32)> = sqlx::query_as(
        "SELECT t.trip_id, t.start_s, t.headsign, COALESCE(pl.parent_stop_id, pl.stop_id), \
                t.start_s + ps.dep_offset_s \
         FROM transit_trips t \
         JOIN transit_patterns p ON p.network = t.network AND p.feed_version = t.feed_version \
              AND p.pattern_id = t.pattern_id \
         JOIN transit_service_days d ON d.network = t.network AND d.feed_version = t.feed_version \
              AND d.service_id = t.service_id AND d.service_date = $4 \
         JOIN transit_pattern_stops ps ON ps.network = t.network AND ps.feed_version = t.feed_version \
              AND ps.pattern_id = t.pattern_id \
         JOIN transit_stops pl ON pl.network = ps.network AND pl.feed_version = ps.feed_version \
              AND pl.stop_id = ps.stop_id \
         WHERE t.network = $1 AND t.feed_version = $2 AND t.line_id = $3 AND NOT t.replacement_bus \
           AND p.direction = $5 AND t.start_s >= $6 AND t.start_s < $7 \
           AND ($8::text[] IS NULL OR t.pattern_id = ANY($8)) \
         ORDER BY t.start_s, t.trip_id, ps.seq",
    )
    .bind(network)
    .bind(&act.feed_version)
    .bind(line)
    .bind(date)
    .bind(direction_id)
    .bind(from_s)
    .bind(to_s)
    .bind(only_patterns)
    .fetch_all(pool)
    .await?;
    let trips = timetable_grid(rows, &stations);
    Ok(Some(LineTimetable {
        date,
        direction: direction.to_owned(),
        branch,
        stations,
        trips,
        outside_horizon: false,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(ids: &str) -> Vec<String> {
        ids.split(' ').map(str::to_owned).collect()
    }

    #[test]
    fn branches_merge_short_runs_and_name_twins_by_where_they_differ() {
        // A Northern-like line, outbound only: two ends north (E, H), a
        // trunk (C), two ways through the middle (B for Bank, X for
        // Charing Cross) and two ends south (M, P); short workings, and a
        // rare depot run.
        let pattern = |id: &str, stations: &str, trips: i64| LinePattern {
            pattern_id: id.to_owned(),
            direction: 0,
            stations: run(stations),
            trips,
        };
        let patterns = [
            pattern("p1", "E C B M", 40),
            pattern("p2", "H C X P", 30),
            pattern("p3", "E C X M", 20),
            // Short workings inside p1 and p2.
            pattern("p4", "C B M", 6),
            pattern("p5", "H C", 2),
            // A depot run: under 5% of the direction's trips.
            pattern("p6", "Q E", 2),
        ];
        let names: HashMap<String, String> = [
            ("B", "Bank"),
            ("C", "Camden"),
            ("E", "Edgware"),
            ("H", "High Barnet"),
            ("M", "Morden"),
            ("P", "Battersea"),
            ("X", "Charing Cross"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();
        let branches = branches_of(&patterns, &names);
        let got: Vec<(&str, &str, u32)> = branches
            .iter()
            .map(|b| (b.id.as_str(), b.name.as_str(), b.share_percent))
            .collect();
        assert_eq!(
            got,
            [
                ("outbound-1", "Edgware to Morden via Bank", 46),
                ("outbound-2", "High Barnet to Battersea", 32),
                ("outbound-3", "Edgware to Morden via Charing Cross", 20),
            ]
        );
        assert_eq!(branches[0].stations[2].name, "Bank");
        assert!(contains_run(&run("E C B M"), &run("C B")));
        assert!(!contains_run(&run("E C B M"), &run("C M")));
    }

    #[test]
    fn frequency_bands_take_gaps_inside_each_band() {
        let at = |h: i32, m: i32| h * 3600 + m * 60;
        let times = [
            at(5, 30),
            at(6, 0),
            at(7, 0),
            at(7, 2),
            at(7, 2) + 20,
            at(7, 6),
            at(23, 50),
            at(24, 20),
        ];
        let bands = bands_of(&times);
        let names: Vec<&str> = bands.iter().map(|b| b.band.as_str()).collect();
        assert_eq!(names, ["early", "morning-peak", "late"]);
        assert_eq!(bands[0].departures, 2);
        assert_eq!(bands[0].min_gap_minutes, Some(30));
        // 07:02 and 07:02:20 are two branches meeting: not a 0-minute gap.
        assert_eq!(bands[1].departures, 4);
        assert_eq!(bands[1].min_gap_minutes, Some(2));
        assert_eq!(bands[1].max_gap_minutes, Some(4));
        assert_eq!(
            (bands[1].from.as_str(), bands[1].to.as_str()),
            ("07:00", "10:00")
        );
        // Past midnight belongs to the late band, which ends at 00:00.
        assert_eq!(bands[2].departures, 2);
        assert_eq!(bands[2].max_gap_minutes, Some(30));
        assert_eq!(bands[2].to, "00:00");
        assert!(bands_of(&[]).is_empty());
        assert_eq!(bands_of(&[at(12, 0)])[0].min_gap_minutes, None);
    }

    #[test]
    fn clocks_and_cursors() {
        assert_eq!(clock(7 * 3600 + 5 * 60), "07:05");
        assert_eq!(clock(24 * 3600 + 20 * 60), "00:20");
        assert_eq!(
            parse_cursor("3600.T1.9400ZZTTAAA1"),
            Some((3600, "T1".to_owned(), "9400ZZTTAAA1".to_owned()))
        );
        assert_eq!(parse_cursor("x.T1.P"), None);
        let date = NaiveDate::from_ymd_opt(2026, 10, 14).unwrap();
        assert_eq!(london_time(date, 7 * 3600), "2026-10-14T07:00:00+01:00");
        assert_eq!(
            london_time(date, 24 * 3600 + 600),
            "2026-10-15T00:10:00+01:00"
        );
    }
}

/// The database tests: as the api's role in CI's per-service step, seeding
/// through `test_support::transit` as the owner.
#[cfg(test)]
mod db_tests {
    use super::*;
    use crate::test_support::transit::{A, B, cleanup, feed, seed, seed_feed};

    const NETWORK: &str = "test-read";

    async fn setup() -> (PgPool, NaiveDate) {
        let pool = crate::test_support::connect().await;
        let today = NaiveDate::from_ymd_opt(2026, 10, 14).unwrap();
        seed(&pool, NETWORK, today).await;
        (pool, today)
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL (a migrated database)"]
    async fn networks_lines_and_stops_read_the_active_version() {
        let (pool, _) = setup().await;
        let networks = list_networks(&pool, &[NETWORK.to_owned(), "nope".to_owned()])
            .await
            .unwrap();
        assert_eq!(networks.len(), 1);
        assert_eq!(networks[0].name, "Test Tram");
        assert!(networks[0].timetable_as_of.is_some());
        let lines = list_lines(&pool, NETWORK).await.unwrap();
        assert_eq!(lines[0].id, "test-read:blue");
        let line = get_line(&pool, NETWORK, "blue").await.unwrap().unwrap();
        assert_eq!(
            line.directions[0].stations[0],
            StationRef {
                id: A.to_owned(),
                name: "Aaa".to_owned()
            }
        );
        assert!(get_line(&pool, NETWORK, "red").await.unwrap().is_none());
        let stops = list_stops(&pool, NETWORK).await.unwrap();
        assert_eq!(stops.len(), 2);
        assert_eq!(stops[0].lines, ["test-read:blue"]);
        assert_eq!(stops[0].rail_links[0].crs, "ZZT");
        let stop = get_stop(&pool, NETWORK, A).await.unwrap().unwrap();
        assert_eq!(stop.platforms.len(), 1);
        assert_eq!(stop.platforms[0].indicator.as_deref(), Some("To the city"));
        assert!(
            get_stop(&pool, NETWORK, "9400ZZTTXXX")
                .await
                .unwrap()
                .is_none()
        );
        cleanup(&pool, NETWORK).await;
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL (a migrated database)"]
    async fn the_board_runs_in_order_across_midnight_and_pages() {
        let (pool, today) = setup().await;
        let board = stop_departures(&pool, NETWORK, A, today, 0, None, 50)
            .await
            .unwrap()
            .unwrap();
        let trips: Vec<&str> = board
            .departures
            .iter()
            .map(|d| d.trip_id.as_str())
            .collect();
        // T2450 of yesterday departs at 00:10 today; today's T2450 is
        // tomorrow's. T0730 runs the other way (A is its last call).
        assert_eq!(trips, ["T2450", "T0600", "T0700", "B0800", "T2350"]);
        assert!(
            board.departures[0]
                .scheduled_departure
                .ends_with("T00:10:00+01:00")
        );
        let bus = &board.departures[3];
        assert!(bus.replacement_bus && bus.line.is_none());
        assert_eq!(bus.destination, "Bbb");
        assert_eq!(
            board.departures[1].line.as_ref().unwrap().id,
            "test-read:blue"
        );
        // Paging with a cursor: no gap, no duplicate.
        let first = stop_departures(&pool, NETWORK, A, today, 0, None, 2)
            .await
            .unwrap()
            .unwrap();
        let cursor = first.next_cursor.clone().unwrap();
        let second = stop_departures(&pool, NETWORK, A, today, 0, Some(&cursor), 10)
            .await
            .unwrap()
            .unwrap();
        let paged: Vec<String> = first
            .departures
            .iter()
            .chain(&second.departures)
            .map(|d| d.trip_id.clone())
            .collect();
        assert_eq!(paged, trips);
        assert!(second.next_cursor.is_none());
        // `after`: from 07:00.
        let later = stop_departures(&pool, NETWORK, A, today, 7 * 3600, None, 10)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(later.departures[0].trip_id, "T0700");
        // Outside the horizon: empty and flagged, not "no service".
        let far = today.checked_add_days(Days::new(30)).unwrap();
        let outside = stop_departures(&pool, NETWORK, A, far, 0, None, 10)
            .await
            .unwrap()
            .unwrap();
        assert!(outside.outside_horizon && outside.departures.is_empty());
        assert!(
            stop_departures(&pool, NETWORK, "9400ZZTTXXX", today, 0, None, 10)
                .await
                .unwrap()
                .is_none()
        );
        cleanup(&pool, NETWORK).await;
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL (a migrated database)"]
    async fn the_line_timetable_is_a_grid_without_buses() {
        let (pool, today) = setup().await;
        let window = |direction, from_s, to_s, branch| TimetableWindow {
            date: today,
            direction,
            from_s,
            to_s,
            branch,
        };
        let grid = line_timetable(
            &pool,
            NETWORK,
            "blue",
            window("outbound", 6 * 3600, 9 * 3600, None),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(grid.stations.len(), 2);
        let trips: Vec<&str> = grid.trips.iter().map(|t| t.trip_id.as_str()).collect();
        assert_eq!(trips, ["T0600", "T0700"]);
        assert_eq!(
            grid.trips[0].times,
            [Some("06:00".to_owned()), Some("06:30".to_owned())]
        );
        assert_eq!(grid.branch, None);
        // The line's one outbound branch: the same trips; another
        // direction's branch, or an unknown one, is the whole direction.
        let branch = line_timetable(
            &pool,
            NETWORK,
            "blue",
            window("outbound", 6 * 3600, 9 * 3600, Some("outbound-1")),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(branch.branch.as_deref(), Some("outbound-1"));
        assert_eq!(branch.trips.len(), 2);
        let other = line_timetable(
            &pool,
            NETWORK,
            "blue",
            window("outbound", 6 * 3600, 9 * 3600, Some("inbound-1")),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(other.branch, None);
        assert_eq!(other.trips.len(), 2);
        let inbound = line_timetable(&pool, NETWORK, "blue", window("inbound", 0, 86_400, None))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(inbound.stations[0].id, B);
        assert_eq!(inbound.trips.len(), 1);
        assert!(
            line_timetable(&pool, NETWORK, "blue", window("sideways", 0, 1, None))
                .await
                .unwrap()
                .is_none()
        );
        let line = get_line(&pool, NETWORK, "blue").await.unwrap().unwrap();
        let branches: Vec<(&str, &str, u32)> = line
            .branches
            .iter()
            .map(|b| (b.id.as_str(), b.name.as_str(), b.share_percent))
            .collect();
        assert_eq!(
            branches,
            [
                ("outbound-1", "Aaa to Bbb", 100),
                ("inbound-1", "Bbb to Aaa", 100)
            ]
        );
        cleanup(&pool, NETWORK).await;
    }

    /// `TfL`'s shape: stations whose calls are all at their platforms, tens
    /// of thousands of pattern stops. The stations list joined stations to
    /// pattern stops on "the station's or a platform's id" (an `OR`); on
    /// production's Postgres 16, with the stations underestimated, that
    /// filtered every pattern stop of the network per station and platform:
    /// 30 s, the statement timeout, on `TfL`'s 1,143 stops and 216,766
    /// pattern stops, and `/transit/tfl` failed (DS-223). Postgres 18 (CI)
    /// turns that `OR` into an index condition, so this guards the scale
    /// and the lines through platforms rather than replaying the plan.
    #[tokio::test]
    #[ignore = "requires DATABASE_URL (a migrated database)"]
    #[expect(clippy::too_many_lines, reason = "one generated feed, part by part")]
    async fn the_stations_list_scales_to_a_tfl_sized_network() {
        use common::transit::{
            Direction, StopKind, TimetableRows, TransitLine, TransitLineDirection, TransitPattern,
            TransitPatternStop, TransitStop,
        };
        use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

        const SCALE: &str = "test-scale";
        const STATIONS: usize = 300;
        const PATTERNS: usize = 3000;
        const CALLS: usize = 30;
        let pool = crate::test_support::connect().await;
        let today = NaiveDate::from_ymd_opt(2026, 10, 14).unwrap();
        let station = |i: usize| format!("9400ZZTS{i:03}");
        let stop = |id: String, parent: Option<String>, name: String| TransitStop {
            stop_id: id,
            kind: if parent.is_some() {
                StopKind::Platform
            } else {
                StopKind::Station
            },
            parent_stop_id: parent,
            name,
            indicator: None,
            lat: None,
            lon: None,
            wheelchair: None,
        };
        let call = |stop_id: String, k: usize| TransitPatternStop {
            stop_id,
            arr_offset_s: i32::try_from(k * 60).unwrap(),
            dep_offset_s: i32::try_from(k * 60).unwrap(),
            pickup: 0,
            drop_off: 0,
        };
        let (mut parts, mut manifest) = feed(SCALE, today);
        for (part, _) in &mut parts {
            match &mut part.rows {
                TimetableRows::Lines(lines) => lines.push(TransitLine {
                    line_id: "red".to_owned(),
                    name: "Red Line".to_owned(),
                    colour: None,
                    sort_order: 2,
                    mode: None,
                    directions: vec![TransitLineDirection {
                        direction: Direction::Outbound,
                        headsign: None,
                        stations: vec![station(0)],
                    }],
                }),
                TimetableRows::Stops(stops) => {
                    for i in 0..STATIONS {
                        stops.push(stop(station(i), None, format!("Gen {i:03}")));
                        for p in 1..=3 {
                            let platform = format!("{}{p}", station(i));
                            stops.push(stop(platform, Some(station(i)), format!("Gen {i:03}")));
                        }
                    }
                    // A station nothing calls at.
                    stops.push(stop("9400ZZTSNONE".to_owned(), None, "Gen none".to_owned()));
                }
                TimetableRows::Patterns(patterns) => {
                    for i in 0..PATTERNS {
                        patterns.push(TransitPattern {
                            pattern_id: format!("g{i}"),
                            line_id: Some("red".to_owned()),
                            direction: Direction::Outbound,
                            headsign: None,
                            stops: (0..CALLS)
                                .map(|k| {
                                    let at = (i * 7 + k) % STATIONS;
                                    call(format!("{}{}", station(at), (i + k) % 3 + 1), k)
                                })
                                .collect(),
                        });
                    }
                    // A call at a station's own id, not a platform's.
                    patterns.push(TransitPattern {
                        pattern_id: "direct".to_owned(),
                        line_id: Some("blue".to_owned()),
                        direction: Direction::Outbound,
                        headsign: None,
                        stops: vec![call(station(0), 0), call(station(1), 1)],
                    });
                }
                _ => {}
            }
        }
        manifest.counts.lines += 1;
        manifest.counts.stops += u32::try_from(STATIONS * 4 + 1).unwrap();
        manifest.counts.patterns += u32::try_from(PATTERNS + 1).unwrap();
        seed_feed(&pool, &parts, &manifest, today).await;

        // Analysed, as production's tables were when `/transit/tfl` failed:
        // a plan from statistics older than the seed is the planner's
        // guess, and no shape of the query is safe from every guess.
        let owner = crate::test_support::schedule_fixture_pool(&pool).await;
        sqlx::query("ANALYZE transit_stops, transit_patterns, transit_pattern_stops")
            .execute(&owner)
            .await
            .unwrap();
        // Well inside production's 30 s.
        let options: PgConnectOptions = std::env::var("DATABASE_URL")
            .unwrap()
            .parse::<PgConnectOptions>()
            .unwrap()
            .options([("statement_timeout", "5s")]);
        let bounded = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .unwrap();
        let stops = list_stops(&bounded, SCALE).await.unwrap();
        assert_eq!(stops.len(), 2 + STATIONS + 1);
        let lines_of = |id: &str| {
            stops
                .iter()
                .find(|s| s.id == id)
                .map(|s| s.lines.clone())
                .unwrap()
        };
        // Through its platforms (red) and its own id (blue), in line order.
        assert_eq!(lines_of(&station(0)), ["test-scale:blue", "test-scale:red"]);
        assert_eq!(lines_of(&station(150)), ["test-scale:red"]);
        assert!(lines_of("9400ZZTSNONE").is_empty());
        assert_eq!(lines_of(A), ["test-scale:blue"]);
        let one = get_stop(&bounded, SCALE, &station(0))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(one.stop.lines, ["test-scale:blue", "test-scale:red"]);
        assert_eq!(one.platforms.len(), 3);
        cleanup(&pool, SCALE).await;
    }
}
