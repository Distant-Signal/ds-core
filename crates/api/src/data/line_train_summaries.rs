//! `line_train_summaries`: one slim row per train per line per service
//! date, derived from `schedule_line_population` when it is published
//! (docs/superpowers/specs/2026-10-06-line-page-trains-design.md §5).
//!
//! The line page's summary (`/public/lines/{id}/trains?view=summary`) and
//! the full-day timetable (`/public/lines/{id}/timetable`) need, per
//! train, only its membership, its time on the line, its public calls at
//! the line's stations and its two bookable ends. Working those out from
//! the population JSONB costs ~0.3-0.6 s of SQL for a main line on every
//! request (the population must be expanded and the calling points
//! shipped and decoded). This table holds the answer instead, written in
//! the same transaction as the population itself.
//!
//! **Derivation.** [`derive_row`] is the one place a population entry
//! becomes a [`SummaryRow`]; the JSONB fallback path of the summary
//! route calls it too, so both paths agree by construction. It depends on
//! two inputs besides the entry: the TIPLOC -> CRS crosswalk (read at
//! publish time) and the line's catalogue stations and aliases. The
//! latter are hashed into [`derivation_fingerprint`], stored on every
//! row: a reader only uses rows whose fingerprint matches the running
//! `api`'s catalogue, so a catalogue change (or a [`DERIVATION_VERSION`]
//! bump) falls back to the JSONB path until the next publish (or
//! `backfill_line_train_summaries`) rewrites them. A crosswalk change is
//! not fingerprinted; rows follow it at the next publish.
//!
//! **Writes.** [`upsert_population_with_summaries`] replaces the
//! `(line_id, service_date)` rows whenever the population changes or the
//! stored rows' fingerprint is not the current one; an identical
//! re-publish with current rows writes nothing (the population upsert is
//! already a no-op then). A population that cannot be derived (malformed
//! for the slim decode, or a uid listed twice -- never seen in
//! production, but the primary key could not hold it) leaves no rows, and
//! readers fall back to the JSONB.
//!
//! **Pruning** follows `schedule_line_population`'s retention
//! (`aggregator::queries::prune_line_train_summaries`).

use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::Result;
use chrono::{NaiveTime, Timelike};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Transaction};

use super::queries;

/// Minutes in a day.
pub const DAY: i32 = 24 * 60;

/// Bumped whenever [`derive_row`]'s output changes for the same input:
/// rows derived by an older `api` then stop matching
/// [`derivation_fingerprint`] and readers fall back to the JSONB until the
/// rows are rewritten.
pub const DERIVATION_VERSION: &str = "1";

/// How late past its last on-line call a train may be and still count as
/// a running candidate (its live delay decides).
pub const RUNNING_DELAY_GRACE_MINUTES: i32 = 180;

/// A time with its day offset as minutes after the service date's
/// midnight.
pub fn minute_of(time: NaiveTime, day_offset: u8) -> i32 {
    // Both terms are small: hour < 24, minute < 60, day_offset a few days.
    i32::from(day_offset) * DAY + i32::try_from(time.hour() * 60 + time.minute()).unwrap_or(0)
}

/// One public call at one of the line's stations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnLineStop {
    pub crs: String,
    /// Public departure, else public arrival (minutes after midnight of
    /// the service date).
    pub minute: i32,
    /// Public arrival, else public departure -- when the train is AT the
    /// station; the run ends at its last stop's arrival.
    #[serde(rename = "arrival")]
    pub arrival_minute: i32,
}

/// The line's stations as membership sees them: catalogue CRS plus the
/// timetable's alias CRS.
#[derive(Debug, Clone, Default)]
pub struct LineStations {
    pub stations: HashSet<String>,
    pub aliases: HashMap<String, String>,
}

impl LineStations {
    pub fn from_definition(line: &common::LineDefinition) -> Self {
        Self {
            stations: line.stations.iter().map(|s| s.crs.to_uppercase()).collect(),
            aliases: line
                .crs_aliases
                .iter()
                .map(|(from, to)| (from.to_uppercase(), to.to_uppercase()))
                .collect(),
        }
    }

    /// For a line `api`'s catalogue does not know: no stations.
    pub fn for_line(line: Option<&common::LineDefinition>) -> Self {
        line.map_or_else(Self::default, Self::from_definition)
    }

    /// The catalogue CRS a timetable CRS counts as, if it is on the line.
    fn station(&self, crs: &str) -> Option<String> {
        if self.stations.contains(crs) {
            return Some(crs.to_string());
        }
        self.aliases
            .get(crs)
            .filter(|to| self.stations.contains(to.as_str()))
            .cloned()
    }
}

/// Identifies what a row was derived against: [`DERIVATION_VERSION`] and
/// the line's stations and aliases (sorted). 16 hex characters of a
/// SHA-256.
pub fn derivation_fingerprint(line: &LineStations) -> String {
    let mut stations: Vec<&str> = line.stations.iter().map(String::as_str).collect();
    stations.sort_unstable();
    let mut aliases: Vec<(&str, &str)> = line
        .aliases
        .iter()
        .map(|(a, b)| (a.as_str(), b.as_str()))
        .collect();
    aliases.sort_unstable();
    let mut hasher = Sha256::new();
    hasher.update(DERIVATION_VERSION.as_bytes());
    hasher.update(b"|");
    hasher.update(stations.join(",").as_bytes());
    hasher.update(b"|");
    for (from, to) in aliases {
        hasher.update(from.as_bytes());
        hasher.update(b">");
        hasher.update(to.as_bytes());
        hasher.update(b",");
    }
    hasher
        .finalize()
        .iter()
        .take(8)
        .fold(String::with_capacity(16), |mut hex, b| {
            use std::fmt::Write as _;
            let _ = write!(hex, "{b:02x}");
            hex
        })
}

fn tiploc_crs<'a>(tiploc_to_crs: &'a HashMap<String, String>, tiploc: &str) -> Option<&'a str> {
    tiploc_to_crs
        .get(&tiploc.trim().to_uppercase())
        .map(String::as_str)
}

/// A train's public calls at the line's stations, in order, consecutive
/// calls at one station (several TIPLOCs, e.g. platform groups) merged.
pub(crate) fn on_line_stops(
    calling_points: &[schedule_query::CallingPoint],
    tiploc_to_crs: &HashMap<String, String>,
    line: &LineStations,
) -> Vec<OnLineStop> {
    let mut stops: Vec<OnLineStop> = Vec::new();
    for cp in calling_points {
        let departure = cp
            .public_departure
            .map(|t| minute_of(t, cp.departure_day_offset()));
        let arrival = cp.public_arrival.map(|t| minute_of(t, cp.day_offset));
        let Some(minute) = departure.or(arrival) else {
            continue;
        };
        let Some(crs) = tiploc_crs(tiploc_to_crs, &cp.tiploc).and_then(|crs| line.station(crs))
        else {
            continue;
        };
        if let Some(last) = stops.last_mut().filter(|s| s.crs == crs) {
            last.minute = minute;
            continue;
        }
        stops.push(OnLineStop {
            crs,
            minute,
            arrival_minute: arrival.unwrap_or(minute),
        });
    }
    stops
}

/// The schedule's first or last bookable station: the nearest calling
/// point (from the given end) whose TIPLOC resolves to a real station CRS.
/// Walks past depots, junctions and pseudo-CRS (`X..`) ends, which is what
/// left 379 South West Main Line rows without a destination.
pub(crate) fn endpoint_crs<'a>(
    mut calling_points: impl Iterator<Item = &'a schedule_query::CallingPoint>,
    tiploc_to_crs: &HashMap<String, String>,
) -> Option<String> {
    calling_points.find_map(|cp| {
        let calls = cp.public_arrival.is_some()
            || cp.public_departure.is_some()
            || cp.booked_arrival.is_some()
            || cp.booked_departure.is_some();
        tiploc_crs(tiploc_to_crs, &cp.tiploc)
            .filter(|crs| calls && queries::is_bookable_crs(crs))
            .map(str::to_string)
    })
}

/// A population entry's small fields, as the summary reads them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EntryFields {
    pub uid: String,
    pub operator_atoc: Option<String>,
    /// CIF Train Status (one character).
    pub train_status: Option<String>,
    pub scope: Option<String>,
    pub direction: Option<String>,
    /// `line_due.time` as published (`"HH:MM:SS"`).
    pub line_due_time: Option<String>,
    pub line_due_day_offset: Option<i32>,
}

/// `line_due` as minutes after the service date's midnight.
pub fn parse_due(time: Option<&str>, day_offset: Option<i32>) -> Option<i32> {
    let time = NaiveTime::parse_from_str(time?, "%H:%M:%S").ok()?;
    let offset = u8::try_from(day_offset.unwrap_or(0)).ok()?;
    Some(minute_of(time, offset))
}

/// One train on one line on one service date, worked out: what a
/// `line_train_summaries` row holds, and what the summary and timetable
/// routes render.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SummaryRow {
    pub uid: String,
    pub operator_atoc: Option<String>,
    pub train_status: Option<String>,
    pub scope: Option<String>,
    pub direction: Option<String>,
    /// First public call on the line (`lineDue`), minutes after the
    /// service date's midnight.
    pub due_minute: Option<i32>,
    /// The schedule's first and last bookable stations (see
    /// [`endpoint_crs`]); `None` when none resolves.
    pub origin_crs: Option<String>,
    pub destination_crs: Option<String>,
    pub stops: Vec<OnLineStop>,
}

impl SummaryRow {
    /// The last on-line public arrival: when the train's run on the line
    /// ends.
    pub fn end_minute(&self) -> Option<i32> {
        self.stops.last().map(|s| s.arrival_minute)
    }
}

/// Works one population entry out. `has_scope` is whether the population
/// carries train membership; without it the due time is the first on-line
/// public call (a population from before `line_due` existed).
pub(crate) fn derive_row(
    fields: EntryFields,
    calling_points: &[schedule_query::CallingPoint],
    has_scope: bool,
    tiploc_to_crs: &HashMap<String, String>,
    line: &LineStations,
) -> SummaryRow {
    let stops = on_line_stops(calling_points, tiploc_to_crs, line);
    let due_minute = parse_due(fields.line_due_time.as_deref(), fields.line_due_day_offset)
        .or_else(|| {
            (!has_scope)
                .then(|| stops.first().map(|s| s.minute))
                .flatten()
        });
    SummaryRow {
        uid: fields.uid,
        operator_atoc: fields.operator_atoc,
        train_status: fields.train_status,
        scope: fields.scope,
        direction: fields.direction,
        due_minute,
        origin_crs: endpoint_crs(calling_points.iter(), tiploc_to_crs),
        destination_crs: endpoint_crs(calling_points.iter().rev(), tiploc_to_crs),
        stops,
    }
}

/// The slim decode of one population element: only what [`derive_row`]
/// reads. Mirrors the SQL projection of
/// `queries::list_line_train_summary_rows` (string fields as `->>` reads
/// them; `scope` kept as a raw value so "present but null" still counts
/// for `has_scope`, as the SQL's `? 'scope'` does).
#[derive(Debug, Deserialize)]
struct SlimEntry {
    uid: String,
    #[serde(default)]
    calling_points: Vec<schedule_query::CallingPoint>,
    #[serde(default)]
    operator_atoc: Option<String>,
    #[serde(default)]
    train_status: Option<String>,
    #[serde(default)]
    scope: Option<serde_json::Value>,
    #[serde(default)]
    direction: Option<String>,
    #[serde(default)]
    line_due: Option<SlimDue>,
}

#[derive(Debug, Deserialize)]
struct SlimDue {
    #[serde(default)]
    time: Option<String>,
    #[serde(default)]
    day_offset: Option<serde_json::Value>,
}

/// A decoded population: its entries and whether it carries membership.
#[derive(Debug, Default)]
pub struct DecodedPopulation {
    pub entries: Vec<(EntryFields, Vec<schedule_query::CallingPoint>)>,
    pub has_scope: bool,
}

/// Decodes a population's JSON text for [`derive_row`]. `Err` for
/// anything the slim decode cannot read (the caller then writes no rows).
pub fn decode_population(text: &str) -> Result<DecodedPopulation> {
    let entries: Vec<SlimEntry> = serde_json::from_str(text)?;
    let has_scope = entries.first().is_none_or(|e| e.scope.is_some());
    let entries = entries
        .into_iter()
        .map(|e| {
            let (line_due_time, line_due_day_offset) = match e.line_due {
                Some(due) => (
                    due.time,
                    due.day_offset
                        .as_ref()
                        .and_then(serde_json::Value::as_i64)
                        .and_then(|n| i32::try_from(n).ok()),
                ),
                None => (None, None),
            };
            let fields = EntryFields {
                uid: e.uid,
                operator_atoc: e.operator_atoc,
                train_status: e.train_status,
                scope: e.scope.and_then(|s| s.as_str().map(str::to_string)),
                direction: e.direction,
                line_due_time,
                line_due_day_offset,
            };
            (fields, e.calling_points)
        })
        .collect();
    Ok(DecodedPopulation { entries, has_scope })
}

/// Every TIPLOC the decoded population names, normalised as the crosswalk
/// lookup keys them.
fn population_tiplocs(population: &DecodedPopulation) -> Vec<String> {
    population
        .entries
        .iter()
        .flat_map(|(_, cps)| cps.iter().map(|cp| cp.tiploc.trim().to_uppercase()))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect()
}

/// Every row of a decoded population, or `None` when a uid appears twice
/// (the table cannot hold it; readers fall back to the JSONB, which lists
/// both).
pub(crate) fn derive_rows(
    population: DecodedPopulation,
    tiploc_to_crs: &HashMap<String, String>,
    line: &LineStations,
) -> Option<Vec<SummaryRow>> {
    let mut seen = HashSet::new();
    let has_scope = population.has_scope;
    let mut rows = Vec::with_capacity(population.entries.len());
    for (fields, cps) in population.entries {
        if !seen.insert(fields.uid.clone()) {
            return None;
        }
        rows.push(derive_row(fields, &cps, has_scope, tiploc_to_crs, line));
    }
    Some(rows)
}

/// What [`upsert_population_with_summaries`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteOutcome {
    /// The population row was inserted or changed.
    pub population_changed: bool,
    /// Summary rows written, or `None` when the stored ones were current
    /// and left alone.
    pub summaries_written: Option<usize>,
}

/// Upserts one line's population for one date (exactly
/// `queries::upsert_schedule_line_population`'s statement, so an
/// identical re-publish leaves the row alone) and, in the same
/// transaction, rewrites its `line_train_summaries` rows when the
/// population changed or the stored rows are not current (see the module
/// doc). `line` is the catalogue line (`None`: no stations, every row
/// without on-line stops, as the JSONB path renders it).
///
/// The population text is decoded on a blocking thread: a main line's is
/// ~20 MB.
pub async fn upsert_population_with_summaries(
    pool: &PgPool,
    line: Option<&common::LineDefinition>,
    line_id: &str,
    service_date: chrono::NaiveDate,
    population_json: Box<str>,
) -> Result<WriteOutcome> {
    let stations = LineStations::for_line(line);
    let fingerprint = derivation_fingerprint(&stations);
    let mut tx = pool.begin().await?;
    let changed: Option<(i32,)> = sqlx::query_as(
        r"
        INSERT INTO schedule_line_population (line_id, service_date, population, updated_at)
        VALUES ($1, $2, $3::jsonb, now())
        ON CONFLICT (line_id, service_date) DO UPDATE SET
            population = EXCLUDED.population,
            updated_at = EXCLUDED.updated_at
        WHERE schedule_line_population.population IS DISTINCT FROM EXCLUDED.population
        RETURNING 1
        ",
    )
    .bind(line_id)
    .bind(service_date)
    .bind(&*population_json)
    .fetch_optional(&mut *tx)
    .await?;
    let population_changed = changed.is_some();
    if !population_changed
        && stored_fingerprint(&mut tx, line_id, service_date).await? == Some(fingerprint.clone())
    {
        tx.commit().await?;
        return Ok(WriteOutcome {
            population_changed,
            summaries_written: None,
        });
    }
    let written = derive_and_replace(
        &mut tx,
        pool,
        &stations,
        &fingerprint,
        line_id,
        service_date,
        population_json,
    )
    .await?;
    tx.commit().await?;
    Ok(WriteOutcome {
        population_changed,
        summaries_written: Some(written),
    })
}

/// Rewrites one `(line_id, service_date)`'s rows from the stored
/// population (`backfill_line_train_summaries`), unless they are already
/// current and `force` is off. `None` when no population is stored;
/// `Some(None)` when the rows were current.
pub async fn rebuild_summaries(
    pool: &PgPool,
    line: Option<&common::LineDefinition>,
    line_id: &str,
    service_date: chrono::NaiveDate,
    force: bool,
) -> Result<Option<Option<usize>>> {
    let stations = LineStations::for_line(line);
    let fingerprint = derivation_fingerprint(&stations);
    let mut tx = pool.begin().await?;
    // FOR UPDATE: serialises with a concurrent publish of the same row
    // (its upsert takes the same row lock).
    let text: Option<(String,)> = sqlx::query_as(
        "SELECT population::text FROM schedule_line_population \
         WHERE line_id = $1 AND service_date = $2 FOR UPDATE",
    )
    .bind(line_id)
    .bind(service_date)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((text,)) = text else {
        return Ok(None);
    };
    if !force
        && stored_fingerprint(&mut tx, line_id, service_date).await? == Some(fingerprint.clone())
    {
        tx.commit().await?;
        return Ok(Some(None));
    }
    let written = derive_and_replace(
        &mut tx,
        pool,
        &stations,
        &fingerprint,
        line_id,
        service_date,
        text.into_boxed_str(),
    )
    .await?;
    tx.commit().await?;
    Ok(Some(Some(written)))
}

/// The fingerprint the stored rows were derived with, if there are any.
async fn stored_fingerprint(
    tx: &mut Transaction<'_, Postgres>,
    line_id: &str,
    service_date: chrono::NaiveDate,
) -> Result<Option<String>> {
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT derivation FROM line_train_summaries \
         WHERE line_id = $1 AND service_date = $2 LIMIT 1",
    )
    .bind(line_id)
    .bind(service_date)
    .fetch_optional(&mut **tx)
    .await?;
    Ok(row.map(|(f,)| f))
}

/// Decodes and derives `population_json`, then replaces the
/// `(line_id, service_date)` rows inside `tx`. Returns the rows written
/// (0 when the population cannot be derived; see the module doc).
async fn derive_and_replace(
    tx: &mut Transaction<'_, Postgres>,
    pool: &PgPool,
    stations: &LineStations,
    fingerprint: &str,
    line_id: &str,
    service_date: chrono::NaiveDate,
    population_json: Box<str>,
) -> Result<usize> {
    let decoded = tokio::task::spawn_blocking(move || decode_population(&population_json)).await?;
    sqlx::query("DELETE FROM line_train_summaries WHERE line_id = $1 AND service_date = $2")
        .bind(line_id)
        .bind(service_date)
        .execute(&mut **tx)
        .await?;
    let decoded = match decoded {
        Ok(decoded) => decoded,
        Err(err) => {
            tracing::warn!(error = ?err, line_id, %service_date,
                "line population does not decode for line_train_summaries; readers use the population");
            return Ok(0);
        }
    };
    let has_scope = decoded.has_scope;
    // The crosswalk is reference data, read outside the transaction.
    let tiploc_to_crs = queries::crs_for_tiplocs_batch(pool, &population_tiplocs(&decoded)).await?;
    let Some(rows) = derive_rows(decoded, &tiploc_to_crs, stations) else {
        tracing::warn!(line_id, %service_date,
            "line population lists a uid twice; no line_train_summaries rows, readers use the population");
        return Ok(0);
    };
    insert_rows(tx, line_id, service_date, has_scope, fingerprint, &rows).await?;
    Ok(rows.len())
}

/// Rows per `INSERT ... UNNEST` statement.
const INSERT_CHUNK: usize = 2000;

async fn insert_rows(
    tx: &mut Transaction<'_, Postgres>,
    line_id: &str,
    service_date: chrono::NaiveDate,
    has_scope: bool,
    fingerprint: &str,
    rows: &[SummaryRow],
) -> Result<()> {
    for chunk in rows.chunks(INSERT_CHUNK) {
        let uid: Vec<&str> = chunk.iter().map(|r| r.uid.as_str()).collect();
        let scope: Vec<Option<&str>> = chunk.iter().map(|r| r.scope.as_deref()).collect();
        let direction: Vec<Option<&str>> = chunk.iter().map(|r| r.direction.as_deref()).collect();
        let due: Vec<Option<i32>> = chunk.iter().map(|r| r.due_minute).collect();
        let end: Vec<Option<i32>> = chunk.iter().map(SummaryRow::end_minute).collect();
        let operator: Vec<Option<&str>> =
            chunk.iter().map(|r| r.operator_atoc.as_deref()).collect();
        let status: Vec<Option<&str>> = chunk.iter().map(|r| r.train_status.as_deref()).collect();
        let origin: Vec<Option<&str>> = chunk.iter().map(|r| r.origin_crs.as_deref()).collect();
        let destination: Vec<Option<&str>> =
            chunk.iter().map(|r| r.destination_crs.as_deref()).collect();
        let stops: Vec<String> = chunk
            .iter()
            .map(|r| serde_json::to_string(&r.stops))
            .collect::<Result<_, _>>()?;
        sqlx::query(
            r"
            INSERT INTO line_train_summaries
                (line_id, service_date, uid, scope, direction, due_minute, end_minute,
                 operator_atoc, train_status, origin_crs, destination_crs, on_line_stops,
                 has_scope, derivation)
            SELECT $1, $2, i.uid, i.scope, i.direction, i.due, i.end_m, i.operator, i.status,
                   i.origin, i.destination, i.stops::jsonb, $13, $14
            FROM UNNEST($3::text[], $4::text[], $5::text[], $6::int[], $7::int[], $8::text[],
                        $9::text[], $10::text[], $11::text[], $12::text[])
                 AS i(uid, scope, direction, due, end_m, operator, status, origin, destination, stops)
            ",
        )
        .bind(line_id)
        .bind(service_date)
        .bind(&uid)
        .bind(&scope)
        .bind(&direction)
        .bind(&due)
        .bind(&end)
        .bind(&operator)
        .bind(&status)
        .bind(&origin)
        .bind(&destination)
        .bind(&stops)
        .bind(has_scope)
        .bind(fingerprint)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

/// Which rows of a summary read carry their stops and ends: those the
/// response can render. Every row when there is no window; otherwise the
/// rows due in `window`, plus with `at` the running candidates (due at
/// or before `at`, last on-line arrival no more than
/// [`RUNNING_DELAY_GRACE_MINUTES`] before it).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ShipRule {
    pub window: Option<(i32, i32)>,
    pub at: Option<i32>,
}

/// One decoded `line_train_summaries` summary read, column for column.
type SummaryRowTuple = (
    Option<String>,
    bool,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i32>,
    Option<String>,
    Option<String>,
    Option<String>,
);

fn summary_row_from_tuple(
    (
        uid,
        _,
        operator_atoc,
        train_status,
        scope,
        direction,
        due_minute,
        origin_crs,
        destination_crs,
        stops,
    ): SummaryRowTuple,
) -> Result<SummaryRow> {
    Ok(SummaryRow {
        uid: uid.unwrap_or_default(),
        operator_atoc,
        train_status,
        scope,
        direction,
        due_minute,
        origin_crs,
        destination_crs,
        stops: stops
            .map(|text| serde_json::from_str(&text))
            .transpose()?
            .unwrap_or_default(),
    })
}

/// The `(line_id, service_date)` rows derived with `fingerprint`,
/// narrowed to `scopes` (unless the population predates membership),
/// each with its stops and ends only when `ship` says so. `None` when
/// there are no such rows (the caller falls back to the population).
/// Table order; the caller sorts.
pub async fn list_summary_rows(
    pool: &PgPool,
    line_id: &str,
    service_date: chrono::NaiveDate,
    fingerprint: &str,
    scopes: Option<&[String]>,
    ship: ShipRule,
) -> Result<Option<(Vec<SummaryRow>, bool)>> {
    let rows: Vec<SummaryRowTuple> = sqlx::query_as(
        r"
        WITH hs AS (
            SELECT has_scope FROM line_train_summaries
            WHERE line_id = $1 AND service_date = $2 AND derivation = $3
            LIMIT 1
        ),
        r AS (
            SELECT t.*, $5::int IS NULL
                     OR (t.due_minute >= $5::int AND t.due_minute < $6::int)
                     OR ($7::int IS NOT NULL AND t.due_minute <= $7::int
                         AND t.end_minute + $8::int >= $7::int) AS ship
            FROM hs
            JOIN line_train_summaries t
              ON t.line_id = $1 AND t.service_date = $2 AND t.derivation = $3
             AND ($4::text[] IS NULL OR NOT hs.has_scope OR t.scope = ANY($4::text[]))
        )
        SELECT r.uid, hs.has_scope, r.operator_atoc, r.train_status, r.scope, r.direction,
               r.due_minute,
               CASE WHEN r.ship THEN r.origin_crs END,
               CASE WHEN r.ship THEN r.destination_crs END,
               CASE WHEN r.ship THEN r.on_line_stops::text END
        FROM hs LEFT JOIN r ON true
        ",
    )
    .bind(line_id)
    .bind(service_date)
    .bind(fingerprint)
    .bind(scopes)
    .bind(ship.window.map(|(from, _)| from))
    .bind(ship.window.map(|(_, to)| to))
    .bind(ship.at)
    .bind(RUNNING_DELAY_GRACE_MINUTES)
    .fetch_all(pool)
    .await?;
    let Some(has_scope) = rows.first().map(|row| row.1) else {
        return Ok(None);
    };
    let rows = rows
        .into_iter()
        .filter(|row| row.0.is_some())
        .map(summary_row_from_tuple)
        .collect::<Result<Vec<_>>>()?;
    Ok(Some((rows, has_scope)))
}

/// `GET /public/lines/{id}/timetable`'s filters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TimetableFilter {
    /// `None`: every scope.
    pub scopes: Option<Vec<String>>,
    /// `None`: every direction.
    pub directions: Option<Vec<String>>,
    /// Catalogue CRS: only trains with a public call there, timed by it.
    pub from: Option<String>,
    /// Catalogue CRS: only trains with a public call there (after `from`'s).
    pub to: Option<String>,
    /// Only trains whose time is at or after this minute.
    pub at: Option<i32>,
    /// Keyset cursor: only trains after this `(time, uid)`.
    pub after: Option<(i32, String)>,
    pub limit: usize,
}

/// One timetable row: the train, its time (at `from`, else on the line)
/// and its arrival at `to`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimetableEntry {
    pub row: SummaryRow,
    pub minute: i32,
    pub to_arrival: Option<i32>,
}

/// One page of the timetable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimetablePage {
    pub has_scope: bool,
    pub entries: Vec<TimetableEntry>,
    /// The cursor for the next page, `None` on the last.
    pub next: Option<(i32, String)>,
    /// Trains per scope and direction (`None` for a train without one)
    /// for the whole day under the scope and station filters -- before
    /// `directions`, `at` and the cursor. Ordered by scope, direction.
    pub counts: Vec<(Option<String>, Option<String>, i64)>,
}

/// The time a train is listed at under `filter`'s stations, and its
/// arrival at `to`: `None` when it does not call at them in order.
fn station_times(row: &SummaryRow, filter: &TimetableFilter) -> Option<(i32, Option<i32>)> {
    let from_index = match &filter.from {
        None => None,
        Some(from) => Some(row.stops.iter().position(|s| &s.crs == from)?),
    };
    let minute = match from_index {
        None => row.due_minute?,
        Some(i) => row.stops.get(i)?.minute,
    };
    let to_arrival = match &filter.to {
        None => None,
        Some(to) => {
            let start = from_index.map_or(0, |i| i + 1);
            Some(
                row.stops
                    .get(start..)?
                    .iter()
                    .find(|s| &s.crs == to)?
                    .arrival_minute,
            )
        }
    };
    Some((minute, to_arrival))
}

/// The timetable page over rows already in memory: the population
/// fallback, and the reference [`timetable_page`]'s SQL is tested
/// against.
pub fn timetable_page_in_memory(
    rows: Vec<SummaryRow>,
    has_scope: bool,
    filter: &TimetableFilter,
) -> TimetablePage {
    let timed: Vec<TimetableEntry> = rows
        .into_iter()
        .filter(|r| {
            !has_scope
                || filter
                    .scopes
                    .as_ref()
                    .is_none_or(|s| r.scope.as_ref().is_some_and(|own| s.contains(own)))
        })
        .filter_map(|row| {
            let (minute, to_arrival) = station_times(&row, filter)?;
            Some(TimetableEntry {
                row,
                minute,
                to_arrival,
            })
        })
        .collect();
    let mut counts: BTreeMap<(Option<String>, Option<String>), i64> = BTreeMap::new();
    for e in &timed {
        *counts
            .entry((e.row.scope.clone(), e.row.direction.clone()))
            .or_default() += 1;
    }
    let mut listed: Vec<TimetableEntry> = timed
        .into_iter()
        .filter(|e| {
            filter
                .directions
                .as_ref()
                .is_none_or(|d| e.row.direction.as_ref().is_some_and(|own| d.contains(own)))
        })
        .filter(|e| filter.at.is_none_or(|at| e.minute >= at))
        .filter(|e| {
            filter
                .after
                .as_ref()
                .is_none_or(|(m, uid)| (e.minute, e.row.uid.as_str()) > (*m, uid.as_str()))
        })
        .collect();
    listed.sort_by(|a, b| (a.minute, &a.row.uid).cmp(&(b.minute, &b.row.uid)));
    let next = next_cursor(&listed, filter.limit);
    listed.truncate(filter.limit);
    TimetablePage {
        has_scope,
        entries: listed,
        next,
        counts: counts
            .into_iter()
            .map(|((scope, direction), n)| (scope, direction, n))
            .collect(),
    }
}

/// The cursor after a page of `limit` out of `listed` (which holds at
/// least one more when there is a next page).
fn next_cursor(listed: &[TimetableEntry], limit: usize) -> Option<(i32, String)> {
    if listed.len() <= limit {
        return None;
    }
    listed
        .get(limit.checked_sub(1)?)
        .map(|e| (e.minute, e.row.uid.clone()))
}

/// The timetable's rows with their station times, as SQL (`$1` line,
/// `$2` date, `$3` fingerprint, `$4` scopes, `$5` from, `$6` to):
/// [`station_times`] for every row of the date, filtered by scope and
/// stations. `uid` is in the "C" collation so the keyset order is Rust's
/// byte order.
const TIMETABLE_BASE_SQL: &str = r#"
    hs AS (
        SELECT has_scope FROM line_train_summaries
        WHERE line_id = $1 AND service_date = $2 AND derivation = $3
        LIMIT 1
    ),
    base AS (
        SELECT t.uid COLLATE "C" AS uid, t.operator_atoc, t.train_status, t.scope,
               t.direction, t.due_minute, t.origin_crs, t.destination_crs, t.on_line_stops,
               CASE WHEN $5::text IS NULL THEN t.due_minute ELSE f.minute END AS minute,
               a.arrival AS to_arrival
        FROM hs
        JOIN line_train_summaries t
          ON t.line_id = $1 AND t.service_date = $2 AND t.derivation = $3
         AND ($4::text[] IS NULL OR NOT hs.has_scope OR t.scope = ANY($4::text[]))
        LEFT JOIN LATERAL (
            SELECT (x.e ->> 'minute')::int AS minute, x.i
            FROM jsonb_array_elements(t.on_line_stops) WITH ORDINALITY AS x(e, i)
            WHERE x.e ->> 'crs' = $5::text
            ORDER BY x.i LIMIT 1
        ) f ON true
        LEFT JOIN LATERAL (
            SELECT (x.e ->> 'arrival')::int AS arrival
            FROM jsonb_array_elements(t.on_line_stops) WITH ORDINALITY AS x(e, i)
            WHERE x.e ->> 'crs' = $6::text AND x.i > COALESCE(f.i, 0)
            ORDER BY x.i LIMIT 1
        ) a ON true
        WHERE ($5::text IS NULL OR f.minute IS NOT NULL)
          AND ($6::text IS NULL OR a.arrival IS NOT NULL)
    )
"#;

/// One decoded timetable row, column for column.
type TimetableTuple = (
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i32>,
    Option<String>,
    Option<String>,
    String,
    i32,
    Option<i32>,
);

fn timetable_entry_from_tuple(
    (
        uid,
        operator_atoc,
        train_status,
        scope,
        direction,
        due_minute,
        origin_crs,
        destination_crs,
        stops,
        minute,
        to_arrival,
    ): TimetableTuple,
) -> Result<TimetableEntry> {
    Ok(TimetableEntry {
        row: SummaryRow {
            uid,
            operator_atoc,
            train_status,
            scope,
            direction,
            due_minute,
            origin_crs,
            destination_crs,
            stops: serde_json::from_str(&stops)?,
        },
        minute,
        to_arrival,
    })
}

/// One timetable page from the table: the same answer as
/// [`timetable_page_in_memory`] over the rows of `(line_id, service_date)`
/// derived with `fingerprint`, filtered, ordered and cut in SQL. `None`
/// when there are no such rows (the caller falls back to the population).
pub async fn timetable_page(
    pool: &PgPool,
    line_id: &str,
    service_date: chrono::NaiveDate,
    fingerprint: &str,
    filter: &TimetableFilter,
) -> Result<Option<TimetablePage>> {
    let has_scope: Option<(bool,)> = sqlx::query_as(
        "SELECT has_scope FROM line_train_summaries \
         WHERE line_id = $1 AND service_date = $2 AND derivation = $3 LIMIT 1",
    )
    .bind(line_id)
    .bind(service_date)
    .bind(fingerprint)
    .fetch_optional(pool)
    .await?;
    let Some((has_scope,)) = has_scope else {
        return Ok(None);
    };
    let limit = i64::try_from(filter.limit).unwrap_or(i64::MAX);
    let rows: Vec<TimetableTuple> = sqlx::query_as(&format!(
        r#"
        WITH {TIMETABLE_BASE_SQL}
        SELECT uid, operator_atoc, train_status, scope, direction, due_minute, origin_crs,
               destination_crs, on_line_stops::text, minute, to_arrival
        FROM base
        WHERE minute IS NOT NULL
          AND ($7::text[] IS NULL OR direction = ANY($7::text[]))
          AND ($8::int IS NULL OR minute >= $8::int)
          AND ($9::int IS NULL OR (minute, uid) > ($9::int, $10::text COLLATE "C"))
        ORDER BY minute, uid
        LIMIT $11
        "#
    ))
    .bind(line_id)
    .bind(service_date)
    .bind(fingerprint)
    .bind(filter.scopes.as_deref())
    .bind(filter.from.as_deref())
    .bind(filter.to.as_deref())
    .bind(filter.directions.as_deref())
    .bind(filter.at)
    .bind(filter.after.as_ref().map(|(m, _)| *m))
    .bind(filter.after.as_ref().map(|(_, uid)| uid.as_str()))
    .bind(limit.saturating_add(1))
    .fetch_all(pool)
    .await?;
    let counts: Vec<(Option<String>, Option<String>, i64)> = sqlx::query_as(&format!(
        r#"
        WITH {TIMETABLE_BASE_SQL}
        SELECT scope, direction, count(*) FROM base
        WHERE minute IS NOT NULL
        GROUP BY scope, direction
        ORDER BY scope COLLATE "C" NULLS FIRST, direction COLLATE "C" NULLS FIRST
        "#
    ))
    .bind(line_id)
    .bind(service_date)
    .bind(fingerprint)
    .bind(filter.scopes.as_deref())
    .bind(filter.from.as_deref())
    .bind(filter.to.as_deref())
    .fetch_all(pool)
    .await?;
    let mut entries = rows
        .into_iter()
        .map(timetable_entry_from_tuple)
        .collect::<Result<Vec<_>>>()?;
    let next = next_cursor(&entries, filter.limit);
    entries.truncate(filter.limit);
    Ok(Some(TimetablePage {
        has_scope,
        entries,
        next,
        counts,
    }))
}
