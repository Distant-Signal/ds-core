//! `line_train_summaries` (moved from the api's `data::line_train_summaries`
//! in ingest architecture plan 2a.2, so schedule-reference's `DbSink`
//! derives the rows in the population's transaction exactly as the api's
//! ingest route does; the api keeps the readers): one slim row per train per line per service
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

use std::collections::{HashMap, HashSet};
use std::hash::BuildHasher;

use anyhow::Result;
use chrono::{NaiveTime, Timelike};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Transaction};

use crate::reference::{crs_for_tiplocs_batch, is_bookable_crs};

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

fn tiploc_crs<'a, S: BuildHasher>(
    tiploc_to_crs: &'a HashMap<String, String, S>,
    tiploc: &str,
) -> Option<&'a str> {
    tiploc_to_crs
        .get(&tiploc.trim().to_uppercase())
        .map(String::as_str)
}

/// A train's public calls at the line's stations, in order, consecutive
/// calls at one station (several TIPLOCs, e.g. platform groups) merged.
pub fn on_line_stops<S: BuildHasher>(
    calling_points: &[schedule_query::CallingPoint],
    tiploc_to_crs: &HashMap<String, String, S>,
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
pub fn endpoint_crs<'a, S: BuildHasher>(
    mut calling_points: impl Iterator<Item = &'a schedule_query::CallingPoint>,
    tiploc_to_crs: &HashMap<String, String, S>,
) -> Option<String> {
    calling_points.find_map(|cp| {
        let calls = cp.public_arrival.is_some()
            || cp.public_departure.is_some()
            || cp.booked_arrival.is_some()
            || cp.booked_departure.is_some();
        tiploc_crs(tiploc_to_crs, &cp.tiploc)
            .filter(|crs| calls && is_bookable_crs(crs))
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
pub fn derive_row<S: BuildHasher>(
    fields: EntryFields,
    calling_points: &[schedule_query::CallingPoint],
    has_scope: bool,
    tiploc_to_crs: &HashMap<String, String, S>,
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
pub fn derive_rows<S: BuildHasher>(
    population: DecodedPopulation,
    tiploc_to_crs: &HashMap<String, String, S>,
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

/// What one [`backfill_all`] pass did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BackfillSummary {
    /// Stored populations walked.
    pub populations: usize,
    /// Populations whose rows were rewritten.
    pub rewritten: usize,
    /// Rows written across those populations.
    pub rows: usize,
    /// Populations whose rows were already current.
    pub current: usize,
}

/// The `line_train_summaries` backfill (`ds-migrate
/// backfill-line-train-summaries`, formerly the api's
/// `backfill_line_train_summaries`): [`rebuild_summaries`] for every stored
/// population, one `(line, date)` at a time, each in its own transaction.
/// `on_rewritten` sees each rewritten population, its row count and how long
/// it took.
pub async fn backfill_all(
    pool: &PgPool,
    lines: &[common::LineDefinition],
    force: bool,
    mut on_rewritten: impl FnMut(&str, chrono::NaiveDate, usize, std::time::Duration),
) -> Result<BackfillSummary> {
    let keys: Vec<(String, chrono::NaiveDate)> = sqlx::query_as(
        "SELECT line_id, service_date FROM schedule_line_population ORDER BY service_date, line_id",
    )
    .fetch_all(pool)
    .await?;
    let mut summary = BackfillSummary {
        populations: keys.len(),
        ..BackfillSummary::default()
    };
    for (line_id, service_date) in &keys {
        let line = lines.iter().find(|l| &l.id == line_id);
        let started = std::time::Instant::now();
        match rebuild_summaries(pool, line, line_id, *service_date, force).await? {
            // Pruned since the key list was read.
            None => {}
            Some(None) => summary.current += 1,
            Some(Some(n)) => {
                summary.rewritten += 1;
                summary.rows += n;
                on_rewritten(line_id, *service_date, n, started.elapsed());
            }
        }
    }
    Ok(summary)
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
    let tiploc_to_crs = crs_for_tiplocs_batch(pool, &population_tiplocs(&decoded)).await?;
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
