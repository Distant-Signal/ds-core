//! Sample writers: the station-sample, full-coverage, Transport for London
//! line-status and island-of-Ireland upserts and their `last_*_fetch`
//! readers, from `queries.rs`, `full_coverage_window.rs` and
//! `island_of_ireland.rs` (spec §5.2). The public readers stay in the api.
//!
//! Moved in plan task 1A.5. The full-coverage window and island-of-Ireland
//! writers keep their own submodules, as they were their own api modules
//! (and share names with the GB writers here).

use std::collections::HashMap;

use anyhow::Result;
use chrono::{DateTime, Utc};
use common::{LineStatusReport, StationFullCoverageSample, StationSample};
use sqlx::{PgConnection, PgPool};

use crate::freshness::{last_per_key, normalize_code, record_ingest};

pub mod full_coverage_window;
pub mod island_of_ireland;

/// The `ingest_freshness` sources the ingest-writer records for the four
/// snapshot schemas it applies (plan 3a.6, D13), at the entry's
/// `produced_at`: "data as of". The api's routes do not record them (their
/// freshness reads use the rows' own times), so these rows exist only once
/// a stream is on `apply`; plan 3a.9's readers use them.
pub mod sources {
    pub const STATION_SAMPLES: &str = "station-samples";
    pub const FULL_COVERAGE_STATS: &str = "full-coverage-stats";
    pub const FULL_COVERAGE_WINDOW_STATS: &str = "full-coverage-window-stats";
    pub const STATION_FULL_COVERAGE_SAMPLES: &str = "station-full-coverage-samples";
}

/// `GREATEST(<row_time>, <the feed's observed time>)`: a row's age when the
/// writer may skip an unchanged row (plan 3a.9, D13, spec §7.8). The feed's
/// observed time is `ingest_freshness.fetched_at` for `source` (one of
/// [`sources`]), which the ingest-writer records at each applied entry's
/// `produced_at`; `GREATEST` ignores its `NULL` (no writer has applied the
/// feed yet), so without it this is `row_time` unchanged.
///
/// Valid only for a table whose every snapshot carries every live key
/// (plan 3a.9's check): there an unchanged row the writer skipped was in
/// the newest snapshot, so it is as fresh as the feed. `row_time` is a
/// trusted SQL expression (a column), never data.
///
/// # Panics
///
/// If `source` is not lowercase letters and hyphens: a programming error
/// (it is spliced into the SQL as a literal).
pub fn feed_observed_at_sql(row_time: &str, source: &str) -> String {
    assert!(
        !source.is_empty() && source.chars().all(|c| c.is_ascii_lowercase() || c == '-'),
        "feed_observed_at_sql: bad source {source:?}"
    );
    format!(
        "GREATEST({row_time}, (SELECT fetched_at FROM ingest_freshness WHERE source = '{source}'))"
    )
}

/// The derived `station_full_coverage_samples.resolved_at` every reader uses
/// (plan 3a.9): [`feed_observed_at_sql`] of the row's `resolved_at` and the
/// `station-full-coverage-samples` feed. With `INGEST_WRITER_CHANGED_ROWS_ONLY`
/// the writer no longer advances an unchanged row's `resolved_at`.
pub const STATION_FULL_COVERAGE_RESOLVED_AT_SQL: &str = "GREATEST(station_full_coverage_samples.resolved_at, \
     (SELECT fetched_at FROM ingest_freshness WHERE source = 'station-full-coverage-samples'))";

/// `AND <guard>`, or nothing. `guard` is a trusted SQL fragment (the
/// ingest-writer's observed-time guard, `ingest_writer::observed::guard`),
/// never data.
pub(crate) fn and_guard(guard: Option<&str>) -> String {
    guard.map_or_else(String::new, |guard| format!(" AND {guard}"))
}

/// A JSON encoding failure as a `sqlx` error, so the `_on` writers keep one
/// error type (it cannot happen for these plain structs).
fn encode_error(err: serde_json::Error) -> sqlx::Error {
    sqlx::Error::Encode(Box::new(err))
}

/// Upserts a batch of station samples (LDBWS departure-board snapshots).
/// No history — this is a point-in-time sample, wholesale-replaced per
/// poll, same rationale as `upsert_stations`/`upsert_tocs`.
pub async fn upsert_station_samples(pool: &PgPool, samples: &[StationSample]) -> Result<u64> {
    let mut conn = pool.acquire().await?;
    upsert_station_samples_on(&mut conn, samples, None).await?;
    Ok(samples.len() as u64)
}

/// [`upsert_station_samples`] on `conn` (the ingest-writer's transaction,
/// plan 3a.6), with `guard` ANDed into the upsert's `WHERE`: the writer
/// passes its observed-time guard on `station_samples.polled_at` (spec
/// §7.4), so an older snapshot never overwrites a newer one. `None` is the
/// api route's upsert, unchanged. Returns the rows written (inserted or
/// updated; an identical or refused row is not); the api route reports the
/// rows posted instead.
///
/// No changed-rows-only form (plan 3a.9): LDBWS visits about 255 of 560
/// stations a cycle, so a feed time would mark unvisited stations fresh;
/// `polled_at` keeps advancing per row.
pub async fn upsert_station_samples_on(
    conn: &mut PgConnection,
    samples: &[StationSample],
    guard: Option<&str>,
) -> sqlx::Result<u64> {
    if samples.is_empty() {
        return Ok(0);
    }
    let batch = last_per_key(samples, |sample| normalize_code(&sample.crs));
    let crs: Vec<String> = batch.iter().map(|s| normalize_code(&s.crs)).collect();
    let polled_at: Vec<DateTime<Utc>> = batch.iter().map(|s| s.polled_at).collect();
    let departures: Vec<serde_json::Value> = batch
        .iter()
        .map(|s| serde_json::to_value(&s.departures))
        .collect::<Result<_, _>>()
        .map_err(encode_error)?;
    // One Postgres array literal per row (UNNEST cannot take a
    // two-dimensional array of ragged rows). See `board_tiplocs`.
    let tiplocs: Vec<String> = batch
        .iter()
        .map(|s| pg_text_array_literal(&board_tiplocs(&s.departures)))
        .collect();

    // `polled_at` is the sample's own age, read per station, so it must
    // advance on every poll even when the board is unchanged. The narrowest
    // write that allows: skip the row only when NOTHING differs, and when
    // only `polled_at` moved, carry the stored `departures` value over
    // (`ELSE station_samples.departures`) rather than rewriting an equal
    // one -- Postgres then reuses the existing TOAST chunks instead of
    // writing new ones and leaving the old ones dead, and the update stays
    // HOT (no indexed column changes).
    let sql = format!(
        r"
        INSERT INTO station_samples (crs, polled_at, departures, tiplocs)
        SELECT crs, polled_at, departures, tiplocs::text[]
        FROM UNNEST($1::text[], $2::timestamptz[], $3::jsonb[], $4::text[])
            AS i(crs, polled_at, departures, tiplocs)
        ON CONFLICT (crs) DO UPDATE SET
            polled_at  = EXCLUDED.polled_at,
            departures = CASE
                WHEN station_samples.departures IS DISTINCT FROM EXCLUDED.departures
                THEN EXCLUDED.departures
                ELSE station_samples.departures
            END,
            tiplocs    = EXCLUDED.tiplocs
        WHERE (station_samples.polled_at, station_samples.departures, station_samples.tiplocs)
              IS DISTINCT FROM (EXCLUDED.polled_at, EXCLUDED.departures, EXCLUDED.tiplocs){}
        ",
        and_guard(guard)
    );
    Ok(sqlx::query(&sql)
        .bind(&crs)
        .bind(&polled_at)
        .bind(&departures)
        .bind(&tiplocs)
        .execute(conn)
        .await?
        .rows_affected())
}

/// The distinct TIPLOCs a board's rows are for, from their serviceIDs
/// (`common::service_id_tiploc`), sorted and upper-cased. Stored as
/// `station_samples.tiplocs` so a stop at a sub-CRS TIPLOC (PADTLL -> PDX)
/// can find the main station's board -- see
/// `migrations/20260928163000_station_samples_tiplocs.sql`.
fn board_tiplocs(departures: &[common::StationDeparture]) -> Vec<String> {
    let mut tiplocs: Vec<String> = departures
        .iter()
        .filter_map(|d| common::service_id_tiploc(&d.service_id))
        .map(str::to_ascii_uppercase)
        .collect();
    tiplocs.sort();
    tiplocs.dedup();
    tiplocs
}

/// `{"A","B"}`. Only for values already restricted to ASCII alphanumerics
/// (TIPLOCs from [`board_tiplocs`]), so no element needs escaping beyond the
/// quotes.
fn pg_text_array_literal(values: &[String]) -> String {
    let quoted: Vec<String> = values.iter().map(|v| format!("\"{v}\"")).collect();
    format!("{{{}}}", quoted.join(","))
}

/// Upserts a batch of per-(crs, operator) full-coverage rows. No
/// history -- wholesale-replaced per producer resolution cycle, same
/// rationale as `upsert_station_samples`. Written by
/// `post_station_full_coverage_samples` (Task 5), a future
/// full-coverage-consumer's real caller once it exists (not built by this
/// plan).
pub async fn upsert_station_full_coverage_samples(
    pool: &PgPool,
    samples: &[StationFullCoverageSample],
) -> Result<u64> {
    let mut conn = pool.acquire().await?;
    upsert_station_full_coverage_samples_on(&mut conn, samples, RowWrites::EveryRow(None)).await?;
    Ok(samples.len() as u64)
}

/// How an `_on` snapshot writer treats a row whose content is unchanged
/// (plan 3a.9, `INGEST_WRITER_CHANGED_ROWS_ONLY`).
#[derive(Clone, Copy, Debug)]
pub enum RowWrites<'a> {
    /// Today's write: the row's own time advances on every snapshot (only
    /// that column when the content is unchanged), with the guard (the
    /// writer's observed-time guard on the row's time; `None` for the api
    /// route) ANDed into the `WHERE`.
    EveryRow(Option<&'a str>),
    /// Changed rows only: a row whose content is unchanged is not written,
    /// so its own time stays at the snapshot that last changed it, and
    /// readers derive its age with [`feed_observed_at_sql`]. The guard must
    /// compare against that same derivation (the writer's
    /// `observed::derived_guard`), since a skipped newer snapshot no longer
    /// advances the row's own time.
    ChangedOnly(&'a str),
}

/// [`upsert_station_full_coverage_samples`] on `conn`; `writes` chooses
/// today's per-row `resolved_at` advance (the api route is
/// `EveryRow(None)`) or changed rows only (plan 3a.9). See
/// [`upsert_station_samples_on`]. Returns the rows written.
pub async fn upsert_station_full_coverage_samples_on(
    conn: &mut PgConnection,
    samples: &[StationFullCoverageSample],
    writes: RowWrites<'_>,
) -> sqlx::Result<u64> {
    if samples.is_empty() {
        return Ok(0);
    }
    let batch = last_per_key(samples, |sample| {
        (normalize_code(&sample.crs), sample.operator.clone())
    });
    let crs: Vec<String> = batch.iter().map(|s| normalize_code(&s.crs)).collect();
    let operators: Vec<&str> = batch.iter().map(|s| s.operator.as_str()).collect();
    let resolved_at: Vec<DateTime<Utc>> = batch.iter().map(|s| s.resolved_at).collect();
    let stats: Vec<serde_json::Value> = batch
        .iter()
        .map(|s| serde_json::to_value(&s.stats))
        .collect::<Result<_, _>>()
        .map_err(encode_error)?;

    // EveryRow, the same shape as `upsert_station_samples`: `resolved_at` is
    // the row's own age and must advance each cycle; an identical row is
    // skipped and an unchanged `stats` value is carried over, not
    // rewritten. ChangedOnly: an unchanged `stats` skips the row.
    let sql = match writes {
        RowWrites::EveryRow(guard) => format!(
            r"
        INSERT INTO station_full_coverage_samples (crs, operator, resolved_at, stats)
        SELECT * FROM UNNEST($1::text[], $2::text[], $3::timestamptz[], $4::jsonb[])
        ON CONFLICT (crs, operator) DO UPDATE SET
            resolved_at = EXCLUDED.resolved_at,
            stats       = CASE
                WHEN station_full_coverage_samples.stats IS DISTINCT FROM EXCLUDED.stats
                THEN EXCLUDED.stats
                ELSE station_full_coverage_samples.stats
            END
        WHERE (station_full_coverage_samples.resolved_at, station_full_coverage_samples.stats)
              IS DISTINCT FROM (EXCLUDED.resolved_at, EXCLUDED.stats){}
        ",
            and_guard(guard)
        ),
        RowWrites::ChangedOnly(guard) => format!(
            r"
        INSERT INTO station_full_coverage_samples (crs, operator, resolved_at, stats)
        SELECT * FROM UNNEST($1::text[], $2::text[], $3::timestamptz[], $4::jsonb[])
        ON CONFLICT (crs, operator) DO UPDATE SET
            resolved_at = EXCLUDED.resolved_at,
            stats       = EXCLUDED.stats
        WHERE station_full_coverage_samples.stats IS DISTINCT FROM EXCLUDED.stats
          AND {guard}
        "
        ),
    };
    Ok(sqlx::query(&sql)
        .bind(&crs)
        .bind(&operators)
        .bind(&resolved_at)
        .bind(&stats)
        .execute(conn)
        .await?
        .rows_affected())
}

/// Pure diff check, factored out of `upsert_tfl_line_status` so it's
/// testable without a database: a `TfL` line's statuses are "changed" if the
/// line is new to us, or if the incoming `statuses` JSON differs from what
/// is stored, ignoring `sample_stats`/`sample_availability` — mirroring the
/// aggregator's own `normalize_for_diff` (`crates/aggregator/src/queries.rs`),
/// which strips the same fields for the same reason: a live delay/
/// cancellation count (and its accompanying availability state) rolls over
/// almost every poll cycle even when nothing about the underlying
/// disruption has changed, and must not participate in change detection or
/// `line_status_history` grows a row every poll cycle. This guard exists
/// ahead of any TfL-sourced line actually populating `sample_stats` (see
/// `crates/poller-tfl/src/dlr`), so it's already in place once one does.
pub fn tfl_statuses_changed(
    existing: Option<&serde_json::Value>,
    incoming: &serde_json::Value,
) -> bool {
    match existing {
        None => true,
        Some(stored) => normalize_for_diff(stored) != normalize_for_diff(incoming),
    }
}

/// Strips `sample_stats`/`sample_availability` (and their Decision-1
/// full-coverage siblings, `full_coverage_stats`/`full_coverage_availability`)
/// from every status entry before comparison. See `tfl_statuses_changed`.
/// The full-coverage pair is stripped symmetrically even though no `TfL` line
/// populates it today (Decision 5: full coverage is scoped to national-rail
/// lines only, out of scope for `TfL`) -- matching this function's own stated
/// rationale for `sample_stats`: strip on principle so a future producer
/// doesn't silently reintroduce spurious `line_status_history` churn.
fn normalize_for_diff(statuses: &serde_json::Value) -> serde_json::Value {
    let mut statuses = statuses.clone();
    if let Some(entries) = statuses.as_array_mut() {
        for entry in entries {
            if let Some(obj) = entry.as_object_mut() {
                obj.remove("sample_stats");
                obj.remove("sample_availability");
                obj.remove("full_coverage_stats");
                obj.remove("full_coverage_availability");
            }
        }
    }
    statuses
}

/// Upserts a batch of `TfL` line-status reports into `line_status` (marked
/// `source = 'tfl'`), appending a `line_status_history` snapshot for each
/// line whose statuses actually changed, and deleting any `TfL` row missing
/// from this batch.
///
/// The whole batch is one transaction — unlike `upsert_incidents`, which
/// chunks to bound its lock-hold window, this is ~20 rows once every 300s.
///
/// An empty batch is a no-op rather than a mass delete: "`TfL` returned
/// nothing" is a fault, not an instruction to forget every line. The poller
/// refuses to post one either (belt and braces, since this is the side that
/// would do the damage).
///
/// **Ownership guard.** `line_status.line_id` is only `TEXT PRIMARY KEY` --
/// nothing in the schema stops a `TfL` line id (`crates/poller-tfl`'s own
/// naming, e.g. `"victoria"`) from colliding with an `aggregator`-owned
/// line id (derived from `lines/*.toml` file stems). The two writers'
/// naming schemes staying disjoint is a CONVENTION, not an enforced
/// constraint (see `20260822120000_line_status_source.sql`'s own migration
/// comment, which introduced `source` for exactly this reason but as a
/// plain unindexed column, not part of the key). Before this fix, a
/// collision was invisible: `ON CONFLICT (line_id) DO UPDATE SET ...
/// source = 'tfl'` would silently steal an `aggregator`-owned row -- and
/// the "is this line changed" read just above only checked
/// `source = 'tfl'` rows, so a colliding `aggregator` row read back as "no
/// existing `TfL` row" (`existing = None`) rather than "an existing row I
/// must not touch", making the theft look like an ordinary first-write.
/// Now: (1) the pre-write read checks the row's actual owner regardless of
/// source, and bails loudly (`anyhow::bail!`, aborting the whole batch's
/// transaction) the moment it finds a same-`line_id` row owned by anyone
/// other than `'tfl'`; (2) the `ON CONFLICT DO UPDATE` itself carries a
/// `WHERE line_status.source = 'tfl'` guard so a same-`line_id` row
/// created by a concurrent `aggregator` write, in the window between that
/// read and this statement, is refused rather than overwritten; and (3) a
/// resulting zero-rows-affected write (only reachable via that race, since
/// the pre-write read already ruled out the non-racy case) is itself
/// treated as the same loud failure, not silently ignored.
pub async fn upsert_tfl_line_status(pool: &PgPool, reports: &[LineStatusReport]) -> Result<u64> {
    if reports.is_empty() {
        return Ok(0);
    }
    let mut tx = pool.begin().await?;
    let applied = write_tfl_line_status(&mut tx, reports, None, true).await?;
    record_ingest(&mut tx, "tfl", None).await?;
    tx.commit().await?;
    Ok(applied.written)
}

/// A `TfL` report whose `line_id` is already owned by another source
/// (`line_status.source`): see [`upsert_tfl_line_status`]'s ownership
/// guard. Aborts the whole batch. Typed, so the ingest-writer can tell this
/// data fault from a database failure.
#[derive(Debug)]
pub struct TflLineOwnedElsewhere {
    pub line_id: String,
    /// The other owner, when the pre-write read saw it; `None` when the row
    /// appeared concurrently (the write affected no row).
    pub owner: Option<String>,
}

impl std::fmt::Display for TflLineOwnedElsewhere {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.owner {
            Some(owner) => write!(
                f,
                "refusing to upsert TfL line status for line_id {:?}: that line_id is \
                 already owned by source {owner:?}, not 'tfl' -- this is a naming collision \
                 between two independent line-id schemes (see upsert_tfl_line_status's \
                 doc comment), not a legitimate TfL update",
                self.line_id
            ),
            None => write!(
                f,
                "refusing to upsert TfL line status for line_id {:?}: the write affected no \
                 rows, which only happens when a same-line_id row owned by a different source \
                 was created concurrently after this function's own ownership check -- \
                 aborting rather than silently no-op'ing what should have been an insert or \
                 update",
                self.line_id
            ),
        }
    }
}

impl std::error::Error for TflLineOwnedElsewhere {}

/// What [`upsert_tfl_line_status_observed`] did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct TflLineStatusApplied {
    /// Lines written (inserted or updated).
    pub written: u64,
    /// Lines the ordering guard refused: the stored row is from a newer
    /// snapshot. Skipped, with no history row.
    pub skipped_older: Vec<String>,
    /// `line_status_history` rows appended.
    pub history: u64,
    /// `TfL` lines deleted because they left the feed.
    pub pruned: u64,
}

/// [`upsert_tfl_line_status`] for the ingest-writer's `tfl-line-status/1`
/// handler (ingest plan 3c.1, decision D13), inside the caller's
/// transaction `conn` (which also holds the entry's `ingest_dedup` row).
///
/// The differences, all from `observed_at` (the envelope's `produced_at`,
/// already clamped to the writer's `now() + 2 min`):
///
/// - `line_status.computed_at`, `line_status.source_updated_at` and the
///   new `line_status_history.computed_at` are `observed_at`, not `NOW()`,
///   so a snapshot applied an hour late is stamped with the time it was
///   true.
/// - **The ordering guard** on `source_updated_at`: a line whose stored row
///   is from a newer snapshot is left alone (`EXCLUDED.source_updated_at >=
///   source_updated_at`; a `NULL`, from the api's route or before the
///   column, counts as older; a row stamped more than 2 minutes in the
///   future is overwritten, see `ingest_writer::observed::guard`). Such a
///   line is reported in [`TflLineStatusApplied::skipped_older`] and writes
///   no history row. It is not mistaken for the other-source refusal,
///   which still aborts with [`TflLineOwnedElsewhere`].
/// - The prune of lines that left the feed runs only when `prune` (the
///   snapshot is whole, not one part of a split one) and spares rows from a
///   newer snapshot, so an older snapshot after a newer one changes
///   neither table.
/// - Freshness (`ingest_freshness('tfl')`) is `observed_at`, never moving
///   backwards ([`record_ingest`]).
///
/// An empty batch writes nothing, as in [`upsert_tfl_line_status`].
pub async fn upsert_tfl_line_status_observed(
    conn: &mut PgConnection,
    reports: &[LineStatusReport],
    observed_at: DateTime<Utc>,
    prune: bool,
) -> Result<TflLineStatusApplied> {
    if reports.is_empty() {
        return Ok(TflLineStatusApplied::default());
    }
    let applied = write_tfl_line_status(conn, reports, Some(observed_at), prune).await?;
    record_ingest(conn, "tfl", Some(observed_at)).await?;
    Ok(applied)
}

/// The shared body of the two `TfL` upserts. `observed_at` `None` is the
/// api route's behaviour (stamped `NOW()`, no ordering guard, always
/// pruned); `Some` is the stream writer's (see
/// [`upsert_tfl_line_status_observed`]).
#[expect(
    clippy::too_many_lines,
    reason = "long but linear; splitting it would scatter its shared state across helpers"
)]
async fn write_tfl_line_status(
    conn: &mut PgConnection,
    reports: &[LineStatusReport],
    observed_at: Option<DateTime<Utc>>,
    prune: bool,
) -> Result<TflLineStatusApplied> {
    // F2: three statements per batch (read owners, upsert, append history)
    // instead of a SELECT and an INSERT per line. A repeated line_id keeps
    // its LAST report, as the old sequential loop's final write did (one
    // multi-row upsert cannot touch the same row twice).
    let batch = last_per_key(reports, |report| report.id.clone());
    let ids: Vec<&str> = batch.iter().map(|r| r.id.as_str()).collect();
    let statuses: Vec<serde_json::Value> = batch
        .iter()
        .map(|r| serde_json::to_value(&r.statuses))
        .collect::<std::result::Result<_, _>>()?;
    let names: Vec<&str> = batch.iter().map(|r| r.name.as_str()).collect();
    let mode_names: Vec<&str> = batch.iter().map(|r| r.mode_name.as_str()).collect();
    // `operators` is a text[] per line; ragged arrays cannot ride in one
    // text[][] parameter, so each travels as a JSON array.
    let operators: Vec<serde_json::Value> = batch
        .iter()
        .map(|r| serde_json::Value::from(r.operators.clone()))
        .collect();

    let existing_rows: Vec<(String, String, Option<serde_json::Value>)> = sqlx::query_as(
        "SELECT line_id, source, CASE WHEN source = 'tfl' THEN statuses ELSE NULL END \
         FROM line_status WHERE line_id = ANY($1)",
    )
    .bind(&ids)
    .fetch_all(&mut *conn)
    .await?;
    if let Some((line_id, owner, _)) = existing_rows.iter().find(|(_, owner, _)| owner != "tfl") {
        return Err(TflLineOwnedElsewhere {
            line_id: line_id.clone(),
            owner: Some(owner.clone()),
        }
        .into());
    }
    let existing: HashMap<&str, &serde_json::Value> = existing_rows
        .iter()
        .filter_map(|(line_id, _, statuses)| statuses.as_ref().map(|s| (line_id.as_str(), s)))
        .collect();

    // `$6` is the observed time (NULL on the api's route): it stamps
    // `computed_at` instead of NOW(), fills `source_updated_at`, and turns
    // on the ordering guard (`ingest_writer::observed::guard`'s fragment on
    // `source_updated_at`). The api's route leaves `source_updated_at` as
    // it is.
    let written: Vec<String> = sqlx::query_scalar(
        r"
        INSERT INTO line_status
            (line_id, name, mode_name, operators, statuses, computed_at, source, source_updated_at)
        SELECT i.line_id, i.name, i.mode_name,
               ARRAY(SELECT jsonb_array_elements_text(i.operators)), i.statuses,
               COALESCE($6::timestamptz, NOW()), 'tfl', $6::timestamptz
          FROM UNNEST($1::text[], $2::text[], $3::text[], $4::jsonb[], $5::jsonb[])
               AS i(line_id, name, mode_name, operators, statuses)
        -- `computed_at` is served per line as the status's own
        -- timestamp, so every TfL row in the feed is still written each
        -- poll. The narrowing: an unchanged `statuses` value is carried
        -- over (`ELSE line_status.statuses`) instead of rewritten, so
        -- its TOAST chunks are reused rather than duplicated and left
        -- dead (prod: 9770 dead vs 614 live TOAST tuples in 20 minutes).
        ON CONFLICT (line_id) DO UPDATE SET
            name        = EXCLUDED.name,
            mode_name   = EXCLUDED.mode_name,
            operators   = EXCLUDED.operators,
            statuses    = CASE
                WHEN line_status.statuses IS DISTINCT FROM EXCLUDED.statuses
                THEN EXCLUDED.statuses
                ELSE line_status.statuses
            END,
            computed_at = EXCLUDED.computed_at,
            source      = 'tfl',
            source_updated_at = COALESCE(EXCLUDED.source_updated_at, line_status.source_updated_at)
        WHERE line_status.source = 'tfl'
          AND ($6::timestamptz IS NULL
               OR line_status.source_updated_at IS NULL
               OR EXCLUDED.source_updated_at >= line_status.source_updated_at
               OR line_status.source_updated_at > now() + interval '2 min')
        RETURNING line_id
        ",
    )
    .bind(&ids)
    .bind(&names)
    .bind(&mode_names)
    .bind(&operators)
    .bind(&statuses)
    .bind(observed_at)
    .fetch_all(&mut *conn)
    .await?;

    let written: std::collections::HashSet<&str> = written.iter().map(String::as_str).collect();
    let mut skipped_older = Vec::new();
    if written.len() != batch.len() {
        let missing: Vec<&str> = ids
            .iter()
            .copied()
            .filter(|id| !written.contains(id))
            .collect();
        // A TfL-owned row the upsert left alone was refused by the ordering
        // guard (only on the stream path); anything else is the concurrent
        // other-source race the ownership read could not see.
        let tfl_owned: Vec<String> = if observed_at.is_some() {
            sqlx::query_scalar(
                "SELECT line_id FROM line_status WHERE line_id = ANY($1) AND source = 'tfl'",
            )
            .bind(&missing)
            .fetch_all(&mut *conn)
            .await?
        } else {
            Vec::new()
        };
        if let Some(refused) = missing
            .iter()
            .find(|id| !tfl_owned.iter().any(|owned| owned == **id))
        {
            return Err(TflLineOwnedElsewhere {
                line_id: (*refused).to_owned(),
                owner: None,
            }
            .into());
        }
        skipped_older = missing.iter().map(|id| (*id).to_owned()).collect();
        tracing::info!(
            skipped = skipped_older.len(),
            lines = ?skipped_older,
            "TfL lines left alone: their stored status is from a newer snapshot"
        );
    }

    let (changed_ids, changed_statuses): (Vec<&str>, Vec<&serde_json::Value>) = ids
        .iter()
        .zip(&statuses)
        .filter(|(id, _)| written.contains(**id))
        .filter(|(id, incoming)| tfl_statuses_changed(existing.get(**id).copied(), incoming))
        .map(|(id, incoming)| (*id, incoming))
        .unzip();
    let mut history = 0;
    if !changed_ids.is_empty() {
        history = sqlx::query(
            "INSERT INTO line_status_history (line_id, statuses, computed_at) \
             SELECT line_id, statuses, COALESCE($3::timestamptz, NOW()) \
               FROM UNNEST($1::text[], $2::jsonb[]) WITH ORDINALITY AS h(line_id, statuses, ord) \
              ORDER BY ord",
        )
        .bind(&changed_ids)
        .bind(&changed_statuses)
        .bind(observed_at)
        .execute(&mut *conn)
        .await?
        .rows_affected();
    }

    // A TfL line that leaves the feed (a renamed id, a withdrawn service)
    // has no other way of disappearing — `/public/lines` derives its TfL
    // entries from exactly these rows. The aggregator's
    // `prune_removed_lines` is the same idea from the other side of the
    // fence; each writer prunes only what it owns. On the stream path a
    // row from a newer snapshot is spared, like the upsert above.
    let mut pruned = 0;
    if prune {
        let ids: Vec<&str> = reports.iter().map(|r| r.id.as_str()).collect();
        pruned = sqlx::query(
            "DELETE FROM line_status WHERE source = 'tfl' AND NOT (line_id = ANY($1)) \
               AND ($2::timestamptz IS NULL \
                    OR source_updated_at IS NULL \
                    OR source_updated_at <= $2::timestamptz \
                    OR source_updated_at > now() + interval '2 min')",
        )
        .bind(&ids)
        .bind(observed_at)
        .execute(&mut *conn)
        .await?
        .rows_affected();
        if pruned > 0 {
            tracing::info!(pruned, "removed TfL lines no longer present in the feed");
        }
    }

    Ok(TflLineStatusApplied {
        written: written.len() as u64,
        skipped_older,
        history,
        pruned,
    })
}

/// Upserts full-coverage stats rows, one per `(line_id, service_date)`.
///
/// **Keyed by day since 2026-09-27** (was `line_id` alone): a line's row
/// for each rail day is kept, not overwritten by the next day's, so a day's
/// final ("available" or `partial`) row can be audited afterwards -- the
/// 2026-09-25/26 rows that restarts had corrupted could not be. Writing the
/// current day's row replaces only that day's.
///
/// **Skip if unchanged** (DB review F3): the consumer posts every line
/// every 60s whether or not anything moved, and an unconditional
/// `DO UPDATE` rewrote every row each time (3,645 updates for 253 live
/// rows in 20 minutes). The `WHERE ... IS DISTINCT FROM` leaves an
/// identical row alone, so `updated_at` now means "last changed", not
/// "last posted". Returns the number of rows actually written.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    reason = "per-day train counts and the stats version are small"
)]
pub async fn upsert_full_coverage_line_stats(
    pool: &PgPool,
    rows: &[common::FullCoverageLineStatsRow],
) -> Result<u64> {
    let mut conn = pool.acquire().await?;
    Ok(upsert_full_coverage_line_stats_on(&mut conn, rows, None).await?)
}

/// The ingest-writer's ordering for [`upsert_full_coverage_line_stats_on`]
/// (plan 3a.5/3a.6, D13): the `/1` body carries no time, so each row gets
/// `source_updated_at` (the entry's clamped `produced_at`), and `guard`
/// (the writer's observed-time guard on
/// `full_coverage_line_stats.source_updated_at`) is ANDed into the `WHERE`.
#[derive(Clone, Copy, Debug)]
pub struct SourceOrdering<'a> {
    pub source_updated_at: DateTime<Utc>,
    pub guard: &'a str,
}

/// The stats columns differ: the F3 skip-if-unchanged test, and what makes
/// `updated_at` mean "last changed".
const LINE_STATS_CHANGED: &str =
    "(full_coverage_line_stats.availability, full_coverage_line_stats.total,
               full_coverage_line_stats.delayed, full_coverage_line_stats.cancelled,
               full_coverage_line_stats.skipped, full_coverage_line_stats.avg_delay_minutes,
               full_coverage_line_stats.partial, full_coverage_line_stats.cancelled_explicit,
               full_coverage_line_stats.cancelled_presumed, full_coverage_line_stats.pending,
               full_coverage_line_stats.unobserved, full_coverage_line_stats.stats_version)
            IS DISTINCT FROM
              (EXCLUDED.availability, EXCLUDED.total, EXCLUDED.delayed, EXCLUDED.cancelled,
               EXCLUDED.skipped, EXCLUDED.avg_delay_minutes, EXCLUDED.partial,
               EXCLUDED.cancelled_explicit, EXCLUDED.cancelled_presumed, EXCLUDED.pending,
               EXCLUDED.unobserved, EXCLUDED.stats_version)";

/// [`upsert_full_coverage_line_stats`] on `conn`. With `ordering` (the
/// ingest-writer), each row's `source_updated_at` is set and advances on
/// every snapshot even when the stats are unchanged (that column alone, a
/// HOT update), so the guard always compares against the newest snapshot
/// applied; `updated_at` still moves only when the stats change. `None` is
/// the api route's upsert, unchanged: it leaves `source_updated_at` alone
/// (`NULL` on a new row, which the guard counts as older).
pub async fn upsert_full_coverage_line_stats_on(
    conn: &mut PgConnection,
    rows: &[common::FullCoverageLineStatsRow],
    ordering: Option<SourceOrdering<'_>>,
) -> sqlx::Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }
    // F2: one UNNEST upsert for the whole batch instead of one statement per
    // (line, date). A key repeated in one batch keeps its LAST row, as the
    // old loop's final write did (one statement cannot touch a row twice).
    let batch = last_per_key(rows, |row| (row.line_id.clone(), row.service_date));
    let mut line_ids = Vec::with_capacity(batch.len());
    let mut service_dates = Vec::with_capacity(batch.len());
    let mut availability = Vec::with_capacity(batch.len());
    let mut total = Vec::with_capacity(batch.len());
    let mut delayed = Vec::with_capacity(batch.len());
    let mut cancelled = Vec::with_capacity(batch.len());
    let mut skipped = Vec::with_capacity(batch.len());
    let mut avg_delay = Vec::with_capacity(batch.len());
    let mut partial = Vec::with_capacity(batch.len());
    let mut cancelled_explicit = Vec::with_capacity(batch.len());
    let mut cancelled_presumed = Vec::with_capacity(batch.len());
    let mut pending = Vec::with_capacity(batch.len());
    let mut unobserved = Vec::with_capacity(batch.len());
    let mut stats_versions = Vec::with_capacity(batch.len());
    for row in batch {
        // The windowed-stats breakdown (2026-09-27). A row without one is
        // the legacy whole-population method: the breakdown columns keep
        // their defaults and `stats_version` is 1.
        let breakdown = row.breakdown.clone().unwrap_or_default();
        let stats_version = match (&row.breakdown, row.stats_version) {
            (_, Some(version)) => version as i16,
            (Some(_), None) => common::full_coverage_window::FULL_COVERAGE_STATS_VERSION as i16,
            (None, None) => 1,
        };
        line_ids.push(row.line_id.as_str());
        service_dates.push(row.service_date);
        availability.push(row.availability.as_str());
        total.push(row.stats.total as i32);
        delayed.push(row.stats.delayed as i32);
        cancelled.push(row.stats.cancelled as i32);
        skipped.push(row.stats.skipped as i32);
        avg_delay.push(row.stats.avg_delay_minutes);
        partial.push(row.partial);
        cancelled_explicit.push(breakdown.cancelled_explicit as i32);
        cancelled_presumed.push(breakdown.cancelled_presumed as i32);
        pending.push(breakdown.pending as i32);
        unobserved.push(breakdown.unobserved as i32);
        stats_versions.push(stats_version);
    }
    // `source_updated_at` is `$15` (NULL for the api route): it advances a
    // row whose stats are unchanged only when given, and then only itself.
    // `updated_at` keeps meaning "the stats last changed".
    let sql = format!(
        r"
        INSERT INTO full_coverage_line_stats
            (line_id, service_date, availability, total, delayed, cancelled, skipped,
             avg_delay_minutes, partial, cancelled_explicit, cancelled_presumed, pending,
             unobserved, stats_version, updated_at, source_updated_at)
        SELECT *, now(), $15::timestamptz
          FROM UNNEST($1::text[], $2::date[], $3::text[], $4::int4[], $5::int4[], $6::int4[],
                      $7::int4[], $8::float8[], $9::bool[], $10::int4[], $11::int4[],
                      $12::int4[], $13::int4[], $14::int2[])
        ON CONFLICT (line_id, service_date) DO UPDATE SET
            availability       = EXCLUDED.availability,
            total              = EXCLUDED.total,
            delayed            = EXCLUDED.delayed,
            cancelled          = EXCLUDED.cancelled,
            skipped            = EXCLUDED.skipped,
            avg_delay_minutes  = EXCLUDED.avg_delay_minutes,
            partial            = EXCLUDED.partial,
            cancelled_explicit = EXCLUDED.cancelled_explicit,
            cancelled_presumed = EXCLUDED.cancelled_presumed,
            pending            = EXCLUDED.pending,
            unobserved         = EXCLUDED.unobserved,
            stats_version      = EXCLUDED.stats_version,
            updated_at         = CASE WHEN {changed}
                                      THEN EXCLUDED.updated_at
                                      ELSE full_coverage_line_stats.updated_at END,
            source_updated_at  = COALESCE(EXCLUDED.source_updated_at,
                                          full_coverage_line_stats.source_updated_at)
        WHERE ({changed}
               OR (EXCLUDED.source_updated_at IS NOT NULL
                   AND full_coverage_line_stats.source_updated_at
                       IS DISTINCT FROM EXCLUDED.source_updated_at)){guard}
        ",
        changed = LINE_STATS_CHANGED,
        guard = and_guard(ordering.map(|ordering| ordering.guard)),
    );
    let result = sqlx::query(&sql)
        .bind(&line_ids)
        .bind(&service_dates)
        .bind(&availability)
        .bind(&total)
        .bind(&delayed)
        .bind(&cancelled)
        .bind(&skipped)
        .bind(&avg_delay)
        .bind(&partial)
        .bind(&cancelled_explicit)
        .bind(&cancelled_presumed)
        .bind(&pending)
        .bind(&unobserved)
        .bind(&stats_versions)
        .bind(ordering.map(|ordering| ordering.source_updated_at))
        .execute(conn)
        .await?;
    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_derived_resolved_at_is_the_feed_observed_at_fragment() {
        assert_eq!(
            STATION_FULL_COVERAGE_RESOLVED_AT_SQL,
            feed_observed_at_sql(
                "station_full_coverage_samples.resolved_at",
                sources::STATION_FULL_COVERAGE_SAMPLES
            )
        );
    }

    #[test]
    #[should_panic(expected = "bad source")]
    fn a_source_that_could_break_out_of_the_literal_is_refused() {
        let _ = feed_observed_at_sql("t.c", "x' OR '1");
    }

    #[test]
    fn a_line_with_no_stored_row_is_always_changed() {
        assert!(tfl_statuses_changed(None, &serde_json::json!([])));
    }

    #[test]
    fn identical_statuses_are_not_changed() {
        let stored = serde_json::json!([{ "severity": 10, "reason": "Good Service" }]);
        let incoming = serde_json::json!([{ "severity": 10, "reason": "Good Service" }]);
        assert!(!tfl_statuses_changed(Some(&stored), &incoming));
    }

    #[test]
    fn a_new_severity_is_changed() {
        let stored = serde_json::json!([{ "severity": 10, "reason": "Good Service" }]);
        let incoming =
            serde_json::json!([{ "severity": 6, "reason": "Signal failure at Oxford Circus" }]);
        assert!(tfl_statuses_changed(Some(&stored), &incoming));
    }

    #[test]
    fn a_second_simultaneous_status_is_changed() {
        // TfL routinely reports several statuses on one line at once — a
        // planned closure alongside a live disruption. Gaining or losing
        // one is a change even if the first entry is untouched.
        let stored = serde_json::json!([{ "severity": 4, "reason": "Planned engineering work" }]);
        let incoming = serde_json::json!([
            { "severity": 4, "reason": "Planned engineering work" },
            { "severity": 6, "reason": "Signal failure at Oxford Circus" },
        ]);
        assert!(tfl_statuses_changed(Some(&stored), &incoming));
    }

    #[test]
    fn tfl_statuses_changed_ignores_sample_stats_only_differences() {
        let existing = serde_json::json!([{
            "severity": "GoodService",
            "reason": "Good Service",
            "validity": { "from_date": "2026-08-22T02:00:00Z", "to_date": null, "is_now": true },
            "data_quality": "tfl",
            "sample_stats": { "total": 40, "delayed": 3, "cancelled": 0, "skipped": 0, "avg_delay_minutes": 1.2 }
        }]);
        let incoming = serde_json::json!([{
            "severity": "GoodService",
            "reason": "Good Service",
            "validity": { "from_date": "2026-08-22T02:00:00Z", "to_date": null, "is_now": true },
            "data_quality": "tfl",
            "sample_stats": { "total": 41, "delayed": 5, "cancelled": 1, "skipped": 0, "avg_delay_minutes": 2.4 }
        }]);
        assert!(!tfl_statuses_changed(Some(&existing), &incoming));
    }

    #[test]
    fn tfl_statuses_changed_ignores_sample_availability_only_differences() {
        let existing = serde_json::json!([{
            "severity": "GoodService",
            "reason": "Good Service",
            "validity": { "from_date": "2026-08-22T02:00:00Z", "to_date": null, "is_now": true },
            "data_quality": "tfl",
            "sample_availability": { "state": "no-coverage" }
        }]);
        let incoming = serde_json::json!([{
            "severity": "GoodService",
            "reason": "Good Service",
            "validity": { "from_date": "2026-08-22T02:00:00Z", "to_date": null, "is_now": true },
            "data_quality": "tfl",
            "sample_availability": { "state": "below-threshold", "observed": 0, "required": 1 }
        }]);
        assert!(!tfl_statuses_changed(Some(&existing), &incoming));
    }

    #[test]
    fn tfl_statuses_changed_ignores_full_coverage_field_only_differences() {
        let existing = serde_json::json!([{
            "severity": "GoodService",
            "reason": "Good Service",
            "validity": { "from_date": "2026-08-22T02:00:00Z", "to_date": null, "is_now": true },
            "data_quality": "tfl",
            "full_coverage_stats": { "total": 40, "delayed": 3, "cancelled": 0, "skipped": 0, "avg_delay_minutes": 1.2 },
            "full_coverage_availability": { "state": "available" }
        }]);
        let incoming = serde_json::json!([{
            "severity": "GoodService",
            "reason": "Good Service",
            "validity": { "from_date": "2026-08-22T02:00:00Z", "to_date": null, "is_now": true },
            "data_quality": "tfl",
            "full_coverage_stats": { "total": 41, "delayed": 5, "cancelled": 1, "skipped": 0, "avg_delay_minutes": 2.4 },
            "full_coverage_availability": { "state": "pending" }
        }]);
        assert!(!tfl_statuses_changed(Some(&existing), &incoming));
    }

    #[test]
    fn tfl_statuses_changed_still_true_when_severity_changes_alongside_sample_stats() {
        let existing = serde_json::json!([{
            "severity": "GoodService",
            "reason": "Good Service",
            "validity": { "from_date": "2026-08-22T02:00:00Z", "to_date": null, "is_now": true },
            "data_quality": "tfl",
            "sample_stats": { "total": 40, "delayed": 3, "cancelled": 0, "skipped": 0, "avg_delay_minutes": 1.2 }
        }]);
        let incoming = serde_json::json!([{
            "severity": "MinorDelays",
            "reason": "Minor Delays",
            "validity": { "from_date": "2026-08-22T02:00:00Z", "to_date": null, "is_now": true },
            "data_quality": "tfl",
            "sample_stats": { "total": 41, "delayed": 5, "cancelled": 1, "skipped": 0, "avg_delay_minutes": 2.4 }
        }]);
        assert!(tfl_statuses_changed(Some(&existing), &incoming));
    }
}

/// DB-gated: the `TfL` ownership guard, and (DB review 2026-09-27 F2)
/// the batched `TfL` and full-coverage upserts' no-op guards.
#[cfg(test)]
mod db_tests {
    use super::*;
    use crate::freshness::last_tfl_line_status_fetch;
    use crate::test_support::connect as test_pool;

    /// Regression test for the Signal Box Audit Low finding on
    /// `upsert_tfl_line_status`: `line_id` is only `TEXT PRIMARY KEY`, so
    /// nothing at the schema level stops a `TfL` line id from colliding with
    /// an `aggregator`-owned one. Before the ownership guard, a colliding
    /// `TfL` post would silently `ON CONFLICT (line_id) DO UPDATE SET ...
    /// source = 'tfl'`, stealing the aggregator's row -- this proves it
    /// now fails loudly (`Err`, whole batch rolled back by the caller
    /// never committing) and leaves the aggregator's row untouched.
    #[tokio::test]
    #[ignore = "requires a live database; run with `cargo test -p ds-store \
                upsert_tfl_line_status_refuses_to_steal_a_non_tfl_owned_row_with_the_same_line_id \
                -- --ignored --test-threads=1`"]
    async fn upsert_tfl_line_status_refuses_to_steal_a_non_tfl_owned_row_with_the_same_line_id() {
        use sqlx::postgres::PgPoolOptions;

        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");

        sqlx::query(
            "INSERT INTO line_status (line_id, name, mode_name, operators, statuses, source) \
             VALUES ('TEST-COLLIDE', 'aggregator owns this', 'national-rail', '{NT}', '[]', \
                     'aggregator') \
             ON CONFLICT (line_id) DO UPDATE SET source = EXCLUDED.source, \
                                                  name = EXCLUDED.name",
        )
        .execute(&pool)
        .await
        .expect("seed a non-TfL-owned row under the colliding line_id");

        let colliding_report = LineStatusReport {
            id: "TEST-COLLIDE".to_string(),
            name: "a TfL line that happens to share this id".to_string(),
            mode_name: "tube".to_string(),
            operators: vec!["TfL".to_string()],
            statuses: vec![],
        };

        let result = upsert_tfl_line_status(&pool, std::slice::from_ref(&colliding_report)).await;
        assert!(
            result.is_err(),
            "an id collision with a non-TfL-owned row must fail loudly, not silently overwrite"
        );

        let (source, name): (String, String) =
            sqlx::query_as("SELECT source, name FROM line_status WHERE line_id = 'TEST-COLLIDE'")
                .fetch_one(&pool)
                .await
                .expect("the aggregator's row must still exist, untouched");
        assert_eq!(
            source, "aggregator",
            "ownership must not have been stolen by the refused TfL write"
        );
        assert_eq!(
            name, "aggregator owns this",
            "the aggregator's own data must not have been overwritten by the refused TfL write"
        );

        sqlx::query("DELETE FROM line_status WHERE line_id = 'TEST-COLLIDE'")
            .execute(&pool)
            .await
            .expect("cleanup fixture row");
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                samples::db_tests -- --ignored --test-threads=1`"]
    async fn tfl_line_status_keeps_computed_at_advancing_without_duplicate_history() {
        let pool = test_pool().await;
        let report = LineStatusReport {
            id: "TEST-GUARD-TFL".to_string(),
            name: "Guard line".to_string(),
            mode_name: "tube".to_string(),
            operators: vec!["TfL".to_string()],
            statuses: vec![],
        };
        upsert_tfl_line_status(&pool, std::slice::from_ref(&report))
            .await
            .unwrap();
        let read = |pool: PgPool| async move {
            sqlx::query_as::<_, (DateTime<Utc>, i64)>(
                "SELECT computed_at, (SELECT COUNT(*) FROM line_status_history \
                                      WHERE line_id = 'TEST-GUARD-TFL') \
                 FROM line_status WHERE line_id = 'TEST-GUARD-TFL'",
            )
            .fetch_one(&pool)
            .await
            .unwrap()
        };
        let (computed_1, history_1) = read(pool.clone()).await;
        upsert_tfl_line_status(&pool, std::slice::from_ref(&report))
            .await
            .unwrap();
        let (computed_2, history_2) = read(pool.clone()).await;
        assert!(computed_2 > computed_1);
        assert_eq!(history_1, history_2);
        assert_eq!(
            last_tfl_line_status_fetch(&pool).await.unwrap(),
            Some(computed_2)
        );
        sqlx::query("DELETE FROM line_status_history WHERE line_id = 'TEST-GUARD-TFL'")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM line_status WHERE line_id = 'TEST-GUARD-TFL'")
            .execute(&pool)
            .await
            .unwrap();
    }

    /// F2: the batched full-coverage stats upsert writes several keys in one
    /// statement, counts only changed rows, and keeps the LAST row of a key
    /// repeated in one batch.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                full_coverage_line_stats_batch -- --ignored --test-threads=1`"]
    async fn full_coverage_line_stats_batch_keeps_the_last_row_per_key() {
        let pool = test_pool().await;
        let cleanup = |pool: PgPool| async move {
            sqlx::query("DELETE FROM full_coverage_line_stats WHERE line_id LIKE 'test-f2-fc-%'")
                .execute(&pool)
                .await
                .unwrap();
        };
        cleanup(pool.clone()).await;
        let row = |line_id: &str, date: &str, total: usize| common::FullCoverageLineStatsRow {
            line_id: line_id.to_string(),
            service_date: date.parse().unwrap(),
            availability: "available".to_string(),
            stats: common::SampleStats {
                total,
                delayed: 1,
                cancelled: 0,
                skipped: 0,
                avg_delay_minutes: 2.5,
            },
            partial: false,
            breakdown: None,
            stats_version: None,
        };
        let batch = vec![
            row("test-f2-fc-a", "2026-09-26", 5),
            row("test-f2-fc-a", "2026-09-27", 6),
            row("test-f2-fc-b", "2026-09-27", 7),
            row("test-f2-fc-b", "2026-09-27", 8),
        ];
        assert_eq!(
            upsert_full_coverage_line_stats(&pool, &batch)
                .await
                .unwrap(),
            3
        );
        assert_eq!(
            upsert_full_coverage_line_stats(&pool, &batch)
                .await
                .unwrap(),
            0
        );
        let totals: Vec<(String, i32)> = sqlx::query_as(
            "SELECT line_id || '/' || service_date, total FROM full_coverage_line_stats \
             WHERE line_id LIKE 'test-f2-fc-%' ORDER BY 1",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            totals,
            vec![
                ("test-f2-fc-a/2026-09-26".to_string(), 5),
                ("test-f2-fc-a/2026-09-27".to_string(), 6),
                ("test-f2-fc-b/2026-09-27".to_string(), 8),
            ]
        );
        cleanup(pool.clone()).await;
    }

    /// F2: the batched `TfL` upsert writes every line in one statement, keeps
    /// each line's operators, appends history only for new or changed lines,
    /// and keeps the LAST report of a `line_id` repeated in one batch.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                tfl_line_status_batch -- --ignored --test-threads=1`"]
    async fn tfl_line_status_batch_writes_each_line_and_history_only_for_changes() {
        let pool = test_pool().await;
        let cleanup = |pool: PgPool| async move {
            sqlx::query("DELETE FROM line_status_history WHERE line_id LIKE 'TEST-F2-TFL-%'")
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("DELETE FROM line_status WHERE line_id LIKE 'TEST-F2-TFL-%'")
                .execute(&pool)
                .await
                .unwrap();
        };
        cleanup(pool.clone()).await;
        let status = |severity: u8| -> Vec<common::LineStatus> {
            serde_json::from_value(serde_json::json!([{
                "severity": severity,
                "reason": format!("severity {severity}"),
                "validity": { "from_date": "2026-09-27T02:00:00Z", "to_date": null, "is_now": true },
                "data_quality": "tfl"
            }]))
            .unwrap()
        };
        let report = |id: &str, severity: u8, operators: &[&str]| LineStatusReport {
            id: id.to_string(),
            name: format!("{id} name"),
            mode_name: "tube".to_string(),
            operators: operators.iter().map(ToString::to_string).collect(),
            statuses: status(severity),
        };
        let history = |pool: PgPool, id: &'static str| async move {
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM line_status_history WHERE line_id = $1",
            )
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap()
        };

        let first = vec![
            report("TEST-F2-TFL-A", 10, &["TfL"]),
            report("TEST-F2-TFL-B", 10, &[]),
        ];
        assert_eq!(upsert_tfl_line_status(&pool, &first).await.unwrap(), 2);
        assert_eq!(history(pool.clone(), "TEST-F2-TFL-A").await, 1);
        assert_eq!(history(pool.clone(), "TEST-F2-TFL-B").await, 1);

        let second = vec![
            report("TEST-F2-TFL-A", 10, &["TfL"]),
            report("TEST-F2-TFL-B", 10, &[]),
            report("TEST-F2-TFL-B", 9, &["TfL", "LO"]),
        ];
        assert_eq!(upsert_tfl_line_status(&pool, &second).await.unwrap(), 2);
        assert_eq!(history(pool.clone(), "TEST-F2-TFL-A").await, 1, "unchanged");
        assert_eq!(
            history(pool.clone(), "TEST-F2-TFL-B").await,
            2,
            "changed once"
        );
        let (operators, severity): (Vec<String>, String) = sqlx::query_as(
            "SELECT operators, statuses->0->>'severity' FROM line_status \
             WHERE line_id = 'TEST-F2-TFL-B'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(operators, vec!["TfL", "LO"]);
        assert_eq!(severity, "9");

        cleanup(pool.clone()).await;
    }
}
