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
use common::{LineStatusReport, StationFullCoverageSample, StationSample};
use sqlx::PgPool;

use crate::freshness::{last_per_key, normalize_code, record_ingest};

pub mod full_coverage_window;
pub mod island_of_ireland;

/// Upserts a batch of station samples (LDBWS departure-board snapshots).
/// No history — this is a point-in-time sample, wholesale-replaced per
/// poll, same rationale as `upsert_stations`/`upsert_tocs`.
pub async fn upsert_station_samples(pool: &PgPool, samples: &[StationSample]) -> Result<u64> {
    if samples.is_empty() {
        return Ok(0);
    }
    let batch = last_per_key(samples, |sample| normalize_code(&sample.crs));
    let crs: Vec<String> = batch.iter().map(|s| normalize_code(&s.crs)).collect();
    let polled_at: Vec<chrono::DateTime<chrono::Utc>> = batch.iter().map(|s| s.polled_at).collect();
    let departures: Vec<serde_json::Value> = batch
        .iter()
        .map(|s| serde_json::to_value(&s.departures))
        .collect::<Result<_, _>>()?;
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
    sqlx::query(
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
              IS DISTINCT FROM (EXCLUDED.polled_at, EXCLUDED.departures, EXCLUDED.tiplocs)
        ",
    )
    .bind(&crs)
    .bind(&polled_at)
    .bind(&departures)
    .bind(&tiplocs)
    .execute(pool)
    .await?;
    Ok(samples.len() as u64)
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
    if samples.is_empty() {
        return Ok(0);
    }
    let batch = last_per_key(samples, |sample| {
        (normalize_code(&sample.crs), sample.operator.clone())
    });
    let crs: Vec<String> = batch.iter().map(|s| normalize_code(&s.crs)).collect();
    let operators: Vec<&str> = batch.iter().map(|s| s.operator.as_str()).collect();
    let resolved_at: Vec<chrono::DateTime<chrono::Utc>> =
        batch.iter().map(|s| s.resolved_at).collect();
    let stats: Vec<serde_json::Value> = batch
        .iter()
        .map(|s| serde_json::to_value(&s.stats))
        .collect::<Result<_, _>>()?;

    // Same shape as `upsert_station_samples`: `resolved_at` is the row's
    // own age and must advance each cycle; an identical row is skipped and
    // an unchanged `stats` value is carried over, not rewritten.
    sqlx::query(
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
              IS DISTINCT FROM (EXCLUDED.resolved_at, EXCLUDED.stats)
        ",
    )
    .bind(&crs)
    .bind(&operators)
    .bind(&resolved_at)
    .bind(&stats)
    .execute(pool)
    .await?;
    Ok(samples.len() as u64)
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
#[expect(
    clippy::too_many_lines,
    reason = "long but linear; splitting it would scatter its shared state across helpers"
)]
pub async fn upsert_tfl_line_status(pool: &PgPool, reports: &[LineStatusReport]) -> Result<u64> {
    if reports.is_empty() {
        return Ok(0);
    }

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

    let mut tx = pool.begin().await?;

    let existing_rows: Vec<(String, String, Option<serde_json::Value>)> = sqlx::query_as(
        "SELECT line_id, source, CASE WHEN source = 'tfl' THEN statuses ELSE NULL END \
         FROM line_status WHERE line_id = ANY($1)",
    )
    .bind(&ids)
    .fetch_all(&mut *tx)
    .await?;
    if let Some((line_id, owner, _)) = existing_rows.iter().find(|(_, owner, _)| owner != "tfl") {
        anyhow::bail!(
            "refusing to upsert TfL line status for line_id {line_id:?}: that line_id is \
             already owned by source {owner:?}, not 'tfl' -- this is a naming collision \
             between two independent line-id schemes (see upsert_tfl_line_status's \
             doc comment), not a legitimate TfL update"
        );
    }
    let existing: HashMap<&str, &serde_json::Value> = existing_rows
        .iter()
        .filter_map(|(line_id, _, statuses)| statuses.as_ref().map(|s| (line_id.as_str(), s)))
        .collect();

    let written: Vec<String> = sqlx::query_scalar(
        r"
        INSERT INTO line_status (line_id, name, mode_name, operators, statuses, computed_at, source)
        SELECT i.line_id, i.name, i.mode_name,
               ARRAY(SELECT jsonb_array_elements_text(i.operators)), i.statuses, NOW(), 'tfl'
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
            computed_at = NOW(),
            source      = 'tfl'
        WHERE line_status.source = 'tfl'
        RETURNING line_id
        ",
    )
    .bind(&ids)
    .bind(&names)
    .bind(&mode_names)
    .bind(&operators)
    .bind(&statuses)
    .fetch_all(&mut *tx)
    .await?;

    if written.len() != batch.len() {
        let written: std::collections::HashSet<&str> = written.iter().map(String::as_str).collect();
        let refused = ids.iter().find(|id| !written.contains(*id));
        anyhow::bail!(
            "refusing to upsert TfL line status for line_id {refused:?}: the write affected no \
             rows, which only happens when a same-line_id row owned by a different source \
             was created concurrently after this function's own ownership check -- \
             aborting rather than silently no-op'ing what should have been an insert or \
             update"
        );
    }

    let (changed_ids, changed_statuses): (Vec<&str>, Vec<&serde_json::Value>) = ids
        .iter()
        .zip(&statuses)
        .filter(|(id, incoming)| tfl_statuses_changed(existing.get(**id).copied(), incoming))
        .map(|(id, incoming)| (*id, incoming))
        .unzip();
    if !changed_ids.is_empty() {
        sqlx::query(
            "INSERT INTO line_status_history (line_id, statuses, computed_at) \
             SELECT line_id, statuses, NOW() \
               FROM UNNEST($1::text[], $2::jsonb[]) WITH ORDINALITY AS h(line_id, statuses, ord) \
              ORDER BY ord",
        )
        .bind(&changed_ids)
        .bind(&changed_statuses)
        .execute(&mut *tx)
        .await?;
    }
    let count = batch.len() as u64;

    record_ingest(&mut tx, "tfl").await?;

    // A TfL line that leaves the feed (a renamed id, a withdrawn service)
    // has no other way of disappearing — `/public/lines` derives its TfL
    // entries from exactly these rows. The aggregator's
    // `prune_removed_lines` is the same idea from the other side of the
    // fence; each writer prunes only what it owns.
    let ids: Vec<&str> = reports.iter().map(|r| r.id.as_str()).collect();
    let pruned =
        sqlx::query("DELETE FROM line_status WHERE source = 'tfl' AND NOT (line_id = ANY($1))")
            .bind(&ids)
            .execute(&mut *tx)
            .await?
            .rows_affected();
    if pruned > 0 {
        tracing::info!(pruned, "removed TfL lines no longer present in the feed");
    }

    tx.commit().await?;
    Ok(count)
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
    let result = sqlx::query(
        r"
        INSERT INTO full_coverage_line_stats
            (line_id, service_date, availability, total, delayed, cancelled, skipped,
             avg_delay_minutes, partial, cancelled_explicit, cancelled_presumed, pending,
             unobserved, stats_version, updated_at)
        SELECT *, now()
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
            updated_at         = EXCLUDED.updated_at
        WHERE (full_coverage_line_stats.availability, full_coverage_line_stats.total,
               full_coverage_line_stats.delayed, full_coverage_line_stats.cancelled,
               full_coverage_line_stats.skipped, full_coverage_line_stats.avg_delay_minutes,
               full_coverage_line_stats.partial, full_coverage_line_stats.cancelled_explicit,
               full_coverage_line_stats.cancelled_presumed, full_coverage_line_stats.pending,
               full_coverage_line_stats.unobserved, full_coverage_line_stats.stats_version)
            IS DISTINCT FROM
              (EXCLUDED.availability, EXCLUDED.total, EXCLUDED.delayed, EXCLUDED.cancelled,
               EXCLUDED.skipped, EXCLUDED.avg_delay_minutes, EXCLUDED.partial,
               EXCLUDED.cancelled_explicit, EXCLUDED.cancelled_presumed, EXCLUDED.pending,
               EXCLUDED.unobserved, EXCLUDED.stats_version)
        ",
    )
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
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}
