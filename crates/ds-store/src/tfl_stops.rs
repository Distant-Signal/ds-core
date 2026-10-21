//! `tfl_stops` and `tfl_stop_crs` (docs/superpowers/specs/2026-10-20-tfl-stop-lookup-design.md
//! §2): the ingest-writer's `tfl-stops/1` handler replaces both from one
//! snapshot.

use anyhow::Result;
use chrono::{DateTime, Utc};
use common::tfl_stops::{NAME_MAX_CHARS, TflStop, is_stop_id, search_key};
use serde::Serialize;
use sqlx::PgConnection;

use crate::freshness::record_ingest;

/// What [`replace`] did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Replaced {
    /// Stops inserted or updated (every stop of the snapshot).
    pub written: u64,
    /// Stops that left `TfL`'s data, deleted.
    pub deleted: u64,
    /// Stop-to-CRS links written.
    pub links: u64,
    /// Rows refused before writing: an id that is not a `NaPTAN` id, an
    /// empty name or no in-scope mode.
    pub invalid: Vec<String>,
    /// The stored stops are from a newer snapshot: nothing was written.
    pub skipped_older: bool,
}

/// One row as the upsert's `jsonb_to_recordset` reads it.
#[derive(Serialize)]
struct Row<'a> {
    stop_id: &'a str,
    name: String,
    search_name: String,
    modes: &'a [String],
    lines: &'a [common::tfl_stops::TflStopLine],
    hub_code: Option<&'a str>,
    latitude: Option<f64>,
    longitude: Option<f64>,
}

/// One stop-to-CRS candidate: the hub code's CRS or a rail `TIPLOC`.
#[derive(Serialize)]
struct Link<'a> {
    stop_id: &'a str,
    code: &'a str,
}

/// Replaces `tfl_stops` with `stops` (a whole snapshot fetched at
/// `observed_at`) inside the caller's transaction, and rebuilds
/// `tfl_stop_crs`:
///
/// - an empty snapshot writes nothing (the producer never sends one; it
///   would read as "`TfL` has no stops");
/// - a snapshot older than the stored stops is skipped
///   ([`Replaced::skipped_older`]);
/// - an invalid row is left out ([`Replaced::invalid`]) and the rest is
///   written;
/// - stops absent from the snapshot are deleted, with their links;
/// - each stop's links are its hub CRS when `stations` or `tiploc_crs`
///   knows that CRS (`source = 'hub'`), and the CRS of each of its rail
///   `TIPLOCs` in `tiploc_crs` (`source = 'tiploc'`);
/// - freshness `tfl-stops` is `observed_at`.
pub async fn replace(
    conn: &mut PgConnection,
    stops: &[TflStop],
    observed_at: DateTime<Utc>,
) -> Result<Replaced> {
    let mut outcome = Replaced::default();
    if stops.is_empty() {
        return Ok(outcome);
    }
    let newest: Option<DateTime<Utc>> = sqlx::query_scalar("SELECT max(fetched_at) FROM tfl_stops")
        .fetch_one(&mut *conn)
        .await?;
    if newest.is_some_and(|newest| newest > observed_at) {
        outcome.skipped_older = true;
        return Ok(outcome);
    }

    let mut rows = Vec::with_capacity(stops.len());
    let mut seen = std::collections::BTreeSet::new();
    for stop in stops {
        let name: String = stop.name.trim().chars().take(NAME_MAX_CHARS).collect();
        if !is_stop_id(&stop.id) || name.is_empty() || stop.modes.is_empty() {
            outcome.invalid.push(stop.id.clone());
            continue;
        }
        if !seen.insert(stop.id.as_str()) {
            continue;
        }
        rows.push(Row {
            stop_id: &stop.id,
            search_name: search_key(&name),
            name,
            modes: &stop.modes,
            lines: &stop.lines,
            hub_code: stop.hub_code.as_deref(),
            latitude: stop.lat.filter(|lat| lat.is_finite()),
            longitude: stop.lon.filter(|lon| lon.is_finite()),
        });
    }
    if rows.is_empty() {
        return Ok(outcome);
    }
    let ids: Vec<&str> = rows.iter().map(|row| row.stop_id).collect();

    outcome.written = sqlx::query(
        r"
        INSERT INTO tfl_stops (stop_id, name, search_name, modes, lines, hub_code,
                               latitude, longitude, fetched_at)
        SELECT r.stop_id, r.name, r.search_name,
               ARRAY(SELECT jsonb_array_elements_text(r.modes)),
               r.lines, r.hub_code, r.latitude, r.longitude, $2
        FROM jsonb_to_recordset($1::jsonb) AS r(
            stop_id TEXT, name TEXT, search_name TEXT, modes JSONB, lines JSONB,
            hub_code TEXT, latitude DOUBLE PRECISION, longitude DOUBLE PRECISION)
        ON CONFLICT (stop_id) DO UPDATE SET
            name        = EXCLUDED.name,
            search_name = EXCLUDED.search_name,
            modes       = EXCLUDED.modes,
            lines       = EXCLUDED.lines,
            hub_code    = EXCLUDED.hub_code,
            latitude    = EXCLUDED.latitude,
            longitude   = EXCLUDED.longitude,
            fetched_at  = EXCLUDED.fetched_at
        ",
    )
    .bind(sqlx::types::Json(&rows))
    .bind(observed_at)
    .execute(&mut *conn)
    .await?
    .rows_affected();

    outcome.deleted = sqlx::query("DELETE FROM tfl_stops WHERE stop_id <> ALL($1::text[])")
        .bind(&ids)
        .execute(&mut *conn)
        .await?
        .rows_affected();

    outcome.links = rebuild_links(conn, stops, &ids).await?;

    record_ingest(conn, common::tfl_stops::FRESHNESS_SOURCE, Some(observed_at)).await?;
    Ok(outcome)
}

/// Rebuilds `tfl_stop_crs` from the snapshot's stops whose ids are in
/// `ids` (the ones written): each stop's hub CRS when `stations` or
/// `tiploc_crs` knows it, and the CRS of each of its rail TIPLOCs. Returns
/// the links written.
async fn rebuild_links(conn: &mut PgConnection, stops: &[TflStop], ids: &[&str]) -> Result<u64> {
    let valid: std::collections::BTreeSet<&str> = ids.iter().copied().collect();
    let hub_links: Vec<Link<'_>> = stops
        .iter()
        .filter(|stop| valid.contains(stop.id.as_str()))
        .flat_map(|stop| {
            stop.crs.iter().map(|crs| Link {
                stop_id: &stop.id,
                code: crs,
            })
        })
        .collect();
    let tiploc_links: Vec<Link<'_>> = stops
        .iter()
        .filter(|stop| valid.contains(stop.id.as_str()))
        .flat_map(|stop| {
            stop.rail_tiplocs.iter().map(|tiploc| Link {
                stop_id: &stop.id,
                code: tiploc,
            })
        })
        .collect();

    sqlx::query("DELETE FROM tfl_stop_crs")
        .execute(&mut *conn)
        .await?;
    let hub = sqlx::query(
        r"
        INSERT INTO tfl_stop_crs (stop_id, crs, source)
        SELECT DISTINCT l.stop_id, upper(l.code), 'hub'
        FROM jsonb_to_recordset($1::jsonb) AS l(stop_id TEXT, code TEXT)
        WHERE upper(l.code) ~ '^[A-Z0-9]{3}$'
          AND (EXISTS (SELECT 1 FROM stations s WHERE s.crs = upper(l.code))
               OR EXISTS (SELECT 1 FROM tiploc_crs t WHERE t.crs = upper(l.code)))
        ON CONFLICT (stop_id, crs) DO NOTHING
        ",
    )
    .bind(sqlx::types::Json(&hub_links))
    .execute(&mut *conn)
    .await?
    .rows_affected();
    let tiploc = sqlx::query(
        r"
        INSERT INTO tfl_stop_crs (stop_id, crs, source)
        SELECT DISTINCT l.stop_id, t.crs, 'tiploc'
        FROM jsonb_to_recordset($1::jsonb) AS l(stop_id TEXT, code TEXT)
        JOIN tiploc_crs t ON t.tiploc = l.code
        WHERE t.crs ~ '^[A-Z0-9]{3}$'
        ON CONFLICT (stop_id, crs) DO NOTHING
        ",
    )
    .bind(sqlx::types::Json(&tiploc_links))
    .execute(&mut *conn)
    .await?
    .rows_affected();
    Ok(hub + tiploc)
}
