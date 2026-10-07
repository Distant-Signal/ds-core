//! Reference data: `upsert_stations`, `upsert_tocs`, the STANOX/TIPLOC to
//! CRS crosswalks (upsert, prune, list, lookup) and `upsert_fixed_links`,
//! from `queries.rs` (spec §5.2). The read-only `list_fixed_links_from_crs`
//! and `is_bookable_crs` stay in the api.
//!
//! Moved in plan task 1A.6.

use std::collections::HashMap;

use anyhow::Result;
use common::{StationReference, TocReference};
use sqlx::PgPool;

use crate::freshness::{last_per_key, normalize_code, record_ingest};

/// Upserts a batch of station reference records. No history — this is
/// reference data, not an event stream (see the reference-data migration's
/// comment).
pub async fn upsert_stations(pool: &PgPool, stations: &[StationReference]) -> Result<u64> {
    if stations.is_empty() {
        return Ok(0);
    }
    let batch = last_per_key(stations, |station| normalize_code(&station.crs));
    let crs: Vec<String> = batch.iter().map(|s| normalize_code(&s.crs)).collect();
    let names: Vec<&str> = batch.iter().map(|s| s.name.as_str()).collect();
    let latitudes: Vec<Option<f64>> = batch.iter().map(|s| s.latitude).collect();
    let longitudes: Vec<Option<f64>> = batch.iter().map(|s| s.longitude).collect();
    let operators: Vec<Option<&str>> = batch
        .iter()
        .map(|s| s.station_operator.as_deref())
        .collect();
    let accessibility: Vec<&serde_json::Value> = batch.iter().map(|s| &s.accessibility).collect();

    let mut tx = pool.begin().await?;
    // `fetched_at` now means "when this row last CHANGED"; the feed-level
    // "last fetched" lives in `ingest_freshness` (see `record_ingest`).
    sqlx::query(
        r"
        INSERT INTO stations (crs, name, latitude, longitude, station_operator, accessibility, fetched_at)
        SELECT crs, name, latitude, longitude, station_operator, accessibility, NOW()
        FROM UNNEST($1::text[], $2::text[], $3::float8[], $4::float8[], $5::text[], $6::jsonb[])
            AS i(crs, name, latitude, longitude, station_operator, accessibility)
        ON CONFLICT (crs) DO UPDATE SET
            name             = EXCLUDED.name,
            latitude         = EXCLUDED.latitude,
            longitude        = EXCLUDED.longitude,
            station_operator = EXCLUDED.station_operator,
            accessibility    = EXCLUDED.accessibility,
            fetched_at       = NOW()
        WHERE (stations.name, stations.latitude, stations.longitude,
               stations.station_operator, stations.accessibility)
              IS DISTINCT FROM
              (EXCLUDED.name, EXCLUDED.latitude, EXCLUDED.longitude,
               EXCLUDED.station_operator, EXCLUDED.accessibility)
        ",
    )
    .bind(&crs)
    .bind(&names)
    .bind(&latitudes)
    .bind(&longitudes)
    .bind(&operators)
    .bind(&accessibility)
    .execute(&mut *tx)
    .await?;
    record_ingest(&mut tx, "stations").await?;
    tx.commit().await?;
    Ok(stations.len() as u64)
}

/// Upserts a batch of TOC reference records. No history, same rationale as
/// `upsert_stations`.
pub async fn upsert_tocs(pool: &PgPool, tocs: &[TocReference]) -> Result<u64> {
    if tocs.is_empty() {
        return Ok(0);
    }
    let batch = last_per_key(tocs, |toc| toc.atoc_code.clone());
    let codes: Vec<&str> = batch.iter().map(|t| t.atoc_code.as_str()).collect();
    let names: Vec<&str> = batch.iter().map(|t| t.name.as_str()).collect();
    let legal_names: Vec<&str> = batch.iter().map(|t| t.legal_name.as_str()).collect();
    let members: Vec<Option<bool>> = batch.iter().map(|t| t.atoc_member).collect();
    let station_operators: Vec<Option<bool>> = batch.iter().map(|t| t.station_operator).collect();

    let mut tx = pool.begin().await?;
    // `fetched_at` now means "when this row last CHANGED"; the feed-level
    // "last fetched" lives in `ingest_freshness` (see `record_ingest`).
    sqlx::query(
        r"
        INSERT INTO tocs (atoc_code, name, legal_name, atoc_member, station_operator, fetched_at)
        SELECT atoc_code, name, legal_name, atoc_member, station_operator, NOW()
        FROM UNNEST($1::text[], $2::text[], $3::text[], $4::bool[], $5::bool[])
            AS i(atoc_code, name, legal_name, atoc_member, station_operator)
        ON CONFLICT (atoc_code) DO UPDATE SET
            name             = EXCLUDED.name,
            legal_name       = EXCLUDED.legal_name,
            atoc_member      = EXCLUDED.atoc_member,
            station_operator = EXCLUDED.station_operator,
            fetched_at       = NOW()
        WHERE (tocs.name, tocs.legal_name, tocs.atoc_member, tocs.station_operator)
              IS DISTINCT FROM
              (EXCLUDED.name, EXCLUDED.legal_name, EXCLUDED.atoc_member, EXCLUDED.station_operator)
        ",
    )
    .bind(&codes)
    .bind(&names)
    .bind(&legal_names)
    .bind(&members)
    .bind(&station_operators)
    .execute(&mut *tx)
    .await?;
    record_ingest(&mut tx, "tocs").await?;
    tx.commit().await?;
    Ok(tocs.len() as u64)
}

/// Upserts a batch of resolved STANOX/CRS rows. Every daily delivery is a
/// full refresh (see this table's migration comment), so this is always a
/// complete-table upsert-by-`stanox`.
///
/// **This alone does NOT remove a stale row (2026-09-25 correction).** An
/// earlier version of this doc comment claimed "no separate delete step is
/// needed, since every successful run re-asserts every row it still
/// resolves" -- true only for a STANOX still present in the source data. A
/// STANOX a later delivery simply stops mentioning at all (decommissioned,
/// renamed, a source correction) was never removed by this function alone,
/// so a stale STANOX->CRS mapping accumulated in this table forever,
/// silently diverging from the real network. See [`prune_stanox_crs_not_in`]
/// for the cleanup half of a real publish cycle, and its own doc comment
/// for why that is a SEPARATE function rather than folded into this one.
pub async fn upsert_stanox_crs(pool: &PgPool, records: &[common::StanoxCrsRecord]) -> Result<u64> {
    if records.is_empty() {
        return Ok(0);
    }
    let batch = last_per_key(records, |record| record.stanox.clone());
    let stanox: Vec<&str> = batch.iter().map(|r| r.stanox.as_str()).collect();
    let crs: Vec<String> = batch.iter().map(|r| normalize_code(&r.crs)).collect();
    let tiploc: Vec<String> = batch.iter().map(|r| normalize_code(&r.tiploc)).collect();
    let station_name: Vec<&str> = batch.iter().map(|r| r.station_name.as_str()).collect();
    let source_sequence: Vec<i32> = batch.iter().map(|r| r.source_sequence).collect();
    let change_time: Vec<Option<i32>> = batch.iter().map(|r| r.change_time_minutes).collect();

    // `updated_at` means "last changed": an unchanged row is left alone
    // (nothing reads `updated_at`; every delivery re-sends the whole table).
    sqlx::query(
        r"
        INSERT INTO stanox_crs (stanox, crs, tiploc, station_name, source_sequence, change_time_minutes, updated_at)
        SELECT stanox, crs, tiploc, station_name, source_sequence, change_time_minutes, NOW()
        FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::int4[], $6::int4[])
            AS i(stanox, crs, tiploc, station_name, source_sequence, change_time_minutes)
        ON CONFLICT (stanox) DO UPDATE SET
            crs                 = EXCLUDED.crs,
            tiploc              = EXCLUDED.tiploc,
            station_name        = EXCLUDED.station_name,
            source_sequence     = EXCLUDED.source_sequence,
            change_time_minutes = EXCLUDED.change_time_minutes,
            updated_at          = NOW()
        WHERE (stanox_crs.crs, stanox_crs.tiploc, stanox_crs.station_name,
               stanox_crs.source_sequence, stanox_crs.change_time_minutes)
              IS DISTINCT FROM
              (EXCLUDED.crs, EXCLUDED.tiploc, EXCLUDED.station_name,
               EXCLUDED.source_sequence, EXCLUDED.change_time_minutes)
        ",
    )
    .bind(&stanox)
    .bind(&crs)
    .bind(&tiploc)
    .bind(&station_name)
    .bind(&source_sequence)
    .bind(&change_time)
    .execute(pool)
    .await?;
    Ok(records.len() as u64)
}

/// Deletes every `stanox_crs` row whose `stanox` is absent from
/// `keep_stanoxes` -- the stale-row cleanup half of a real publish cycle
/// that [`upsert_stanox_crs`] alone never covered (Signal Box Audit Low
/// finding, 2026-09-25; see that function's own corrected doc comment).
/// `routes::ingest::post_stanox_crs` calls this immediately after
/// `upsert_stanox_crs`, in the same HTTP request, passing that SAME
/// delivery's full STANOX set -- one publish cycle, two SQL statements.
///
/// **Deliberately its own function, not folded into `upsert_stanox_crs`
/// itself.** `upsert_stanox_crs` is reused throughout this crate's own test
/// suite (`journey.rs`, `train.rs`, this file's own other test modules) purely
/// to seed a couple of synthetic STANOX/CRS rows for an unrelated feature
/// test -- baking an unconditional "delete every OTHER row in the table"
/// into it would turn every one of those call sites into a table-wide wipe
/// of whatever fixture rows a DIFFERENT test already seeded, the exact
/// "an empty/small batch wipes real data" hazard `upsert_fixed_links`'s own
/// empty-batch guard exists to prevent, just triggered by a SMALL batch
/// instead of an EMPTY one. Keeping the prune a separate, explicitly-called
/// step confines it to the one real caller that actually represents a whole
/// delivery: the ingest route.
///
/// `keep_stanoxes.is_empty()` is a no-op, not "delete everything" -- matches
/// this crate's established "an empty batch must never be read as delete
/// everything" posture ([`upsert_fixed_links`],
/// [`upsert_schedule_destination_departures`]).
pub async fn prune_stanox_crs_not_in(pool: &PgPool, keep_stanoxes: &[String]) -> Result<u64> {
    if keep_stanoxes.is_empty() {
        return Ok(0);
    }
    let result = sqlx::query("DELETE FROM stanox_crs WHERE NOT (stanox = ANY($1))")
        .bind(keep_stanoxes)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

/// Row shape for `list_stanox_crs`'s `SELECT` -- a dedicated `FromRow`
/// struct, matching this file's own established convention for any
/// multi-column query result (see `IncidentRow`; `train_tracking.rs`'s
/// `TrackedTrainRow`/`TrackedTrainListItem`), rather than a bare tuple --
/// this repo reserves raw tuple `query_as` for single-column results only
/// (e.g. `last_stations_fetch`'s `(Option<DateTime<Utc>>,)`).
#[derive(Debug, Clone, sqlx::FromRow)]
struct StanoxCrsRow {
    stanox: String,
    crs: String,
    tiploc: String,
    station_name: String,
    source_sequence: i32,
    change_time_minutes: Option<i32>,
}

impl From<StanoxCrsRow> for common::StanoxCrsRecord {
    fn from(row: StanoxCrsRow) -> Self {
        common::StanoxCrsRecord {
            stanox: row.stanox,
            crs: row.crs,
            tiploc: row.tiploc,
            station_name: row.station_name,
            source_sequence: row.source_sequence,
            change_time_minutes: row.change_time_minutes,
        }
    }
}

/// The full current STANOX/CRS table, ordered by `stanox` for a stable,
/// reviewable response shape -- backs `GET /private/stanox-crs`, which
/// `trust-consumer`'s periodic reload consumes directly (Task 5), unlike
/// every `last_*_fetch` query in this file, which only returns a
/// timestamp.
///
/// With the CORPUS fallback on (`crate::corpus`, off by
/// default) this also lists `corpus_stanox_crs` rows for STANOXes neither
/// `stanox_crs` nor `tiploc_crs` knows (`source_sequence` 0, no change
/// time); a STANOX `tiploc_crs` knows but `stanox_crs` left out was
/// excluded on purpose and stays out.
pub async fn list_stanox_crs(pool: &PgPool) -> Result<Vec<common::StanoxCrsRecord>> {
    list_stanox_crs_with(pool, crate::corpus::fallback_enabled()).await
}

/// [`list_stanox_crs`] with the CORPUS fallback given explicitly.
pub async fn list_stanox_crs_with(
    pool: &PgPool,
    corpus_fallback: bool,
) -> Result<Vec<common::StanoxCrsRecord>> {
    let sql = if corpus_fallback {
        "SELECT stanox, crs, tiploc, station_name, source_sequence, change_time_minutes FROM ( \
             SELECT stanox, crs, tiploc, station_name, source_sequence, change_time_minutes FROM stanox_crs \
             UNION ALL \
             SELECT c.stanox, c.crs, c.tiploc, c.station_name, 0, NULL::integer FROM corpus_stanox_crs c \
             WHERE NOT EXISTS (SELECT 1 FROM stanox_crs s WHERE s.stanox = c.stanox) \
               AND NOT EXISTS (SELECT 1 FROM tiploc_crs t WHERE t.stanox = c.stanox) \
         ) merged ORDER BY stanox"
    } else {
        "SELECT stanox, crs, tiploc, station_name, source_sequence, change_time_minutes FROM stanox_crs ORDER BY stanox"
    };
    let rows = sqlx::query_as::<_, StanoxCrsRow>(sql)
        .fetch_all(pool)
        .await?;

    Ok(rows
        .into_iter()
        .map(common::StanoxCrsRecord::from)
        .collect())
}

/// Every TIPLOC->CRS row for one CRS -- the "which TIPLOCs does this
/// station's code cover" lookup Decision 3 step 3 of
/// docs/superpowers/specs/2026-09-05-schedule-first-train-tracking-design.md
/// calls for (`list_stanox_crs`'s existing `WHERE`-less shape returns
/// everything; this is its `WHERE crs = $1` sibling). The input is
/// normalised because callers pass un-normalised codes --
/// `tracked_trains.pin_origin_crs` is never
/// case-normalized at write time (`validate_pin` doesn't uppercase it),
/// so a case-insensitive compare here is load-bearing, not defensive
/// tidiness.
///
/// Every TIPLOC/CRS equality lookup in this file normalises its input with
/// [`normalize_code`] and compares it to the plain column (here
/// `UPPER(crs)` on `tiploc_crs`, matching `tiploc_crs_crs_idx`, and plain
/// `crs` on `stanox_crs`, matching `stanox_crs_crs`). History: a Signal Box Audit Low
/// finding found the lookups in this file disagreeing -- some raw, some
/// `UPPER`-only, one pair (`crs_for_tiploc`/`crs_for_tiplocs_batch`)
/// already `UPPER`+`TRIM` -- so two functions that both claimed to
/// resolve "the same" code could silently return different answers for a
/// lowercase or whitespace-padded input depending on which one a caller
/// happened to call. The same normalisation is now applied uniformly across
/// every such lookup in this file (see `latest_station_sample`,
/// `latest_station_full_coverage_samples`,
/// `latest_schedule_network_departures`, `list_fixed_links_from_crs`,
/// `station_names_for_crs_batch` for the others), and
/// `queries_crs_tiploc_normalization_tests` below has a regression test
/// proving two of them now agree on the same lowercase/padded input.
///
/// As of docs/superpowers/plans/2026-09-24-tiploc-crs-crosswalk-plan.md
/// (Task 3), this reads the UNION of `tiploc_crs` and `stanox_crs`,
/// preferring a `tiploc_crs` row when the SAME TIPLOC appears in both
/// (`DISTINCT ON (tiploc) ... ORDER BY tiploc, priority`, `tiploc_crs` at
/// priority 1). Every TIPLOC this function used to return from
/// `stanox_crs` alone still comes back unchanged (no existing caller or
/// fixture loses anything); a TIPLOC that exists ONLY in `tiploc_crs` --
/// e.g. Vauxhall's `VAUXHLM`/`VAUXHLW` or Clapham Junction's
/// `CLPHMJM`/`CLPHMJW`, both previously collapsed to one row per STANOX
/// by `stanox_crs`'s `PRIMARY KEY (stanox)` -- now ALSO comes back, which
/// it could not before this plan (see `journey.rs`'s `tiploc_key` doc
/// comment). The return type stays `Vec<common::StanoxCrsRecord>`: this is
/// a generic "which rows cover this CRS" lookup, and `StanoxCrsRecord`'s
/// shape already has every field a `tiploc_crs` row also has.
///
/// With the CORPUS fallback on (`crate::corpus`, off by
/// default) this also returns `corpus_tiploc_crs` TIPLOCs with this CRS
/// (and a STANOX) that neither timetable table has under ANY CRS, so a
/// TIPLOC the timetable maps elsewhere is never pulled in here.
pub async fn list_stanox_crs_for_crs(
    pool: &PgPool,
    crs: &str,
) -> Result<Vec<common::StanoxCrsRecord>> {
    list_stanox_crs_for_crs_with(pool, crs, crate::corpus::fallback_enabled()).await
}

/// [`list_stanox_crs_for_crs`] with the CORPUS fallback given explicitly.
pub async fn list_stanox_crs_for_crs_with(
    pool: &PgPool,
    crs: &str,
    corpus_fallback: bool,
) -> Result<Vec<common::StanoxCrsRecord>> {
    let sql = if corpus_fallback {
        "SELECT DISTINCT ON (tiploc) tiploc, crs, station_name, stanox, source_sequence, change_time_minutes \
         FROM ( \
             SELECT tiploc, crs, station_name, stanox, source_sequence, change_time_minutes, 1 AS priority \
             FROM tiploc_crs WHERE UPPER(crs) = $1 \
             UNION ALL \
             SELECT tiploc, crs, station_name, stanox, source_sequence, change_time_minutes, 2 AS priority \
             FROM stanox_crs WHERE crs = $1 \
             UNION ALL \
             SELECT c.tiploc, c.crs, c.station_name, c.stanox, 0, NULL::integer, 3 AS priority \
             FROM corpus_tiploc_crs c WHERE c.crs = $1 AND c.stanox IS NOT NULL \
               AND NOT EXISTS (SELECT 1 FROM tiploc_crs t WHERE t.tiploc = c.tiploc) \
               AND NOT EXISTS (SELECT 1 FROM stanox_crs s WHERE s.tiploc = c.tiploc) \
         ) merged \
         ORDER BY tiploc, priority"
    } else {
        "SELECT DISTINCT ON (tiploc) tiploc, crs, station_name, stanox, source_sequence, change_time_minutes \
         FROM ( \
             SELECT tiploc, crs, station_name, stanox, source_sequence, change_time_minutes, 1 AS priority \
             FROM tiploc_crs WHERE UPPER(crs) = $1 \
             UNION ALL \
             SELECT tiploc, crs, station_name, stanox, source_sequence, change_time_minutes, 2 AS priority \
             FROM stanox_crs WHERE crs = $1 \
         ) merged \
         ORDER BY tiploc, priority"
    };
    let rows = sqlx::query_as::<_, StanoxCrsRow>(sql)
        .bind(normalize_code(crs))
        .fetch_all(pool)
        .await?;

    Ok(rows
        .into_iter()
        .map(common::StanoxCrsRecord::from)
        .collect())
}

/// Upserts a batch of directly-resolved TIPLOC->CRS rows into the
/// TIPLOC-primary `tiploc_crs` table -- the sibling of `upsert_stanox_crs`
/// above, modeled on it directly (same transaction-per-batch shape, same
/// `ON CONFLICT (tiploc) DO UPDATE SET ...` pattern for every column
/// except `tiploc`/`updated_at`). Every daily delivery is a full refresh,
/// same as `stanox_crs` (see this table's migration comment), so this is
/// always a complete-table upsert-by-`tiploc`. See `common::TiplocCrsRecord`'s
/// own doc comment for why this table keeps EVERY TIPLOC as its own row
/// rather than `stanox_crs`'s one-row-per-STANOX disambiguation.
///
/// **This alone does NOT remove a stale row**, the identical gap
/// `upsert_stanox_crs`'s own doc comment corrects (2026-09-25) -- a TIPLOC a
/// later delivery stops mentioning was never removed. See
/// [`prune_tiploc_crs_not_in`], the direct sibling of
/// [`prune_stanox_crs_not_in`], for the cleanup half.
pub async fn upsert_tiploc_crs(pool: &PgPool, records: &[common::TiplocCrsRecord]) -> Result<u64> {
    if records.is_empty() {
        return Ok(0);
    }
    let batch = last_per_key(records, |record| normalize_code(&record.tiploc));
    let tiploc: Vec<String> = batch.iter().map(|r| normalize_code(&r.tiploc)).collect();
    let crs: Vec<String> = batch.iter().map(|r| normalize_code(&r.crs)).collect();
    let station_name: Vec<&str> = batch.iter().map(|r| r.station_name.as_str()).collect();
    let stanox: Vec<&str> = batch.iter().map(|r| r.stanox.as_str()).collect();
    let source_sequence: Vec<i32> = batch.iter().map(|r| r.source_sequence).collect();
    let change_time: Vec<Option<i32>> = batch.iter().map(|r| r.change_time_minutes).collect();

    // `updated_at` means "last changed", as in `upsert_stanox_crs`.
    sqlx::query(
        r"
        INSERT INTO tiploc_crs (tiploc, crs, station_name, stanox, source_sequence, change_time_minutes, updated_at)
        SELECT tiploc, crs, station_name, stanox, source_sequence, change_time_minutes, NOW()
        FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::int4[], $6::int4[])
            AS i(tiploc, crs, station_name, stanox, source_sequence, change_time_minutes)
        ON CONFLICT (tiploc) DO UPDATE SET
            crs                  = EXCLUDED.crs,
            station_name         = EXCLUDED.station_name,
            stanox               = EXCLUDED.stanox,
            source_sequence      = EXCLUDED.source_sequence,
            change_time_minutes  = EXCLUDED.change_time_minutes,
            updated_at           = NOW()
        WHERE (tiploc_crs.crs, tiploc_crs.station_name, tiploc_crs.stanox,
               tiploc_crs.source_sequence, tiploc_crs.change_time_minutes)
              IS DISTINCT FROM
              (EXCLUDED.crs, EXCLUDED.station_name, EXCLUDED.stanox,
               EXCLUDED.source_sequence, EXCLUDED.change_time_minutes)
        ",
    )
    .bind(&tiploc)
    .bind(&crs)
    .bind(&station_name)
    .bind(&stanox)
    .bind(&source_sequence)
    .bind(&change_time)
    .execute(pool)
    .await?;
    Ok(records.len() as u64)
}

/// Deletes every `tiploc_crs` row whose `tiploc` is absent from
/// `keep_tiplocs` -- the direct sibling of [`prune_stanox_crs_not_in`],
/// same reasoning, same "own function, not folded into the upsert" rationale
/// (see that function's own doc comment), same "empty batch is a no-op"
/// guard. `routes::ingest::post_tiploc_crs` calls this immediately after
/// `upsert_tiploc_crs`, in the same HTTP request.
pub async fn prune_tiploc_crs_not_in(pool: &PgPool, keep_tiplocs: &[String]) -> Result<u64> {
    if keep_tiplocs.is_empty() {
        return Ok(0);
    }
    // Stored TIPLOCs are `normalize_code`d by `upsert_tiploc_crs`.
    let keep: Vec<String> = keep_tiplocs.iter().map(|t| normalize_code(t)).collect();
    let result = sqlx::query("DELETE FROM tiploc_crs WHERE NOT (tiploc = ANY($1))")
        .bind(&keep)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

/// Row shape for `list_tiploc_crs`'s `SELECT` -- a dedicated `FromRow`
/// struct, mirroring `StanoxCrsRow`'s own convention directly above.
#[derive(Debug, Clone, sqlx::FromRow)]
struct TiplocCrsRow {
    tiploc: String,
    crs: String,
    station_name: String,
    stanox: String,
    source_sequence: i32,
    change_time_minutes: Option<i32>,
}

impl From<TiplocCrsRow> for common::TiplocCrsRecord {
    fn from(row: TiplocCrsRow) -> Self {
        common::TiplocCrsRecord {
            tiploc: row.tiploc,
            crs: row.crs,
            station_name: row.station_name,
            stanox: row.stanox,
            source_sequence: row.source_sequence,
            change_time_minutes: row.change_time_minutes,
        }
    }
}

/// The full current `tiploc_crs` table, ordered by `tiploc` for a stable,
/// reviewable response shape -- mirrors `list_stanox_crs`'s own shape.
/// Task 3's `trip_planning.rs` reads this to build its TIPLOC->CRS
/// resolution alongside `stanox_crs`.
///
/// With the CORPUS fallback on (`crate::corpus`, off by
/// default) this also lists `corpus_tiploc_crs` TIPLOCs (with a STANOX)
/// that neither `tiploc_crs` nor `stanox_crs` has (`source_sequence` 0, no
/// change time).
pub async fn list_tiploc_crs(pool: &PgPool) -> Result<Vec<common::TiplocCrsRecord>> {
    list_tiploc_crs_with(pool, crate::corpus::fallback_enabled()).await
}

/// [`list_tiploc_crs`] with the CORPUS fallback given explicitly.
pub async fn list_tiploc_crs_with(
    pool: &PgPool,
    corpus_fallback: bool,
) -> Result<Vec<common::TiplocCrsRecord>> {
    let sql = if corpus_fallback {
        "SELECT tiploc, crs, station_name, stanox, source_sequence, change_time_minutes FROM ( \
             SELECT tiploc, crs, station_name, stanox, source_sequence, change_time_minutes FROM tiploc_crs \
             UNION ALL \
             SELECT c.tiploc, c.crs, c.station_name, c.stanox, 0, NULL::integer FROM corpus_tiploc_crs c \
             WHERE c.stanox IS NOT NULL \
               AND NOT EXISTS (SELECT 1 FROM tiploc_crs t WHERE t.tiploc = c.tiploc) \
               AND NOT EXISTS (SELECT 1 FROM stanox_crs s WHERE s.tiploc = c.tiploc) \
         ) merged ORDER BY tiploc"
    } else {
        "SELECT tiploc, crs, station_name, stanox, source_sequence, change_time_minutes FROM tiploc_crs ORDER BY tiploc"
    };
    let rows = sqlx::query_as::<_, TiplocCrsRow>(sql)
        .fetch_all(pool)
        .await?;

    Ok(rows
        .into_iter()
        .map(common::TiplocCrsRecord::from)
        .collect())
}

/// Makes `fixed_links` hold exactly `records` in one transaction -- see
/// this plan's Judgment Call 4 for why this is a full replace, not a
/// per-row `ON CONFLICT` upsert like `upsert_stanox_crs`: a real ALF row
/// has no natural stable per-row key. Since 2026-09-27 the replace is a
/// diff (whole-row identity, duplicates counted), so an unchanged delivery
/// writes nothing instead of deleting and re-inserting ~4,600 rows. At ~4,222 real rows (confirmed
/// against the sibling `Distant-Signal-MCP` project's own measurement,
/// see this plan's header), this is cheap on every ~30-minute publish
/// cycle. Called only when `schedule-reference` actually found an ALF
/// file this cycle (`routes::ingest::post_fixed_links`'s own caller,
/// `main.rs`'s `publish_fixed_links`) -- an absent ALF file means this
/// function is simply never called that cycle, leaving the previous
/// cycle's rows in place rather than deleting them with nothing to
/// replace them (this plan's Judgment Call 1's "degrade this one product,
/// never wipe it for no reason" posture).
///
/// **An empty `records` is a no-op, and that is load-bearing (2026-09-25
/// fix).** Before this guard, this was the one wholesale-replace publish
/// function in this file WITHOUT it -- the (since removed) legacy schedule
/// chunk upserts already rejected an empty batch before their own `DELETE`,
/// for exactly this reason: a delete-then-insert
/// upsert can wipe a whole product on a client mistake or an upstream
/// parse failure that produces zero rows, in a way a per-row `ON CONFLICT`
/// loop never could. `schedule-reference` itself only calls this when it
/// found real ALF rows to publish, so an empty batch reaching here means
/// something upstream sent (or the caller constructed) a batch it should
/// not have -- exactly the same client-mistake shape the two sibling
/// functions' own doc comments describe, not a legitimate "this cycle's
/// ALF file was empty" signal (that case is `main.rs`'s `publish_fixed_links`
/// never calling this function at all, per the paragraph above).
///
/// Generic over [`sqlx::Acquire`] so production passes the pool (the diff
/// runs in its own transaction, exactly as before) while the DB-gated tests
/// pass an outer transaction they roll back -- `begin` on a transaction is
/// a savepoint, so the whole-table diff can never touch the real rows of
/// the database the tests run against.
#[expect(
    clippy::items_after_statements,
    clippy::too_many_lines,
    reason = "a local type or import sits next to its only use; long but linear; splitting it would scatter its shared state across helpers"
)]
pub async fn upsert_fixed_links<'c>(
    conn: impl sqlx::Acquire<'c, Database = sqlx::Postgres>,
    records: &[common::FixedLinkRecord],
) -> Result<u64> {
    if records.is_empty() {
        return Ok(0);
    }

    // Whole-row identity; CRS codes compared in their stored, normalised form.
    type LinkKey = (String, String, String, i32, String, String, String, i32);

    let mut tx = conn.begin().await?;
    // A multiset diff against the stored table instead of DELETE-everything
    // then re-INSERT-everything (DB review 2026-09-27 F2): a row has no
    // natural key (see above), so identity is the whole row, and an
    // identical row appearing N times is kept N times. Rows that match are
    // left untouched; only surplus stored rows are deleted and only
    // missing incoming rows inserted. `FOR UPDATE` keeps two concurrent
    // publishes from both diffing against the same snapshot.
    type StoredLink = (
        i64,
        String,
        String,
        String,
        i32,
        String,
        String,
        String,
        i32,
    );
    let existing: Vec<StoredLink> = sqlx::query_as(
        "SELECT id, mode, from_crs, to_crs, minutes, valid_from, valid_to, days_mask, \
                    source_sequence \
             FROM fixed_links ORDER BY id FOR UPDATE",
    )
    .fetch_all(&mut *tx)
    .await?;
    let mut stored: HashMap<LinkKey, Vec<i64>> = HashMap::with_capacity(existing.len());
    for (id, mode, from_crs, to_crs, minutes, valid_from, valid_to, days_mask, seq) in existing {
        let k: LinkKey = (
            mode,
            normalize_code(&from_crs),
            normalize_code(&to_crs),
            minutes,
            valid_from,
            valid_to,
            days_mask,
            seq,
        );
        stored.entry(k).or_default().push(id);
    }
    // `pop` below then keeps the OLDEST of identical stored rows.
    for ids in stored.values_mut() {
        ids.reverse();
    }
    let mut to_insert: Vec<LinkKey> = Vec::new();
    for record in records {
        let k: LinkKey = (
            record.mode.clone(),
            normalize_code(&record.from_crs),
            normalize_code(&record.to_crs),
            record.minutes,
            record.valid_from.clone(),
            record.valid_to.clone(),
            record.days_mask.clone(),
            record.source_sequence,
        );
        match stored.get_mut(&k).and_then(Vec::pop) {
            Some(_kept) => {}
            None => to_insert.push(k),
        }
    }
    let to_delete: Vec<i64> = stored.into_values().flatten().collect();

    if !to_delete.is_empty() {
        sqlx::query("DELETE FROM fixed_links WHERE id = ANY($1)")
            .bind(&to_delete)
            .execute(&mut *tx)
            .await?;
    }
    if !to_insert.is_empty() {
        let mut modes = Vec::with_capacity(to_insert.len());
        let mut from = Vec::with_capacity(to_insert.len());
        let mut to = Vec::with_capacity(to_insert.len());
        let mut minutes = Vec::with_capacity(to_insert.len());
        let mut valid_from = Vec::with_capacity(to_insert.len());
        let mut valid_to = Vec::with_capacity(to_insert.len());
        let mut days_mask = Vec::with_capacity(to_insert.len());
        let mut seq = Vec::with_capacity(to_insert.len());
        for (m, f, t, mins, vf, vt, dm, sq) in to_insert {
            modes.push(m);
            from.push(f);
            to.push(t);
            minutes.push(mins);
            valid_from.push(vf);
            valid_to.push(vt);
            days_mask.push(dm);
            seq.push(sq);
        }
        sqlx::query(
            r"
            INSERT INTO fixed_links (mode, from_crs, to_crs, minutes, valid_from, valid_to, days_mask, source_sequence, updated_at)
            SELECT *, NOW()
            FROM UNNEST($1::text[], $2::text[], $3::text[], $4::int4[], $5::text[], $6::text[], $7::text[], $8::int4[])
            ",
        )
        .bind(&modes)
        .bind(&from)
        .bind(&to)
        .bind(&minutes)
        .bind(&valid_from)
        .bind(&valid_to)
        .bind(&days_mask)
        .bind(&seq)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(records.len() as u64)
}

/// Reverse of the above: one CRS for a TIPLOC, or `None` if unmapped.
/// Used to resolve a matched schedule's own terminus CRS
/// (`schedule_destination_crs`) from its last calling point's TIPLOC.
/// `LIMIT 1`: a TIPLOC maps to at most one real station in practice, but
/// this doesn't assume uniqueness at the SQL level (no `UNIQUE`
/// constraint on `stanox_crs.tiploc` -- multiple STANOX rows can share a
/// TIPLOC, e.g. different platforms/areas of one physical location), so
/// this is "a plausible one," not "the guaranteed only one."
///
/// Both sides of the comparison are `TRIM`med as well as case-folded. A CIF
/// schedule-body TIPLOC is a fixed 7-character, space-padded field (see
/// `schedule_query::normalize_tiploc`) while `stanox_crs.tiploc` holds the
/// trimmed form, so a caller that forgets to normalize otherwise gets a
/// silent miss for every TIPLOC shorter than 7 characters -- roughly a
/// third of all real station TIPLOCs, and the cause of the 2026-09-16
/// "Unknown location" journey-page bug (see `journey::tiploc_key`).
/// Callers should still normalize, and all of them do, but correctness
/// must not depend on their remembering to.
///
/// The input is trimmed in Rust rather than in SQL, matching
/// `crs_for_tiplocs_batch`'s own `t.trim()` -- deliberately, so the two
/// siblings cannot diverge *on trimming*: Rust's `str::trim` strips all
/// Unicode whitespace while Postgres `TRIM()` strips spaces only, which is
/// indistinguishable for real space-padded ASCII CIF data but would make
/// the pair disagree on anything exotic. (Case folding is still done
/// SQL-side here and Rust-side in the batch; both are ASCII-identical for
/// a TIPLOC, so that asymmetry is cosmetic rather than a second trap.)
///
/// As of docs/superpowers/plans/2026-09-24-tiploc-crs-crosswalk-plan.md
/// (Task 3), this reads the UNION of `tiploc_crs` and `stanox_crs`,
/// deterministically preferring a `tiploc_crs` row when the SAME TIPLOC
/// exists in both tables with DIFFERENT CRS values (a real possible
/// transient state across delivery cycles) -- same `priority` pattern
/// `list_stanox_crs_for_crs` established (`tiploc_crs` at priority 1,
/// `stanox_crs` at priority 2, `ORDER BY priority` before `LIMIT 1`). A
/// TIPLOC present in `stanox_crs` but not yet in `tiploc_crs` still
/// resolves exactly as before; a TIPLOC present ONLY in `tiploc_crs` --
/// e.g. Vauxhall's/Clapham Junction's previously-dropped sibling TIPLOC --
/// now ALSO resolves, which it could not before this plan.
///
/// **The returned `crs` is `UPPER()`-cased, matching [`crs_for_tiplocs_batch`]'s
/// own `UPPER(crs)` projection -- this function used to select the bare,
/// as-stored `crs` column instead.** Neither `upsert_stanox_crs` nor
/// `upsert_tiploc_crs` case-normalizes `crs` on write (it is stored exactly
/// as `schedule_reference::parser` decoded it from the raw CIF byte range,
/// and no `CHECK` constraint enforces uppercase in either migration), so
/// the two sibling functions could disagree about a single TIPLOC's CRS
/// casing depending on which one a caller happened to call -- the same
/// "two functions that both claim to resolve the same code silently
/// disagree" bug class the Signal Box Audit's `UPPER(TRIM(...))`-everywhere
/// pass already closed for every WHERE-clause comparison in this file (see
/// this module's own doc note on `list_stanox_crs_for_crs`), just on the
/// *output* side instead of the input side, and so missed by that pass.
/// This matters beyond cosmetics: `schedule_matching::find_schedule_match`
/// feeds this function's result straight into
/// `.filter(|crs| queries::is_bookable_crs(crs))`, and `is_bookable_crs`
/// checks `crs.starts_with('X')` -- a case-sensitive, uppercase-only
/// check. An un-normalized lowercase pseudo-CRS (e.g. `"xvr"` instead of
/// `"XVR"`) would silently pass that filter and render as if it were a
/// real, bookable station -- exactly the failure mode `is_bookable_crs`'s
/// own doc comment cites real production evidence for (`XVR`/`XHN`/`XOZ`/
/// `XOD`/`XOE`/`XWI`).
///
/// With the CORPUS fallback on (`crate::corpus`, off by
/// default) `corpus_tiploc_crs` is a third source at priority 3, so it only
/// answers for a TIPLOC neither timetable table has.
pub async fn crs_for_tiploc(pool: &PgPool, tiploc: &str) -> Result<Option<String>> {
    crs_for_tiploc_with(pool, tiploc, crate::corpus::fallback_enabled()).await
}

/// [`crs_for_tiploc`] with the CORPUS fallback given explicitly.
pub async fn crs_for_tiploc_with(
    pool: &PgPool,
    tiploc: &str,
    corpus_fallback: bool,
) -> Result<Option<String>> {
    let sql = if corpus_fallback {
        "SELECT UPPER(crs) FROM ( \
             SELECT crs, 1 AS priority FROM tiploc_crs WHERE tiploc = $1 \
             UNION ALL \
             SELECT crs, 2 AS priority FROM stanox_crs WHERE tiploc = $1 \
             UNION ALL \
             SELECT crs, 3 AS priority FROM corpus_tiploc_crs WHERE tiploc = $1 \
         ) merged \
         ORDER BY priority \
         LIMIT 1"
    } else {
        "SELECT UPPER(crs) FROM ( \
             SELECT crs, 1 AS priority FROM tiploc_crs WHERE tiploc = $1 \
             UNION ALL \
             SELECT crs, 2 AS priority FROM stanox_crs WHERE tiploc = $1 \
         ) merged \
         ORDER BY priority \
         LIMIT 1"
    };
    let row: Option<(String,)> = sqlx::query_as(sql)
        .bind(normalize_code(tiploc))
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|(crs,)| crs))
}

/// Batched sibling of `crs_for_tiploc` -- one `WHERE tiploc = ANY($1)`
/// query resolving every distinct TIPLOC in a calling-point list,
/// instead of one query per TIPLOC. Mirrors the existing single/batch
/// pairing convention `trains::find_or_create_train`/
/// `find_or_create_trains_batch` already establishes. Keys are
/// [`normalize_code`]d TIPLOCs (stored TIPLOCs are in that form) -- see `crs_for_tiploc`'s own doc comment for why
/// the `TRIM` is load-bearing rather than cosmetic, and
/// `journey::tiploc_key` for the matching Rust-side key a caller's `get`
/// has to build. A TIPLOC with no row in either table is simply absent
/// from the map (degrade, don't fabricate -- same posture `crs_for_tiploc`
/// already has for a single lookup).
///
/// As of docs/superpowers/plans/2026-09-24-tiploc-crs-crosswalk-plan.md
/// (Task 3), this reads the UNION of `tiploc_crs` and `stanox_crs`,
/// deterministically preferring a `tiploc_crs` row when the SAME TIPLOC
/// exists in both tables with DIFFERENT CRS values -- see `crs_for_tiploc`
/// above for why that matters and the `priority` pattern both share.
/// `DISTINCT ON (tiploc)` with `ORDER BY tiploc, priority` picks the `tiploc_crs` row (priority 1) first per TIPLOC
/// before `.collect()` builds the map, so the result no longer depends on
/// unspecified `UNION` row order.
///
/// With the CORPUS fallback on, `corpus_tiploc_crs` is a third source at
/// priority 3, exactly as in [`crs_for_tiploc`].
pub async fn crs_for_tiplocs_batch(
    pool: &PgPool,
    tiplocs: &[String],
) -> Result<HashMap<String, String>> {
    crs_for_tiplocs_batch_with(pool, tiplocs, crate::corpus::fallback_enabled()).await
}

/// [`crs_for_tiplocs_batch`] with the CORPUS fallback given explicitly.
pub async fn crs_for_tiplocs_batch_with(
    pool: &PgPool,
    tiplocs: &[String],
    corpus_fallback: bool,
) -> Result<HashMap<String, String>> {
    if tiplocs.is_empty() {
        return Ok(HashMap::new());
    }
    let upper: Vec<String> = tiplocs.iter().map(|t| normalize_code(t)).collect();
    let sql = if corpus_fallback {
        "SELECT DISTINCT ON (tiploc) tiploc, UPPER(crs) FROM ( \
             SELECT tiploc, crs, 1 AS priority FROM tiploc_crs WHERE tiploc = ANY($1) \
             UNION ALL \
             SELECT tiploc, crs, 2 AS priority FROM stanox_crs WHERE tiploc = ANY($1) \
             UNION ALL \
             SELECT tiploc, crs, 3 AS priority FROM corpus_tiploc_crs WHERE tiploc = ANY($1) \
         ) merged \
         ORDER BY tiploc, priority"
    } else {
        "SELECT DISTINCT ON (tiploc) tiploc, UPPER(crs) FROM ( \
             SELECT tiploc, crs, 1 AS priority FROM tiploc_crs WHERE tiploc = ANY($1) \
             UNION ALL \
             SELECT tiploc, crs, 2 AS priority FROM stanox_crs WHERE tiploc = ANY($1) \
         ) merged \
         ORDER BY tiploc, priority"
    };
    let rows: Vec<(String, String)> = sqlx::query_as(sql).bind(&upper).fetch_all(pool).await?;
    Ok(rows.into_iter().collect())
}
