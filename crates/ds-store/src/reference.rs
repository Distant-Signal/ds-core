//! Reference data: `upsert_stations`, `upsert_tocs`, the STANOX/TIPLOC to
//! CRS crosswalks (upsert, prune, list, lookup) and `upsert_fixed_links`,
//! from `queries.rs` (spec §5.2). The read-only `list_fixed_links_from_crs`
//! stays in the api. `replace_tiploc_locations` (the `tiploc_locations`
//! publish) followed in 2a.2, for schedule-reference's `DbSink`.
//!
//! Moved in plan task 1A.6; `is_bookable_crs` followed in 1A.10, with the
//! schedule-match sweep that uses it.

use std::collections::HashMap;

use anyhow::Result;
use common::{StationReference, TocReference};
use sqlx::{PgConnection, PgPool};

use crate::freshness::{last_per_key, normalize_code, record_ingest};

/// One station for [`upsert_stations`]: the columns of a `stations` row.
///
/// `common::StationReference` (the api's `POST /private/stations` body)
/// implements it. poller-stations' direct writer (plan 2b.1) implements it
/// for its own borrowed record, whose passthrough `accessibility` is a map
/// of `&RawValue` slices of the fetched feed: a `serde_json::Value` per
/// station would cost several copies of the 38 MB feed (the 2026-09-27
/// `OOMKilled`). The bind encodes [`Self::Accessibility`] straight into the
/// statement's buffer.
pub trait StationRow {
    /// The passthrough JSON object, stored as `jsonb`.
    type Accessibility: serde::Serialize + ?Sized;

    fn crs(&self) -> &str;
    fn name(&self) -> &str;
    fn latitude(&self) -> Option<f64>;
    fn longitude(&self) -> Option<f64>;
    fn station_operator(&self) -> Option<&str>;
    fn accessibility(&self) -> &Self::Accessibility;
}

impl StationRow for StationReference {
    type Accessibility = serde_json::Value;

    fn crs(&self) -> &str {
        &self.crs
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn latitude(&self) -> Option<f64> {
        self.latitude
    }
    fn longitude(&self) -> Option<f64> {
        self.longitude
    }
    fn station_operator(&self) -> Option<&str> {
        self.station_operator.as_deref()
    }
    fn accessibility(&self) -> &serde_json::Value {
        &self.accessibility
    }
}

/// Rows per `INSERT` statement in [`upsert_stations`]. One statement's bind
/// parameters are encoded into one buffer and then copied into the
/// connection's write buffer (each growing by doubling), so a statement
/// costs about four times its parameters. 100 rows x 14 KB (the live
/// feed's facilities JSON per station) is 1.4 MB, so about 6 MB per
/// statement, against about 29 MB measured for 500 rows and four times the
/// whole 38 MB feed for one statement. A daily refresh is 27 statements in
/// one transaction.
pub const UPSERT_STATIONS_CHUNK: usize = 100;

/// Upserts a batch of station reference records. No history — this is
/// reference data, not an event stream (see the reference-data migration's
/// comment).
///
/// One transaction: the batch is deduplicated by CRS (the last one wins),
/// written [`UPSERT_STATIONS_CHUNK`] rows per statement, then
/// `ingest_freshness('stations')` is recorded. Readers see all of it or
/// none of it, as with the single statement it was until plan 2b.1.
pub async fn upsert_stations<S: StationRow + Sync>(pool: &PgPool, stations: &[S]) -> Result<u64> {
    if stations.is_empty() {
        return Ok(0);
    }
    let batch = last_per_key(stations, |station| normalize_code(station.crs()));

    let mut tx = crate::pool::begin(pool).await?;
    for chunk in batch.chunks(UPSERT_STATIONS_CHUNK) {
        upsert_station_chunk(&mut tx, chunk).await?;
    }
    record_ingest(&mut tx, "stations", None).await?;
    tx.commit().await?;
    Ok(stations.len() as u64)
}

async fn upsert_station_chunk<S: StationRow + Sync>(
    tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    chunk: &[&S],
) -> Result<()> {
    let crs: Vec<String> = chunk.iter().map(|s| normalize_code(s.crs())).collect();
    let names: Vec<&str> = chunk.iter().map(|s| s.name()).collect();
    let latitudes: Vec<Option<f64>> = chunk.iter().map(|s| s.latitude()).collect();
    let longitudes: Vec<Option<f64>> = chunk.iter().map(|s| s.longitude()).collect();
    let operators: Vec<Option<&str>> = chunk.iter().map(|s| s.station_operator()).collect();
    let accessibility: Vec<sqlx::types::Json<&S::Accessibility>> = chunk
        .iter()
        .map(|s| sqlx::types::Json(s.accessibility()))
        .collect();

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
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Upserts a batch of TOC reference records. No history, same rationale as
/// `upsert_stations`.
pub async fn upsert_tocs(pool: &PgPool, tocs: &[TocReference]) -> Result<u64> {
    if tocs.is_empty() {
        return Ok(0);
    }
    let mut tx = pool.begin().await?;
    write_tocs(&mut tx, tocs, None).await?;
    record_ingest(&mut tx, "tocs", None).await?;
    tx.commit().await?;
    Ok(tocs.len() as u64)
}

/// [`upsert_tocs`] for the ingest-writer's `tocs/1` handler (ingest plan
/// 3c.1), inside the caller's transaction `conn` (which also holds the
/// entry's `ingest_dedup` row: tocs have no ordering guard, spec §7.4).
/// A changed row's `fetched_at` and the feed's freshness are `observed_at`
/// (the entry's `produced_at`, decision D13); freshness never moves
/// backwards ([`crate::freshness::record_ingest`]). Returns the number
/// of TOCs received, as [`upsert_tocs`] does.
pub async fn upsert_tocs_observed(
    conn: &mut PgConnection,
    tocs: &[TocReference],
    observed_at: chrono::DateTime<chrono::Utc>,
) -> Result<u64> {
    if tocs.is_empty() {
        return Ok(0);
    }
    write_tocs(conn, tocs, Some(observed_at)).await?;
    record_ingest(conn, "tocs", Some(observed_at)).await?;
    Ok(tocs.len() as u64)
}

/// The shared upsert of [`upsert_tocs`] and [`upsert_tocs_observed`]: a
/// changed row's `fetched_at` is `observed_at`, or `NOW()` when `None`.
async fn write_tocs(
    conn: &mut PgConnection,
    tocs: &[TocReference],
    observed_at: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<()> {
    let batch = last_per_key(tocs, |toc| toc.atoc_code.clone());
    let codes: Vec<&str> = batch.iter().map(|t| t.atoc_code.as_str()).collect();
    let names: Vec<&str> = batch.iter().map(|t| t.name.as_str()).collect();
    let legal_names: Vec<&str> = batch.iter().map(|t| t.legal_name.as_str()).collect();
    let members: Vec<Option<bool>> = batch.iter().map(|t| t.atoc_member).collect();
    let station_operators: Vec<Option<bool>> = batch.iter().map(|t| t.station_operator).collect();

    // `fetched_at` now means "when this row last CHANGED"; the feed-level
    // "last fetched" lives in `ingest_freshness` (see `record_ingest`).
    sqlx::query(
        r"
        INSERT INTO tocs (atoc_code, name, legal_name, atoc_member, station_operator, fetched_at)
        SELECT atoc_code, name, legal_name, atoc_member, station_operator,
               COALESCE($6::timestamptz, NOW())
        FROM UNNEST($1::text[], $2::text[], $3::text[], $4::bool[], $5::bool[])
            AS i(atoc_code, name, legal_name, atoc_member, station_operator)
        ON CONFLICT (atoc_code) DO UPDATE SET
            name             = EXCLUDED.name,
            legal_name       = EXCLUDED.legal_name,
            atoc_member      = EXCLUDED.atoc_member,
            station_operator = EXCLUDED.station_operator,
            fetched_at       = EXCLUDED.fetched_at
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
    .bind(observed_at)
    .execute(&mut *conn)
    .await?;
    Ok(())
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

/// Whether `crs` is a genuine, bookable National Rail station code rather
/// than one of Network Rail's own `X`-prefixed pseudo-codes for a
/// non-passenger location (a junction, siding, or depot that still needs a
/// STANOX->CRS entry for TRUST tracking purposes but was never sold a
/// ticket to) -- see `reference-data/stanox-crs.md`'s own "Extraction and
/// exclusion policy" section and `schedule_reference::parser::resolve`'s
/// doc comment, which both independently document this exact convention.
///
/// Lives here, in `queries` -- rather than staying private to
/// `data::journey` (where it was first added, for the single-train journey
/// timeline) or moving out to `crates/common` -- because every one of its
/// real call sites is a display-bound CRS resolution inside THIS crate, and
/// `queries` is already the one module all four of them import for their
/// own TIPLOC->CRS lookups (`crs_for_tiploc`/`crs_for_tiplocs_batch`,
/// directly above): `api::data::journey::stops_from_calling_points`
/// (the journey timeline's per-calling-point CRS, the original call site),
/// `crate::sweeps::schedule_matching::find_schedule_match`'s
/// `destination_crs` (flows into `trains.destination_crs`, rendered as
/// `train.destinationName ?? train.destinationCrs` on the single-train
/// page), `crate::routes::lines::get_line_trains`'s schedule-side
/// origin/destination resolution (`ScheduleRouteEndpoints`), and
/// `crate::render::schedule_departure_json`'s `destinationCrs` field. A
/// cross-crate `common` helper would be the wrong call: no crate outside
/// `api` resolves a CRS for DISPLAY this way today, and `schedule_query`'s
/// own STANOX-disambiguation policy (`resolve.rs`) deliberately still
/// ACCEPTS a sole X-prefixed candidate for a STANOX -- the opposite
/// question this function answers -- so sharing one helper across that
/// boundary would invite exactly the confusion this doc comment is
/// disambiguating.
///
/// **Real evidence this matters, not a hypothetical.** `schedule_query::
/// resolve`'s STANOX-disambiguation policy accepts an X-prefixed CRS as a
/// STANOX's row whenever it is the SOLE candidate for that STANOX (only
/// excluding a STANOX outright when 2+ non-X or 2+ X candidates tie) -- so
/// a plain junction with no real passenger identity can still come back
/// from [`crs_for_tiplocs_batch`]/[`crs_for_tiploc`] with a resolved,
/// non-`None` `crs`. Confirmed against live production data for train
/// `Y80908` on 2026-09-23 (Birmingham New Street to London Euston):
/// `HANSLPJ` (Hanslope Junction) resolved to CRS `XHN`, `PROOFHJ` (Proof
/// House Junction) to `XOZ`, `LEDBRNJ` (Ledburn Junction) to `XOD`,
/// `BONENDJ` (Bourne End Junction) to `XOE`, and `WLSDWLJ` (Willesden
/// Junction) to `XWI` -- none of them a real station, none of them present
/// in `stations`, yet before this filter each one still carried a
/// non-`None` `crs` that was enough to render as if it were a real, terse
/// station identity wherever a caller displayed it unfiltered.
///
/// **A second, independent case the 2026-09-24 `tiploc_crs` crosswalk
/// widened.** That change (Task 3/4 of
/// docs/superpowers/plans/2026-09-24-tiploc-crs-crosswalk-plan.md) made
/// [`crs_for_tiploc`]/[`crs_for_tiplocs_batch`] resolve roughly a dozen
/// TIPLOCs that previously returned `None` (no STANOX-level
/// disambiguation possible) via the new TIPLOC-keyed `tiploc_crs` table
/// instead -- e.g. `VICTRCR` (a common empty-coaching-stock terminus) now
/// resolves to the X-prefixed pseudo-CRS `XVR` rather than `None`. Any
/// call site that resolves a display-bound CRS without this filter would,
/// as of that change, newly start showing a tracked ECS working's
/// destination as "XVR" instead of correctly showing nothing -- the exact
/// same bug class the `HANSLPJ`/`XHN` case above already documents, just
/// reachable through a second, newly-widened path.
///
/// A stop/row that only resolves to an X-prefixed pseudo-CRS should be
/// treated exactly like one that didn't resolve at all: blank the `crs`
/// (or `destination_crs`) out to `None`/absent rather than passing the
/// pseudo-code through, same degrade every call site above already applies
/// consistently.
pub fn is_bookable_crs(crs: &str) -> bool {
    !crs.starts_with('X')
}

/// `tiploc_locations`' publish (moved from the api's
/// `data::tiploc_locations`, ingest architecture plan 2a.2).
///
/// Replaces the whole table with `records` in one transaction: upsert by
/// TIPLOC (rows that did not change keep their `updated_at`), then delete
/// every TIPLOC `records` does not list. An empty `records` is refused by
/// the route before this is reached; here it is a no-op, never a wipe.
#[expect(
    clippy::too_many_lines,
    reason = "one 20-column upsert, its binds and the prune; splitting would scatter the column list"
)]
pub async fn replace_tiploc_locations(
    pool: &PgPool,
    records: &[common::TiplocLocationRecord],
) -> Result<u64> {
    if records.is_empty() {
        return Ok(0);
    }
    let batch = last_per_key(records, |r| normalize_code(&r.tiploc));
    let col = |f: fn(&common::TiplocLocationRecord) -> Option<String>| -> Vec<Option<String>> {
        batch.iter().map(|r| f(r)).collect()
    };
    let int = |f: fn(&common::TiplocLocationRecord) -> Option<i32>| -> Vec<Option<i32>> {
        batch.iter().map(|r| f(r)).collect()
    };
    let tiploc: Vec<String> = batch.iter().map(|r| normalize_code(&r.tiploc)).collect();
    let location_type: Vec<&str> = batch.iter().map(|r| r.location_type.as_str()).collect();
    let name: Vec<&str> = batch.iter().map(|r| r.name.as_str()).collect();
    let display_name: Vec<&str> = batch.iter().map(|r| r.display_name.as_str()).collect();
    let parent_crs: Vec<Option<String>> = batch
        .iter()
        .map(|r| r.parent_crs.as_deref().map(normalize_code))
        .collect();
    let parent_source: Vec<Option<&str>> = batch
        .iter()
        .map(|r| r.parent_source.map(common::ParentSource::as_str))
        .collect();
    let counts = |f: fn(&common::TiplocLocationRecord) -> i32| -> Vec<i32> {
        batch.iter().map(|r| f(r)).collect()
    };

    let mut tx = pool.begin().await?;
    sqlx::query(
        r"
        INSERT INTO tiploc_locations (
            tiploc, location_type, name, display_name, ti_name, ti_crs, stanox,
            msn_name, msn_code, msn_easting, msn_northing, msn_interchange,
            parent_crs, parent_source, parent_distance_m,
            rail_calls, rail_passes, bus_calls, ship_calls, source_sequence, updated_at)
        SELECT tiploc, location_type, name, display_name, ti_name, ti_crs, stanox,
               msn_name, msn_code, msn_easting, msn_northing, msn_interchange,
               parent_crs, parent_source, parent_distance_m,
               rail_calls, rail_passes, bus_calls, ship_calls, source_sequence, NOW()
        FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[],
                    $7::text[], $8::text[], $9::text[], $10::int4[], $11::int4[], $12::int4[],
                    $13::text[], $14::text[], $15::int4[], $16::int4[], $17::int4[],
                    $18::int4[], $19::int4[], $20::int4[])
            AS i(tiploc, location_type, name, display_name, ti_name, ti_crs, stanox,
                 msn_name, msn_code, msn_easting, msn_northing, msn_interchange,
                 parent_crs, parent_source, parent_distance_m,
                 rail_calls, rail_passes, bus_calls, ship_calls, source_sequence)
        ON CONFLICT (tiploc) DO UPDATE SET
            location_type = EXCLUDED.location_type,
            name = EXCLUDED.name,
            display_name = EXCLUDED.display_name,
            ti_name = EXCLUDED.ti_name,
            ti_crs = EXCLUDED.ti_crs,
            stanox = EXCLUDED.stanox,
            msn_name = EXCLUDED.msn_name,
            msn_code = EXCLUDED.msn_code,
            msn_easting = EXCLUDED.msn_easting,
            msn_northing = EXCLUDED.msn_northing,
            msn_interchange = EXCLUDED.msn_interchange,
            parent_crs = EXCLUDED.parent_crs,
            parent_source = EXCLUDED.parent_source,
            parent_distance_m = EXCLUDED.parent_distance_m,
            rail_calls = EXCLUDED.rail_calls,
            rail_passes = EXCLUDED.rail_passes,
            bus_calls = EXCLUDED.bus_calls,
            ship_calls = EXCLUDED.ship_calls,
            source_sequence = EXCLUDED.source_sequence,
            updated_at = NOW()
        WHERE (tiploc_locations.location_type, tiploc_locations.name,
               tiploc_locations.display_name, tiploc_locations.ti_name,
               tiploc_locations.ti_crs, tiploc_locations.stanox, tiploc_locations.msn_name,
               tiploc_locations.msn_code, tiploc_locations.msn_easting,
               tiploc_locations.msn_northing, tiploc_locations.msn_interchange,
               tiploc_locations.parent_crs, tiploc_locations.parent_source,
               tiploc_locations.parent_distance_m, tiploc_locations.rail_calls,
               tiploc_locations.rail_passes, tiploc_locations.bus_calls,
               tiploc_locations.ship_calls, tiploc_locations.source_sequence)
              IS DISTINCT FROM
              (EXCLUDED.location_type, EXCLUDED.name, EXCLUDED.display_name,
               EXCLUDED.ti_name, EXCLUDED.ti_crs, EXCLUDED.stanox, EXCLUDED.msn_name,
               EXCLUDED.msn_code, EXCLUDED.msn_easting, EXCLUDED.msn_northing,
               EXCLUDED.msn_interchange, EXCLUDED.parent_crs, EXCLUDED.parent_source,
               EXCLUDED.parent_distance_m, EXCLUDED.rail_calls, EXCLUDED.rail_passes,
               EXCLUDED.bus_calls, EXCLUDED.ship_calls, EXCLUDED.source_sequence)
        ",
    )
    .bind(&tiploc)
    .bind(&location_type)
    .bind(&name)
    .bind(&display_name)
    .bind(col(|r| r.ti_name.clone()))
    .bind(col(|r| r.ti_crs.clone()))
    .bind(col(|r| r.stanox.clone()))
    .bind(col(|r| r.msn_name.clone()))
    .bind(col(|r| r.msn_code.clone()))
    .bind(int(|r| r.msn_easting))
    .bind(int(|r| r.msn_northing))
    .bind(int(|r| r.msn_interchange))
    .bind(&parent_crs)
    .bind(&parent_source)
    .bind(int(|r| r.parent_distance_m))
    .bind(counts(|r| r.rail_calls))
    .bind(counts(|r| r.rail_passes))
    .bind(counts(|r| r.bus_calls))
    .bind(counts(|r| r.ship_calls))
    .bind(counts(|r| r.source_sequence))
    .execute(&mut *tx)
    .await?;
    sqlx::query("DELETE FROM tiploc_locations WHERE NOT (tiploc = ANY($1))")
        .bind(&tiploc)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(batch.len() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    // `is_bookable_crs`'s own tests -- real TIPLOC/CRS pairs reused from
    // that function's own doc comment (the `Y80908`/Hanslope Junction
    // incident this filter exists for).
    #[test]
    fn is_bookable_crs_rejects_only_the_x_prefixed_convention() {
        assert!(is_bookable_crs("WAT"));
        assert!(is_bookable_crs("EUS"));
        assert!(!is_bookable_crs("XHN"));
        assert!(!is_bookable_crs("XOZ"));
        assert!(!is_bookable_crs("XVR"));
        // A real CRS that merely happens to CONTAIN an 'X' is unaffected --
        // only a LEADING 'X' is Network Rail's pseudo-code convention.
        assert!(is_bookable_crs("BOX"));
    }
}

/// DB-gated tests for `list_stanox_crs_for_crs`/`crs_for_tiploc` (Task 5 of
/// docs/superpowers/plans/2026-09-05-schedule-first-train-tracking-plan.md),
/// the `stanox_crs`/`tiploc_crs` writers and the prunes. The fixed-links
/// tests stay in the api: they read back through its
/// `list_fixed_links_from_crs`.
#[cfg(test)]
mod stanox_crs_lookup_query_tests {
    use super::*;
    use crate::test_support::connect as test_pool;

    #[tokio::test]
    #[ignore = "requires a live database; see this plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p ds-store \
                list_stanox_crs_for_crs -- --ignored --test-threads=1`"]
    async fn list_stanox_crs_for_crs_returns_only_matching_rows_case_insensitively() {
        let pool = test_pool().await;
        upsert_stanox_crs(
            &pool,
            &[
                common::StanoxCrsRecord {
                    stanox: "TEST-EUS".to_string(),
                    crs: "EUS".to_string(),
                    tiploc: "EUSTON".to_string(),
                    station_name: "LONDON EUSTON".to_string(),
                    source_sequence: 1,
                    change_time_minutes: None,
                },
                common::StanoxCrsRecord {
                    stanox: "TEST-WAT".to_string(),
                    crs: "WAT".to_string(),
                    tiploc: "WATRLMN".to_string(),
                    station_name: "LONDON WATERLOO".to_string(),
                    source_sequence: 1,
                    change_time_minutes: None,
                },
            ],
        )
        .await
        .expect("seed stanox_crs");

        let rows = list_stanox_crs_for_crs(&pool, "eus").await.expect("query");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tiploc, "EUSTON");

        sqlx::query("DELETE FROM stanox_crs WHERE stanox IN ('TEST-EUS', 'TEST-WAT')")
            .execute(&pool)
            .await
            .expect("cleanup");
    }

    #[tokio::test]
    #[ignore = "requires a live database; see this plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p ds-store \
                crs_for_tiploc -- --ignored --test-threads=1`"]
    async fn crs_for_tiploc_resolves_a_known_tiploc_and_none_for_an_unknown_one() {
        let pool = test_pool().await;
        upsert_stanox_crs(
            &pool,
            &[common::StanoxCrsRecord {
                stanox: "TEST-CRE".to_string(),
                crs: "CRE".to_string(),
                tiploc: "CREWE".to_string(),
                station_name: "CREWE".to_string(),
                source_sequence: 1,
                change_time_minutes: None,
            }],
        )
        .await
        .expect("seed stanox_crs");

        assert_eq!(
            crs_for_tiploc(&pool, "crewe").await.unwrap(),
            Some("CRE".to_string())
        );
        assert_eq!(crs_for_tiploc(&pool, "NOWHERE").await.unwrap(), None);

        sqlx::query("DELETE FROM stanox_crs WHERE stanox = 'TEST-CRE'")
            .execute(&pool)
            .await
            .expect("cleanup");
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                crs_for_tiploc_uppercases_a_lower_case_stored_crs_matching_the_batch_sibling \
                -- --ignored --test-threads=1`"]
    async fn crs_for_tiploc_uppercases_a_lower_case_stored_crs_matching_the_batch_sibling() {
        // Neither `upsert_stanox_crs` nor `upsert_tiploc_crs` case-normalizes
        // `crs` on write (see `crs_for_tiploc`'s own doc comment) -- this
        // seeds a lower-case `crs` directly to prove `crs_for_tiploc` itself
        // uppercases on read, the same defense `crs_for_tiplocs_batch`
        // already applies via its own `UPPER(crs)` projection. Before this
        // fix, `crs_for_tiploc` returned the bare `"xvr"` here, which would
        // silently defeat `is_bookable_crs`'s case-sensitive
        // `starts_with('X')` check at this function's real
        // `find_schedule_match` call site.
        let pool = test_pool().await;
        upsert_stanox_crs(
            &pool,
            &[common::StanoxCrsRecord {
                stanox: "TEST-LOWER-XVR".to_string(),
                crs: "xvr".to_string(),
                tiploc: "TEST-LOWER-VICTRCR".to_string(),
                station_name: "VICTORIA CARRIAGE ROAD".to_string(),
                source_sequence: 1,
                change_time_minutes: None,
            }],
        )
        .await
        .expect("seed stanox_crs");

        assert_eq!(
            crs_for_tiploc(&pool, "test-lower-victrcr").await.unwrap(),
            Some("XVR".to_string()),
            "crs_for_tiploc must uppercase a lower-case-stored crs, matching \
             crs_for_tiplocs_batch's own UPPER(crs) projection"
        );

        sqlx::query("DELETE FROM stanox_crs WHERE stanox = 'TEST-LOWER-XVR'")
            .execute(&pool)
            .await
            .expect("cleanup");
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                crs_for_tiplocs_batch_resolves_every_known_tiploc_and_omits_unknown_ones \
                -- --ignored --test-threads=1`"]
    async fn crs_for_tiplocs_batch_resolves_every_known_tiploc_and_omits_unknown_ones() {
        let pool = test_pool().await;
        upsert_stanox_crs(
            &pool,
            &[
                common::StanoxCrsRecord {
                    stanox: "TEST-JS-CRE".to_string(),
                    crs: "CRE".to_string(),
                    tiploc: "TEST-JS-CREWE".to_string(),
                    station_name: "CREWE".to_string(),
                    source_sequence: 1,
                    change_time_minutes: None,
                },
                common::StanoxCrsRecord {
                    stanox: "TEST-JS-EUS".to_string(),
                    crs: "EUS".to_string(),
                    tiploc: "TEST-JS-EUSTON".to_string(),
                    station_name: "EUSTON".to_string(),
                    source_sequence: 1,
                    change_time_minutes: None,
                },
            ],
        )
        .await
        .expect("seed stanox_crs");

        let result = crs_for_tiplocs_batch(
            &pool,
            &[
                "test-js-crewe".to_string(),
                "TEST-JS-EUSTON".to_string(),
                "TEST-JS-UNKNOWN".to_string(),
            ],
        )
        .await
        .expect("crs_for_tiplocs_batch");

        assert_eq!(result.get("TEST-JS-CREWE"), Some(&"CRE".to_string()));
        assert_eq!(result.get("TEST-JS-EUSTON"), Some(&"EUS".to_string()));
        assert_eq!(result.get("TEST-JS-UNKNOWN"), None);
        assert_eq!(result.len(), 2);

        sqlx::query("DELETE FROM stanox_crs WHERE tiploc LIKE 'TEST-JS-%'")
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `cargo test -p ds-store \
                prune_stanox_crs_not_in -- --ignored --test-threads=1`"]
    async fn prune_stanox_crs_not_in_deletes_only_rows_absent_from_the_keep_set() {
        // The Signal Box Audit Low finding this closes: `upsert_stanox_crs`
        // alone never removed a STANOX a later delivery simply stopped
        // mentioning, so a stale mapping accumulated forever. This proves
        // the cleanup half on its own, independent of the ingest route.
        let pool = test_pool().await;
        upsert_stanox_crs(
            &pool,
            &[
                common::StanoxCrsRecord {
                    stanox: "TEST-PRUNE-KEEP".to_string(),
                    crs: "EUS".to_string(),
                    tiploc: "TEST-PRUNE-EUSTON".to_string(),
                    station_name: "EUSTON".to_string(),
                    source_sequence: 1,
                    change_time_minutes: None,
                },
                common::StanoxCrsRecord {
                    stanox: "TEST-PRUNE-STALE".to_string(),
                    crs: "CRE".to_string(),
                    tiploc: "TEST-PRUNE-CREWE".to_string(),
                    station_name: "CREWE".to_string(),
                    source_sequence: 1,
                    change_time_minutes: None,
                },
            ],
        )
        .await
        .expect("seed two rows");

        let deleted = prune_stanox_crs_not_in(&pool, &["TEST-PRUNE-KEEP".to_string()])
            .await
            .expect("prune");
        assert_eq!(deleted, 1);

        let remaining = list_stanox_crs_for_crs(&pool, "EUS")
            .await
            .expect("read back kept row");
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].stanox, "TEST-PRUNE-KEEP");

        let gone = list_stanox_crs_for_crs(&pool, "CRE")
            .await
            .expect("read back stale row");
        assert!(
            gone.is_empty(),
            "the STANOX absent from the keep set must be gone"
        );

        sqlx::query("DELETE FROM stanox_crs WHERE stanox LIKE 'TEST-PRUNE-%'")
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `cargo test -p ds-store \
                prune_stanox_crs_not_in -- --ignored --test-threads=1`"]
    async fn prune_stanox_crs_not_in_with_an_empty_keep_set_is_a_no_op() {
        // Mirrors `upsert_fixed_links`'s own empty-batch guard: an empty
        // keep set must never be read as "delete everything" -- that would
        // just move the exact hazard this fix closes into the prune step
        // itself.
        let pool = test_pool().await;
        upsert_stanox_crs(
            &pool,
            &[common::StanoxCrsRecord {
                stanox: "TEST-PRUNE-EMPTY".to_string(),
                crs: "EUS".to_string(),
                tiploc: "TEST-PRUNE-EMPTY-TPL".to_string(),
                station_name: "EUSTON".to_string(),
                source_sequence: 1,
                change_time_minutes: None,
            }],
        )
        .await
        .expect("seed a row");

        let deleted = prune_stanox_crs_not_in(&pool, &[]).await.expect("prune");
        assert_eq!(deleted, 0);

        let remaining = list_stanox_crs_for_crs(&pool, "EUS")
            .await
            .expect("read back");
        assert!(
            remaining.iter().any(|r| r.stanox == "TEST-PRUNE-EMPTY"),
            "an empty keep set must not delete the real row"
        );

        sqlx::query("DELETE FROM stanox_crs WHERE stanox LIKE 'TEST-PRUNE-%'")
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `cargo test -p ds-store \
                prune_tiploc_crs_not_in -- --ignored --test-threads=1`"]
    async fn prune_tiploc_crs_not_in_deletes_only_rows_absent_from_the_keep_set() {
        let pool = test_pool().await;
        upsert_tiploc_crs(
            &pool,
            &[
                common::TiplocCrsRecord {
                    tiploc: "TEST-PRUNE-TPL-KEEP".to_string(),
                    crs: "EUS".to_string(),
                    station_name: "EUSTON".to_string(),
                    stanox: "TEST-PRUNE-TPL-KEEP-STX".to_string(),
                    source_sequence: 1,
                    change_time_minutes: None,
                },
                common::TiplocCrsRecord {
                    tiploc: "TEST-PRUNE-TPL-STALE".to_string(),
                    crs: "CRE".to_string(),
                    station_name: "CREWE".to_string(),
                    stanox: "TEST-PRUNE-TPL-STALE-STX".to_string(),
                    source_sequence: 1,
                    change_time_minutes: None,
                },
            ],
        )
        .await
        .expect("seed two rows");

        let deleted = prune_tiploc_crs_not_in(&pool, &["TEST-PRUNE-TPL-KEEP".to_string()])
            .await
            .expect("prune");
        assert_eq!(deleted, 1);

        let remaining = list_tiploc_crs(&pool).await.expect("read back");
        assert!(
            remaining.iter().any(|r| r.tiploc == "TEST-PRUNE-TPL-KEEP"),
            "the kept TIPLOC must remain"
        );
        assert!(
            !remaining.iter().any(|r| r.tiploc == "TEST-PRUNE-TPL-STALE"),
            "the TIPLOC absent from the keep set must be gone"
        );

        sqlx::query("DELETE FROM tiploc_crs WHERE tiploc LIKE 'TEST-PRUNE-TPL-%'")
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `cargo test -p ds-store \
                upsert_stanox_crs_change_time -- --ignored --test-threads=1`"]
    async fn upsert_stanox_crs_change_time_minutes_round_trips_including_none() {
        let pool = test_pool().await;
        let records = vec![
            common::StanoxCrsRecord {
                stanox: "TEST-STANOX-WITH-CHANGE-TIME".to_string(),
                crs: "ZZZ".to_string(),
                tiploc: "ZZZTPL".to_string(),
                station_name: "TEST STATION".to_string(),
                source_sequence: 1,
                change_time_minutes: Some(5),
            },
            common::StanoxCrsRecord {
                stanox: "TEST-STANOX-NO-CHANGE-TIME".to_string(),
                crs: "YYY".to_string(),
                tiploc: "YYYTPL".to_string(),
                station_name: "TEST STATION 2".to_string(),
                source_sequence: 1,
                change_time_minutes: None,
            },
        ];
        upsert_stanox_crs(&pool, &records).await.expect("upsert");

        let all = list_stanox_crs(&pool).await.expect("read back");
        let with_time = all
            .iter()
            .find(|r| r.stanox == "TEST-STANOX-WITH-CHANGE-TIME")
            .expect("row present");
        assert_eq!(with_time.change_time_minutes, Some(5));
        let without_time = all
            .iter()
            .find(|r| r.stanox == "TEST-STANOX-NO-CHANGE-TIME")
            .expect("row present");
        assert_eq!(without_time.change_time_minutes, None);

        sqlx::query("DELETE FROM stanox_crs WHERE stanox LIKE 'TEST-STANOX-%'")
            .execute(&pool)
            .await
            .ok();
    }

    /// The actual end-to-end proof of Task 3 of
    /// docs/superpowers/plans/2026-09-24-tiploc-crs-crosswalk-plan.md's
    /// union-read fix: seeds ONLY `tiploc_crs` (via `upsert_tiploc_crs`,
    /// never touching `stanox_crs` at all) with Vauxhall's two real
    /// TIPLOCs, both sharing one STANOX and CRS -- exactly the shape
    /// `stanox_crs`'s `PRIMARY KEY (stanox)` could never have represented
    /// simultaneously. Both `crs_for_tiplocs_batch` and
    /// `list_stanox_crs_for_crs` must resolve BOTH TIPLOCs even though
    /// neither ever queries `tiploc_crs` alone in this codebase -- proving
    /// the union SQL actually reads the new table, not just the old one.
    #[tokio::test]
    #[ignore = "requires a live database; see this plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p ds-store \
                both_of_vauxhalls_tiplocs_resolve_through_the_union_when_seeded_only_in_tiploc_crs \
                -- --ignored --test-threads=1`"]
    async fn both_of_vauxhalls_tiplocs_resolve_through_the_union_when_seeded_only_in_tiploc_crs() {
        let pool = test_pool().await;
        upsert_tiploc_crs(
            &pool,
            &[
                common::TiplocCrsRecord {
                    tiploc: "TEST-VAUXHLM".to_string(),
                    crs: "VXH".to_string(),
                    station_name: "VAUXHALL".to_string(),
                    stanox: "TEST-VXH-STANOX".to_string(),
                    source_sequence: 1,
                    change_time_minutes: None,
                },
                common::TiplocCrsRecord {
                    tiploc: "TEST-VAUXHLW".to_string(),
                    crs: "VXH".to_string(),
                    station_name: "VAUXHALL".to_string(),
                    stanox: "TEST-VXH-STANOX".to_string(),
                    source_sequence: 1,
                    change_time_minutes: None,
                },
            ],
        )
        .await
        .expect("seed tiploc_crs (stanox_crs is deliberately left untouched)");

        let batch = crs_for_tiplocs_batch(
            &pool,
            &["TEST-VAUXHLM".to_string(), "TEST-VAUXHLW".to_string()],
        )
        .await
        .expect("crs_for_tiplocs_batch");
        assert_eq!(
            batch.get("TEST-VAUXHLM"),
            Some(&"VXH".to_string()),
            "TEST-VAUXHLM exists only in tiploc_crs -- must still resolve via the union"
        );
        assert_eq!(
            batch.get("TEST-VAUXHLW"),
            Some(&"VXH".to_string()),
            "TEST-VAUXHLW exists only in tiploc_crs -- must still resolve via the union"
        );

        let for_crs = list_stanox_crs_for_crs(&pool, "VXH")
            .await
            .expect("list_stanox_crs_for_crs");
        assert!(
            for_crs.iter().any(|r| r.tiploc == "TEST-VAUXHLM"),
            "TEST-VAUXHLM must be present in list_stanox_crs_for_crs('VXH')"
        );
        assert!(
            for_crs.iter().any(|r| r.tiploc == "TEST-VAUXHLW"),
            "TEST-VAUXHLW must be present in list_stanox_crs_for_crs('VXH')"
        );

        sqlx::query("DELETE FROM tiploc_crs WHERE tiploc IN ('TEST-VAUXHLM', 'TEST-VAUXHLW')")
            .execute(&pool)
            .await
            .expect("cleanup");
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `cargo test -p ds-store \
                a_re_post_with_a_changed_crs_overwrites_the_existing_row -- --ignored --test-threads=1`"]
    async fn a_re_post_with_a_changed_crs_overwrites_the_existing_row() {
        use sqlx::postgres::PgPoolOptions;

        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        let pool = PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres");

        let first = common::StanoxCrsRecord {
            stanox: "99999".to_string(),
            crs: "TST".to_string(),
            tiploc: "TESTLOC".to_string(),
            station_name: "TEST STATION".to_string(),
            source_sequence: 942,
            change_time_minutes: None,
        };
        upsert_stanox_crs(&pool, &[first])
            .await
            .expect("first upsert");

        let second = common::StanoxCrsRecord {
            stanox: "99999".to_string(),
            crs: "TS2".to_string(),
            tiploc: "TESTLOC".to_string(),
            station_name: "TEST STATION".to_string(),
            source_sequence: 943,
            change_time_minutes: None,
        };
        upsert_stanox_crs(&pool, &[second])
            .await
            .expect("re-upsert with changed crs");

        let rows = list_stanox_crs(&pool).await.expect("list_stanox_crs");
        let row = rows
            .iter()
            .find(|r| r.stanox == "99999")
            .expect("row present");
        assert_eq!(row.crs, "TS2", "the re-POST must overwrite, not duplicate");
        assert_eq!(row.source_sequence, 943);

        sqlx::query("DELETE FROM stanox_crs WHERE stanox = '99999'")
            .execute(&pool)
            .await
            .expect("cleanup fixture row");
    }
}

/// DB-gated tests for `upsert_tiploc_crs`/`list_tiploc_crs` (Task 2 of
/// docs/superpowers/plans/2026-09-24-tiploc-crs-crosswalk-plan.md). Same
/// shape/doc-comment convention as `stanox_crs_lookup_query_tests` above.
#[cfg(test)]
#[expect(
    clippy::similar_names,
    reason = "test code: paired test values share names"
)]
mod tiploc_crs_query_tests {
    use super::*;
    use crate::test_support::connect as test_pool;

    #[tokio::test]
    #[ignore = "requires a live database; see this plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p ds-store \
                two_tiplocs_sharing_a_stanox_both_persist_and_both_come_back -- --ignored --test-threads=1`"]
    async fn two_tiplocs_sharing_a_stanox_both_persist_and_both_come_back() {
        // The direct DB-level proof this plan's whole point (two TIPLOCs,
        // one STANOX, both persisted) actually works against a real
        // schema -- independent of the `stanox_crs` table entirely, which
        // would only ever keep one of these two rows under its own
        // STANOX-keyed disambiguation. See `common::TiplocCrsRecord`'s own
        // doc comment.
        let pool = test_pool().await;

        upsert_tiploc_crs(
            &pool,
            &[
                common::TiplocCrsRecord {
                    tiploc: "TEST-VAUXHLM".to_string(),
                    crs: "VXH".to_string(),
                    station_name: "VAUXHALL".to_string(),
                    stanox: "TEST-87214".to_string(),
                    source_sequence: 1,
                    change_time_minutes: None,
                },
                common::TiplocCrsRecord {
                    tiploc: "TEST-VAUXHLW".to_string(),
                    crs: "VXH".to_string(),
                    station_name: "VAUXHALL".to_string(),
                    stanox: "TEST-87214".to_string(),
                    source_sequence: 1,
                    change_time_minutes: None,
                },
            ],
        )
        .await
        .expect("seed tiploc_crs");

        let rows = list_tiploc_crs(&pool).await.expect("list_tiploc_crs");
        let vauxhlm = rows.iter().find(|r| r.tiploc == "TEST-VAUXHLM");
        let vauxhlw = rows.iter().find(|r| r.tiploc == "TEST-VAUXHLW");
        assert!(
            vauxhlm.is_some(),
            "TEST-VAUXHLM should be present alongside TEST-VAUXHLW, both sharing STANOX TEST-87214"
        );
        assert!(
            vauxhlw.is_some(),
            "TEST-VAUXHLW should be present alongside TEST-VAUXHLM, both sharing STANOX TEST-87214"
        );
        assert_eq!(vauxhlm.unwrap().crs, "VXH");
        assert_eq!(vauxhlm.unwrap().stanox, "TEST-87214");
        assert_eq!(vauxhlw.unwrap().crs, "VXH");
        assert_eq!(vauxhlw.unwrap().stanox, "TEST-87214");

        sqlx::query("DELETE FROM tiploc_crs WHERE tiploc IN ('TEST-VAUXHLM', 'TEST-VAUXHLW')")
            .execute(&pool)
            .await
            .expect("cleanup");
    }
}

/// DB review 2026-09-27 (F1/F2/F10): the reference upserts' no-op guards
/// leave an unchanged row physically untouched (same `xmin`) and the
/// lookups normalise their input. The rest of the module (the readers'
/// side) stays in the api's `queries`.
#[cfg(test)]
mod db_review_guard_and_normalisation_tests {
    use super::*;
    use crate::freshness::{data_freshness, last_stations_fetch, last_tocs_fetch};
    use crate::test_support::{connect as test_pool, station_reference as station, xmin};

    /// Plan 2b.1: a batch larger than [`UPSERT_STATIONS_CHUNK`] is written
    /// in several statements but deduplicated across all of them (a code
    /// repeated in a later chunk wins, and no statement touches a row
    /// twice), in one transaction.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                db_review_guard_and_normalisation_tests -- --ignored --test-threads=1`"]
    async fn a_batch_over_one_chunk_is_deduplicated_across_chunks() {
        let pool = test_pool().await;
        let letter = |i: usize| char::from(b'A' + u8::try_from(i % 26).unwrap());
        let codes: Vec<String> = (0..UPSERT_STATIONS_CHUNK + 20)
            .map(|i| format!("Y{}{}", letter(i / 26), letter(i)))
            .collect();
        sqlx::query("DELETE FROM stations WHERE crs = ANY($1)")
            .bind(&codes)
            .execute(&pool)
            .await
            .unwrap();
        let mut batch: Vec<StationReference> =
            codes.iter().map(|crs| station(crs, "Chunk Test")).collect();
        // The first code again, in the last chunk, renamed and lower-case.
        batch.push(station(&codes[0].to_lowercase(), "Chunk Test (last)"));

        assert_eq!(
            upsert_stations(&pool, &batch).await.unwrap(),
            batch.len() as u64
        );
        let (rows, last_name): (i64, String) = sqlx::query_as(
            "SELECT count(*), max(name) FILTER (WHERE crs = $2) FROM stations WHERE crs = ANY($1)",
        )
        .bind(&codes)
        .bind(&codes[0])
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(rows, i64::try_from(codes.len()).unwrap());
        assert_eq!(last_name, "Chunk Test (last)");

        sqlx::query("DELETE FROM stations WHERE crs = ANY($1)")
            .bind(&codes)
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                db_review_guard_and_normalisation_tests -- --ignored --test-threads=1`"]
    async fn stations_and_tocs_upserts_skip_unchanged_rows_and_record_freshness() {
        let pool = test_pool().await;
        let before = last_stations_fetch(&pool).await.unwrap();

        upsert_stations(&pool, &[station(" zqa ", "Test Guard A")])
            .await
            .unwrap();
        let first = xmin(&pool, "SELECT xmin::text FROM stations WHERE crs = 'ZQA'").await;
        let fresh = last_stations_fetch(&pool).await.unwrap();
        assert!(fresh.is_some() && fresh >= before);

        upsert_stations(&pool, &[station("ZQA", "Test Guard A")])
            .await
            .unwrap();
        assert_eq!(
            xmin(&pool, "SELECT xmin::text FROM stations WHERE crs = 'ZQA'").await,
            first,
            "an identical station must not be rewritten"
        );
        assert!(last_stations_fetch(&pool).await.unwrap() >= fresh);

        upsert_stations(&pool, &[station("ZQA", "Test Guard A (renamed)")])
            .await
            .unwrap();
        assert_ne!(
            xmin(&pool, "SELECT xmin::text FROM stations WHERE crs = 'ZQA'").await,
            first,
            "a changed station must be written"
        );

        let toc = |name: &str| TocReference {
            atoc_code: "Z9".to_string(),
            name: name.to_string(),
            legal_name: "Test Guard Rail Ltd".to_string(),
            atoc_member: Some(true),
            station_operator: None,
        };
        upsert_tocs(&pool, &[toc("Test Guard Rail")]).await.unwrap();
        let first = xmin(&pool, "SELECT xmin::text FROM tocs WHERE atoc_code = 'Z9'").await;
        upsert_tocs(&pool, &[toc("Test Guard Rail")]).await.unwrap();
        assert_eq!(
            xmin(&pool, "SELECT xmin::text FROM tocs WHERE atoc_code = 'Z9'").await,
            first
        );
        upsert_tocs(&pool, &[toc("Test Guard Rail 2")])
            .await
            .unwrap();
        assert_ne!(
            xmin(&pool, "SELECT xmin::text FROM tocs WHERE atoc_code = 'Z9'").await,
            first
        );
        assert!(last_tocs_fetch(&pool).await.unwrap().is_some());

        let [stations, tocs, ..] = data_freshness(&pool).await.unwrap();
        assert_eq!(stations, last_stations_fetch(&pool).await.unwrap());
        assert_eq!(tocs, last_tocs_fetch(&pool).await.unwrap());

        sqlx::query("DELETE FROM stations WHERE crs = 'ZQA'")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM tocs WHERE atoc_code = 'Z9'")
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                db_review_guard_and_normalisation_tests -- --ignored --test-threads=1`"]
    async fn crosswalk_upserts_skip_unchanged_rows_and_lookups_normalise_input() {
        let pool = test_pool().await;
        let stanox = |name: &str| common::StanoxCrsRecord {
            stanox: "TEST-GUARD-STANOX".to_string(),
            crs: "zqc".to_string(),
            tiploc: " zqctest".to_string(),
            station_name: name.to_string(),
            source_sequence: 1,
            change_time_minutes: None,
        };
        upsert_stanox_crs(&pool, &[stanox("GUARD C")])
            .await
            .unwrap();
        let sql = "SELECT xmin::text FROM stanox_crs WHERE stanox = 'TEST-GUARD-STANOX'";
        let first = xmin(&pool, sql).await;
        upsert_stanox_crs(&pool, &[stanox("GUARD C")])
            .await
            .unwrap();
        assert_eq!(xmin(&pool, sql).await, first);
        upsert_stanox_crs(&pool, &[stanox("GUARD C2")])
            .await
            .unwrap();
        assert_ne!(xmin(&pool, sql).await, first);

        let tiploc = |name: &str| common::TiplocCrsRecord {
            tiploc: "zqdtest ".to_string(),
            crs: " zqd".to_string(),
            station_name: name.to_string(),
            stanox: "TEST-GUARD-STANOX-D".to_string(),
            source_sequence: 1,
            change_time_minutes: Some(5),
        };
        upsert_tiploc_crs(&pool, &[tiploc("GUARD D")])
            .await
            .unwrap();
        let sql = "SELECT xmin::text FROM tiploc_crs WHERE tiploc = 'ZQDTEST'";
        let first = xmin(&pool, sql).await;
        upsert_tiploc_crs(&pool, &[tiploc("GUARD D")])
            .await
            .unwrap();
        assert_eq!(xmin(&pool, sql).await, first);

        // Stored normalised; lowercase/padded input still resolves.
        assert_eq!(
            crs_for_tiploc(&pool, " zqctest ").await.unwrap().as_deref(),
            Some("ZQC")
        );
        assert_eq!(
            crs_for_tiploc(&pool, "ZQDtest").await.unwrap().as_deref(),
            Some("ZQD")
        );
        let batch = crs_for_tiplocs_batch(&pool, &["zqctest ".to_string(), " zqdtest".to_string()])
            .await
            .unwrap();
        assert_eq!(batch.get("ZQCTEST").map(String::as_str), Some("ZQC"));
        assert_eq!(batch.get("ZQDTEST").map(String::as_str), Some("ZQD"));
        let rows = list_stanox_crs_for_crs(&pool, " zqc ").await.unwrap();
        assert!(rows.iter().any(|r| r.tiploc == "ZQCTEST"), "{rows:?}");
        let rows = list_stanox_crs_for_crs(&pool, "zqd").await.unwrap();
        assert!(rows.iter().any(|r| r.tiploc == "ZQDTEST"), "{rows:?}");

        sqlx::query("DELETE FROM stanox_crs WHERE stanox = 'TEST-GUARD-STANOX'")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM tiploc_crs WHERE tiploc = 'ZQDTEST'")
            .execute(&pool)
            .await
            .unwrap();
    }
}
