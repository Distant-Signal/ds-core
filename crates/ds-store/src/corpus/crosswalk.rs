//! The CORPUS-derived crosswalk (`corpus_tiploc_crs`, `corpus_stanox_crs`,
//! migration `20260928170000_corpus_crosswalk.sql`) and the OFF-BY-DEFAULT
//! runtime fallback that reads it.
//!
//! **Derivation.** `common::corpus_inference` (the same conservative rule
//! `line-catalogue-validator` regenerates `reference-data/crs-tiploc.csv`
//! with) runs over the whole current `corpus_locations`; [`write`] stores its
//! unambiguous keys whose CRS is a Knowledgebase station (`stations`; see
//! [`corpus_inference::restrict_to_stations`]). That happens in the same
//! transaction as every CORPUS load
//! ([`super::replace_corpus_locations`]), and
//! ([`rebuild_if_stale`]) at startup and after every `stations` refresh
//! when the stored build is older than the newest delivery, than this
//! build's `RULES_VERSION`, or than the current set of `stations` CRS codes
//! (tracked by a fingerprint in `corpus_crosswalk_build`). With no CORPUS
//! loaded, the check is one `MAX()` over the empty `corpus_deliveries`.
//!
//! **Why filter at build time, and rebuild on a `stations` change**, rather
//! than join `stations` in every lookup: the lookups stay exactly the SQL
//! they were (no extra join in the per-stop hot paths or in the whole-table
//! `list_*` reads), the comparison report and the stored rows apply the one
//! same Rust filter, and `stations` changes rarely (a new Knowledgebase
//! station a few times a year), so a rebuild per changed station set costs
//! next to nothing. The fingerprint makes this self-healing: a crash
//! between a `stations` refresh and its rebuild is caught at the next
//! refresh or startup.
//!
//! **Fallback** (`CORPUS_FALLBACK_ENABLED=true`, chart
//! `api.corpusFallback.enabled`, default false). When on, the TIPLOC/STANOX
//! lookups in [`crate::reference`] (`crs_for_tiploc`,
//! `crs_for_tiplocs_batch`, `list_stanox_crs`, `list_stanox_crs_for_crs`,
//! `list_tiploc_crs`) add the CORPUS rows AFTER the timetable-derived
//! `tiploc_crs`/`stanox_crs`:
//!
//! - the timetable always wins: a CORPUS row is used only for a TIPLOC
//!   neither timetable table has, or a STANOX the timetable does not know
//!   at all (a STANOX that appears in `tiploc_crs` but not `stanox_crs` was
//!   deliberately left out by `schedule-reference` as shared by two
//!   stations, and stays out);
//! - when off, every lookup runs exactly the SQL it ran before this module
//!   existed.
//!
//! The flag is process-wide ([`super::init_fallback_from_env`], read once at
//! startup) because the lookups take only a pool and have dozens of
//! callers; each lookup also has a `*_with` variant taking the flag
//! explicitly, which is what the tests use.

use std::collections::BTreeSet;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use common::corpus_inference::{self, CorpusCrsTiploc, CorpusRow, Crosswalk};
use sqlx::{PgPool, Postgres, Transaction};

use super::CorpusLocation;

/// `corpus_locations` rows as the inference's input.
pub fn corpus_rows(locations: &[CorpusLocation]) -> Vec<CorpusRow> {
    locations
        .iter()
        .map(|l| CorpusRow {
            nlc: Some(l.nlc.clone()),
            stanox: l.stanox.clone(),
            tiploc: l.tiploc.clone(),
            crs: l.crs.clone(),
            nlc_desc: l.nlc_desc.clone(),
        })
        .collect()
}

/// Runs the shared inference over `rows` and narrows it to the crosswalk.
pub fn derive(rows: &[CorpusRow]) -> (CorpusCrsTiploc, Crosswalk) {
    let inferred = corpus_inference::infer_crs_tiploc(rows);
    let crosswalk = corpus_inference::crosswalk(rows, &inferred);
    (inferred, crosswalk)
}

/// Every `corpus_locations` row, for a rebuild or a comparison.
pub async fn load_corpus_locations<'e, E>(executor: E) -> Result<Vec<CorpusLocation>>
where
    E: sqlx::PgExecutor<'e>,
{
    type Row = (
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT nlc, stanox, tiploc, crs, uic, nlc_desc, nlc_desc16 FROM corpus_locations ORDER BY id",
    )
    .fetch_all(executor)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(nlc, stanox, tiploc, crs, uic, nlc_desc, nlc_desc16)| CorpusLocation {
                nlc,
                stanox,
                tiploc,
                crs,
                uic,
                nlc_desc,
                nlc_desc16,
            },
        )
        .collect())
}

/// The CRS codes in `stations` (Knowledgebase), which the stored crosswalk
/// is restricted to, and a fingerprint of that set.
#[derive(Debug, Clone, Default)]
pub struct StationSet {
    pub crs: BTreeSet<String>,
    pub fingerprint: String,
}

/// `md5` of the sorted, comma-joined `stations` CRS codes: changes exactly
/// when a station is added or removed. An aggregate over ~2,600 rows, read
/// on every [`rebuild_if_stale`] call.
const STATIONS_FINGERPRINT_SQL: &str =
    "md5(COALESCE(string_agg(UPPER(TRIM(crs)), ',' ORDER BY UPPER(TRIM(crs))), ''))";

/// Reads the station set and its fingerprint in one statement (one
/// snapshot, so the two always agree).
pub async fn load_station_set<'e, E>(executor: E) -> Result<StationSet>
where
    E: sqlx::PgExecutor<'e>,
{
    let (crs, fingerprint): (Vec<String>, String) = sqlx::query_as(&format!(
        "SELECT COALESCE(array_agg(UPPER(TRIM(crs))), '{{}}'), {STATIONS_FINGERPRINT_SQL} FROM stations"
    ))
    .fetch_one(executor)
    .await
    .context("reading the stations CRS set")?;
    Ok(StationSet {
        crs: crs.into_iter().collect(),
        fingerprint,
    })
}

async fn stations_fingerprint<'e, E>(executor: E) -> Result<String>
where
    E: sqlx::PgExecutor<'e>,
{
    let (fingerprint,): (String,) =
        sqlx::query_as(&format!("SELECT {STATIONS_FINGERPRINT_SQL} FROM stations"))
            .fetch_one(executor)
            .await
            .context("fingerprinting stations")?;
    Ok(fingerprint)
}

/// What [`write`] stored, and what the stations filter left out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuildCounts {
    pub tiplocs: usize,
    pub stanoxes: usize,
    pub tiplocs_excluded: usize,
    pub stanoxes_excluded: usize,
}

/// Replaces the stored crosswalk with `crosswalk` (as [`derive`] gave it,
/// derived from the delivery `delivered_at`) restricted to the CRS codes
/// currently in `stations`
/// ([`corpus_inference::restrict_to_stations`]), inside the caller's
/// transaction (which must hold the CORPUS load lock). Records the station
/// set's fingerprint, so a later change to `stations` makes
/// [`rebuild_if_stale`] rebuild.
pub async fn write(
    tx: &mut Transaction<'_, Postgres>,
    delivered_at: DateTime<Utc>,
    crosswalk: Crosswalk,
) -> Result<BuildCounts> {
    let stations = load_station_set(&mut **tx).await?;
    let (crosswalk, excluded) =
        corpus_inference::restrict_to_stations(crosswalk, |crs| stations.crs.contains(crs));
    let counts = BuildCounts {
        tiplocs: crosswalk.tiplocs.len(),
        stanoxes: crosswalk.stanoxes.len(),
        tiplocs_excluded: excluded.tiplocs.len(),
        stanoxes_excluded: excluded.stanoxes.len(),
    };
    sqlx::query("DELETE FROM corpus_tiploc_crs")
        .execute(&mut **tx)
        .await?;
    sqlx::query("DELETE FROM corpus_stanox_crs")
        .execute(&mut **tx)
        .await?;
    let t = &crosswalk.tiplocs;
    let tiploc: Vec<&str> = t.iter().map(|r| r.tiploc.as_str()).collect();
    let crs: Vec<&str> = t.iter().map(|r| r.crs.as_str()).collect();
    let stanox: Vec<Option<&str>> = t.iter().map(|r| r.stanox.as_deref()).collect();
    let name: Vec<&str> = t.iter().map(|r| r.station_name.as_str()).collect();
    let rule: Vec<&str> = t.iter().map(|r| r.rule.as_str()).collect();
    sqlx::query(
        "INSERT INTO corpus_tiploc_crs (tiploc, crs, stanox, station_name, rule) \
         SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[])",
    )
    .bind(&tiploc)
    .bind(&crs)
    .bind(&stanox)
    .bind(&name)
    .bind(&rule)
    .execute(&mut **tx)
    .await?;
    let s = &crosswalk.stanoxes;
    let stanox: Vec<&str> = s.iter().map(|r| r.stanox.as_str()).collect();
    let crs: Vec<&str> = s.iter().map(|r| r.crs.as_str()).collect();
    let tiploc: Vec<&str> = s.iter().map(|r| r.tiploc.as_str()).collect();
    let name: Vec<&str> = s.iter().map(|r| r.station_name.as_str()).collect();
    sqlx::query(
        "INSERT INTO corpus_stanox_crs (stanox, crs, tiploc, station_name) \
         SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[])",
    )
    .bind(&stanox)
    .bind(&crs)
    .bind(&tiploc)
    .bind(&name)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "INSERT INTO corpus_crosswalk_build \
             (singleton, delivered_at, rules_version, tiploc_rows, stanox_rows, \
              stations_fingerprint, tiploc_rows_excluded, stanox_rows_excluded, built_at) \
         VALUES (TRUE, $1, $2, $3, $4, $5, $6, $7, now()) \
         ON CONFLICT (singleton) DO UPDATE SET \
             delivered_at = EXCLUDED.delivered_at, \
             rules_version = EXCLUDED.rules_version, \
             tiploc_rows = EXCLUDED.tiploc_rows, \
             stanox_rows = EXCLUDED.stanox_rows, \
             stations_fingerprint = EXCLUDED.stations_fingerprint, \
             tiploc_rows_excluded = EXCLUDED.tiploc_rows_excluded, \
             stanox_rows_excluded = EXCLUDED.stanox_rows_excluded, \
             built_at = EXCLUDED.built_at",
    )
    .bind(delivered_at)
    .bind(corpus_inference::RULES_VERSION)
    .bind(i32::try_from(counts.tiplocs)?)
    .bind(i32::try_from(counts.stanoxes)?)
    .bind(&stations.fingerprint)
    .bind(i32::try_from(counts.tiplocs_excluded)?)
    .bind(i32::try_from(counts.stanoxes_excluded)?)
    .execute(&mut **tx)
    .await?;
    Ok(counts)
}

/// What a stored build was derived from.
type BuildKey = (DateTime<Utc>, i32, Option<String>);

/// Rebuilds the stored crosswalk from `corpus_locations` when it was derived
/// from an older delivery, by older rules, or against a different set of
/// `stations` (or never). Returns the delivery it rebuilt from, or `None`
/// when there was nothing to do -- always the case before the first CORPUS
/// load, which costs one `MAX()` over an empty table.
///
/// Called at startup and after every `stations` refresh
/// (`POST /private/stations`), so a newly published Knowledgebase station
/// gets its CORPUS fills without waiting for the next CORPUS delivery. An
/// up-to-date check is three single-row reads (the `stations` fingerprint
/// aggregates ~2,600 CRS codes); a rebuild -- once per changed station set
/// -- re-derives from `corpus_locations` in one transaction.
pub async fn rebuild_if_stale(pool: &PgPool) -> Result<Option<DateTime<Utc>>> {
    let is_stale = |latest: Option<DateTime<Utc>>, built: Option<BuildKey>, stations: String| {
        latest.filter(|latest| {
            built != Some((*latest, corpus_inference::RULES_VERSION, Some(stations)))
        })
    };
    let (latest,): (Option<DateTime<Utc>>,) =
        sqlx::query_as("SELECT MAX(delivered_at) FROM corpus_deliveries")
            .fetch_one(pool)
            .await?;
    if latest.is_none() {
        return Ok(None);
    }
    let built = stored_build(pool).await?;
    let stations = stations_fingerprint(pool).await?;
    if is_stale(latest, built, stations).is_none() {
        return Ok(None);
    }

    // Stale: redo the check under the load lock, so a load or rebuild
    // committing meanwhile (which writes its own crosswalk) is not
    // overwritten with rows derived from an older set.
    let mut tx = pool.begin().await?;
    super::take_load_lock(&mut tx).await?;
    let (latest,): (Option<DateTime<Utc>>,) =
        sqlx::query_as("SELECT MAX(delivered_at) FROM corpus_deliveries")
            .fetch_one(&mut *tx)
            .await?;
    let built = stored_build(&mut *tx).await?;
    let stations = stations_fingerprint(&mut *tx).await?;
    let Some(latest) = is_stale(latest, built, stations) else {
        return Ok(None);
    };
    let locations = load_corpus_locations(&mut *tx).await?;
    let (_, crosswalk) = derive(&corpus_rows(&locations));
    let counts = write(&mut tx, latest, crosswalk).await?;
    tx.commit().await?;
    tracing::info!(
        delivered_at = %latest,
        rules_version = corpus_inference::RULES_VERSION,
        tiplocs = counts.tiplocs,
        stanoxes = counts.stanoxes,
        tiplocs_not_stations = counts.tiplocs_excluded,
        stanoxes_not_stations = counts.stanoxes_excluded,
        "rebuilt the CORPUS crosswalk"
    );
    Ok(Some(latest))
}

async fn stored_build<'e, E>(executor: E) -> Result<Option<BuildKey>>
where
    E: sqlx::PgExecutor<'e>,
{
    sqlx::query_as(
        "SELECT delivered_at, rules_version, stations_fingerprint FROM corpus_crosswalk_build",
    )
    .fetch_optional(executor)
    .await
    .context("reading corpus_crosswalk_build")
}
