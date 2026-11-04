//! The CORPUS-derived crosswalk (`corpus_tiploc_crs`, `corpus_stanox_crs`,
//! migration `20260928170000_corpus_crosswalk.sql`) and the OFF-BY-DEFAULT
//! runtime fallback that reads it.
//!
//! **Derivation.** `common::corpus_inference` (the same conservative rule
//! `line-catalogue-validator` regenerates `reference-data/crs-tiploc.csv`
//! with) runs over the whole current `corpus_locations` -- per layer, once
//! an NLC XML snapshot sits over a frozen JSON extract ([`CorpusLayers`],
//! [`derive_layers`]: the XML's pairs, then the JSON's for keys the XML
//! does not resolve); [`write`] stores its
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

use std::collections::{BTreeSet, HashSet};

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

/// Whether `source_file` names an NLC XML snapshot (`NLC20261030.xml.gz`,
/// CORPUS API v2): the `NLC` prefix and the `.xml.gz` extension, case
/// insensitive. The same rule as `schedule-ingest`'s
/// `pattern::is_nlc_xml_name`. Every other source file is the JSON extract
/// (`CORPUSExtract.json.gz`).
pub fn is_xml_snapshot_name(source_file: &str) -> bool {
    let lower = source_file.to_ascii_lowercase();
    lower.starts_with("nlc") && lower.ends_with(".xml.gz") && lower.len() >= "nlc.xml.gz".len()
}

/// `corpus_locations` as two layers.
///
/// Once an XML snapshot (CORPUS API v2) has been loaded with
/// [`super::replace_corpus_locations_keeping_json_fallback`], the table
/// holds that snapshot (`primary`) and, frozen beside it, the rows of the
/// last JSON extract loaded before it (`fallback`), each row keeping its
/// own `source_file` and `delivered_at`. The JSON extract carries every
/// location (~56,000 rows) and the XML only those with a TIPLOC (~12,100),
/// so the frozen rows keep every code that resolved before the switch.
/// Without an XML row, or after a JSON load, there is one layer: `primary`
/// is every row and `fallback` is empty.
///
/// The XML always wins where both have a code: see [`Self::effective`]
/// for the flat row set and [`derive_layers`] for the crosswalk. The
/// fallback is dropped by the next JSON load, or by deleting the rows
/// whose `source_file` is not an XML snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CorpusLayers {
    pub primary: Vec<CorpusLocation>,
    pub fallback: Vec<CorpusLocation>,
}

impl CorpusLayers {
    /// One layer: `locations` alone.
    pub fn single(locations: Vec<CorpusLocation>) -> Self {
        Self {
            primary: locations,
            fallback: Vec::new(),
        }
    }

    /// Splits rows tagged with their `source_file`: with any XML snapshot
    /// row, the XML rows are primary and the rest the fallback; otherwise
    /// every row is primary.
    pub fn split(rows: Vec<(String, CorpusLocation)>) -> Self {
        if !rows.iter().any(|(file, _)| is_xml_snapshot_name(file)) {
            return Self::single(rows.into_iter().map(|(_, l)| l).collect());
        }
        let (primary, fallback): (Vec<_>, Vec<_>) = rows
            .into_iter()
            .partition(|(file, _)| is_xml_snapshot_name(file));
        Self {
            primary: primary.into_iter().map(|(_, l)| l).collect(),
            fallback: fallback.into_iter().map(|(_, l)| l).collect(),
        }
    }

    /// The fallback rows the primary layer does not cover: a row with a
    /// TIPLOC the primary layer lacks, or a row without a TIPLOC whose NLC
    /// the primary layer lacks.
    pub fn uncovered_fallback(&self) -> impl Iterator<Item = &CorpusLocation> {
        let tiplocs: HashSet<&str> = self
            .primary
            .iter()
            .filter_map(|l| l.tiploc.as_deref())
            .collect();
        let nlcs: HashSet<&str> = self.primary.iter().map(|l| l.nlc.as_str()).collect();
        self.fallback
            .iter()
            .filter(move |l| match l.tiploc.as_deref() {
                Some(tiploc) => !tiplocs.contains(tiploc),
                None => !nlcs.contains(l.nlc.as_str()),
            })
    }

    /// One flat row set: every primary row, then the fallback rows it does
    /// not cover ([`Self::uncovered_fallback`]).
    pub fn effective(&self) -> Vec<CorpusLocation> {
        let mut rows = self.primary.clone();
        rows.extend(self.uncovered_fallback().cloned());
        rows
    }
}

/// The crosswalk of `layers`: the primary layer's, then every TIPLOC and
/// STANOX the fallback layer's own crosswalk has and the primary's lacks.
/// Each layer is derived on its own, so the fallback contributes exactly
/// the pairs it gave when it was loaded alone, and never changes a key the
/// primary layer resolves. The inference report is the primary layer's.
pub fn derive_layers(layers: &CorpusLayers) -> (CorpusCrsTiploc, Crosswalk) {
    let (inferred, mut crosswalk) = derive(&corpus_rows(&layers.primary));
    if layers.fallback.is_empty() {
        return (inferred, crosswalk);
    }
    let (_, fallback) = derive(&corpus_rows(&layers.fallback));
    let tiplocs: HashSet<String> = crosswalk.tiplocs.iter().map(|t| t.tiploc.clone()).collect();
    let stanoxes: HashSet<String> = crosswalk
        .stanoxes
        .iter()
        .map(|s| s.stanox.clone())
        .collect();
    crosswalk.tiplocs.extend(
        fallback
            .tiplocs
            .into_iter()
            .filter(|t| !tiplocs.contains(&t.tiploc)),
    );
    crosswalk.stanoxes.extend(
        fallback
            .stanoxes
            .into_iter()
            .filter(|s| !stanoxes.contains(&s.stanox)),
    );
    (inferred, crosswalk)
}

/// Every `corpus_locations` row with its `source_file`, as
/// [`CorpusLayers`], for a rebuild or a comparison.
pub async fn load_corpus_layers<'e, E>(executor: E) -> Result<CorpusLayers>
where
    E: sqlx::PgExecutor<'e>,
{
    type Row = (
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT source_file, nlc, stanox, tiploc, crs, uic, nlc_desc, nlc_desc16 \
         FROM corpus_locations ORDER BY id",
    )
    .fetch_all(executor)
    .await?;
    Ok(CorpusLayers::split(
        rows.into_iter()
            .map(
                |(source_file, nlc, stanox, tiploc, crs, uic, nlc_desc, nlc_desc16)| {
                    (
                        source_file,
                        CorpusLocation {
                            nlc,
                            stanox,
                            tiploc,
                            crs,
                            uic,
                            nlc_desc,
                            nlc_desc16,
                        },
                    )
                },
            )
            .collect(),
    ))
}

/// `corpus_locations` as one flat row set: [`CorpusLayers::effective`] of
/// [`load_corpus_layers`] (every row, while there is one layer).
pub async fn load_corpus_locations<'e, E>(executor: E) -> Result<Vec<CorpusLocation>>
where
    E: sqlx::PgExecutor<'e>,
{
    Ok(load_corpus_layers(executor).await?.effective())
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
/// gets its CORPUS fills without waiting for the next CORPUS delivery.
/// A refresh poller-stations writes directly (`INGEST_SINK=db`, plan 2b)
/// makes no POST: the ingest-writer's `CORPUS_CROSSWALK` loop
/// ([`crate::loops::corpus_crosswalk`], every 10 minutes) picks it up
/// instead. An
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
    let layers = load_corpus_layers(&mut *tx).await?;
    let (_, crosswalk) = derive_layers(&layers);
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

/// Database-gated: each test gets its own throwaway database
/// (`#[sqlx::test]`), because a CORPUS load replaces whole tables. Needs a
/// `DATABASE_URL` whose role may create databases.
#[cfg(test)]
#[expect(
    clippy::items_after_statements,
    reason = "test code: fixtures sit next to their use"
)]
mod db_tests {
    use chrono::TimeZone;

    use super::*;
    use crate::corpus::{fallback_enabled, replace_corpus_locations};
    use crate::reference as queries;

    fn loc(nlc: &str, stanox: &str, tiploc: &str, crs: &str, desc: &str) -> CorpusLocation {
        let opt = |s: &str| (!s.is_empty()).then(|| s.to_owned());
        CorpusLocation {
            nlc: nlc.to_owned(),
            stanox: opt(stanox),
            tiploc: opt(tiploc),
            crs: opt(crs),
            uic: None,
            nlc_desc: opt(desc),
            nlc_desc16: None,
        }
    }

    /// A small hand-made CORPUS: Clapham Junction with a platform TIPLOC
    /// and a loop, a TIPLOC the timetable maps to another CRS, a station
    /// the timetable lacks, a station at a STANOX the timetable left out
    /// of `stanox_crs` on purpose, and two CORPUS codes that are not
    /// stations (a pseudo `X` code and an Underground-style one).
    fn corpus() -> Vec<CorpusLocation> {
        vec![
            loc(
                "559500",
                "87219",
                "CLPHMJN",
                "CLJ",
                "CLAPHAM JUNCTION LONDON",
            ),
            loc("559572", "87219", "CLPHMJW", "", "CLAPHAM JN (WINDSOR)"),
            loc("559595", "87219", "CLPHJLP", "", "CLAPHAM JUNCTION LOOP"),
            loc("111100", "11111", "CONFLCT", "AAA", "CONFLICT A"),
            loc("222200", "22222", "NEWSTN", "NEW", "NEW STATION"),
            loc("333300", "33333", "SHARED1", "SHA", "SHARED ONE"),
            loc("000800", "", "", "", "MERSEYRAIL ELECTRICS-HQ INPUT"),
            loc("999900", "99999", "XBUSSTP", "XBS", "SOMEWHERE BUS STATION"),
            loc("888800", "88888", "LULSTOP", "LUA", "SOMEWHERE LUL"),
        ]
    }

    /// The Knowledgebase stations: every CORPUS CRS above except the two
    /// non-stations (`XBS`, `LUA`).
    const STATIONS: [&str; 4] = ["CLJ", "AAA", "NEW", "SHA"];

    async fn seed_stations(pool: &PgPool, codes: &[&str]) {
        for crs in codes {
            sqlx::query("INSERT INTO stations (crs, name) VALUES ($1, $1)")
                .bind(crs)
                .execute(pool)
                .await
                .unwrap();
        }
    }

    async fn stored_tiplocs(pool: &PgPool) -> Vec<String> {
        sqlx::query_as::<_, (String,)>("SELECT tiploc FROM corpus_tiploc_crs ORDER BY tiploc")
            .fetch_all(pool)
            .await
            .unwrap()
            .into_iter()
            .map(|(t,)| t)
            .collect()
    }

    async fn seed_timetable(pool: &PgPool) {
        for (tiploc, crs, stanox) in [
            ("CLPHMJN", "CLJ", "87219"),
            ("CONFLCT", "BBB", "11111"),
            ("TTSHARE", "TTS", "33333"),
            ("TTONLY", "TTO", "44444"),
        ] {
            sqlx::query(
                "INSERT INTO tiploc_crs (tiploc, crs, station_name, stanox, source_sequence) \
                 VALUES ($1, $2, $1, $3, 1)",
            )
            .bind(tiploc)
            .bind(crs)
            .bind(stanox)
            .execute(pool)
            .await
            .unwrap();
        }
        for (stanox, crs, tiploc) in [
            ("87219", "CLJ", "CLPHMJN"),
            ("11111", "BBB", "CONFLCT"),
            ("44444", "TTO", "TTONLY"),
        ] {
            sqlx::query(
                "INSERT INTO stanox_crs (stanox, crs, tiploc, station_name, source_sequence) \
                 VALUES ($1, $2, $3, $3, 1)",
            )
            .bind(stanox)
            .bind(crs)
            .bind(tiploc)
            .execute(pool)
            .await
            .unwrap();
        }
    }

    fn at() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 1, 3, 0, 0).unwrap()
    }

    /// Everything the five lookups return, as JSON, for byte-for-byte
    /// comparison.
    async fn snapshot(pool: &PgPool, fallback: bool) -> serde_json::Value {
        let tiplocs: Vec<String> = [
            "CLPHMJN", "CLPHMJW", "CLPHJLP", "CONFLCT", "NEWSTN", "TTONLY", "NOPE", "XBUSSTP",
            "LULSTOP",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let mut single = Vec::new();
        for t in &tiplocs {
            single.push(
                queries::crs_for_tiploc_with(pool, t, fallback)
                    .await
                    .unwrap(),
            );
        }
        let batch: std::collections::BTreeMap<String, String> =
            queries::crs_for_tiplocs_batch_with(pool, &tiplocs, fallback)
                .await
                .unwrap()
                .into_iter()
                .collect();
        let mut for_crs = Vec::new();
        for crs in ["CLJ", "AAA", "BBB", "NEW", "SHA"] {
            for_crs.push(
                queries::list_stanox_crs_for_crs_with(pool, crs, fallback)
                    .await
                    .unwrap(),
            );
        }
        serde_json::json!({
            "single": single,
            "batch": batch,
            "stanox_crs": queries::list_stanox_crs_with(pool, fallback).await.unwrap(),
            "tiploc_crs": queries::list_tiploc_crs_with(pool, fallback).await.unwrap(),
            "for_crs": for_crs,
        })
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn a_load_stores_the_crosswalk_and_marks_the_build(pool: PgPool) {
        assert_eq!(rebuild_if_stale(&pool).await.unwrap(), None);
        seed_stations(&pool, &STATIONS).await;
        replace_corpus_locations(&pool, at(), "CORPUSExtract.json.gz", &corpus())
            .await
            .unwrap();
        let tiplocs: Vec<(String, String, Option<String>, String)> = sqlx::query_as(
            "SELECT tiploc, crs, stanox, rule FROM corpus_tiploc_crs ORDER BY tiploc",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        let s = |v: &str| v.to_owned();
        assert_eq!(
            tiplocs,
            vec![
                (s("CLPHMJN"), s("CLJ"), Some(s("87219")), s("direct")),
                (s("CLPHMJW"), s("CLJ"), Some(s("87219")), s("station_name")),
                (s("CONFLCT"), s("AAA"), Some(s("11111")), s("direct")),
                (s("NEWSTN"), s("NEW"), Some(s("22222")), s("direct")),
                (s("SHARED1"), s("SHA"), Some(s("33333")), s("direct")),
            ]
        );
        type Build = (
            DateTime<Utc>,
            i32,
            i32,
            Option<i32>,
            Option<i32>,
            Option<String>,
        );
        let (delivered_at, version, stanoxes, tiplocs_out, stanoxes_out, fingerprint): Build =
            sqlx::query_as(
                "SELECT delivered_at, rules_version, stanox_rows, tiploc_rows_excluded, \
                        stanox_rows_excluded, stations_fingerprint FROM corpus_crosswalk_build",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            (delivered_at, version, stanoxes, tiplocs_out, stanoxes_out),
            (at(), corpus_inference::RULES_VERSION, 4, Some(2), Some(2))
        );
        assert_eq!(
            fingerprint,
            Some(load_station_set(&pool).await.unwrap().fingerprint)
        );
        // Up to date: nothing to rebuild.
        assert_eq!(rebuild_if_stale(&pool).await.unwrap(), None);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn an_outdated_build_is_rebuilt_from_corpus_locations(pool: PgPool) {
        seed_stations(&pool, &STATIONS).await;
        replace_corpus_locations(&pool, at(), "CORPUSExtract.json.gz", &corpus())
            .await
            .unwrap();
        let expected = ["CLPHMJN", "CLPHMJW", "CONFLCT", "NEWSTN", "SHARED1"];
        // As built by RULES_VERSION 1 (before the stations filter): every
        // unambiguous key, non-stations included, and no fingerprint.
        sqlx::query(
            "INSERT INTO corpus_tiploc_crs (tiploc, crs, stanox, station_name, rule) VALUES \
             ('XBUSSTP', 'XBS', '99999', 'SOMEWHERE BUS STATION', 'direct'), \
             ('LULSTOP', 'LUA', '88888', 'SOMEWHERE LUL', 'direct')",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE corpus_crosswalk_build SET rules_version = 1, stations_fingerprint = NULL",
        )
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(stored_tiplocs(&pool).await.len(), 7);
        assert_eq!(rebuild_if_stale(&pool).await.unwrap(), Some(at()));
        assert_eq!(stored_tiplocs(&pool).await, expected);
        assert_eq!(rebuild_if_stale(&pool).await.unwrap(), None);

        // An older rules version alone also rebuilds.
        sqlx::query("DELETE FROM corpus_tiploc_crs")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE corpus_crosswalk_build SET rules_version = 1")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(rebuild_if_stale(&pool).await.unwrap(), Some(at()));
        assert_eq!(stored_tiplocs(&pool).await, expected);
        assert_eq!(rebuild_if_stale(&pool).await.unwrap(), None);
    }

    /// Flag on: a TIPLOC or STANOX whose CORPUS CRS is not a station
    /// (pseudo `XBS`, non-station `LUA`) is never filled; a platform TIPLOC
    /// of a real station (`CLPHMJW`, Clapham Junction's Windsor side) is.
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn the_stations_filter_drops_non_station_fills_and_keeps_platform_fills(pool: PgPool) {
        seed_timetable(&pool).await;
        seed_stations(&pool, &STATIONS).await;
        replace_corpus_locations(&pool, at(), "CORPUSExtract.json.gz", &corpus())
            .await
            .unwrap();
        let crs = |t: &'static str| {
            let pool = pool.clone();
            async move { queries::crs_for_tiploc_with(&pool, t, true).await.unwrap() }
        };
        assert_eq!(crs("CLPHMJW").await.as_deref(), Some("CLJ"));
        assert_eq!(crs("XBUSSTP").await, None);
        assert_eq!(crs("LULSTOP").await, None);
        let batch = queries::crs_for_tiplocs_batch_with(
            &pool,
            &["CLPHMJW".into(), "XBUSSTP".into(), "LULSTOP".into()],
            true,
        )
        .await
        .unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch["CLPHMJW"], "CLJ");
        let stanoxes: Vec<String> = queries::list_stanox_crs_with(&pool, true)
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.stanox)
            .collect();
        assert!(!stanoxes.contains(&"99999".to_owned()));
        assert!(!stanoxes.contains(&"88888".to_owned()));
        assert!(
            queries::list_stanox_crs_for_crs_with(&pool, "XBS", true)
                .await
                .unwrap()
                .is_empty()
        );
    }

    /// A station added to (or removed from) `stations` after the CORPUS load
    /// changes the fingerprint, so the next check rebuilds and the fallback
    /// picks it up.
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn a_stations_change_rebuilds_the_crosswalk(pool: PgPool) {
        seed_stations(&pool, &STATIONS).await;
        replace_corpus_locations(&pool, at(), "CORPUSExtract.json.gz", &corpus())
            .await
            .unwrap();
        assert_eq!(rebuild_if_stale(&pool).await.unwrap(), None);
        assert_eq!(
            queries::crs_for_tiploc_with(&pool, "LULSTOP", true)
                .await
                .unwrap(),
            None
        );

        // The Knowledgebase publishes LUA as a station.
        seed_stations(&pool, &["LUA"]).await;
        assert_eq!(rebuild_if_stale(&pool).await.unwrap(), Some(at()));
        assert_eq!(
            queries::crs_for_tiploc_with(&pool, "LULSTOP", true)
                .await
                .unwrap()
                .as_deref(),
            Some("LUA")
        );
        assert_eq!(rebuild_if_stale(&pool).await.unwrap(), None);

        // An unrelated update (a renamed station) is not a change.
        sqlx::query("UPDATE stations SET name = 'Renamed' WHERE crs = 'LUA'")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(rebuild_if_stale(&pool).await.unwrap(), None);

        // And it is withdrawn again.
        sqlx::query("DELETE FROM stations WHERE crs = 'LUA'")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(rebuild_if_stale(&pool).await.unwrap(), Some(at()));
        assert!(!stored_tiplocs(&pool).await.contains(&"LULSTOP".to_owned()));
    }

    /// Plan 2b.2: with poller-stations on `INGEST_SINK=db` nothing calls
    /// `rebuild_if_stale` after a stations refresh (the api's
    /// `POST /private/stations` did). The ingest-writer's `CORPUS_CROSSWALK`
    /// loop does: a station written the direct-writer way
    /// (`reference::upsert_stations`) gets its CORPUS fills on the loop's
    /// next tick.
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn a_new_station_gets_its_fills_on_the_next_crosswalk_loop_tick(pool: PgPool) {
        use crate::loops::{LockSession, LoopRunner, TickOutcome};

        seed_stations(&pool, &STATIONS).await;
        replace_corpus_locations(&pool, at(), "CORPUSExtract.json.gz", &corpus())
            .await
            .unwrap();
        let lookup = || queries::crs_for_tiploc_with(&pool, "LULSTOP", true);
        assert_eq!(lookup().await.unwrap(), None);

        // The daily refresh: the Knowledgebase now lists LUA.
        queries::upsert_stations(
            &pool,
            &[crate::test_support::station_reference("LUA", "Somewhere")],
        )
        .await
        .unwrap();
        assert_eq!(lookup().await.unwrap(), None, "no fill before the tick");

        let runner = LoopRunner::new(pool.clone(), LockSession::from_pool(pool.clone()));
        let spec = crate::loops::corpus_crosswalk(crate::loops::CORPUS_CROSSWALK_DEFAULT_INTERVAL);
        assert_eq!(runner.run_once(&spec).await, TickOutcome::Ran);
        assert_eq!(lookup().await.unwrap().as_deref(), Some("LUA"));
        // The tick after that has nothing to do.
        assert_eq!(rebuild_if_stale(&pool).await.unwrap(), None);
    }

    /// Flag off: every lookup returns exactly what it returned before
    /// CORPUS was loaded, and exactly what the process-wide default (off)
    /// returns.
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn with_the_flag_off_corpus_changes_nothing(pool: PgPool) {
        seed_timetable(&pool).await;
        seed_stations(&pool, &STATIONS).await;
        let before = snapshot(&pool, false).await;
        replace_corpus_locations(&pool, at(), "CORPUSExtract.json.gz", &corpus())
            .await
            .unwrap();
        assert_eq!(snapshot(&pool, false).await, before);
        assert!(!fallback_enabled());
        assert_eq!(
            queries::crs_for_tiploc(&pool, "CLPHMJW").await.unwrap(),
            None
        );
        assert_eq!(
            serde_json::to_value(queries::list_stanox_crs(&pool).await.unwrap()).unwrap(),
            before["stanox_crs"]
        );
    }

    /// Flag on: CORPUS fills what the timetable lacks, and the timetable
    /// wins every conflict.
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn with_the_flag_on_corpus_fills_gaps_and_the_timetable_wins(pool: PgPool) {
        seed_timetable(&pool).await;
        seed_stations(&pool, &STATIONS).await;
        replace_corpus_locations(&pool, at(), "CORPUSExtract.json.gz", &corpus())
            .await
            .unwrap();
        let crs = |t: &'static str| {
            let pool = pool.clone();
            async move { queries::crs_for_tiploc_with(&pool, t, true).await.unwrap() }
        };
        // Fills.
        assert_eq!(crs("CLPHMJW").await.as_deref(), Some("CLJ"));
        assert_eq!(crs("NEWSTN").await.as_deref(), Some("NEW"));
        // Timetable wins; a loop stays unresolved; timetable-only unchanged.
        assert_eq!(crs("CONFLCT").await.as_deref(), Some("BBB"));
        assert_eq!(crs("CLPHJLP").await, None);
        assert_eq!(crs("TTONLY").await.as_deref(), Some("TTO"));

        let batch = queries::crs_for_tiplocs_batch_with(
            &pool,
            &["CLPHMJW".into(), "CONFLCT".into(), "TTONLY".into()],
            true,
        )
        .await
        .unwrap();
        assert_eq!(batch["CLPHMJW"], "CLJ");
        assert_eq!(batch["CONFLCT"], "BBB");
        assert_eq!(batch["TTONLY"], "TTO");

        let stanoxes: Vec<(String, String)> = queries::list_stanox_crs_with(&pool, true)
            .await
            .unwrap()
            .into_iter()
            .map(|r| (r.stanox, r.crs))
            .collect();
        let s = |a: &str, b: &str| (a.to_owned(), b.to_owned());
        // 22222 filled; 33333 (in tiploc_crs, left out of stanox_crs) not;
        // 11111 keeps the timetable's BBB.
        assert_eq!(
            stanoxes,
            vec![
                s("11111", "BBB"),
                s("22222", "NEW"),
                s("44444", "TTO"),
                s("87219", "CLJ")
            ]
        );

        let for_crs = |c: &'static str| {
            let pool = pool.clone();
            async move {
                queries::list_stanox_crs_for_crs_with(&pool, c, true)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|r| r.tiploc)
                    .collect::<Vec<_>>()
            }
        };
        assert_eq!(for_crs("CLJ").await, ["CLPHMJN", "CLPHMJW"]);
        // CONFLCT is the timetable's BBB, so CORPUS's AAA lists nothing.
        assert_eq!(for_crs("AAA").await, [] as [String; 0]);
        assert_eq!(for_crs("BBB").await, ["CONFLCT"]);

        let tiplocs: Vec<(String, String, i32)> = queries::list_tiploc_crs_with(&pool, true)
            .await
            .unwrap()
            .into_iter()
            .map(|r| (r.tiploc, r.crs, r.source_sequence))
            .collect();
        assert_eq!(
            tiplocs,
            vec![
                ("CLPHMJN".to_owned(), "CLJ".to_owned(), 1),
                ("CLPHMJW".to_owned(), "CLJ".to_owned(), 0),
                ("CONFLCT".to_owned(), "BBB".to_owned(), 1),
                ("NEWSTN".to_owned(), "NEW".to_owned(), 0),
                ("SHARED1".to_owned(), "SHA".to_owned(), 0),
                ("TTONLY".to_owned(), "TTO".to_owned(), 1),
                ("TTSHARE".to_owned(), "TTS".to_owned(), 1),
            ]
        );
    }

    /// The last JSON extract's rows, kept as a frozen fallback.
    fn xml_snapshot() -> Vec<CorpusLocation> {
        vec![
            loc(
                "559500",
                "87219",
                "CLPHMJN",
                "CLJ",
                "CLAPHAM JUNCTION LONDON",
            ),
            // The XML moves CONFLCT to another station: the XML wins.
            loc("111100", "11111", "CONFLCT", "SHA", "CONFLICT A"),
            loc("333300", "33333", "SHARED1", "SHA", "SHARED ONE"),
        ]
    }

    async fn pairs(pool: &PgPool) -> (Vec<(String, String)>, Vec<(String, String)>) {
        let tiplocs = sqlx::query_as("SELECT tiploc, crs FROM corpus_tiploc_crs ORDER BY tiploc")
            .fetch_all(pool)
            .await
            .unwrap();
        let stanoxes = sqlx::query_as("SELECT stanox, crs FROM corpus_stanox_crs ORDER BY stanox")
            .fetch_all(pool)
            .await
            .unwrap();
        (tiplocs, stanoxes)
    }

    async fn rows_by_source(pool: &PgPool) -> Vec<(String, DateTime<Utc>, i64)> {
        sqlx::query_as(
            "SELECT source_file, delivered_at, COUNT(*) FROM corpus_locations \
             GROUP BY source_file, delivered_at ORDER BY delivered_at",
        )
        .fetch_all(pool)
        .await
        .unwrap()
    }

    /// An NLC XML snapshot loaded over the JSON extract: every pair only the
    /// JSON gives (NEWSTN, CLPHMJW, STANOX 22222) still resolves, the XML
    /// wins the key both give (CONFLCT), the JSON rows stay marked by their
    /// own source file and delivery, a later snapshot keeps them, and a
    /// JSON load replaces both layers.
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn an_xml_load_keeps_every_json_only_pair(pool: PgPool) {
        use crate::corpus::{
            DeliveredFileProvenance, replace_corpus_locations_keeping_json_fallback,
        };
        seed_stations(&pool, &STATIONS).await;
        replace_corpus_locations(&pool, at(), "CORPUSExtract.json.gz", &corpus())
            .await
            .unwrap();
        let (json_tiplocs, json_stanoxes) = pairs(&pool).await;
        let s = |a: &str, b: &str| (a.to_owned(), b.to_owned());
        assert!(json_tiplocs.contains(&s("NEWSTN", "NEW")));
        assert!(json_stanoxes.contains(&s("22222", "NEW")));

        let xml_at = Utc.with_ymd_and_hms(2026, 11, 4, 19, 0, 5).unwrap();
        let inserted = replace_corpus_locations_keeping_json_fallback(
            &pool,
            xml_at,
            "NLC20261030.xml.gz",
            &DeliveredFileProvenance::default(),
            &xml_snapshot(),
        )
        .await
        .unwrap();
        assert_eq!(inserted, 3);
        let (tiplocs, stanoxes) = pairs(&pool).await;
        assert_eq!(
            tiplocs,
            vec![
                s("CLPHMJN", "CLJ"),
                s("CLPHMJW", "CLJ"),
                s("CONFLCT", "SHA"),
                s("NEWSTN", "NEW"),
                s("SHARED1", "SHA"),
            ]
        );
        assert_eq!(
            stanoxes,
            vec![
                s("11111", "SHA"),
                s("22222", "NEW"),
                s("33333", "SHA"),
                s("87219", "CLJ"),
            ]
        );
        // No JSON-only pair is lost.
        for pair in &json_tiplocs {
            assert!(
                tiplocs.iter().any(|(tiploc, _)| *tiploc == pair.0),
                "{pair:?} no longer resolves"
            );
        }
        for pair in &json_stanoxes {
            assert!(
                stanoxes.iter().any(|(stanox, _)| *stanox == pair.0),
                "{pair:?} no longer resolves"
            );
        }
        assert_eq!(
            rows_by_source(&pool).await,
            vec![
                ("CORPUSExtract.json.gz".to_owned(), at(), 9),
                ("NLC20261030.xml.gz".to_owned(), xml_at, 3),
            ]
        );
        assert_eq!(
            crate::corpus::last_corpus_delivery(&pool).await.unwrap(),
            Some(xml_at)
        );
        let layers = load_corpus_layers(&pool).await.unwrap();
        assert_eq!((layers.primary.len(), layers.fallback.len()), (3, 9));
        // The flat view: the XML rows, then the six JSON rows they don't cover.
        assert_eq!(load_corpus_locations(&pool).await.unwrap().len(), 9);

        // A rebuild (the ingest-writer's loop) derives the same crosswalk.
        sqlx::query("UPDATE corpus_crosswalk_build SET rules_version = 1")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(rebuild_if_stale(&pool).await.unwrap(), Some(xml_at));
        assert_eq!(pairs(&pool).await, (tiplocs.clone(), stanoxes.clone()));

        // The next snapshot replaces the last one; the JSON rows stay.
        let next_at = Utc.with_ymd_and_hms(2026, 11, 7, 19, 0, 0).unwrap();
        replace_corpus_locations_keeping_json_fallback(
            &pool,
            next_at,
            "NLC20261106.xml.gz",
            &DeliveredFileProvenance::default(),
            &xml_snapshot()[..2],
        )
        .await
        .unwrap();
        assert_eq!(
            rows_by_source(&pool).await,
            vec![
                ("CORPUSExtract.json.gz".to_owned(), at(), 9),
                ("NLC20261106.xml.gz".to_owned(), next_at, 2),
            ]
        );
        // SHARED1 left the XML: the JSON's pair for it is back.
        assert!(pairs(&pool).await.0.contains(&s("SHARED1", "SHA")));

        // A JSON load replaces both layers.
        let json_at = Utc.with_ymd_and_hms(2026, 11, 10, 3, 0, 0).unwrap();
        replace_corpus_locations(&pool, json_at, "CORPUSExtract.json.gz", &corpus()[..2])
            .await
            .unwrap();
        assert_eq!(
            rows_by_source(&pool).await,
            vec![("CORPUSExtract.json.gz".to_owned(), json_at, 2)]
        );
    }

    /// Only an XML snapshot loads over the fallback.
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn a_json_file_is_not_loaded_over_a_fallback(pool: PgPool) {
        let refused = crate::corpus::replace_corpus_locations_keeping_json_fallback(
            &pool,
            at(),
            "CORPUSExtract.json.gz",
            &crate::corpus::DeliveredFileProvenance::default(),
            &corpus(),
        )
        .await;
        assert!(refused.is_err());
    }
}

#[cfg(test)]
mod layer_tests {
    use super::*;

    fn loc(nlc: &str, stanox: &str, tiploc: &str, crs: &str, desc: &str) -> CorpusLocation {
        let opt = |s: &str| (!s.is_empty()).then(|| s.to_owned());
        CorpusLocation {
            nlc: nlc.to_owned(),
            stanox: opt(stanox),
            tiploc: opt(tiploc),
            crs: opt(crs),
            uic: None,
            nlc_desc: opt(desc),
            nlc_desc16: None,
        }
    }

    #[test]
    fn xml_snapshot_names() {
        assert!(is_xml_snapshot_name("NLC20261030.xml.gz"));
        assert!(is_xml_snapshot_name("nlc20261030.XML.GZ"));
        assert!(is_xml_snapshot_name("NLC.xml.gz"));
        assert!(!is_xml_snapshot_name("CORPUSExtract.json.gz"));
        assert!(!is_xml_snapshot_name("NLC20261030.xml"));
        assert!(!is_xml_snapshot_name("XNLC20261030.xml.gz"));
    }

    #[test]
    fn rows_split_into_layers_only_with_an_xml_snapshot() {
        let json = loc("100100", "10010", "JSONONE", "JSO", "JSON ONE");
        let xml = loc("200200", "20020", "XMLONE", "XMO", "XML ONE");
        let one = CorpusLayers::split(vec![("CORPUSExtract.json.gz".to_owned(), json.clone())]);
        assert_eq!(one, CorpusLayers::single(vec![json.clone()]));
        let two = CorpusLayers::split(vec![
            ("CORPUSExtract.json.gz".to_owned(), json.clone()),
            ("NLC20261030.xml.gz".to_owned(), xml.clone()),
        ]);
        assert_eq!(two.primary, vec![xml]);
        assert_eq!(two.fallback, vec![json]);
    }

    #[test]
    fn the_effective_rows_are_the_xml_and_the_json_rows_it_lacks() {
        let layers = CorpusLayers {
            primary: vec![loc("100100", "10010", "SAMETPL", "AAA", "SAME")],
            fallback: vec![
                loc("100100", "10010", "SAMETPL", "OLD", "SAME"),
                loc("100100", "10010", "OTHERTP", "AAA", "SAME, ANOTHER TIPLOC"),
                loc("100100", "", "", "", "SAME NLC, NO TIPLOC"),
                loc("300300", "", "", "", "NLC ONLY"),
            ],
        };
        let nlcs_and_tiplocs: Vec<(String, Option<String>)> = layers
            .effective()
            .into_iter()
            .map(|l| (l.nlc, l.tiploc))
            .collect();
        assert_eq!(
            nlcs_and_tiplocs,
            vec![
                ("100100".to_owned(), Some("SAMETPL".to_owned())),
                ("100100".to_owned(), Some("OTHERTP".to_owned())),
                ("300300".to_owned(), None),
            ]
        );
    }

    /// The XML wins a key both resolve; every key only the JSON resolves
    /// is kept, exactly as the JSON alone derives it.
    #[test]
    fn the_layered_crosswalk_keeps_json_only_pairs_and_the_xml_wins() {
        let json = vec![
            loc("100100", "10010", "BOTHTPL", "OLD", "BOTH STATION"),
            loc("200200", "20020", "JSONTPL", "JSN", "JSON STATION"),
        ];
        let xml = vec![loc("100100", "10010", "BOTHTPL", "NEW", "BOTH STATION")];
        let (_, alone) = derive(&corpus_rows(&json));
        let (_, crosswalk) = derive_layers(&CorpusLayers {
            primary: xml,
            fallback: json,
        });
        let tiplocs: Vec<(&str, &str)> = crosswalk
            .tiplocs
            .iter()
            .map(|t| (t.tiploc.as_str(), t.crs.as_str()))
            .collect();
        assert_eq!(tiplocs, [("BOTHTPL", "NEW"), ("JSONTPL", "JSN")]);
        let stanoxes: Vec<(&str, &str)> = crosswalk
            .stanoxes
            .iter()
            .map(|s| (s.stanox.as_str(), s.crs.as_str()))
            .collect();
        assert_eq!(stanoxes, [("10010", "NEW"), ("20020", "JSN")]);
        let json_only = alone
            .tiplocs
            .iter()
            .find(|t| t.tiploc == "JSONTPL")
            .unwrap();
        assert!(crosswalk.tiplocs.contains(json_only));
    }
}
