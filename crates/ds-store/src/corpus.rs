//! CORPUS locations: `corpus.rs` and `corpus_crosswalk.rs` (whole),
//! `corpus_comparison` (whole: `log_after_load` needs all of it), and the
//! load checks `CorpusLoadRequest`, `corpus_load_problem` and
//! `is_sha256_hex` from `routes/ingest.rs`.
//!
//! The CORPUS fallback flag (`CORPUS_FALLBACK_ENABLED`) moved first, in
//! wave 0; the rest moved in plan task 1A.6. The crosswalk and the
//! comparison are the submodules [`crosswalk`] and [`comparison`].
//!
//! Network Rail CORPUS location reference data (`corpus_locations`,
//! `corpus_deliveries`), loaded by `schedule-ingest` through
//! `POST /private/corpus-locations`. See
//! `crates/ds-store/migrations/20260928100000_corpus_locations.sql` and
//! docs/superpowers/specs/2026-09-28-corpus-sftp-ingest-design.md.
//!
//! Every load also rebuilds the CORPUS-derived crosswalk in the same
//! transaction ([`crosswalk`]), which only the off-by-default lookup
//! fallback reads, and the route logs a comparison against the timetable
//! crosswalk ([`comparison`]).

use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

pub mod comparison;
pub mod crosswalk;

/// Env var turning the fallback on. Unset or `false`: off.
pub const FALLBACK_ENV: &str = "CORPUS_FALLBACK_ENABLED";

static FALLBACK_ENABLED: AtomicBool = AtomicBool::new(false);

/// Whether the lookups fall back to CORPUS. Always false unless
/// [`init_fallback_from_env`] read `true`.
pub fn fallback_enabled() -> bool {
    FALLBACK_ENABLED.load(Ordering::Relaxed)
}

/// Parses a [`FALLBACK_ENV`] value: unset or blank is off, anything other
/// than `true`/`false` is a startup error rather than a silent default.
pub fn parse_fallback_flag(value: Option<&str>) -> Result<bool> {
    match value.map(str::trim) {
        None | Some("") => Ok(false),
        Some(v) if v.eq_ignore_ascii_case("true") => Ok(true),
        Some(v) if v.eq_ignore_ascii_case("false") => Ok(false),
        Some(v) => anyhow::bail!("{FALLBACK_ENV} must be true or false, got {v:?}"),
    }
}

/// Reads [`FALLBACK_ENV`] once at startup and logs the outcome.
pub fn init_fallback_from_env() -> Result<()> {
    let enabled = parse_fallback_flag(std::env::var(FALLBACK_ENV).ok().as_deref())?;
    FALLBACK_ENABLED.store(enabled, Ordering::Relaxed);
    if enabled {
        tracing::info!(
            "CORPUS fallback ON: TIPLOC/STANOX lookups fall back to corpus_tiploc_crs/corpus_stanox_crs after the timetable crosswalk"
        );
    }
    Ok(())
}

/// Serialises concurrent loads (`pg_advisory_xact_lock` key). Arbitrary but
/// fixed; only this module takes it.
const CORPUS_LOAD_LOCK_KEY: i64 = 0x0C0B_9053;

/// One CORPUS location, already normalised by `schedule-ingest` (trimmed,
/// blank as `None`, digit codes zero-padded). Mirrors
/// `schedule-ingest::corpus::CorpusLocation` field for field.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CorpusLocation {
    pub nlc: String,
    pub stanox: Option<String>,
    pub tiploc: Option<String>,
    pub crs: Option<String>,
    pub uic: Option<String>,
    pub nlc_desc: Option<String>,
    pub nlc_desc16: Option<String>,
}

/// Replaces every row of `corpus_locations` with `locations` and records the
/// delivery in `corpus_deliveries`, all in one transaction. Returns the
/// number of rows inserted.
///
/// `DELETE` rather than `TRUNCATE`: `TRUNCATE` takes an `ACCESS EXCLUSIVE`
/// lock that would block every reader until commit, while a `DELETE` leaves
/// readers on the old snapshot until the new one commits. The ~56k dead
/// tuples a month are autovacuum's job.
///
/// The derived crosswalk (`corpus_tiploc_crs`/`corpus_stanox_crs`) is
/// rebuilt from `locations` in the same transaction, restricted to the
/// current `stations`, so it always matches the stored set.
///
/// Callers must reject an empty `locations` first (the route does): an
/// empty load would wipe the table.
pub async fn replace_corpus_locations(
    pool: &PgPool,
    delivered_at: DateTime<Utc>,
    source_file: &str,
    locations: &[CorpusLocation],
) -> Result<u64> {
    replace_corpus_locations_with_provenance(
        pool,
        delivered_at,
        source_file,
        &DeliveredFileProvenance::default(),
        locations,
    )
    .await
}

/// The delivered CORPUS file's size and SHA-256 as `schedule-ingest` read
/// it, recorded in `corpus_deliveries` (migration
/// `20261001150000_schedule_feed_delivery_sha256.sql`). `None` from an
/// older `schedule-ingest`.
#[derive(Debug, Clone, Copy, Default)]
pub struct DeliveredFileProvenance<'a> {
    pub bytes: Option<i64>,
    pub sha256: Option<&'a str>,
}

/// [`replace_corpus_locations`], also recording the delivered file's
/// provenance in its `corpus_deliveries` row.
pub async fn replace_corpus_locations_with_provenance(
    pool: &PgPool,
    delivered_at: DateTime<Utc>,
    source_file: &str,
    provenance: &DeliveredFileProvenance<'_>,
    locations: &[CorpusLocation],
) -> Result<u64> {
    anyhow::ensure!(
        !locations.is_empty(),
        "refusing to replace corpus_locations with an empty delivery"
    );
    let row_count = i32::try_from(locations.len())?;
    let nlc: Vec<&str> = locations.iter().map(|l| l.nlc.as_str()).collect();
    let column = |f: fn(&CorpusLocation) -> &Option<String>| -> Vec<Option<&str>> {
        locations.iter().map(|l| f(l).as_deref()).collect()
    };
    let stanox = column(|l| &l.stanox);
    let tiploc = column(|l| &l.tiploc);
    let crs = column(|l| &l.crs);
    let uic = column(|l| &l.uic);
    let nlc_desc = column(|l| &l.nlc_desc);
    let nlc_desc16 = column(|l| &l.nlc_desc16);

    let (_, crosswalk) = crosswalk::derive(&crosswalk::corpus_rows(locations));

    let mut tx = pool.begin().await?;
    take_load_lock(&mut tx).await?;
    sqlx::query("DELETE FROM corpus_locations")
        .execute(&mut *tx)
        .await?;
    let inserted = sqlx::query(
        r"
        INSERT INTO corpus_locations
            (nlc, stanox, tiploc, crs, uic, nlc_desc, nlc_desc16, delivered_at, source_file)
        SELECT nlc, stanox, tiploc, crs, uic, nlc_desc, nlc_desc16, $8, $9
        FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[])
            AS i(nlc, stanox, tiploc, crs, uic, nlc_desc, nlc_desc16)
        ",
    )
    .bind(&nlc)
    .bind(&stanox)
    .bind(&tiploc)
    .bind(&crs)
    .bind(&uic)
    .bind(&nlc_desc)
    .bind(&nlc_desc16)
    .bind(delivered_at)
    .bind(source_file)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    sqlx::query(
        "INSERT INTO corpus_deliveries \
             (delivered_at, source_file, row_count, loaded_at, source_bytes, sha256) \
         VALUES ($1, $2, $3, now(), $4, $5) \
         ON CONFLICT (delivered_at) DO UPDATE SET \
             source_file = EXCLUDED.source_file, \
             row_count = EXCLUDED.row_count, \
             loaded_at = EXCLUDED.loaded_at, \
             source_bytes = EXCLUDED.source_bytes, \
             sha256 = EXCLUDED.sha256",
    )
    .bind(delivered_at)
    .bind(source_file)
    .bind(row_count)
    .bind(provenance.bytes)
    .bind(provenance.sha256)
    .execute(&mut *tx)
    .await?;
    crosswalk::write(&mut tx, delivered_at, crosswalk).await?;
    tx.commit().await?;
    Ok(inserted)
}

/// Takes the CORPUS load lock for the rest of `tx`: loads and crosswalk
/// rebuilds serialise on it.
pub(crate) async fn take_load_lock(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> Result<()> {
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(CORPUS_LOAD_LOCK_KEY)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// The newest loaded delivery's `delivered_at`, or `None` before the first
/// load -- the CORPUS freshness read.
pub async fn last_corpus_delivery(pool: &PgPool) -> Result<Option<DateTime<Utc>>> {
    let (latest,): (Option<DateTime<Utc>>,) =
        sqlx::query_as("SELECT MAX(delivered_at) FROM corpus_deliveries")
            .fetch_one(pool)
            .await?;
    Ok(latest)
}

/// `distant_signal_api_corpus_last_delivered_at_seconds`: the newest loaded
/// CORPUS delivery's `delivered_at` as a Unix timestamp, the series behind
/// the chart's `DistantSignalCorpusStale` alert.
///
/// Set from `corpus_deliveries` (the durable marker), not from the request
/// that just loaded: at startup ([`refresh_last_delivery_metric`] from
/// `main`'s background loops) and after every load. `schedule-ingest`'s own
/// `schedule_feed_corpus_last_load_delivered_at_seconds` only exists in the
/// process that performed a load, so a restart between monthly deliveries
/// would make any alert on it silently absent; this one survives restarts.
/// Not set at all before the first load, so the alert cannot fire on a
/// freshly enabled install that is still waiting for its first delivery.
pub const LAST_DELIVERY_METRIC: &str = "api_corpus_last_delivered_at_seconds";

/// Reads the newest delivery and, if there is one, sets
/// [`LAST_DELIVERY_METRIC`]. Returns what it read.
#[expect(
    clippy::cast_precision_loss,
    reason = "metric gauges take f64, and these counts and timestamps stay far below 2^52"
)]
pub async fn refresh_last_delivery_metric(pool: &PgPool) -> Result<Option<DateTime<Utc>>> {
    let latest = last_corpus_delivery(pool).await?;
    if let Some(delivered_at) = latest {
        metrics::gauge!(common::metrics::metric_name(LAST_DELIVERY_METRIC))
            .set(delivered_at.timestamp() as f64);
    }
    Ok(latest)
}

/// Whether `value` is a SHA-256 as `schedule-ingest` writes it: 64
/// lowercase hex digits. The columns' CHECK constraints say the same; this
/// turns a bad value into a 422 rather than a 500.
pub fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// One Network Rail CORPUS delivery from `schedule-ingest`'s CORPUS mode.
/// Mirrors `schedule-ingest::corpus::CorpusLoadRequest` field for field.
/// `delivered_at` is the delivered file's own mtime.
/// `source_bytes`/`sha256` describe the delivered file as `schedule-ingest`
/// read it; absent from an older `schedule-ingest`.
#[derive(Debug, Deserialize)]
pub struct CorpusLoadRequest {
    pub delivered_at: DateTime<Utc>,
    pub source_file: String,
    pub locations: Vec<CorpusLocation>,
    #[serde(default)]
    pub source_bytes: Option<u64>,
    #[serde(default)]
    pub sha256: Option<String>,
}

pub fn corpus_load_problem(req: &CorpusLoadRequest) -> Option<String> {
    if req.locations.is_empty() {
        return Some("a CORPUS delivery must carry at least one location".to_string());
    }
    if req.source_file.trim().is_empty() {
        return Some("source_file must not be blank".to_string());
    }
    req.locations
        .iter()
        .position(|l| l.nlc.trim().is_empty())
        .map(|i| format!("location {i} has a blank nlc"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_flag_is_strict_and_off_by_default() {
        assert!(!parse_fallback_flag(None).unwrap());
        assert!(!parse_fallback_flag(Some(" ")).unwrap());
        assert!(!parse_fallback_flag(Some("false")).unwrap());
        assert!(parse_fallback_flag(Some("true")).unwrap());
        assert!(parse_fallback_flag(Some("TRUE")).unwrap());
        assert!(parse_fallback_flag(Some("1")).is_err());
        assert!(parse_fallback_flag(Some("yes")).is_err());
        assert!(!fallback_enabled());
    }
}

#[cfg(test)]
mod corpus_load_validation_tests {
    use super::*;

    fn request(body: serde_json::Value) -> CorpusLoadRequest {
        serde_json::from_value(body).expect("valid CorpusLoadRequest JSON")
    }

    fn location(nlc: &str) -> serde_json::Value {
        serde_json::json!({
            "nlc": nlc, "stanox": "87219", "tiploc": "CLPHMJN", "crs": "CLJ",
            "uic": "55950", "nlc_desc": "CLAPHAM JUNCTION LONDON", "nlc_desc16": null
        })
    }

    #[test]
    fn a_well_formed_delivery_has_no_problem() {
        let req = request(serde_json::json!({
            "delivered_at": "2026-09-28T03:00:00Z",
            "source_file": "CORPUSExtract.json.gz",
            "locations": [location("559500")]
        }));
        assert_eq!(corpus_load_problem(&req), None);
    }

    #[test]
    fn an_empty_delivery_a_blank_nlc_or_a_blank_source_is_refused() {
        let empty = request(serde_json::json!({
            "delivered_at": "2026-09-28T03:00:00Z",
            "source_file": "CORPUSExtract.json.gz",
            "locations": []
        }));
        assert!(corpus_load_problem(&empty).is_some());

        let blank_nlc = request(serde_json::json!({
            "delivered_at": "2026-09-28T03:00:00Z",
            "source_file": "CORPUSExtract.json.gz",
            "locations": [location("559500"), location(" ")]
        }));
        assert_eq!(
            corpus_load_problem(&blank_nlc).as_deref(),
            Some("location 1 has a blank nlc")
        );

        let blank_source = request(serde_json::json!({
            "delivered_at": "2026-09-28T03:00:00Z",
            "source_file": "",
            "locations": [location("559500")]
        }));
        assert!(corpus_load_problem(&blank_source).is_some());
    }

    #[test]
    fn corpus_provenance_is_optional() {
        let req = request(serde_json::json!({
            "delivered_at": "2026-09-28T03:00:00Z",
            "source_file": "CORPUSExtract.json.gz",
            "locations": [location("559500")],
            "source_bytes": 295_957,
            "sha256": "0f".repeat(32)
        }));
        assert_eq!(req.source_bytes, Some(295_957));
        assert!(req.sha256.as_deref().is_some_and(is_sha256_hex));
    }
}

/// Database-gated: each test gets its own throwaway database
/// (`#[sqlx::test]`, migrated from `crates/ds-store/migrations`), because a
/// load replaces the WHOLE table and must never run against a shared
/// database that may hold a real extract (see `crate::test_support`'s module
/// doc). Needs a `DATABASE_URL` whose role may create databases.
#[cfg(test)]
#[expect(
    clippy::items_after_statements,
    reason = "test code: fixtures sit next to their use"
)]
mod db_tests {
    use chrono::{DateTime, TimeZone, Utc};

    use super::*;

    fn location(nlc: &str, tiploc: Option<&str>, crs: Option<&str>) -> CorpusLocation {
        CorpusLocation {
            nlc: nlc.to_string(),
            stanox: Some("87219".to_string()),
            tiploc: tiploc.map(str::to_string),
            crs: crs.map(str::to_string),
            uic: None,
            nlc_desc: Some("CLAPHAM JUNCTION LONDON".to_string()),
            nlc_desc16: None,
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn a_load_replaces_the_whole_set_and_records_the_delivery(pool: PgPool) {
        let first = Utc.with_ymd_and_hms(2026, 9, 1, 3, 0, 0).unwrap();
        let second = Utc.with_ymd_and_hms(2026, 10, 1, 3, 0, 0).unwrap();
        assert_eq!(last_corpus_delivery(&pool).await.unwrap(), None);

        let n = replace_corpus_locations(
            &pool,
            first,
            "CORPUSExtract.json.gz",
            &[
                location("559500", Some("CLPHMJN"), Some("CLJ")),
                location("559569", Some("CLPHMJC"), None),
                location("000800", None, None),
            ],
        )
        .await
        .unwrap();
        assert_eq!(n, 3);

        let n = replace_corpus_locations(
            &pool,
            second,
            "CORPUSExtract.json.gz",
            &[location("559500", Some("CLPHMJN"), Some("CLJ"))],
        )
        .await
        .unwrap();
        assert_eq!(n, 1);

        type Row = (String, Option<String>, Option<String>, DateTime<Utc>);
        let rows: Vec<Row> = sqlx::query_as(
            "SELECT nlc, tiploc, crs, delivered_at FROM corpus_locations ORDER BY nlc",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            rows,
            vec![(
                "559500".to_string(),
                Some("CLPHMJN".to_string()),
                Some("CLJ".to_string()),
                second
            )]
        );
        assert_eq!(last_corpus_delivery(&pool).await.unwrap(), Some(second));
        let (deliveries,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM corpus_deliveries")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(deliveries, 2);
    }

    /// Loading the same delivery twice (a restart between the load and the
    /// file's archive move) is idempotent.
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn reloading_the_same_delivery_is_idempotent(pool: PgPool) {
        let at = Utc.with_ymd_and_hms(2026, 9, 1, 3, 0, 0).unwrap();
        let rows = [location("559500", Some("CLPHMJN"), Some("CLJ"))];
        for _ in 0..2 {
            replace_corpus_locations(&pool, at, "CORPUSExtract.json.gz", &rows)
                .await
                .unwrap();
        }
        let (locations, deliveries): (i64, i64) = sqlx::query_as(
            "SELECT (SELECT COUNT(*) FROM corpus_locations), (SELECT COUNT(*) FROM corpus_deliveries)",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!((locations, deliveries), (1, 1));
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn an_empty_load_is_refused_and_keeps_the_current_set(pool: PgPool) {
        let at = Utc.with_ymd_and_hms(2026, 9, 1, 3, 0, 0).unwrap();
        replace_corpus_locations(
            &pool,
            at,
            "CORPUSExtract.json.gz",
            &[location("559500", Some("CLPHMJN"), Some("CLJ"))],
        )
        .await
        .unwrap();
        assert!(
            replace_corpus_locations(&pool, at, "CORPUSExtract.json.gz", &[])
                .await
                .is_err()
        );
        let (locations,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM corpus_locations")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(locations, 1);
    }

    /// The delivered file's size and SHA-256 land in `corpus_deliveries`;
    /// the provenance-less wrapper leaves them NULL.
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn a_load_records_the_delivered_file_provenance(pool: PgPool) {
        let at = Utc.with_ymd_and_hms(2026, 9, 1, 3, 0, 0).unwrap();
        let sha = "0f".repeat(32);
        let rows = [location("559500", Some("CLPHMJN"), Some("CLJ"))];
        replace_corpus_locations_with_provenance(
            &pool,
            at,
            "CORPUSExtract.json.gz",
            &DeliveredFileProvenance {
                bytes: Some(295_957),
                sha256: Some(&sha),
            },
            &rows,
        )
        .await
        .unwrap();
        let read = || async {
            let row: (Option<i64>, Option<String>) =
                sqlx::query_as("SELECT source_bytes, sha256 FROM corpus_deliveries")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            row
        };
        assert_eq!(read().await, (Some(295_957), Some(sha.clone())));

        replace_corpus_locations(&pool, at, "CORPUSExtract.json.gz", &rows)
            .await
            .unwrap();
        assert_eq!(read().await, (None, None));
    }
}
