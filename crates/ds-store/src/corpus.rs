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
/// delivery in `corpus_deliveries`, all in one transaction. Returns what it
/// did: a delivery whose content matches the last applied one
/// ([`content_sha256`]) is [`CorpusLoad::Unchanged`] and writes nothing but
/// that delivery's redelivery marker.
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
) -> Result<CorpusLoad> {
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
/// `20261001150000_schedule_feed_delivery_sha256.sql`), and the delivery
/// source it came from, one of [`DELIVERY_SOURCES`]
/// (`20261021090500_schedule_delivery_source.sql`). `None` from an older
/// `schedule-ingest`.
#[derive(Debug, Clone, Copy, Default)]
pub struct DeliveredFileProvenance<'a> {
    pub bytes: Option<i64>,
    pub sha256: Option<&'a str>,
    pub delivery_source: Option<&'a str>,
}

/// The `delivery_source` values of `schedule_feed_ingests` and
/// `corpus_deliveries` (their CHECKs): `schedule-ingest`'s SFTP push and
/// GCS bucket sources.
pub const DELIVERY_SOURCES: [&str; 2] = ["sftp", "bucket"];

/// Why a record's `delivery_source` must be refused, or `None`: anything
/// but absent or one of [`DELIVERY_SOURCES`].
pub fn delivery_source_problem(delivery_source: Option<&str>) -> Option<String> {
    delivery_source
        .filter(|source| !DELIVERY_SOURCES.contains(source))
        .map(|_| "delivery_source must be sftp or bucket".to_string())
}

/// [`replace_corpus_locations`], also recording the delivered file's
/// provenance in its `corpus_deliveries` row.
pub async fn replace_corpus_locations_with_provenance(
    pool: &PgPool,
    delivered_at: DateTime<Utc>,
    source_file: &str,
    provenance: &DeliveredFileProvenance<'_>,
    locations: &[CorpusLocation],
) -> Result<CorpusLoad> {
    replace(
        pool,
        delivered_at,
        source_file,
        provenance,
        locations,
        Keep::Nothing,
    )
    .await
}

/// SQL: the rows that came from an NLC XML snapshot, as
/// [`crosswalk::is_xml_snapshot_name`] decides.
const XML_SOURCE_FILE: &str = r"source_file ~* '^nlc.*\.xml\.gz$'";

/// Loads an NLC XML snapshot (CORPUS API v2) as the primary layer and keeps
/// the rows of the last JSON extract as a frozen fallback
/// ([`crosswalk::CorpusLayers`]). In one transaction, under the load lock:
/// deletes only the rows of earlier XML snapshots, inserts `locations`,
/// records the delivery and rebuilds the crosswalk from both layers (the
/// XML wins every key it resolves).
///
/// The JSON rows keep their own `source_file` and `delivered_at`, so they
/// can be told apart and dropped later; a JSON load
/// ([`replace_corpus_locations_with_provenance`]) replaces both layers.
/// With no JSON rows in the table this is a plain whole-table replace.
pub async fn replace_corpus_locations_keeping_json_fallback(
    pool: &PgPool,
    delivered_at: DateTime<Utc>,
    source_file: &str,
    provenance: &DeliveredFileProvenance<'_>,
    locations: &[CorpusLocation],
) -> Result<CorpusLoad> {
    anyhow::ensure!(
        crosswalk::is_xml_snapshot_name(source_file),
        "only an NLC XML snapshot is loaded over a frozen JSON fallback, not {source_file:?}"
    );
    replace(
        pool,
        delivered_at,
        source_file,
        provenance,
        locations,
        Keep::JsonRows,
    )
    .await
}

/// What a load keeps of the current `corpus_locations`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Keep {
    Nothing,
    /// The rows whose `source_file` is not an XML snapshot.
    JsonRows,
}

/// What a CORPUS load did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorpusLoad {
    /// The delivery replaced `corpus_locations` (and rebuilt the
    /// crosswalk): `rows` inserted.
    Applied { rows: u64 },
    /// The delivery's content ([`content_sha256`]) matches the last applied
    /// delivery's, so nothing was replaced or rebuilt: only that delivery's
    /// `redelivered_at` and `redelivery_count` moved. A redelivery of the
    /// same rows in a differently compressed file (DS-390).
    Unchanged {
        /// The applied delivery this one repeats.
        applied_delivered_at: DateTime<Utc>,
    },
}

/// Version tag of [`content_sha256`]'s encoding: a change to it makes every
/// stored hash differ, so the next load applies (never a missed change).
const CONTENT_HASH_VERSION: &str = "ds-corpus-content-v1";

/// The SHA-256 (64 lowercase hex digits) of what a load applies:
/// `source_file`, whether the JSON fallback is kept, and the normalised
/// rows in order. Two files with different bytes (a new gzip header or
/// mtime) but the same rows hash the same; any change to a row, the row
/// order, the file name or the kind of load hashes differently. Every
/// value is length-prefixed and absent values are tagged, so no two
/// different inputs share an encoding.
pub fn content_sha256(
    source_file: &str,
    keep_json_fallback: bool,
    locations: &[CorpusLocation],
) -> String {
    use sha2::{Digest, Sha256};
    fn put(hasher: &mut Sha256, value: Option<&str>) {
        match value {
            None => hasher.update([0]),
            Some(value) => {
                hasher.update([1]);
                hasher.update((value.len() as u64).to_be_bytes());
                hasher.update(value.as_bytes());
            }
        }
    }
    let mut hasher = Sha256::new();
    put(&mut hasher, Some(CONTENT_HASH_VERSION));
    hasher.update([u8::from(keep_json_fallback)]);
    put(&mut hasher, Some(source_file));
    hasher.update((locations.len() as u64).to_be_bytes());
    for l in locations {
        for value in [
            Some(l.nlc.as_str()),
            l.stanox.as_deref(),
            l.tiploc.as_deref(),
            l.crs.as_deref(),
            l.uic.as_deref(),
            l.nlc_desc.as_deref(),
            l.nlc_desc16.as_deref(),
        ] {
            put(&mut hasher, value);
        }
    }
    hasher
        .finalize()
        .iter()
        .fold(String::with_capacity(64), |mut hex, b| {
            use std::fmt::Write as _;
            let _ = write!(hex, "{b:02x}");
            hex
        })
}

/// The last applied CORPUS delivery: the `corpus_deliveries` row with the
/// newest `loaded_at`, the one whose rows `corpus_locations` holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedDelivery {
    pub delivered_at: DateTime<Utc>,
    /// `None` for a delivery loaded before content hashes (migration
    /// `20261107140000_corpus_deliveries_content_sha256.sql`) or by an
    /// older `schedule-ingest`.
    pub content_sha256: Option<String>,
}

impl AppliedDelivery {
    /// What identifies this delivery's content: its content hash, else
    /// (no hash) its `delivered_at`, so a hash-less delivery still counts
    /// as new content.
    pub fn content_key(&self) -> String {
        self.content_sha256
            .clone()
            .unwrap_or_else(|| format!("delivered_at:{}", self.delivered_at.to_rfc3339()))
    }
}

/// The last applied delivery ([`AppliedDelivery`]), or `None` before the
/// first load. One read of a small table.
pub async fn applied_corpus_delivery<'e, E>(executor: E) -> Result<Option<AppliedDelivery>>
where
    E: sqlx::PgExecutor<'e>,
{
    let row: Option<(DateTime<Utc>, Option<String>)> = sqlx::query_as(
        "SELECT delivered_at, content_sha256 FROM corpus_deliveries \
         ORDER BY loaded_at DESC, delivered_at DESC LIMIT 1",
    )
    .fetch_optional(executor)
    .await?;
    Ok(row.map(|(delivered_at, content_sha256)| AppliedDelivery {
        delivered_at,
        content_sha256,
    }))
}

async fn replace(
    pool: &PgPool,
    delivered_at: DateTime<Utc>,
    source_file: &str,
    provenance: &DeliveredFileProvenance<'_>,
    locations: &[CorpusLocation],
    keep: Keep,
) -> Result<CorpusLoad> {
    anyhow::ensure!(
        !locations.is_empty(),
        "refusing to replace corpus_locations with an empty delivery"
    );
    // Before the redelivery check, so an unchanged delivery is refused as
    // the column's CHECK refuses a changed one.
    if let Some(problem) = delivery_source_problem(provenance.delivery_source) {
        anyhow::bail!(problem);
    }
    let content = content_sha256(source_file, keep == Keep::JsonRows, locations);
    // Under the load lock, so the last applied delivery cannot change
    // between this check and the write.
    let mut tx = pool.begin().await?;
    take_load_lock(&mut tx).await?;
    if let Some(applied) = applied_corpus_delivery(&mut *tx).await?
        && applied.content_sha256.as_deref() == Some(content.as_str())
    {
        sqlx::query(
            "UPDATE corpus_deliveries SET \
                 redelivered_at = GREATEST(redelivered_at, $2), \
                 redelivery_count = redelivery_count + 1 \
             WHERE delivered_at = $1",
        )
        .bind(applied.delivered_at)
        .bind(delivered_at)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        tracing::info!(
            delivered_at = %delivered_at,
            source_file,
            applied_delivered_at = %applied.delivered_at,
            content_sha256 = %content,
            "CORPUS delivery repeats the applied one's content; nothing replaced"
        );
        return Ok(CorpusLoad::Unchanged {
            applied_delivered_at: applied.delivered_at,
        });
    }
    apply(
        tx,
        delivered_at,
        source_file,
        provenance,
        locations,
        keep,
        &content,
    )
    .await
}

/// [`replace`]'s write, in `tx` (which holds the load lock): the delete,
/// the insert, the delivery row and the crosswalk.
async fn apply(
    mut tx: sqlx::Transaction<'_, sqlx::Postgres>,
    delivered_at: DateTime<Utc>,
    source_file: &str,
    provenance: &DeliveredFileProvenance<'_>,
    locations: &[CorpusLocation],
    keep: Keep,
    content: &str,
) -> Result<CorpusLoad> {
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

    match keep {
        Keep::Nothing => {
            sqlx::query("DELETE FROM corpus_locations")
                .execute(&mut *tx)
                .await?;
        }
        Keep::JsonRows => {
            sqlx::query(&format!(
                "DELETE FROM corpus_locations WHERE {XML_SOURCE_FILE}"
            ))
            .execute(&mut *tx)
            .await?;
        }
    }
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
             (delivered_at, source_file, row_count, loaded_at, source_bytes, sha256, \
              delivery_source, content_sha256) \
         VALUES ($1, $2, $3, clock_timestamp(), $4, $5, $6, $7) \
         ON CONFLICT (delivered_at) DO UPDATE SET \
             source_file = EXCLUDED.source_file, \
             row_count = EXCLUDED.row_count, \
             loaded_at = EXCLUDED.loaded_at, \
             source_bytes = EXCLUDED.source_bytes, \
             sha256 = EXCLUDED.sha256, \
             delivery_source = EXCLUDED.delivery_source, \
             content_sha256 = EXCLUDED.content_sha256, \
             redelivered_at = NULL, \
             redelivery_count = 0",
    )
    .bind(delivered_at)
    .bind(source_file)
    .bind(row_count)
    .bind(provenance.bytes)
    .bind(provenance.sha256)
    .bind(provenance.delivery_source)
    .bind(content)
    .execute(&mut *tx)
    .await?;
    let (_, crosswalk) = match keep {
        Keep::Nothing => crosswalk::derive(&crosswalk::corpus_rows(locations)),
        Keep::JsonRows => crosswalk::derive_layers(&crosswalk::load_corpus_layers(&mut *tx).await?),
    };
    crosswalk::write(&mut tx, delivered_at, crosswalk).await?;
    tx.commit().await?;
    Ok(CorpusLoad::Applied { rows: inserted })
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

/// SQL: the newest CORPUS delivery received, applied or an unchanged
/// redelivery ([`CorpusLoad::Unchanged`]): the CORPUS freshness. `GREATEST`
/// ignores the NULL `redelivered_at` of a delivery never repeated.
pub(crate) const LAST_DELIVERY_SQL: &str =
    "SELECT GREATEST(MAX(delivered_at), MAX(redelivered_at)) FROM corpus_deliveries";

/// The newest received delivery's mtime (`delivered_at`, or a later
/// identical copy's `redelivered_at`), or `None` before the first load --
/// the CORPUS freshness read. An unchanged redelivery still counts: the
/// feed is alive and the stored CORPUS is current as of that copy.
pub async fn last_corpus_delivery(pool: &PgPool) -> Result<Option<DateTime<Utc>>> {
    let (latest,): (Option<DateTime<Utc>>,) =
        sqlx::query_as(LAST_DELIVERY_SQL).fetch_one(pool).await?;
    Ok(latest)
}

/// `distant_signal_store_corpus_last_delivered_at_seconds`: the newest loaded
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
pub const LAST_DELIVERY_METRIC: &str = "store_corpus_last_delivered_at_seconds";

/// Reads the newest delivery and, if there is one, sets
/// [`LAST_DELIVERY_METRIC`]. Returns what it read.
#[expect(
    clippy::cast_precision_loss,
    reason = "metric gauges take f64, and these counts and timestamps stay far below 2^52"
)]
pub async fn refresh_last_delivery_metric(pool: &PgPool) -> Result<Option<DateTime<Utc>>> {
    let latest = last_corpus_delivery(pool).await?;
    if let Some(delivered_at) = latest {
        metrics::gauge!(ds_runtime::metrics::metric_name(LAST_DELIVERY_METRIC))
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
/// read it, `delivery_source` the source it came from; absent from an older
/// `schedule-ingest`.
#[derive(Debug, Deserialize)]
pub struct CorpusLoadRequest {
    pub delivered_at: DateTime<Utc>,
    pub source_file: String,
    pub locations: Vec<CorpusLocation>,
    #[serde(default)]
    pub source_bytes: Option<u64>,
    #[serde(default)]
    pub sha256: Option<String>,
    #[serde(default)]
    pub delivery_source: Option<String>,
}

/// Why a CORPUS load must be refused (the api's 400), or `None`: an empty
/// delivery, a blank `source_file` or a blank NLC. See
/// [`corpus_delivery_problem`].
pub fn corpus_load_problem(req: &CorpusLoadRequest) -> Option<String> {
    corpus_delivery_problem(&req.source_file, &req.locations)
}

/// [`corpus_load_problem`] on the parts of a load, for a caller holding
/// them borrowed (`schedule-ingest`'s direct sink, plan 2d.1).
pub fn corpus_delivery_problem(source_file: &str, locations: &[CorpusLocation]) -> Option<String> {
    if locations.is_empty() {
        return Some("a CORPUS delivery must carry at least one location".to_string());
    }
    if source_file.trim().is_empty() {
        return Some("source_file must not be blank".to_string());
    }
    locations
        .iter()
        .position(|l| l.nlc.trim().is_empty())
        .map(|i| format!("location {i} has a blank nlc"))
}

/// Why a CORPUS load's provenance must be refused (the api's 422), or
/// `None`: a `sha256` that is not 64 lowercase hex digits, or a
/// `source_bytes` too large for the `bigint` column.
pub fn corpus_provenance_problem(
    sha256: Option<&str>,
    source_bytes: Option<u64>,
) -> Option<String> {
    (sha256.is_some_and(|s| !is_sha256_hex(s))
        || source_bytes.is_some_and(|b| i64::try_from(b).is_err()))
    .then(|| "sha256 must be 64 lowercase hex digits and source_bytes in range".to_string())
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
mod content_hash_tests {
    use super::*;

    fn row(nlc: &str, tiploc: Option<&str>) -> CorpusLocation {
        CorpusLocation {
            nlc: nlc.to_string(),
            stanox: Some("87219".to_string()),
            tiploc: tiploc.map(str::to_string),
            crs: Some("CLJ".to_string()),
            uic: None,
            nlc_desc: Some("CLAPHAM JUNCTION LONDON".to_string()),
            nlc_desc16: None,
        }
    }

    #[test]
    fn the_same_rows_hash_the_same_and_any_change_differs() {
        let rows = [row("559500", Some("CLPHMJN")), row("000800", None)];
        let hash = content_sha256("NLC20261106.xml.gz", true, &rows);
        assert!(is_sha256_hex(&hash), "{hash}");
        assert_eq!(
            hash,
            content_sha256("NLC20261106.xml.gz", true, &rows.clone())
        );

        let mut changed_value = rows.clone();
        changed_value[1].nlc_desc16 = Some("CLAPHAM".to_string());
        let mut blank_not_absent = rows.clone();
        blank_not_absent[1].tiploc = Some(String::new());
        // The same characters moved between two fields.
        let mut shifted = rows.clone();
        shifted[0].crs = Some("CLJ8".to_string());
        shifted[0].stanox = Some("7219".to_string());
        let reordered = [rows[1].clone(), rows[0].clone()];
        let others = [
            content_sha256("NLC20261107.xml.gz", true, &rows),
            content_sha256("NLC20261106.xml.gz", false, &rows),
            content_sha256("NLC20261106.xml.gz", true, &rows[..1]),
            content_sha256("NLC20261106.xml.gz", true, &changed_value),
            content_sha256("NLC20261106.xml.gz", true, &blank_not_absent),
            content_sha256("NLC20261106.xml.gz", true, &shifted),
            content_sha256("NLC20261106.xml.gz", true, &reordered),
        ];
        for (i, other) in others.iter().enumerate() {
            assert_ne!(*other, hash, "change {i} hashed the same");
        }
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

    #[test]
    fn malformed_provenance_is_refused_and_absent_provenance_is_not() {
        let sha = "0f".repeat(32);
        assert_eq!(corpus_provenance_problem(None, None), None);
        assert_eq!(corpus_provenance_problem(Some(&sha), Some(295_957)), None);
        let problem = Some("sha256 must be 64 lowercase hex digits and source_bytes in range");
        assert_eq!(
            corpus_provenance_problem(Some(&sha.to_uppercase()), None).as_deref(),
            problem
        );
        assert_eq!(
            corpus_provenance_problem(None, Some(u64::MAX)).as_deref(),
            problem
        );
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
        assert_eq!(n, CorpusLoad::Applied { rows: 3 });

        let n = replace_corpus_locations(
            &pool,
            second,
            "CORPUSExtract.json.gz",
            &[location("559500", Some("CLPHMJN"), Some("CLJ"))],
        )
        .await
        .unwrap();
        assert_eq!(n, CorpusLoad::Applied { rows: 1 });

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

    /// The delivered file's size, SHA-256 and delivery source land in
    /// `corpus_deliveries`; the provenance-less wrapper leaves them NULL,
    /// and the column's CHECK refuses an unknown source.
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
                delivery_source: Some("bucket"),
            },
            &rows,
        )
        .await
        .unwrap();
        let read = || async {
            let row: (Option<i64>, Option<String>, Option<String>) = sqlx::query_as(
                "SELECT source_bytes, sha256, delivery_source FROM corpus_deliveries",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            row
        };
        assert_eq!(
            read().await,
            (Some(295_957), Some(sha.clone()), Some("bucket".to_string()))
        );

        // Different rows: a load that applies (the same rows would be an
        // unchanged redelivery, which keeps the applied provenance).
        let changed = [location("559500", Some("CLPHMJN"), None)];
        replace_corpus_locations(&pool, at, "CORPUSExtract.json.gz", &changed)
            .await
            .unwrap();
        assert_eq!(read().await, (None, None, None));

        for rows in [&rows[..], &changed[..]] {
            let refused = replace_corpus_locations_with_provenance(
                &pool,
                at,
                "CORPUSExtract.json.gz",
                &DeliveredFileProvenance {
                    delivery_source: Some("ftp"),
                    ..DeliveredFileProvenance::default()
                },
                rows,
            )
            .await;
            assert!(refused.is_err(), "an unknown source is refused");
            assert_eq!(read().await, (None, None, None), "rolled back");
        }
    }

    /// Counts `(corpus_locations rows, corpus_deliveries rows)` and the
    /// `corpus_locations` ids, which a rewrite would change.
    async fn stored(pool: &PgPool) -> (i64, Vec<i64>) {
        let (deliveries,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM corpus_deliveries")
            .fetch_one(pool)
            .await
            .unwrap();
        let ids: Vec<(i64,)> = sqlx::query_as("SELECT id FROM corpus_locations ORDER BY id")
            .fetch_all(pool)
            .await
            .unwrap();
        (deliveries, ids.into_iter().map(|(id,)| id).collect())
    }

    async fn redeliveries(pool: &PgPool) -> Vec<(DateTime<Utc>, Option<DateTime<Utc>>, i32)> {
        sqlx::query_as(
            "SELECT delivered_at, redelivered_at, redelivery_count FROM corpus_deliveries \
             ORDER BY delivered_at",
        )
        .fetch_all(pool)
        .await
        .unwrap()
    }

    /// DS-390: the same rows in a new file (another mtime and file
    /// SHA-256, as a fresh gzip of the same content has) replace nothing:
    /// no new delivery, the stored rows and crosswalk untouched, only the
    /// applied delivery's redelivery marker moves, and freshness reads the
    /// redelivery. A changed delivery after it applies in full.
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn an_identical_redelivery_is_a_no_op_and_a_change_applies(pool: PgPool) {
        let hour = |h| Utc.with_ymd_and_hms(2026, 11, 7, h, 12, 0).unwrap();
        let rows = [
            location("559500", Some("CLPHMJN"), Some("CLJ")),
            location("000800", None, None),
        ];
        async fn load(
            pool: &PgPool,
            at: DateTime<Utc>,
            sha: &str,
            rows: &[CorpusLocation],
        ) -> CorpusLoad {
            let provenance = DeliveredFileProvenance {
                bytes: Some(349_552),
                sha256: Some(sha),
                delivery_source: Some("sftp"),
            };
            replace_corpus_locations_with_provenance(
                pool,
                at,
                "CORPUSExtract.json.gz",
                &provenance,
                rows,
            )
            .await
            .unwrap()
        }
        let (sha_a, sha_b, sha_c) = ("aa".repeat(32), "bb".repeat(32), "cc".repeat(32));
        assert_eq!(
            load(&pool, hour(0), &sha_a, &rows).await,
            CorpusLoad::Applied { rows: 2 }
        );
        let before = stored(&pool).await;
        let build = || async {
            let (at,): (DateTime<Utc>,) =
                sqlx::query_as("SELECT delivered_at FROM corpus_crosswalk_build")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            at
        };
        assert_eq!(build().await, hour(0));

        for h in 1..=3 {
            assert_eq!(
                load(&pool, hour(h), &sha_b, &rows).await,
                CorpusLoad::Unchanged {
                    applied_delivered_at: hour(0)
                }
            );
        }
        assert_eq!(stored(&pool).await, before, "nothing rewritten or added");
        assert_eq!(build().await, hour(0), "the crosswalk was not rebuilt");
        assert_eq!(redeliveries(&pool).await, vec![(hour(0), Some(hour(3)), 3)]);
        let (sha,): (Option<String>,) = sqlx::query_as("SELECT sha256 FROM corpus_deliveries")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            sha.as_deref(),
            Some(sha_a.as_str()),
            "the applied file's provenance stays"
        );
        assert_eq!(last_corpus_delivery(&pool).await.unwrap(), Some(hour(3)));
        assert_eq!(
            crate::freshness::data_freshness(&pool).await.unwrap()[5],
            Some(hour(3))
        );
        let applied = applied_corpus_delivery(&pool).await.unwrap().unwrap();
        assert_eq!(applied.delivered_at, hour(0));
        assert_eq!(
            applied.content_sha256.as_deref(),
            Some(content_sha256("CORPUSExtract.json.gz", false, &rows).as_str())
        );

        // One changed row: applied in full, as a new delivery.
        let mut changed = rows.to_vec();
        changed[1].nlc_desc = Some("RENAMED".to_string());
        assert_eq!(
            load(&pool, hour(4), &sha_c, &changed).await,
            CorpusLoad::Applied { rows: 2 }
        );
        let (deliveries, ids) = stored(&pool).await;
        assert_eq!(deliveries, 2);
        assert!(
            ids.iter().all(|id| !before.1.contains(id)),
            "rows rewritten"
        );
        let (desc,): (Option<String>,) =
            sqlx::query_as("SELECT nlc_desc FROM corpus_locations WHERE nlc = '000800'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(desc.as_deref(), Some("RENAMED"));
        assert_eq!(build().await, hour(4));
        assert_eq!(
            applied_corpus_delivery(&pool)
                .await
                .unwrap()
                .unwrap()
                .delivered_at,
            hour(4)
        );
        assert_ne!(
            applied_corpus_delivery(&pool)
                .await
                .unwrap()
                .unwrap()
                .content_key(),
            applied.content_key()
        );

        // Back to the first content: compared with the last APPLIED
        // delivery (the changed one), so it applies again.
        assert_eq!(
            load(&pool, hour(5), &sha_a, &rows).await,
            CorpusLoad::Applied { rows: 2 }
        );
        let (desc,): (Option<String>,) =
            sqlx::query_as("SELECT nlc_desc FROM corpus_locations WHERE nlc = '000800'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(desc.as_deref(), Some("CLAPHAM JUNCTION LONDON"));
        assert_eq!(last_corpus_delivery(&pool).await.unwrap(), Some(hour(5)));
    }

    /// A delivery with no content hash (loaded before the column, or by an
    /// older schedule-ingest) is never matched: the same rows apply again.
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn a_delivery_without_a_content_hash_never_matches(pool: PgPool) {
        let first = Utc.with_ymd_and_hms(2026, 11, 6, 0, 0, 0).unwrap();
        let second = Utc.with_ymd_and_hms(2026, 11, 7, 0, 0, 0).unwrap();
        let rows = [location("559500", Some("CLPHMJN"), Some("CLJ"))];
        replace_corpus_locations(&pool, first, "CORPUSExtract.json.gz", &rows)
            .await
            .unwrap();
        sqlx::query("UPDATE corpus_deliveries SET content_sha256 = NULL")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            replace_corpus_locations(&pool, second, "CORPUSExtract.json.gz", &rows)
                .await
                .unwrap(),
            CorpusLoad::Applied { rows: 1 }
        );
        assert_eq!(stored(&pool).await.0, 2);
    }

    /// The XML path dedups too, and the frozen JSON fallback is kept
    /// through an unchanged XML redelivery.
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn an_identical_xml_redelivery_keeps_both_layers(pool: PgPool) {
        let json_at = Utc.with_ymd_and_hms(2026, 10, 1, 3, 0, 0).unwrap();
        replace_corpus_locations(
            &pool,
            json_at,
            "CORPUSExtract.json.gz",
            &[location("000800", None, None)],
        )
        .await
        .unwrap();
        let xml = [location("559500", Some("CLPHMJN"), Some("CLJ"))];
        let at = |h| Utc.with_ymd_and_hms(2026, 11, 7, h, 12, 0).unwrap();
        let provenance = DeliveredFileProvenance::default();
        let load = |h| {
            replace_corpus_locations_keeping_json_fallback(
                &pool,
                at(h),
                "NLC20261106.xml.gz",
                &provenance,
                &xml,
            )
        };
        assert_eq!(load(0).await.unwrap(), CorpusLoad::Applied { rows: 1 });
        let before = stored(&pool).await;
        assert_eq!(before.1.len(), 2, "XML plus the JSON fallback");
        assert_eq!(
            load(1).await.unwrap(),
            CorpusLoad::Unchanged {
                applied_delivered_at: at(0)
            }
        );
        assert_eq!(stored(&pool).await, before);
        let layers = crosswalk::load_corpus_layers(&pool).await.unwrap();
        assert_eq!((layers.primary.len(), layers.fallback.len()), (1, 1));
    }

    /// The comparing body runs on a database with no CORPUS yet (nothing
    /// to compare) and, after a load, compares and keeps succeeding.
    /// `#[sqlx::test]`: a CORPUS load replaces the whole table. Lives here,
    /// not in `loops`, so the non-superuser CI steps skip it with the other
    /// `corpus::db_tests` (they cannot create a database).
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn the_comparing_loop_ticks_before_and_after_a_load(pool: PgPool) {
        let spec = crate::loops::corpus_crosswalk_comparing(
            crate::loops::CORPUS_CROSSWALK_DEFAULT_INTERVAL,
        );
        spec.run_body(pool.clone()).await.unwrap();
        let at = "2026-09-01T03:00:00Z".parse().unwrap();
        let location = CorpusLocation {
            nlc: "559500".to_string(),
            stanox: Some("87219".to_string()),
            tiploc: Some("CLPHMJN".to_string()),
            crs: Some("CLJ".to_string()),
            uic: None,
            nlc_desc: Some("CLAPHAM JUNCTION LONDON".to_string()),
            nlc_desc16: None,
        };
        replace_corpus_locations(&pool, at, "CORPUSExtract.json.gz", &[location])
            .await
            .unwrap();
        for _ in 0..2 {
            spec.run_body(pool.clone()).await.unwrap();
        }
        assert_eq!(last_corpus_delivery(&pool).await.unwrap(), Some(at));
    }
}
