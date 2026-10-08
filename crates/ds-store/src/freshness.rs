//! Ingest freshness markers: `record_ingest`, every `last_*_fetch`,
//! `data_freshness`, `last_per_key` and `normalize_code`, from
//! `api/src/data/queries.rs`.

use std::collections::HashMap;

use anyhow::Result;
use sqlx::PgPool;

/// The one normal form for a CRS or TIPLOC code: surrounding whitespace
/// trimmed, ASCII upper-cased. Every lookup in the data layer normalises its
/// INPUT with this and compares against the plain stored column, and every
/// writer stores codes in this form, so the columns' own primary keys and
/// indexes serve the lookups. (The lookups used to apply
/// `UPPER(TRIM(column))` on the stored side instead, which no index could
/// serve -- a sequential scan per call; DB review 2026-09-27 F1. Production
/// held no un-normalised code in any of these columns when that changed.)
pub fn normalize_code(raw: &str) -> String {
    raw.trim().to_ascii_uppercase()
}

/// `items` with only the LAST item per key kept, in input order -- what a
/// per-row upsert loop left behind for a batch naming one key twice. A
/// single `INSERT ... SELECT FROM UNNEST ... ON CONFLICT DO UPDATE` refuses
/// to touch one row twice, so every batched upsert below dedups first.
pub fn last_per_key<T, K: Eq + std::hash::Hash>(items: &[T], key: impl Fn(&T) -> K) -> Vec<&T> {
    let mut last: HashMap<K, usize> = HashMap::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        last.insert(key(item), index);
    }
    items
        .iter()
        .enumerate()
        .filter(|(index, item)| last.get(&key(item)) == Some(index))
        .map(|(_, item)| item)
        .collect()
}

/// Records that `source`'s feed delivered a (non-empty) batch whose data
/// is as of `observed_at`, or as of the transaction's `NOW()` when `None`.
/// `ingest_freshness` is what the `last_*_fetch` freshness reads use for
/// these sources: the upserts leave an unchanged row completely
/// untouched (no-op guards, DB review 2026-09-27 F3), so the per-row
/// `fetched_at`/`computed_at` columns can no longer answer "when did this
/// feed last land" by `MAX()` -- and one row per source is also what lets
/// `/public/freshness` be a single cheap query.
///
/// **"Data as of", never backwards** (ingest architecture D13, spec §7.8,
/// plan 3a.6): the stored time is `GREATEST(stored, observed_at)`. The
/// ingest-writer passes the stream entry's `produced_at`, so a snapshot
/// applied late (after a writer outage, or reordered by `XAUTOCLAIM`)
/// records when its data was true, and an older one applied after a newer
/// one does not move the marker back. The api and the direct writers pass
/// `None` (their fetch time is now), so their behaviour is unchanged.
pub async fn record_ingest(
    conn: &mut sqlx::PgConnection,
    source: &str,
    observed_at: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO ingest_freshness (source, fetched_at) VALUES ($1, COALESCE($2, NOW())) \
         ON CONFLICT (source) DO UPDATE \
         SET fetched_at = GREATEST(ingest_freshness.fetched_at, EXCLUDED.fetched_at)",
    )
    .bind(source)
    .bind(observed_at)
    .execute(conn)
    .await?;
    Ok(())
}

/// Timestamp of the most recent `TfL` line-status ingest, or `None` if none
/// has ever landed. Backs both `GET /private/tfl-line-status` (the poller's
/// startup freshness check) and the public `/public/freshness` endpoint.
pub async fn last_tfl_line_status_fetch(
    pool: &PgPool,
) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
    last_ingest(pool, "tfl").await
}

/// `ingest_freshness.fetched_at` for one source (see `record_ingest`), or
/// `None` if that source has never delivered.
async fn last_ingest(pool: &PgPool, source: &str) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
    Ok(
        sqlx::query_scalar("SELECT fetched_at FROM ingest_freshness WHERE source = $1")
            .bind(source)
            .fetch_optional(pool)
            .await?,
    )
}

/// Every `/public/freshness` timestamp in ONE query (it used to be five
/// concurrent `MAX()` scans on five pooled connections per request; DB
/// review 2026-09-27 F10/F11). Same values as the five `last_*_fetch`
/// functions plus `data::corpus::last_corpus_delivery`:
/// `(stations, tocs, incidents, tfl, schedule_feed, corpus)`. The CORPUS
/// `MAX()` is an index-only read of `corpus_deliveries`' primary key (about
/// a dozen rows a year).
pub async fn data_freshness(pool: &PgPool) -> Result<[Option<chrono::DateTime<chrono::Utc>>; 6]> {
    type Ts = Option<chrono::DateTime<chrono::Utc>>;
    let row: (Ts, Ts, Ts, Ts, Ts, Ts) = sqlx::query_as(
        "SELECT \
            (SELECT fetched_at FROM ingest_freshness WHERE source = 'stations'), \
            (SELECT fetched_at FROM ingest_freshness WHERE source = 'tocs'), \
            (SELECT fetched_at FROM ingest_freshness WHERE source = 'incidents'), \
            (SELECT fetched_at FROM ingest_freshness WHERE source = 'tfl'), \
            (SELECT MAX(delivered_at) FROM schedule_feed_ingests), \
            (SELECT MAX(delivered_at) FROM corpus_deliveries)",
    )
    .fetch_one(pool)
    .await?;
    Ok([row.0, row.1, row.2, row.3, row.4, row.5])
}

/// Timestamp of the most recent successful ingest for each poller-fed
/// table, or `None` if the table has never been populated. Backs the
/// `GET /private/*` freshness-check endpoints
/// (`crates/api/src/routes/ingest.rs`) each poller calls once at startup
/// to decide whether to skip an immediately-redundant first fetch (see
/// `common::ingest::time_until_next_poll`). `MAX(...)` over zero rows
/// returns one row with a `NULL` column, not zero rows — `fetch_one`
/// (not `fetch_optional`) is deliberate here, matching that: it's the
/// *column* that's optional, not the row.
pub async fn last_stations_fetch(pool: &PgPool) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
    last_ingest(pool, "stations").await
}

pub async fn last_tocs_fetch(pool: &PgPool) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
    last_ingest(pool, "tocs").await
}

pub async fn last_incidents_fetch(pool: &PgPool) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
    last_ingest(pool, "incidents").await
}

pub async fn last_station_samples_fetch(
    pool: &PgPool,
) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
    let (polled_at,): (Option<chrono::DateTime<chrono::Utc>>,) =
        sqlx::query_as("SELECT MAX(polled_at) FROM station_samples")
            .fetch_one(pool)
            .await?;
    Ok(polled_at)
}

pub async fn last_station_full_coverage_samples_fetch(
    pool: &PgPool,
) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
    let (fetched_at,): (Option<chrono::DateTime<chrono::Utc>>,) =
        sqlx::query_as("SELECT MAX(resolved_at) FROM station_full_coverage_samples")
            .fetch_one(pool)
            .await?;
    Ok(fetched_at)
}

/// The most recent `updated_at` across every `full_coverage_line_stats`
/// row -- the freshness-only GET shape (Correction 2), mirroring
/// `last_station_samples_fetch`'s own shape. The real reader of the rows
/// themselves is `aggregator`'s own direct SQL
/// (`load_full_coverage_line_stats`, Task 14), not this route. Since the
/// skip-if-unchanged upsert guard, this is when the stats last CHANGED.
pub async fn last_full_coverage_line_stats_fetch(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
    let (fetched_at,): (Option<chrono::DateTime<chrono::Utc>>,) =
        sqlx::query_as("SELECT MAX(updated_at) FROM full_coverage_line_stats")
            .fetch_one(executor)
            .await?;
    Ok(fetched_at)
}
