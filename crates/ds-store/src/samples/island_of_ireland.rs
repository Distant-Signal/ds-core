//! Island-of-Ireland writes: `island_of_ireland_stations`/`island_of_ireland_lines`
//! (Tier A) and `island_of_ireland_station_samples` (Tier B), and their
//! `last_*_fetch` freshness reads --
//! docs/superpowers/specs/2026-09-05-ireland-rail-support-design.md. Same
//! upsert-on-id, no-history shape as [`crate::reference`]'s
//! `upsert_stations`/`upsert_tocs`. The public readers stay in the api
//! (`data::island_of_ireland`).

use anyhow::Result;
use common::island_of_ireland::{
    IslandOfIrelandLineDefinition, IslandOfIrelandNetwork, IslandOfIrelandStation,
    IslandOfIrelandStationSample,
};
use sqlx::PgPool;

pub fn network_wire(network: IslandOfIrelandNetwork) -> &'static str {
    match network {
        IslandOfIrelandNetwork::NorthernIreland => "northern-ireland",
        IslandOfIrelandNetwork::RepublicOfIreland => "republic-of-ireland",
    }
}

/// `ingest_freshness` sources for the two Tier A feeds. The upserts skip an
/// unchanged row (R-016 class, Train Register N1), so a row's `fetched_at`
/// now means "last changed" and the feed-level "last fetched" is recorded
/// here instead, as for the GB reference tables (`queries::record_ingest`).
const STATIONS_SOURCE: &str = "island_of_ireland_stations";
const LINES_SOURCE: &str = "island_of_ireland_lines";

/// One `INSERT ... SELECT FROM UNNEST ... ON CONFLICT` for the whole batch,
/// leaving a row whose values are unchanged untouched (no new tuple, no
/// WAL). The last entry wins when a batch names one id twice. Returns the
/// number of stations received, as before.
pub async fn upsert_stations(pool: &PgPool, stations: &[IslandOfIrelandStation]) -> Result<u64> {
    if stations.is_empty() {
        return Ok(0);
    }
    let batch = crate::freshness::last_per_key(stations, |station| station.id.clone());
    let ids: Vec<&str> = batch.iter().map(|s| s.id.as_str()).collect();
    let names: Vec<&str> = batch.iter().map(|s| s.name.as_str()).collect();
    let networks: Vec<&str> = batch.iter().map(|s| network_wire(s.network)).collect();
    let latitudes: Vec<Option<f64>> = batch.iter().map(|s| s.latitude).collect();
    let longitudes: Vec<Option<f64>> = batch.iter().map(|s| s.longitude).collect();

    let mut tx = pool.begin().await?;
    sqlx::query(
        r"
        INSERT INTO island_of_ireland_stations (id, name, network, latitude, longitude, fetched_at)
        SELECT id, name, network, latitude, longitude, NOW()
        FROM UNNEST($1::text[], $2::text[], $3::text[], $4::float8[], $5::float8[])
            AS i(id, name, network, latitude, longitude)
        ON CONFLICT (id) DO UPDATE SET
            name       = EXCLUDED.name,
            network    = EXCLUDED.network,
            latitude   = EXCLUDED.latitude,
            longitude  = EXCLUDED.longitude,
            fetched_at = NOW()
        WHERE (island_of_ireland_stations.name, island_of_ireland_stations.network,
               island_of_ireland_stations.latitude, island_of_ireland_stations.longitude)
              IS DISTINCT FROM
              (EXCLUDED.name, EXCLUDED.network, EXCLUDED.latitude, EXCLUDED.longitude)
        ",
    )
    .bind(&ids)
    .bind(&names)
    .bind(&networks)
    .bind(&latitudes)
    .bind(&longitudes)
    .execute(&mut *tx)
    .await?;
    crate::freshness::record_ingest(&mut tx, STATIONS_SOURCE).await?;
    tx.commit().await?;
    Ok(stations.len() as u64)
}

/// [`upsert_stations`] for lines: one batched statement, unchanged rows
/// left untouched.
pub async fn upsert_lines(pool: &PgPool, lines: &[IslandOfIrelandLineDefinition]) -> Result<u64> {
    if lines.is_empty() {
        return Ok(0);
    }
    let batch = crate::freshness::last_per_key(lines, |line| line.id.clone());
    let ids: Vec<&str> = batch.iter().map(|l| l.id.as_str()).collect();
    let names: Vec<&str> = batch.iter().map(|l| l.name.as_str()).collect();
    let networks: Vec<&str> = batch.iter().map(|l| network_wire(l.network)).collect();
    let stations: Vec<serde_json::Value> = batch
        .iter()
        .map(|l| serde_json::to_value(&l.stations))
        .collect::<Result<_, _>>()?;

    let mut tx = pool.begin().await?;
    sqlx::query(
        r"
        INSERT INTO island_of_ireland_lines (id, name, network, stations, fetched_at)
        SELECT id, name, network, stations, NOW()
        FROM UNNEST($1::text[], $2::text[], $3::text[], $4::jsonb[])
            AS i(id, name, network, stations)
        ON CONFLICT (id) DO UPDATE SET
            name       = EXCLUDED.name,
            network    = EXCLUDED.network,
            stations   = EXCLUDED.stations,
            fetched_at = NOW()
        WHERE (island_of_ireland_lines.name, island_of_ireland_lines.network,
               island_of_ireland_lines.stations)
              IS DISTINCT FROM
              (EXCLUDED.name, EXCLUDED.network, EXCLUDED.stations)
        ",
    )
    .bind(&ids)
    .bind(&names)
    .bind(&networks)
    .bind(&stations)
    .execute(&mut *tx)
    .await?;
    crate::freshness::record_ingest(&mut tx, LINES_SOURCE).await?;
    tx.commit().await?;
    Ok(lines.len() as u64)
}

/// When `source`'s feed last landed: its `ingest_freshness` row, or, for
/// data written before that row existed, the table's newest `fetched_at`
/// (`GREATEST` ignores a NULL). `max_fetched_at_sql` is a constant.
async fn last_fetch(
    pool: &PgPool,
    source: &str,
    max_fetched_at_sql: &str,
) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
    let sql = format!(
        "SELECT GREATEST((SELECT fetched_at FROM ingest_freshness WHERE source = $1), ({max_fetched_at_sql}))"
    );
    let (fetched_at,): (Option<chrono::DateTime<chrono::Utc>>,) =
        sqlx::query_as(&sql).bind(source).fetch_one(pool).await?;
    Ok(fetched_at)
}

pub async fn last_stations_fetch(pool: &PgPool) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
    last_fetch(
        pool,
        STATIONS_SOURCE,
        "SELECT MAX(fetched_at) FROM island_of_ireland_stations",
    )
    .await
}

pub async fn last_lines_fetch(pool: &PgPool) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
    last_fetch(
        pool,
        LINES_SOURCE,
        "SELECT MAX(fetched_at) FROM island_of_ireland_lines",
    )
    .await
}

/// One batched statement for the whole poll, the shape of
/// `queries::upsert_station_samples`: `polled_at` is the sample's age, read
/// per station, so it advances on every poll; a row is skipped only when
/// nothing differs, and an unchanged board keeps its stored `departures`
/// value (no new TOAST chunks). The last entry wins when a batch names one
/// station twice.
pub async fn upsert_station_samples(
    pool: &PgPool,
    samples: &[IslandOfIrelandStationSample],
) -> Result<u64> {
    if samples.is_empty() {
        return Ok(0);
    }
    let batch = crate::freshness::last_per_key(samples, |sample| sample.station_id.clone());
    let ids: Vec<&str> = batch.iter().map(|s| s.station_id.as_str()).collect();
    let networks: Vec<&str> = batch.iter().map(|s| network_wire(s.network)).collect();
    let polled_at: Vec<chrono::DateTime<chrono::Utc>> = batch.iter().map(|s| s.polled_at).collect();
    let departures: Vec<serde_json::Value> = batch
        .iter()
        .map(|s| serde_json::to_value(&s.departures))
        .collect::<Result<_, _>>()?;

    sqlx::query(
        r"
        INSERT INTO island_of_ireland_station_samples (station_id, network, polled_at, departures)
        SELECT station_id, network, polled_at, departures
        FROM UNNEST($1::text[], $2::text[], $3::timestamptz[], $4::jsonb[])
            AS i(station_id, network, polled_at, departures)
        ON CONFLICT (station_id) DO UPDATE SET
            network    = EXCLUDED.network,
            polled_at  = EXCLUDED.polled_at,
            departures = CASE
                WHEN island_of_ireland_station_samples.departures IS DISTINCT FROM EXCLUDED.departures
                THEN EXCLUDED.departures
                ELSE island_of_ireland_station_samples.departures
            END
        WHERE (island_of_ireland_station_samples.network,
               island_of_ireland_station_samples.polled_at,
               island_of_ireland_station_samples.departures)
              IS DISTINCT FROM
              (EXCLUDED.network, EXCLUDED.polled_at, EXCLUDED.departures)
        ",
    )
    .bind(&ids)
    .bind(&networks)
    .bind(&polled_at)
    .bind(&departures)
    .execute(pool)
    .await?;
    Ok(samples.len() as u64)
}

pub async fn last_station_samples_fetch(
    pool: &PgPool,
) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
    let (polled_at,): (Option<chrono::DateTime<chrono::Utc>>,) =
        sqlx::query_as("SELECT MAX(polled_at) FROM island_of_ireland_station_samples")
            .fetch_one(pool)
            .await?;
    Ok(polled_at)
}
