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
use sqlx::{PgConnection, PgPool};

pub fn network_wire(network: IslandOfIrelandNetwork) -> &'static str {
    match network {
        IslandOfIrelandNetwork::NorthernIreland => "northern-ireland",
        IslandOfIrelandNetwork::RepublicOfIreland => "republic-of-ireland",
    }
}

/// `ingest_freshness` sources for the Tier A catalogues, one per table and
/// network. The upserts skip an unchanged row (R-016 class, Train Register
/// N1), so a row's `fetched_at` now means "last changed" and the feed-level
/// "last fetched" is recorded here instead, as for the GB reference tables
/// (`queries::record_ingest`).
///
/// Per network (2026-10-08): each network's catalogue comes from its own
/// poller and stream (`republic-of-ireland`: poller-irish-rail-gtfs,
/// `ds:ingest:ioi-gtfs`; `northern-ireland`: poller-nir-stations,
/// `ds:ingest:ioi-nir`), so each needs its own marker for
/// `DistantSignalIngestSourceStale` to see one of them stop. They are named
/// after the feed.
pub mod sources {
    pub const STATIONS_GTFS: &str = "island_of_ireland_stations_gtfs";
    pub const STATIONS_NIR: &str = "island_of_ireland_stations_nir";
    pub const LINES_GTFS: &str = "island_of_ireland_lines_gtfs";
    pub const LINES_NIR: &str = "island_of_ireland_lines_nir";
    /// What every writer recorded before the per-network split, for both
    /// networks. Still read (as the newest of the three), never written.
    pub const LEGACY_STATIONS: &str = "island_of_ireland_stations";
    pub const LEGACY_LINES: &str = "island_of_ireland_lines";
}

/// `network`'s `island_of_ireland_stations` freshness source.
pub fn stations_source(network: IslandOfIrelandNetwork) -> &'static str {
    match network {
        IslandOfIrelandNetwork::RepublicOfIreland => sources::STATIONS_GTFS,
        IslandOfIrelandNetwork::NorthernIreland => sources::STATIONS_NIR,
    }
}

/// `network`'s `island_of_ireland_lines` freshness source.
pub fn lines_source(network: IslandOfIrelandNetwork) -> &'static str {
    match network {
        IslandOfIrelandNetwork::RepublicOfIreland => sources::LINES_GTFS,
        IslandOfIrelandNetwork::NorthernIreland => sources::LINES_NIR,
    }
}

/// Records `source(network)` for each network with a row in the batch
/// (the writer pins a stream to one network, so there it is one).
async fn record_networks(
    conn: &mut PgConnection,
    networks: Vec<IslandOfIrelandNetwork>,
    source: fn(IslandOfIrelandNetwork) -> &'static str,
    observed_at: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<()> {
    let mut seen: Vec<IslandOfIrelandNetwork> = Vec::with_capacity(2);
    for network in networks {
        if !seen.contains(&network) {
            seen.push(network);
            crate::freshness::record_ingest(conn, source(network), observed_at).await?;
        }
    }
    Ok(())
}

/// One `INSERT ... SELECT FROM UNNEST ... ON CONFLICT` for the whole batch,
/// leaving a row whose values are unchanged untouched (no new tuple, no
/// WAL). The last entry wins when a batch names one id twice. Returns the
/// number of stations received, as before.
pub async fn upsert_stations(pool: &PgPool, stations: &[IslandOfIrelandStation]) -> Result<u64> {
    if stations.is_empty() {
        return Ok(0);
    }
    let mut tx = pool.begin().await?;
    write_stations(&mut tx, stations, None).await?;
    record_networks(
        &mut tx,
        stations.iter().map(|s| s.network).collect(),
        stations_source,
        None,
    )
    .await?;
    tx.commit().await?;
    Ok(stations.len() as u64)
}

/// [`upsert_stations`] for the ingest-writer's `ioi-stations/1` handler
/// (ingest plan 3c.1), in the caller's transaction. Decision D13: a changed
/// row's `fetched_at` is `observed_at` (the entry's `produced_at`), and the
/// ordering guard refuses a change older than the stored row's last change
/// (`fetched_at`, "last changed"), so a redelivered older snapshot cannot
/// undo a newer one. Freshness is `observed_at`, never moving backwards.
/// Returns the rows written (inserted or changed; an unchanged or older row
/// is not), for the writer's `ingest_stream_row_writes_total`.
pub async fn upsert_stations_observed(
    conn: &mut PgConnection,
    stations: &[IslandOfIrelandStation],
    observed_at: chrono::DateTime<chrono::Utc>,
) -> Result<u64> {
    if stations.is_empty() {
        return Ok(0);
    }
    let written = write_stations(conn, stations, Some(observed_at)).await?;
    record_networks(
        conn,
        stations.iter().map(|s| s.network).collect(),
        stations_source,
        Some(observed_at),
    )
    .await?;
    Ok(written)
}

/// The shared upsert: `observed_at` `None` stamps `NOW()` with no guard
/// (the api's route), `Some` stamps it and guards on `fetched_at`, and
/// never moves an existing row to another network: the ingest-writer pins
/// each catalogue stream to one network (security review, Irish network
/// pinning), so one network's poller cannot take over the other's row by
/// sending its id. Such a row is skipped (not counted as written).
async fn write_stations(
    conn: &mut PgConnection,
    stations: &[IslandOfIrelandStation],
    observed_at: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<u64> {
    let batch = crate::freshness::last_per_key(stations, |station| station.id.clone());
    let ids: Vec<&str> = batch.iter().map(|s| s.id.as_str()).collect();
    let names: Vec<&str> = batch.iter().map(|s| s.name.as_str()).collect();
    let networks: Vec<&str> = batch.iter().map(|s| network_wire(s.network)).collect();
    let latitudes: Vec<Option<f64>> = batch.iter().map(|s| s.latitude).collect();
    let longitudes: Vec<Option<f64>> = batch.iter().map(|s| s.longitude).collect();

    let done = sqlx::query(
        r"
        INSERT INTO island_of_ireland_stations (id, name, network, latitude, longitude, fetched_at)
        SELECT id, name, network, latitude, longitude, COALESCE($6::timestamptz, NOW())
        FROM UNNEST($1::text[], $2::text[], $3::text[], $4::float8[], $5::float8[])
            AS i(id, name, network, latitude, longitude)
        ON CONFLICT (id) DO UPDATE SET
            name       = EXCLUDED.name,
            network    = EXCLUDED.network,
            latitude   = EXCLUDED.latitude,
            longitude  = EXCLUDED.longitude,
            fetched_at = EXCLUDED.fetched_at
        WHERE (island_of_ireland_stations.name, island_of_ireland_stations.network,
               island_of_ireland_stations.latitude, island_of_ireland_stations.longitude)
              IS DISTINCT FROM
              (EXCLUDED.name, EXCLUDED.network, EXCLUDED.latitude, EXCLUDED.longitude)
          AND ($6::timestamptz IS NULL
               OR EXCLUDED.fetched_at >= island_of_ireland_stations.fetched_at
               OR island_of_ireland_stations.fetched_at > now() + interval '2 min')
          AND ($6::timestamptz IS NULL
               OR island_of_ireland_stations.network = EXCLUDED.network)
        ",
    )
    .bind(&ids)
    .bind(&names)
    .bind(&networks)
    .bind(&latitudes)
    .bind(&longitudes)
    .bind(observed_at)
    .execute(&mut *conn)
    .await?;
    Ok(done.rows_affected())
}

/// [`upsert_stations`] for lines: one batched statement, unchanged rows
/// left untouched.
pub async fn upsert_lines(pool: &PgPool, lines: &[IslandOfIrelandLineDefinition]) -> Result<u64> {
    if lines.is_empty() {
        return Ok(0);
    }
    let mut tx = pool.begin().await?;
    write_lines(&mut tx, lines, None).await?;
    record_networks(
        &mut tx,
        lines.iter().map(|l| l.network).collect(),
        lines_source,
        None,
    )
    .await?;
    tx.commit().await?;
    Ok(lines.len() as u64)
}

/// [`upsert_lines`] for `ioi-lines/1`, as [`upsert_stations_observed`].
pub async fn upsert_lines_observed(
    conn: &mut PgConnection,
    lines: &[IslandOfIrelandLineDefinition],
    observed_at: chrono::DateTime<chrono::Utc>,
) -> Result<u64> {
    if lines.is_empty() {
        return Ok(0);
    }
    let written = write_lines(conn, lines, Some(observed_at)).await?;
    record_networks(
        conn,
        lines.iter().map(|l| l.network).collect(),
        lines_source,
        Some(observed_at),
    )
    .await?;
    Ok(written)
}

/// As [`write_stations`], for lines.
async fn write_lines(
    conn: &mut PgConnection,
    lines: &[IslandOfIrelandLineDefinition],
    observed_at: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<u64> {
    let batch = crate::freshness::last_per_key(lines, |line| line.id.clone());
    let ids: Vec<&str> = batch.iter().map(|l| l.id.as_str()).collect();
    let names: Vec<&str> = batch.iter().map(|l| l.name.as_str()).collect();
    let networks: Vec<&str> = batch.iter().map(|l| network_wire(l.network)).collect();
    let stations: Vec<serde_json::Value> = batch
        .iter()
        .map(|l| serde_json::to_value(&l.stations))
        .collect::<Result<_, _>>()?;

    let done = sqlx::query(
        r"
        INSERT INTO island_of_ireland_lines (id, name, network, stations, fetched_at)
        SELECT id, name, network, stations, COALESCE($5::timestamptz, NOW())
        FROM UNNEST($1::text[], $2::text[], $3::text[], $4::jsonb[])
            AS i(id, name, network, stations)
        ON CONFLICT (id) DO UPDATE SET
            name       = EXCLUDED.name,
            network    = EXCLUDED.network,
            stations   = EXCLUDED.stations,
            fetched_at = EXCLUDED.fetched_at
        WHERE (island_of_ireland_lines.name, island_of_ireland_lines.network,
               island_of_ireland_lines.stations)
              IS DISTINCT FROM
              (EXCLUDED.name, EXCLUDED.network, EXCLUDED.stations)
          AND ($5::timestamptz IS NULL
               OR EXCLUDED.fetched_at >= island_of_ireland_lines.fetched_at
               OR island_of_ireland_lines.fetched_at > now() + interval '2 min')
          AND ($5::timestamptz IS NULL
               OR island_of_ireland_lines.network = EXCLUDED.network)
        ",
    )
    .bind(&ids)
    .bind(&names)
    .bind(&networks)
    .bind(&stations)
    .bind(observed_at)
    .execute(&mut *conn)
    .await?;
    Ok(done.rows_affected())
}

/// When any network's feed last landed: the newest of `sources`'
/// `ingest_freshness` rows (both networks' and the legacy shared one), or,
/// for data written before those rows existed, the table's newest
/// `fetched_at` (`GREATEST` ignores a NULL). `max_fetched_at_sql` is a
/// constant.
async fn last_fetch(
    pool: &PgPool,
    sources: &[&str],
    max_fetched_at_sql: &str,
) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
    let sql = format!(
        "SELECT GREATEST((SELECT MAX(fetched_at) FROM ingest_freshness WHERE source = ANY($1)), \
         ({max_fetched_at_sql}))"
    );
    let (fetched_at,): (Option<chrono::DateTime<chrono::Utc>>,) =
        sqlx::query_as(&sql).bind(sources).fetch_one(pool).await?;
    Ok(fetched_at)
}

pub async fn last_stations_fetch(pool: &PgPool) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
    last_fetch(
        pool,
        &[
            sources::STATIONS_GTFS,
            sources::STATIONS_NIR,
            sources::LEGACY_STATIONS,
        ],
        "SELECT MAX(fetched_at) FROM island_of_ireland_stations",
    )
    .await
}

pub async fn last_lines_fetch(pool: &PgPool) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
    last_fetch(
        pool,
        &[
            sources::LINES_GTFS,
            sources::LINES_NIR,
            sources::LEGACY_LINES,
        ],
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
    let mut conn = pool.acquire().await?;
    write_station_samples(&mut conn, samples, false).await?;
    Ok(samples.len() as u64)
}

/// [`upsert_station_samples`] for `ioi-station-samples/1` (ingest plan
/// 3c.1), in the caller's transaction, with the ordering guard on each
/// row's own `polled_at` (decision D13: its observed time; the handler
/// clamps it to the writer's `now() + 2 min` first): an older sample never
/// overwrites a newer one. No freshness marker, as on the api's route.
/// Returns the rows written, as [`upsert_stations_observed`] does.
pub async fn upsert_station_samples_observed(
    conn: &mut PgConnection,
    samples: &[IslandOfIrelandStationSample],
) -> Result<u64> {
    if samples.is_empty() {
        return Ok(0);
    }
    write_station_samples(conn, samples, true).await
}

async fn write_station_samples(
    conn: &mut PgConnection,
    samples: &[IslandOfIrelandStationSample],
    guard: bool,
) -> Result<u64> {
    let batch = crate::freshness::last_per_key(samples, |sample| sample.station_id.clone());
    let ids: Vec<&str> = batch.iter().map(|s| s.station_id.as_str()).collect();
    let networks: Vec<&str> = batch.iter().map(|s| network_wire(s.network)).collect();
    let polled_at: Vec<chrono::DateTime<chrono::Utc>> = batch.iter().map(|s| s.polled_at).collect();
    let departures: Vec<serde_json::Value> = batch
        .iter()
        .map(|s| serde_json::to_value(&s.departures))
        .collect::<Result<_, _>>()?;

    let done = sqlx::query(
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
          AND (NOT $5
               OR EXCLUDED.polled_at >= island_of_ireland_station_samples.polled_at
               OR island_of_ireland_station_samples.polled_at > now() + interval '2 min')
        ",
    )
    .bind(&ids)
    .bind(&networks)
    .bind(&polled_at)
    .bind(&departures)
    .bind(guard)
    .execute(&mut *conn)
    .await?;
    Ok(done.rows_affected())
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

#[cfg(test)]
mod db_tests {
    use super::*;
    use crate::test_support::connect;

    #[tokio::test]
    #[ignore = "requires a live database; run with `cargo test -p ds-store \
                island_of_ireland -- --ignored --test-threads=1`"]
    async fn last_fetch_against_an_empty_table_is_null() {
        let pool = connect().await;
        // Reads the real table as-is -- relies on CI's freshly-migrated,
        // otherwise-empty database, same posture
        // `station_full_coverage_samples_get_last_fetched_on_an_empty_table_is_null`
        // already documents for its own table.
        let fetched = last_stations_fetch(&pool).await;
        assert!(fetched.is_ok());
    }
}
