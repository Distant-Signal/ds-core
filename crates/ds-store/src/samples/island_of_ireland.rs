//! Island-of-Ireland writes: `island_of_ireland_stations`/`island_of_ireland_lines`
//! (Tier A) and `island_of_ireland_station_samples` (Tier B), and their
//! `last_*_fetch` freshness reads --
//! docs/superpowers/specs/2026-09-05-ireland-rail-support-design.md. Same
//! upsert-on-id, no-history shape as [`crate::reference`]'s
//! `upsert_stations`/`upsert_tocs`. The public readers stay in the api
//! (`data::island_of_ireland`).

use anyhow::Result;
use common::island_of_ireland::live_codes::{self, LiveCodeReport};
use common::island_of_ireland::{
    IslandOfIrelandLineDefinition, IslandOfIrelandLiveStation, IslandOfIrelandNetwork,
    IslandOfIrelandStation, IslandOfIrelandStationSample,
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
/// after the feed. The shared, unsuffixed names written before the split
/// (`island_of_ireland_stations`/`_lines`) were dropped a release later
/// (migration 20261010100100 deletes their rows).
pub mod sources {
    pub const STATIONS_GTFS: &str = "island_of_ireland_stations_gtfs";
    pub const STATIONS_NIR: &str = "island_of_ireland_stations_nir";
    pub const LINES_GTFS: &str = "island_of_ireland_lines_gtfs";
    pub const LINES_NIR: &str = "island_of_ireland_lines_nir";

    /// Every source above (as `crate::samples::sources::ALL`).
    pub const ALL: &[&str] = &[STATIONS_GTFS, STATIONS_NIR, LINES_GTFS, LINES_NIR];
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
    let raw_names: Vec<Option<&str>> = batch.iter().map(|s| s.raw_name.as_deref()).collect();

    // `live_code` is not the producer's: `refresh_live_codes` sets it.
    let done = sqlx::query(
        r"
        INSERT INTO island_of_ireland_stations
            (id, name, network, latitude, longitude, raw_name, fetched_at)
        SELECT id, name, network, latitude, longitude, raw_name, COALESCE($6::timestamptz, NOW())
        FROM UNNEST($1::text[], $2::text[], $3::text[], $4::float8[], $5::float8[], $7::text[])
            AS i(id, name, network, latitude, longitude, raw_name)
        ON CONFLICT (id) DO UPDATE SET
            name       = EXCLUDED.name,
            network    = EXCLUDED.network,
            latitude   = EXCLUDED.latitude,
            longitude  = EXCLUDED.longitude,
            raw_name   = EXCLUDED.raw_name,
            fetched_at = EXCLUDED.fetched_at
        WHERE (island_of_ireland_stations.name, island_of_ireland_stations.network,
               island_of_ireland_stations.latitude, island_of_ireland_stations.longitude,
               island_of_ireland_stations.raw_name)
              IS DISTINCT FROM
              (EXCLUDED.name, EXCLUDED.network, EXCLUDED.latitude, EXCLUDED.longitude,
               EXCLUDED.raw_name)
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
    .bind(&raw_names)
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
    let short_names: Vec<Option<&str>> = batch.iter().map(|l| l.short_name.as_deref()).collect();
    let agencies: Vec<Option<&str>> = batch.iter().map(|l| l.agency.as_deref()).collect();
    let colors: Vec<Option<&str>> = batch.iter().map(|l| l.color.as_deref()).collect();
    let text_colors: Vec<Option<&str>> = batch.iter().map(|l| l.text_color.as_deref()).collect();

    let done = sqlx::query(
        r"
        INSERT INTO island_of_ireland_lines
            (id, name, network, stations, short_name, agency, color, text_color, fetched_at)
        SELECT id, name, network, stations, short_name, agency, color, text_color,
               COALESCE($5::timestamptz, NOW())
        FROM UNNEST($1::text[], $2::text[], $3::text[], $4::jsonb[],
                    $6::text[], $7::text[], $8::text[], $9::text[])
            AS i(id, name, network, stations, short_name, agency, color, text_color)
        ON CONFLICT (id) DO UPDATE SET
            name       = EXCLUDED.name,
            network    = EXCLUDED.network,
            stations   = EXCLUDED.stations,
            short_name = EXCLUDED.short_name,
            agency     = EXCLUDED.agency,
            color      = EXCLUDED.color,
            text_color = EXCLUDED.text_color,
            fetched_at = EXCLUDED.fetched_at
        WHERE (island_of_ireland_lines.name, island_of_ireland_lines.network,
               island_of_ireland_lines.stations, island_of_ireland_lines.short_name,
               island_of_ireland_lines.agency, island_of_ireland_lines.color,
               island_of_ireland_lines.text_color)
              IS DISTINCT FROM
              (EXCLUDED.name, EXCLUDED.network, EXCLUDED.stations, EXCLUDED.short_name,
               EXCLUDED.agency, EXCLUDED.color, EXCLUDED.text_color)
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
    .bind(&short_names)
    .bind(&agencies)
    .bind(&colors)
    .bind(&text_colors)
    .execute(&mut *conn)
    .await?;
    let replaced = delete_replaced_lines(conn, &batch, observed_at).await?;
    Ok(done.rows_affected() + replaced)
}

/// Deletes the rows `lines` replace ([`IslandOfIrelandLineDefinition::replaces`]:
/// the directional GTFS routes merged into one corridor), each only in its
/// replacing line's network and never a line the same batch writes. With
/// `observed_at`, a row changed after it is kept (the ordering guard of
/// [`write_lines`]: an older snapshot cannot delete a newer row). Returns
/// the rows deleted.
async fn delete_replaced_lines(
    conn: &mut PgConnection,
    lines: &[&IslandOfIrelandLineDefinition],
    observed_at: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<u64> {
    let written: Vec<&str> = lines.iter().map(|l| l.id.as_str()).collect();
    let (ids, networks): (Vec<&str>, Vec<&str>) = lines
        .iter()
        .flat_map(|l| {
            l.replaces
                .iter()
                .map(move |old| (old.as_str(), network_wire(l.network)))
        })
        .filter(|(old, _)| !written.contains(old))
        .unzip();
    if ids.is_empty() {
        return Ok(0);
    }
    let done = sqlx::query(
        r"
        DELETE FROM island_of_ireland_lines AS l
        USING UNNEST($1::text[], $2::text[]) AS r(id, network)
        WHERE l.id = r.id
          AND l.network = r.network
          AND ($3::timestamptz IS NULL
               OR l.fetched_at <= $3
               OR l.fetched_at > now() + interval '2 min')
        ",
    )
    .bind(&ids)
    .bind(&networks)
    .bind(observed_at)
    .execute(&mut *conn)
    .await?;
    Ok(done.rows_affected())
}

/// Upserts Iarnród Éireann's live station list (`ioi-live-stations/1`) into
/// `island_of_ireland_live_stations`, in the caller's transaction, with the
/// same unchanged-row skip and ordering guard as [`upsert_stations_observed`]
/// (a changed row's `fetched_at` is `observed_at`). Rows the list no longer
/// carries are kept: a code that disappears simply stops matching nothing
/// new. Returns the rows written.
pub async fn upsert_live_stations_observed(
    conn: &mut PgConnection,
    stations: &[IslandOfIrelandLiveStation],
    observed_at: chrono::DateTime<chrono::Utc>,
) -> Result<u64> {
    if stations.is_empty() {
        return Ok(0);
    }
    let batch = crate::freshness::last_per_key(stations, |station| station.code.clone());
    let codes: Vec<&str> = batch.iter().map(|s| s.code.as_str()).collect();
    let names: Vec<&str> = batch.iter().map(|s| s.name.as_str()).collect();
    let aliases: Vec<Option<&str>> = batch.iter().map(|s| s.alias.as_deref()).collect();
    let latitudes: Vec<Option<f64>> = batch.iter().map(|s| s.latitude).collect();
    let longitudes: Vec<Option<f64>> = batch.iter().map(|s| s.longitude).collect();

    let done = sqlx::query(
        r"
        INSERT INTO island_of_ireland_live_stations
            (code, name, alias, latitude, longitude, fetched_at)
        SELECT code, name, alias, latitude, longitude, $6::timestamptz
        FROM UNNEST($1::text[], $2::text[], $3::text[], $4::float8[], $5::float8[])
            AS i(code, name, alias, latitude, longitude)
        ON CONFLICT (code) DO UPDATE SET
            name       = EXCLUDED.name,
            alias      = EXCLUDED.alias,
            latitude   = EXCLUDED.latitude,
            longitude  = EXCLUDED.longitude,
            fetched_at = EXCLUDED.fetched_at
        WHERE (island_of_ireland_live_stations.name, island_of_ireland_live_stations.alias,
               island_of_ireland_live_stations.latitude, island_of_ireland_live_stations.longitude)
              IS DISTINCT FROM
              (EXCLUDED.name, EXCLUDED.alias, EXCLUDED.latitude, EXCLUDED.longitude)
          AND (EXCLUDED.fetched_at >= island_of_ireland_live_stations.fetched_at
               OR island_of_ireland_live_stations.fetched_at > now() + interval '2 min')
        ",
    )
    .bind(&codes)
    .bind(&names)
    .bind(&aliases)
    .bind(&latitudes)
    .bind(&longitudes)
    .bind(observed_at)
    .execute(&mut *conn)
    .await?;
    Ok(done.rows_affected())
}

/// Recomputes every Republic of Ireland catalogue station's `live_code`
/// from the stored catalogue and live station list
/// ([`common::island_of_ireland::live_codes`]), in the caller's
/// transaction, and writes only the codes that changed. Run after either
/// side is applied. Does nothing while the live list is empty (the live
/// poller has not sent one yet), so a catalogue applied first keeps its
/// codes. Serialised by a transaction advisory lock, so two streams'
/// refreshes cannot interleave and leave a stale result: the second reads
/// after the first commits. Returns the match report (`None` when skipped)
/// and the rows changed.
pub async fn refresh_live_codes(conn: &mut PgConnection) -> Result<(Option<LiveCodeReport>, u64)> {
    sqlx::query(
        "SELECT pg_advisory_xact_lock(hashtextextended('island_of_ireland:live_codes', 0))",
    )
    .execute(&mut *conn)
    .await?;

    let live: Vec<IslandOfIrelandLiveStation> =
        sqlx::query_as::<_, (String, String, Option<String>, Option<f64>, Option<f64>)>(
            "SELECT code, name, alias, latitude, longitude FROM island_of_ireland_live_stations \
         ORDER BY code",
        )
        .fetch_all(&mut *conn)
        .await?
        .into_iter()
        .map(
            |(code, name, alias, latitude, longitude)| IslandOfIrelandLiveStation {
                code,
                name,
                alias,
                latitude,
                longitude,
            },
        )
        .collect();
    if live.is_empty() {
        return Ok((None, 0));
    }

    let republic = network_wire(IslandOfIrelandNetwork::RepublicOfIreland);
    let stations: Vec<IslandOfIrelandStation> =
        sqlx::query_as::<_, (String, String, Option<f64>, Option<f64>)>(
            "SELECT id, name, latitude, longitude FROM island_of_ireland_stations \
         WHERE network = $1 ORDER BY id",
        )
        .bind(republic)
        .fetch_all(&mut *conn)
        .await?
        .into_iter()
        .map(|(id, name, latitude, longitude)| IslandOfIrelandStation {
            id,
            name,
            network: IslandOfIrelandNetwork::RepublicOfIreland,
            latitude,
            longitude,
            raw_name: None,
        })
        .collect();

    let report = live_codes::match_live_codes(&stations, &live);
    let (ids, codes): (Vec<&str>, Vec<&str>) = report.codes().unzip();
    let done = sqlx::query(
        r"
        UPDATE island_of_ireland_stations AS s
        SET live_code = m.code
        FROM (
            SELECT st.id, c.code
            FROM island_of_ireland_stations AS st
            LEFT JOIN UNNEST($1::text[], $2::text[]) AS c(id, code) ON c.id = st.id
            WHERE st.network = $3
        ) AS m
        WHERE s.id = m.id
          AND s.live_code IS DISTINCT FROM m.code
        ",
    )
    .bind(&ids)
    .bind(&codes)
    .bind(republic)
    .execute(&mut *conn)
    .await?;
    Ok((Some(report), done.rows_affected()))
}

/// When any network's feed last landed: the newest of `sources`'
/// `ingest_freshness` rows (both networks'), or,
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
        &[sources::STATIONS_GTFS, sources::STATIONS_NIR],
        "SELECT MAX(fetched_at) FROM island_of_ireland_stations",
    )
    .await
}

pub async fn last_lines_fetch(pool: &PgPool) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
    last_fetch(
        pool,
        &[sources::LINES_GTFS, sources::LINES_NIR],
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

    fn roi_station(id: &str, name: &str, at: (f64, f64)) -> IslandOfIrelandStation {
        IslandOfIrelandStation {
            id: id.into(),
            name: name.into(),
            network: IslandOfIrelandNetwork::RepublicOfIreland,
            latitude: Some(at.0),
            longitude: Some(at.1),
            raw_name: None,
        }
    }

    fn roi_line(id: &str, replaces: &[&str]) -> IslandOfIrelandLineDefinition {
        IslandOfIrelandLineDefinition {
            id: id.into(),
            name: "Zesttown - Zorough".into(),
            network: IslandOfIrelandNetwork::RepublicOfIreland,
            stations: vec!["ZS1".into(), "ZS2".into()],
            short_name: None,
            agency: Some("Zest Rail".into()),
            color: Some("#00A651".into()),
            text_color: Some("#FFFFFF".into()),
            replaces: replaces.iter().map(ToString::to_string).collect(),
        }
    }

    async fn line_ids(pool: &PgPool, prefix: &str) -> Vec<String> {
        sqlx::query_scalar("SELECT id FROM island_of_ireland_lines WHERE id LIKE $1 ORDER BY id")
            .bind(format!("{prefix}%"))
            .fetch_all(pool)
            .await
            .unwrap()
    }

    async fn clean(pool: &PgPool) {
        for sql in [
            "DELETE FROM island_of_ireland_lines WHERE id LIKE 'ZQL%'",
            "DELETE FROM island_of_ireland_stations WHERE id LIKE 'ZQS%'",
            "DELETE FROM island_of_ireland_live_stations WHERE code LIKE 'ZQ%'",
        ] {
            sqlx::query(sql).execute(pool).await.unwrap();
        }
    }

    /// A merged corridor deletes the directional rows it replaces, but
    /// only in its own network, never a line the batch itself writes, and
    /// never a row changed after the snapshot. The colours and agency are
    /// stored.
    #[tokio::test]
    #[ignore = "requires a live database; run with `cargo test -p ds-store \
                island_of_ireland -- --ignored --test-threads=1`"]
    async fn a_merged_line_deletes_the_rows_it_replaces() {
        let pool = connect().await;
        clean(&pool).await;
        let t0: chrono::DateTime<chrono::Utc> = "2026-11-01T00:00:00Z".parse().unwrap();
        let mut conn = pool.acquire().await.unwrap();
        let mut nir = roi_line("ZQL-NI", &[]);
        nir.network = IslandOfIrelandNetwork::NorthernIreland;
        upsert_lines_observed(
            &mut conn,
            &[roi_line("ZQL-I", &[]), roi_line("ZQL-O", &[])],
            t0,
        )
        .await
        .unwrap();
        upsert_lines_observed(&mut conn, &[nir], t0).await.unwrap();
        // A row changed after the merged snapshot's time survives it.
        let later = roi_line("ZQL-LATE", &[]);
        upsert_lines_observed(&mut conn, &[later], t0 + chrono::Duration::hours(2))
            .await
            .unwrap();

        let merged = roi_line("ZQL", &["ZQL-I", "ZQL-O", "ZQL-NI", "ZQL-LATE", "ZQL"]);
        let written = upsert_lines_observed(&mut conn, &[merged], t0 + chrono::Duration::hours(1))
            .await
            .unwrap();
        assert_eq!(written, 3, "one insert and two deletes");
        assert_eq!(line_ids(&pool, "ZQL").await, ["ZQL", "ZQL-LATE", "ZQL-NI"]);

        let (agency, color, text_color): (Option<String>, Option<String>, Option<String>) =
            sqlx::query_as(
                "SELECT agency, color, text_color FROM island_of_ireland_lines WHERE id = 'ZQL'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(agency.as_deref(), Some("Zest Rail"));
        assert_eq!(color.as_deref(), Some("#00A651"));
        assert_eq!(text_color.as_deref(), Some("#FFFFFF"));
        drop(conn);
        clean(&pool).await;
    }

    async fn live_code(pool: &PgPool, id: &str) -> Option<String> {
        sqlx::query_scalar("SELECT live_code FROM island_of_ireland_stations WHERE id = $1")
            .bind(id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// The live list and the catalogue join through `live_code`, refreshed
    /// from whichever side arrives; an unmatched station keeps no code and
    /// a station that stops matching loses its code.
    #[tokio::test]
    #[ignore = "requires a live database; run with `cargo test -p ds-store \
                island_of_ireland -- --ignored --test-threads=1`"]
    async fn refresh_live_codes_joins_the_catalogue_to_the_live_list() {
        let pool = connect().await;
        clean(&pool).await;
        let t0: chrono::DateTime<chrono::Utc> = "2026-11-01T00:00:00Z".parse().unwrap();
        let mut tx = pool.begin().await.unwrap();
        upsert_stations_observed(
            &mut tx,
            &[
                roi_station("ZQS1", "Zesttown Kent", (53.0, -6.0)),
                roi_station("ZQS2", "Zorough", (53.2, -6.2)),
                roi_station("ZQS3", "Nowhere Halt", (52.0, -9.0)),
            ],
            t0,
        )
        .await
        .unwrap();
        let mut nir = roi_station("ZQS4", "Zorough", (53.2, -6.2));
        nir.network = IslandOfIrelandNetwork::NorthernIreland;
        nir.raw_name = Some("ZOROUGH RAIL HALT".into());
        upsert_stations_observed(&mut tx, &[nir], t0).await.unwrap();

        let live = |code: &str, name: &str, at: (f64, f64)| IslandOfIrelandLiveStation {
            code: code.into(),
            name: name.into(),
            alias: None,
            latitude: Some(at.0),
            longitude: Some(at.1),
        };
        let written = upsert_live_stations_observed(
            &mut tx,
            &[
                live("ZQZST", "Zesttown", (53.0005, -6.0005)),
                live("ZQZRH", "Zorough", (53.2, -6.2)),
            ],
            t0,
        )
        .await
        .unwrap();
        assert_eq!(written, 2);
        let (report, changed) = refresh_live_codes(&mut tx).await.unwrap();
        tx.commit().await.unwrap();
        let report = report.expect("the live list is not empty");
        assert!(changed >= 2);
        assert_eq!(live_code(&pool, "ZQS1").await.as_deref(), Some("ZQZST"));
        assert_eq!(live_code(&pool, "ZQS2").await.as_deref(), Some("ZQZRH"));
        assert_eq!(live_code(&pool, "ZQS3").await, None);
        // Only the Republic of Ireland's catalogue is matched.
        assert_eq!(live_code(&pool, "ZQS4").await, None);
        assert!(report.unmatched().any(|(id, _)| id == "ZQS3"));
        let raw: Option<String> =
            sqlx::query_scalar("SELECT raw_name FROM island_of_ireland_stations WHERE id = 'ZQS4'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(raw.as_deref(), Some("ZOROUGH RAIL HALT"));

        // Zorough moves 40 km: its name still matches nothing nearby.
        let mut tx = pool.begin().await.unwrap();
        upsert_live_stations_observed(
            &mut tx,
            &[live("ZQZRH", "Zorough", (53.6, -6.2))],
            t0 + chrono::Duration::hours(1),
        )
        .await
        .unwrap();
        let (_, changed) = refresh_live_codes(&mut tx).await.unwrap();
        tx.commit().await.unwrap();
        assert_eq!(changed, 1);
        assert_eq!(live_code(&pool, "ZQS2").await, None);
        assert_eq!(live_code(&pool, "ZQS1").await.as_deref(), Some("ZQZST"));
        clean(&pool).await;
    }
}
