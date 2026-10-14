//! The ingest-writer's side of a network's versioned timetable (transit
//! spec §9.2): [`stage_part`] inserts one `transit-timetable/1` part under
//! its `(network, feed_version)`, and [`commit`] activates the version when
//! its `transit-timetable-commit/1` arrives and every part it lists is
//! there.
//!
//! Both run inside the writer's entry transaction (the `ingest_dedup` row is
//! already in it), take a transaction-scoped advisory lock on the network,
//! and never update a versioned row: a part's rows are inserted with
//! `ON CONFLICT DO NOTHING`, and a version is pruned by deleting its
//! `transit_feed_versions` row, which every versioned table cascades from.
//! The writer role's grants (db-grants.yaml) are exactly this.
//!
//! Observed time (ingest D13): a version's `staged_at` is its first part's
//! envelope `produced_at`, its `produced_at` its commit's; the timetable's
//! freshness marker (`transit-<network>-timetable`) is the commit's
//! `produced_at`, written for an activated or unchanged feed only.

use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, NaiveDate, Utc};
use common::transit::{
    NetworkId, TimetableCommit, TimetablePart, TimetablePartKind, TimetableRows, TransitLine,
    TransitNetworkRow, TransitPattern, TransitRailLink, TransitServiceDays, TransitStop,
    TransitTrip, service_date, timetable_freshness_source,
};
use sqlx::PgConnection;

use crate::freshness::record_ingest;

/// Which part of its kind a [`TimetablePart`] is: the envelope's batch
/// `part` of `parts` (1-based).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartIndex {
    pub part: u32,
    pub parts: u32,
}

/// What [`stage_part`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageOutcome {
    /// The part's rows were inserted (those not already there).
    Staged { rows: usize },
    /// The version is already `active` or `previous`: a re-sent unchanged
    /// feed. Nothing was written.
    NotStaging,
}

/// What [`commit`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitOutcome {
    /// The version is now active; the old active one is `previous` and
    /// anything older is deleted. Freshness recorded.
    Activated,
    /// The active version has the same content: only freshness recorded
    /// (and a re-staged copy under another `feed_version` deleted).
    Unchanged,
    /// The active version was produced after this commit: an out-of-order
    /// redelivery. Nothing written.
    Older,
    /// The staged parts differ from the commit's manifest (a part was
    /// dead-lettered or trimmed). Nothing written; the writer dead-letters
    /// the commit as `incomplete_feed_version`.
    Incomplete(String),
    /// The staged version does not hang together (a trip with no pattern,
    /// a row count off the manifest, no service today). Nothing written;
    /// dead-lettered as `inconsistent_feed_version`.
    Inconsistent(String),
}

/// Serializes one network's stages and commits within their transactions.
pub(super) async fn lock_network(conn: &mut PgConnection, network: &NetworkId) -> Result<()> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('transit:' || $1, 0))")
        .bind(network.as_str())
        .execute(&mut *conn)
        .await
        .context("transit network lock")?;
    Ok(())
}

fn to_i32(value: u32, what: &str) -> Result<i32> {
    i32::try_from(value).with_context(|| format!("{what} {value} out of range"))
}

/// Inserts `part` under its `(network, feed_version)`, creating the
/// version's `staging` row on its first part, and records the part in
/// `transit_feed_parts`. A version that is already `active` or `previous`
/// is left alone ([`StageOutcome::NotStaging`]). Idempotent: a redelivered
/// part inserts nothing new.
///
/// The caller has pinned the part's network to its stream and checked its
/// ids; the tables' CHECKs refuse anything else as a data error.
pub async fn stage_part(
    conn: &mut PgConnection,
    part: &TimetablePart,
    index: PartIndex,
    produced_at: DateTime<Utc>,
) -> Result<StageOutcome> {
    if index.part == 0 || index.part > index.parts {
        bail!("part {} of {} is not a part", index.part, index.parts);
    }
    let network = part.network.as_str();
    let version = part.feed_version.as_str();
    lock_network(conn, &part.network).await?;
    sqlx::query(
        "INSERT INTO transit_feed_versions (network, feed_version, state, staged_at) \
         VALUES ($1, $2, 'staging', $3) ON CONFLICT (network, feed_version) DO NOTHING",
    )
    .bind(network)
    .bind(version)
    .bind(produced_at)
    .execute(&mut *conn)
    .await?;
    let state: String = sqlx::query_scalar(
        "SELECT state FROM transit_feed_versions WHERE network = $1 AND feed_version = $2",
    )
    .bind(network)
    .bind(version)
    .fetch_one(&mut *conn)
    .await?;
    if state != "staging" {
        return Ok(StageOutcome::NotStaging);
    }
    sqlx::query(
        "INSERT INTO transit_feed_parts (network, feed_version, kind, part, parts) \
         VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
    )
    .bind(network)
    .bind(version)
    .bind(part.rows.kind().as_str())
    .bind(to_i32(index.part, "part")?)
    .bind(to_i32(index.parts, "parts")?)
    .execute(&mut *conn)
    .await?;
    let keys = (network, version);
    match &part.rows {
        TimetableRows::Network(rows) => {
            let [row] = rows.as_slice() else {
                bail!("a network part has {} rows, not 1", rows.len());
            };
            sqlx::query(
                "UPDATE transit_feed_versions SET network_row = $3 \
                 WHERE network = $1 AND feed_version = $2",
            )
            .bind(network)
            .bind(version)
            .bind(serde_json::to_value(row)?)
            .execute(&mut *conn)
            .await?;
        }
        TimetableRows::Lines(rows) => insert_lines(conn, keys, rows).await?,
        TimetableRows::Stops(rows) => insert_stops(conn, keys, rows).await?,
        TimetableRows::Patterns(rows) => insert_patterns(conn, keys, rows).await?,
        TimetableRows::Trips(rows) => insert_trips(conn, keys, rows).await?,
        TimetableRows::ServiceDays(rows) => insert_service_days(conn, keys, rows).await?,
        TimetableRows::RailLinks(rows) => {
            // Not versioned: kept with the version until it is activated.
            // A duplicate is dropped then (ON CONFLICT DO NOTHING).
            sqlx::query(
                "UPDATE transit_feed_versions SET rail_links = rail_links || $3 \
                 WHERE network = $1 AND feed_version = $2",
            )
            .bind(network)
            .bind(version)
            .bind(serde_json::to_value(rows)?)
            .execute(&mut *conn)
            .await?;
        }
    }
    Ok(StageOutcome::Staged {
        rows: part.rows.len(),
    })
}

type Keys<'a> = (&'a str, &'a str);

async fn insert_lines(conn: &mut PgConnection, keys: Keys<'_>, rows: &[TransitLine]) -> Result<()> {
    let mut directions = Vec::with_capacity(rows.len());
    for row in rows {
        directions.push(serde_json::to_value(&row.directions)?);
    }
    sqlx::query(
        "INSERT INTO transit_lines \
         (network, feed_version, line_id, name, colour, sort_order, directions) \
         SELECT $1, $2, * FROM UNNEST($3::text[], $4::text[], $5::text[], $6::int[], $7::jsonb[]) \
         ON CONFLICT DO NOTHING",
    )
    .bind(keys.0)
    .bind(keys.1)
    .bind(rows.iter().map(|r| r.line_id.clone()).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.name.clone()).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.colour.clone()).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.sort_order).collect::<Vec<_>>())
    .bind(directions)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

async fn insert_stops(conn: &mut PgConnection, keys: Keys<'_>, rows: &[TransitStop]) -> Result<()> {
    sqlx::query(
        "INSERT INTO transit_stops \
         (network, feed_version, stop_id, kind, parent_stop_id, name, indicator, lat, lon, wheelchair) \
         SELECT $1, $2, * FROM UNNEST($3::text[], $4::text[], $5::text[], $6::text[], $7::text[], \
             $8::float8[], $9::float8[], $10::bool[]) \
         ON CONFLICT DO NOTHING",
    )
    .bind(keys.0)
    .bind(keys.1)
    .bind(rows.iter().map(|r| r.stop_id.clone()).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.kind.as_str()).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.parent_stop_id.clone()).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.name.clone()).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.indicator.clone()).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.lat).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.lon).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.wheelchair).collect::<Vec<_>>())
    .execute(&mut *conn)
    .await?;
    Ok(())
}

async fn insert_patterns(
    conn: &mut PgConnection,
    keys: Keys<'_>,
    rows: &[TransitPattern],
) -> Result<()> {
    sqlx::query(
        "INSERT INTO transit_patterns (network, feed_version, pattern_id, line_id, direction, headsign) \
         SELECT $1, $2, * FROM UNNEST($3::text[], $4::text[], $5::int2[], $6::text[]) \
         ON CONFLICT DO NOTHING",
    )
    .bind(keys.0)
    .bind(keys.1)
    .bind(rows.iter().map(|r| r.pattern_id.clone()).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.line_id.clone()).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.direction.id()).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.headsign.clone()).collect::<Vec<_>>())
    .execute(&mut *conn)
    .await?;
    let mut pattern_ids = Vec::new();
    let mut seqs = Vec::new();
    let mut stop_ids = Vec::new();
    let mut arrs = Vec::new();
    let mut deps = Vec::new();
    let mut pickups = Vec::new();
    let mut drop_offs = Vec::new();
    for row in rows {
        for (seq, stop) in row.stops.iter().enumerate() {
            pattern_ids.push(row.pattern_id.clone());
            seqs.push(i32::try_from(seq).context("pattern too long")?);
            stop_ids.push(stop.stop_id.clone());
            arrs.push(stop.arr_offset_s);
            deps.push(stop.dep_offset_s);
            pickups.push(i16::from(stop.pickup));
            drop_offs.push(i16::from(stop.drop_off));
        }
    }
    sqlx::query(
        "INSERT INTO transit_pattern_stops \
         (network, feed_version, pattern_id, seq, stop_id, arr_offset_s, dep_offset_s, pickup, drop_off) \
         SELECT $1, $2, * FROM UNNEST($3::text[], $4::int[], $5::text[], $6::int[], $7::int[], \
             $8::int2[], $9::int2[]) \
         ON CONFLICT DO NOTHING",
    )
    .bind(keys.0)
    .bind(keys.1)
    .bind(pattern_ids)
    .bind(seqs)
    .bind(stop_ids)
    .bind(arrs)
    .bind(deps)
    .bind(pickups)
    .bind(drop_offs)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

async fn insert_trips(conn: &mut PgConnection, keys: Keys<'_>, rows: &[TransitTrip]) -> Result<()> {
    sqlx::query(
        "INSERT INTO transit_trips \
         (network, feed_version, trip_id, line_id, pattern_id, service_id, start_s, headsign, replacement_bus) \
         SELECT $1, $2, * FROM UNNEST($3::text[], $4::text[], $5::text[], $6::text[], $7::int[], \
             $8::text[], $9::bool[]) \
         ON CONFLICT DO NOTHING",
    )
    .bind(keys.0)
    .bind(keys.1)
    .bind(rows.iter().map(|r| r.trip_id.clone()).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.line_id.clone()).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.pattern_id.clone()).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.service_id.clone()).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.start_s).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.headsign.clone()).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.replacement_bus).collect::<Vec<_>>())
    .execute(&mut *conn)
    .await?;
    Ok(())
}

async fn insert_service_days(
    conn: &mut PgConnection,
    keys: Keys<'_>,
    rows: &[TransitServiceDays],
) -> Result<()> {
    let mut services: Vec<String> = Vec::new();
    let mut dates: Vec<NaiveDate> = Vec::new();
    for row in rows {
        for date in &row.dates {
            services.push(row.service_id.clone());
            dates.push(*date);
        }
    }
    sqlx::query(
        "INSERT INTO transit_service_days (network, feed_version, service_id, service_date) \
         SELECT $1, $2, * FROM UNNEST($3::text[], $4::date[]) \
         ON CONFLICT DO NOTHING",
    )
    .bind(keys.0)
    .bind(keys.1)
    .bind(services)
    .bind(dates)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// The active version's feed version, content hash and commit time.
async fn active_version(
    conn: &mut PgConnection,
    network: &str,
) -> Result<Option<(String, Option<String>, Option<DateTime<Utc>>)>> {
    Ok(sqlx::query_as(
        "SELECT v.feed_version, v.content_sha256, v.produced_at \
         FROM transit_networks n JOIN transit_feed_versions v \
           ON v.network = n.network AND v.feed_version = n.active_feed_version \
         WHERE n.network = $1",
    )
    .bind(network)
    .fetch_optional(&mut *conn)
    .await?)
}

/// Activates `commit`'s version (spec §9.2), in order:
///
/// 1. the same content as the active version: [`CommitOutcome::Unchanged`],
///    freshness only (a copy staged under another `feed_version` is
///    deleted);
/// 2. the active version was produced later: [`CommitOutcome::Older`];
/// 3. the staged parts must be exactly the manifest's
///    ([`CommitOutcome::Incomplete`]);
/// 4. the rows must hang together: every trip's pattern and line, every
///    call's stop and every pattern's line exist, the row counts equal the
///    manifest's, and `today` (the commit's `produced_at` as a London date)
///    has service ([`CommitOutcome::Inconsistent`]);
/// 5. the flip: the old active version becomes `previous`, every other
///    version of the network is deleted (its rows cascade), this one
///    becomes `active`, `transit_networks` points at it, and
///    `transit_rail_links` is replaced with its rail links;
/// 6. the freshness marker, at `produced_at`.
pub async fn commit(
    conn: &mut PgConnection,
    commit: &TimetableCommit,
    produced_at: DateTime<Utc>,
) -> Result<CommitOutcome> {
    let network = commit.network.as_str();
    let version = commit.feed_version.as_str();
    lock_network(conn, &commit.network).await?;
    let freshness = timetable_freshness_source(&commit.network);

    let active = active_version(conn, network).await?;
    if let Some((active_version, active_sha, active_at)) = &active {
        if active_version == version
            || active_sha.as_deref() == Some(commit.content_sha256.as_str())
        {
            if active_version != version {
                sqlx::query(
                    "DELETE FROM transit_feed_versions \
                     WHERE network = $1 AND feed_version = $2 AND state = 'staging'",
                )
                .bind(network)
                .bind(version)
                .execute(&mut *conn)
                .await?;
            }
            record_ingest(conn, &freshness, Some(produced_at)).await?;
            return Ok(CommitOutcome::Unchanged);
        }
        if active_at.is_some_and(|at| at > produced_at) {
            return Ok(CommitOutcome::Older);
        }
    }

    let staged: Option<(String, Option<serde_json::Value>, serde_json::Value)> = sqlx::query_as(
        "SELECT state, network_row, rail_links FROM transit_feed_versions \
         WHERE network = $1 AND feed_version = $2",
    )
    .bind(network)
    .bind(version)
    .fetch_optional(&mut *conn)
    .await?;
    let Some((_, network_row, rail_links)) = staged else {
        return Ok(CommitOutcome::Incomplete(format!(
            "no part of {network} {version} was staged"
        )));
    };
    if let Some(missing) = missing_parts(conn, commit).await? {
        return Ok(CommitOutcome::Incomplete(missing));
    }
    let Some(network_row) = network_row else {
        return Ok(CommitOutcome::Inconsistent("no network part".to_owned()));
    };
    let network_row: TransitNetworkRow =
        serde_json::from_value(network_row).context("the staged network row")?;
    let rail_links: Vec<TransitRailLink> =
        serde_json::from_value(rail_links).context("the staged rail links")?;
    let today = service_date(produced_at);
    if let Some(why) = inconsistency(conn, commit, today).await? {
        return Ok(CommitOutcome::Inconsistent(why));
    }

    let old_active = active.map(|(v, _, _)| v);
    activate(
        conn,
        commit,
        old_active.as_deref(),
        &network_row,
        produced_at,
    )
    .await?;
    replace_rail_links(conn, network, &rail_links, produced_at).await?;
    record_ingest(conn, &freshness, Some(produced_at)).await?;
    Ok(CommitOutcome::Activated)
}

/// [`commit`] step 5's flip: the old active version becomes `previous`,
/// every other version of the network is deleted (its rows cascade), this
/// one becomes `active`, and `transit_networks` points at it.
async fn activate(
    conn: &mut PgConnection,
    commit: &TimetableCommit,
    old_active: Option<&str>,
    network_row: &TransitNetworkRow,
    produced_at: DateTime<Utc>,
) -> Result<()> {
    let network = commit.network.as_str();
    let version = commit.feed_version.as_str();
    sqlx::query(
        "DELETE FROM transit_feed_versions \
         WHERE network = $1 AND feed_version <> $2 AND feed_version IS DISTINCT FROM $3",
    )
    .bind(network)
    .bind(version)
    .bind(old_active)
    .execute(&mut *conn)
    .await?;
    sqlx::query(
        "UPDATE transit_feed_versions SET state = 'previous' \
         WHERE network = $1 AND feed_version = $2",
    )
    .bind(network)
    .bind(old_active)
    .execute(&mut *conn)
    .await?;
    sqlx::query(
        "UPDATE transit_feed_versions SET state = 'active', content_sha256 = $3, counts = $4, \
             horizon_from = $5, horizon_to = $6, source = $7, produced_at = $8, activated_at = now() \
         WHERE network = $1 AND feed_version = $2",
    )
    .bind(network)
    .bind(version)
    .bind(&commit.content_sha256)
    .bind(serde_json::to_value(commit.counts)?)
    .bind(commit.horizon.from)
    .bind(commit.horizon.to)
    .bind(serde_json::to_value(&commit.source)?)
    .bind(produced_at)
    .execute(&mut *conn)
    .await?;
    sqlx::query(
        "INSERT INTO transit_networks \
         (network, name, mode, timezone, attribution, active_feed_version, activated_at, updated_at) \
         VALUES ($1, $2, $3, $4, $5, $6, now(), now()) \
         ON CONFLICT (network) DO UPDATE SET name = EXCLUDED.name, mode = EXCLUDED.mode, \
             timezone = EXCLUDED.timezone, attribution = EXCLUDED.attribution, \
             active_feed_version = EXCLUDED.active_feed_version, \
             activated_at = EXCLUDED.activated_at, updated_at = EXCLUDED.updated_at",
    )
    .bind(network)
    .bind(&network_row.name)
    .bind(&network_row.mode)
    .bind(&network_row.timezone)
    .bind(serde_json::to_value(&network_row.attribution)?)
    .bind(version)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// `None` when the staged parts are exactly `commit.parts` (a kind listed
/// with 0 parts must have none); else what is missing or extra.
async fn missing_parts(
    conn: &mut PgConnection,
    commit: &TimetableCommit,
) -> Result<Option<String>> {
    let staged: Vec<(String, i32, i64)> = sqlx::query_as(
        "SELECT kind, max(parts), count(*) FROM transit_feed_parts \
         WHERE network = $1 AND feed_version = $2 GROUP BY kind",
    )
    .bind(commit.network.as_str())
    .bind(&commit.feed_version)
    .fetch_all(&mut *conn)
    .await?;
    let staged: BTreeMap<String, (i32, i64)> = staged
        .into_iter()
        .map(|(kind, parts, seen)| (kind, (parts, seen)))
        .collect();
    let mut problems = Vec::new();
    for kind in TimetablePartKind::ALL {
        let want = commit.parts.get(&kind).copied().unwrap_or(0);
        let (parts, seen) = staged.get(kind.as_str()).copied().unwrap_or((0, 0));
        if i64::from(want) != seen || (want > 0 && i64::from(parts) != i64::from(want)) {
            problems.push(format!("{kind}: {seen} of {want} parts staged"));
        }
    }
    Ok((!problems.is_empty()).then(|| problems.join("; ")))
}

/// `None` when the staged version hangs together (see [`commit`] step 4).
async fn inconsistency(
    conn: &mut PgConnection,
    commit: &TimetableCommit,
    today: NaiveDate,
) -> Result<Option<String>> {
    let network = commit.network.as_str();
    let version = commit.feed_version.as_str();
    let checks: [(&str, &str); 4] = [
        (
            "a trip whose pattern is missing",
            "SELECT t.trip_id FROM transit_trips t WHERE t.network = $1 AND t.feed_version = $2 \
             AND NOT EXISTS (SELECT 1 FROM transit_patterns p WHERE p.network = t.network \
               AND p.feed_version = t.feed_version AND p.pattern_id = t.pattern_id) LIMIT 1",
        ),
        (
            "a trip whose line is missing",
            "SELECT t.trip_id FROM transit_trips t WHERE t.network = $1 AND t.feed_version = $2 \
             AND t.line_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM transit_lines l \
               WHERE l.network = t.network AND l.feed_version = t.feed_version \
               AND l.line_id = t.line_id) LIMIT 1",
        ),
        (
            "a pattern whose line is missing",
            "SELECT p.pattern_id FROM transit_patterns p WHERE p.network = $1 \
             AND p.feed_version = $2 AND p.line_id IS NOT NULL AND NOT EXISTS \
               (SELECT 1 FROM transit_lines l WHERE l.network = p.network \
               AND l.feed_version = p.feed_version AND l.line_id = p.line_id) LIMIT 1",
        ),
        (
            "a call at a missing stop",
            "SELECT s.pattern_id || ':' || s.stop_id FROM transit_pattern_stops s \
             WHERE s.network = $1 AND s.feed_version = $2 AND NOT EXISTS \
               (SELECT 1 FROM transit_stops t WHERE t.network = s.network \
               AND t.feed_version = s.feed_version AND t.stop_id = s.stop_id) LIMIT 1",
        ),
    ];
    for (what, sql) in checks {
        let found: Option<String> = sqlx::query_scalar(sql)
            .bind(network)
            .bind(version)
            .fetch_optional(&mut *conn)
            .await?;
        if let Some(id) = found {
            return Ok(Some(format!("{what}: {id}")));
        }
    }
    let counts: (i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT \
           (SELECT count(*) FROM transit_lines WHERE network = $1 AND feed_version = $2), \
           (SELECT count(*) FROM transit_stops WHERE network = $1 AND feed_version = $2), \
           (SELECT count(*) FROM transit_patterns WHERE network = $1 AND feed_version = $2), \
           (SELECT count(*) FROM transit_trips WHERE network = $1 AND feed_version = $2), \
           (SELECT count(*) FROM transit_service_days WHERE network = $1 AND feed_version = $2)",
    )
    .bind(network)
    .bind(version)
    .fetch_one(&mut *conn)
    .await?;
    let want = commit.counts;
    let manifest = (
        i64::from(want.lines),
        i64::from(want.stops),
        i64::from(want.patterns),
        i64::from(want.trips),
        i64::from(want.service_days),
    );
    if counts != manifest {
        return Ok(Some(format!(
            "rows (lines, stops, patterns, trips, service days) {counts:?}, manifest {manifest:?}"
        )));
    }
    let service_today: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM transit_service_days \
         WHERE network = $1 AND feed_version = $2 AND service_date = $3)",
    )
    .bind(network)
    .bind(version)
    .bind(today)
    .fetch_one(&mut *conn)
    .await?;
    if !service_today {
        return Ok(Some(format!("no service on {today}")));
    }
    Ok(None)
}

/// Replaces `network`'s rail links with `links` (duplicates dropped).
async fn replace_rail_links(
    conn: &mut PgConnection,
    network: &str,
    links: &[TransitRailLink],
    observed_at: DateTime<Utc>,
) -> Result<()> {
    sqlx::query("DELETE FROM transit_rail_links WHERE network = $1")
        .bind(network)
        .execute(&mut *conn)
        .await?;
    sqlx::query(
        "INSERT INTO transit_rail_links (network, stop_id, crs, tiploc, kind, note, observed_at) \
         SELECT $1, stop_id, crs, tiploc, kind, note, $7 \
         FROM UNNEST($2::text[], $3::text[], $4::text[], $5::text[], $6::text[]) \
           AS l(stop_id, crs, tiploc, kind, note) \
         ON CONFLICT DO NOTHING",
    )
    .bind(network)
    .bind(links.iter().map(|l| l.stop_id.clone()).collect::<Vec<_>>())
    .bind(links.iter().map(|l| l.crs.clone()).collect::<Vec<_>>())
    .bind(links.iter().map(|l| l.tiploc.clone()).collect::<Vec<_>>())
    .bind(links.iter().map(|l| l.kind.as_str()).collect::<Vec<_>>())
    .bind(links.iter().map(|l| l.note.clone()).collect::<Vec<_>>())
    .bind(observed_at)
    .execute(&mut *conn)
    .await?;
    Ok(())
}
