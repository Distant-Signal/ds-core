//! The ingest-writer's side of a network's situations (transit spec §9.3,
//! §10.3; plan 4.3): [`apply_situations`] for each `transit-situations/1`
//! snapshot, inside the writer's entry transaction.
//!
//! A snapshot is the network's whole current set. In one transaction, under
//! the network's advisory lock:
//!
//! 1. **Ordering** (ingest D13). A snapshot produced before the newest one
//!    applied (the network's situations freshness marker) is an
//!    out-of-order redelivery and changes nothing ([`SituationsOutcome::Older`]).
//!    A marker more than 2 minutes in the future is not trusted.
//! 2. Every situation is upserted (`observed_at` is the envelope's
//!    `produced_at`; `first_seen_at` is kept), with the row guard
//!    `EXCLUDED.observed_at >= observed_at OR observed_at > now() + 2 min`.
//! 3. The network's situations missing from the snapshot are deleted: they
//!    ended.
//! 4. The lines' statuses are derived from the snapshot
//!    (`common::transit::status::derive`, at `produced_at`) against the
//!    active timetable's lines, and written to `line_status` with
//!    `source = 'transit'`, `line_id = '<network>:<line>'`,
//!    `mode_name = '<network>'` and `computed_at = source_updated_at =
//!    produced_at`. A history row is appended only when a line's statuses
//!    change (validity dates aside, see [`statuses_changed`]); an unchanged
//!    line keeps its stored `statuses` value.
//! 5. The network's transit lines no longer in the timetable are deleted
//!    (`source = 'transit' AND line_id LIKE '<network>:%'`), so the
//!    aggregator's and `TfL`'s rows are never touched.
//! 6. Freshness: `transit-<network>-situations` is `produced_at`.
//!
//! `operators` is empty: a tram line has no National Rail operator, and an
//! operator code there would put it on the operator pages (spec §10.5).

use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use common::transit::status::{DerivedLineStatus, NetworkLine, derive};
use common::transit::{
    NetworkId, TransitLineDirection, TransitSituations, situations_freshness_source,
};
use sqlx::PgConnection;

use super::write::lock_network;
use crate::freshness::record_ingest;

/// What [`apply_situations`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SituationsOutcome {
    Applied(SituationsApplied),
    /// The snapshot is older than the newest applied: nothing written.
    Older,
}

/// The counts of an applied snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SituationsApplied {
    /// Situations inserted or updated (the row guard's refusals aside).
    pub situations: u64,
    /// Situations deleted because the snapshot no longer lists them.
    pub ended: u64,
    /// `line_status` rows written.
    pub lines: u64,
    /// `line_status_history` rows appended.
    pub history: u64,
    /// The network's `line_status` rows deleted (lines gone from the
    /// timetable).
    pub pruned: u64,
}

/// A `line_status` row this network's handler would have written belongs
/// to another source. Never expected (the CHECK and the id grammar keep
/// transit ids apart); poison if it happens.
#[derive(Debug)]
pub struct TransitLineOwnedElsewhere(pub String);

impl std::fmt::Display for TransitLineOwnedElsewhere {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "refusing to write transit line status for {:?}: a row with that id belongs to another source",
            self.0
        )
    }
}

impl std::error::Error for TransitLineOwnedElsewhere {}

/// Whether a line's statuses changed, for the history: their JSON with
/// every entry's `validity` removed. "No reported disruption" is dated at
/// each snapshot, and a situation's period moving is not a status change.
pub fn statuses_changed(stored: Option<&serde_json::Value>, incoming: &serde_json::Value) -> bool {
    fn strip(value: &serde_json::Value) -> serde_json::Value {
        let mut value = value.clone();
        if let Some(entries) = value.as_array_mut() {
            for entry in entries {
                if let Some(obj) = entry.as_object_mut() {
                    obj.remove("validity");
                }
            }
        }
        value
    }
    stored.is_none_or(|stored| strip(stored) != strip(incoming))
}

/// The network's lines in its active timetable, with every station each
/// serves.
async fn network_lines(conn: &mut PgConnection, network: &NetworkId) -> Result<Vec<NetworkLine>> {
    let rows: Vec<(String, String, sqlx::types::Json<Vec<TransitLineDirection>>)> = sqlx::query_as(
        "SELECT l.line_id, l.name, l.directions \
           FROM transit_lines l \
           JOIN transit_networks n \
             ON n.network = l.network AND n.active_feed_version = l.feed_version \
          WHERE l.network = $1 \
          ORDER BY l.line_id",
    )
    .bind(network.as_str())
    .fetch_all(&mut *conn)
    .await
    .context("transit lines")?;
    Ok(rows
        .into_iter()
        .map(|(line_id, name, directions)| NetworkLine {
            line_id,
            name,
            stations: directions.0.into_iter().flat_map(|d| d.stations).collect(),
        })
        .collect())
}

/// Applies one `transit-situations/1` snapshot (see the module docs).
pub async fn apply_situations(
    conn: &mut PgConnection,
    snapshot: &TransitSituations,
    produced_at: DateTime<Utc>,
) -> Result<SituationsOutcome> {
    let network = &snapshot.network;
    lock_network(conn, network).await?;
    let source = situations_freshness_source(network);
    let newest: Option<bool> = sqlx::query_scalar(
        "SELECT fetched_at > $2 AND fetched_at <= now() + interval '2 min' \
           FROM ingest_freshness WHERE source = $1",
    )
    .bind(&source)
    .bind(produced_at)
    .fetch_optional(&mut *conn)
    .await
    .context("situations freshness")?;
    if newest == Some(true) {
        return Ok(SituationsOutcome::Older);
    }

    let mut applied = SituationsApplied {
        situations: upsert_situations(conn, snapshot, produced_at).await?,
        ..SituationsApplied::default()
    };
    let ids: Vec<&str> = snapshot
        .situations
        .iter()
        .map(|s| s.situation_id.as_str())
        .collect();
    applied.ended = sqlx::query(
        "DELETE FROM transit_situations WHERE network = $1 AND NOT (situation_id = ANY($2))",
    )
    .bind(network.as_str())
    .bind(&ids)
    .execute(&mut *conn)
    .await
    .context("delete ended situations")?
    .rows_affected();

    let lines = network_lines(conn, network).await?;
    let derived = derive(network, &snapshot.situations, &lines, produced_at);
    write_line_status(conn, network, &derived, produced_at, &mut applied).await?;

    record_ingest(conn, &source, Some(produced_at)).await?;
    Ok(SituationsOutcome::Applied(applied))
}

async fn upsert_situations(
    conn: &mut PgConnection,
    snapshot: &TransitSituations,
    produced_at: DateTime<Utc>,
) -> Result<u64> {
    if snapshot.situations.is_empty() {
        return Ok(0);
    }
    let rows: serde_json::Value = serde_json::to_value(&snapshot.situations)?;
    let written = sqlx::query(
        r"
        INSERT INTO transit_situations
            (network, situation_id, version, progress, planned, reason, summary, description,
             info_url, validity, publication, consequences, line_ids, stop_ids, scope,
             observed_at, first_seen_at)
        SELECT $1, s.situation_id, s.version, s.progress, s.planned, s.reason, s.summary,
               s.description, s.info_url, s.validity, s.publication, s.consequences,
               ARRAY(SELECT jsonb_array_elements_text(s.line_ids)),
               ARRAY(SELECT jsonb_array_elements_text(s.stop_ids)),
               s.scope, $3, $3
          FROM jsonb_to_recordset($2) AS s(
               situation_id TEXT, version INT, progress TEXT, planned BOOLEAN, reason TEXT,
               summary TEXT, description TEXT, info_url TEXT, validity JSONB,
               publication JSONB, consequences JSONB, line_ids JSONB, stop_ids JSONB,
               scope TEXT)
        ON CONFLICT (network, situation_id) DO UPDATE SET
            version = EXCLUDED.version,
            progress = EXCLUDED.progress,
            planned = EXCLUDED.planned,
            reason = EXCLUDED.reason,
            summary = EXCLUDED.summary,
            description = EXCLUDED.description,
            info_url = EXCLUDED.info_url,
            validity = EXCLUDED.validity,
            publication = EXCLUDED.publication,
            consequences = EXCLUDED.consequences,
            line_ids = EXCLUDED.line_ids,
            stop_ids = EXCLUDED.stop_ids,
            scope = EXCLUDED.scope,
            observed_at = EXCLUDED.observed_at
        WHERE EXCLUDED.observed_at >= transit_situations.observed_at
           OR transit_situations.observed_at > now() + interval '2 min'
        ",
    )
    .bind(snapshot.network.as_str())
    .bind(&rows)
    .bind(produced_at)
    .execute(&mut *conn)
    .await
    .context("upsert situations")?
    .rows_affected();
    Ok(written)
}

#[expect(
    clippy::too_many_lines,
    reason = "read, upsert, history and prune of one batch; linear, sharing its arrays"
)]
async fn write_line_status(
    conn: &mut PgConnection,
    network: &NetworkId,
    derived: &[DerivedLineStatus],
    produced_at: DateTime<Utc>,
    applied: &mut SituationsApplied,
) -> Result<()> {
    let prefix = format!("{}:", network.as_str());
    let mut ids: Vec<String> = Vec::with_capacity(derived.len());
    let mut names: Vec<&str> = Vec::with_capacity(derived.len());
    let mut statuses: Vec<serde_json::Value> = Vec::with_capacity(derived.len());
    let mut upcoming: Vec<serde_json::Value> = Vec::with_capacity(derived.len());
    for row in derived {
        if row.line_id.network() != network {
            bail!("derived line {} is outside network {network}", row.line_id);
        }
        ids.push(row.line_id.to_string());
        names.push(&row.name);
        statuses.push(serde_json::to_value(&row.statuses)?);
        upcoming.push(serde_json::to_value(&row.upcoming)?);
    }

    let stored_rows: Vec<(String, String, serde_json::Value)> =
        sqlx::query_as("SELECT line_id, source, statuses FROM line_status WHERE line_id = ANY($1)")
            .bind(&ids)
            .fetch_all(&mut *conn)
            .await
            .context("stored transit line statuses")?;
    if let Some((line_id, _, _)) = stored_rows
        .iter()
        .find(|(_, source, _)| source != "transit")
    {
        return Err(TransitLineOwnedElsewhere(line_id.clone()).into());
    }
    let stored: HashMap<&str, &serde_json::Value> = stored_rows
        .iter()
        .map(|(id, _, statuses)| (id.as_str(), statuses))
        .collect();
    let changed: Vec<bool> = ids
        .iter()
        .zip(&statuses)
        .map(|(id, incoming)| statuses_changed(stored.get(id.as_str()).copied(), incoming))
        .collect();
    // An unchanged line keeps its stored value (so its TOAST chunks are
    // reused, as for TfL rows): the upsert's CASE then leaves it alone.
    let to_write: Vec<&serde_json::Value> = ids
        .iter()
        .zip(&statuses)
        .zip(&changed)
        .map(|((id, incoming), changed)| match stored.get(id.as_str()) {
            Some(stored) if !changed => *stored,
            _ => incoming,
        })
        .collect();

    if !ids.is_empty() {
        let written: Vec<String> = sqlx::query_scalar(
            r"
            INSERT INTO line_status
                (line_id, name, mode_name, operators, statuses, upcoming, computed_at, source,
                 source_updated_at)
            SELECT i.line_id, i.name, $5, '{}', i.statuses, i.upcoming, $6, 'transit', $6
              FROM UNNEST($1::text[], $2::text[], $3::jsonb[], $4::jsonb[])
                   AS i(line_id, name, statuses, upcoming)
            ON CONFLICT (line_id) DO UPDATE SET
                name = EXCLUDED.name,
                mode_name = EXCLUDED.mode_name,
                operators = EXCLUDED.operators,
                statuses = CASE
                    WHEN line_status.statuses IS DISTINCT FROM EXCLUDED.statuses
                    THEN EXCLUDED.statuses
                    ELSE line_status.statuses
                END,
                upcoming = EXCLUDED.upcoming,
                computed_at = EXCLUDED.computed_at,
                source_updated_at = EXCLUDED.source_updated_at
            WHERE line_status.source = 'transit'
            RETURNING line_id
            ",
        )
        .bind(&ids)
        .bind(&names)
        .bind(&to_write)
        .bind(&upcoming)
        .bind(network.as_str())
        .bind(produced_at)
        .fetch_all(&mut *conn)
        .await
        .context("upsert transit line status")?;
        if let Some(refused) = ids.iter().find(|id| !written.contains(id)) {
            return Err(TransitLineOwnedElsewhere(refused.clone()).into());
        }
        applied.lines = written.len() as u64;

        let (history_ids, history_statuses): (Vec<&str>, Vec<&serde_json::Value>) = ids
            .iter()
            .zip(&statuses)
            .zip(&changed)
            .filter(|(_, changed)| **changed)
            .map(|((id, statuses), _)| (id.as_str(), statuses))
            .unzip();
        if !history_ids.is_empty() {
            applied.history = sqlx::query(
                "INSERT INTO line_status_history (line_id, statuses, computed_at) \
                 SELECT line_id, statuses, $3 \
                   FROM UNNEST($1::text[], $2::jsonb[]) WITH ORDINALITY AS h(line_id, statuses, ord) \
                  ORDER BY ord",
            )
            .bind(&history_ids)
            .bind(&history_statuses)
            .bind(produced_at)
            .execute(&mut *conn)
            .await
            .context("transit line status history")?
            .rows_affected();
        }
    }

    applied.pruned = sqlx::query(
        "DELETE FROM line_status \
          WHERE source = 'transit' AND starts_with(line_id, $1) AND NOT (line_id = ANY($2))",
    )
    .bind(&prefix)
    .bind(&ids)
    .execute(&mut *conn)
    .await
    .context("prune transit line status")?
    .rows_affected();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validity_alone_is_not_a_change() {
        let a = serde_json::json!([{"severity": 27, "reason": "x",
            "validity": {"from_date": "2026-10-14T10:00:00Z", "to_date": null, "is_now": true}}]);
        let b = serde_json::json!([{"severity": 27, "reason": "x",
            "validity": {"from_date": "2026-10-14T10:05:00Z", "to_date": null, "is_now": true}}]);
        let c = serde_json::json!([{"severity": 9, "reason": "x",
            "validity": {"from_date": "2026-10-14T10:05:00Z", "to_date": null, "is_now": true}}]);
        assert!(!statuses_changed(Some(&a), &b));
        assert!(statuses_changed(Some(&a), &c));
        assert!(statuses_changed(None, &a));
    }
}
