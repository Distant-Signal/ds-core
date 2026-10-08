//! The incident snapshot: [`apply_snapshot`] and its helpers,
//! `incident_removal.rs` (whole, as [`removal`]) and `parse_snapshot`
//! (today's `incident_snapshot_from_body`).
//!
//! Moved here from `api/src/data/queries.rs` by plan task 1A.8, and split
//! by plan task 2c.1 so Redis stays out of this crate. A snapshot is
//! written in three steps, in this order, by both of its writers (the api's
//! `POST /private/incidents` and poller-incidents' `DbSink`):
//!
//! 1. [`apply_snapshot`]: the content upserts, in committed chunks; returns
//!    the ids whose text changed;
//! 2. the caller XADDs those ids to `incident-text-changed`, best effort,
//!    after every chunk has committed;
//! 3. [`removal::infer_removals`]: "Ended (no longer listed)".
//!
//! # The row heartbeat (plan 2c.6, spec §9.4)
//!
//! [`RowHeartbeat::On`] (`INCIDENTS_ROW_HEARTBEAT=true`, the default) is
//! today's behaviour: every listed row gets `fetched_at = NOW()` on every
//! poll, about 600k row updates a day. [`RowHeartbeat::Off`] writes only
//! what changed: the content upsert, the rows coming back
//! (`source_missing_polls`/`source_removed_at` reset), the cleared rows the
//! feed still lists (see below), and one `incident_feed_state` row per
//! snapshot (`last_snapshot_at`, `previous_snapshot_at`). Readers derive the
//! display time with [`FETCHED_AT_SQL`], which returns the same value in
//! both modes (up to the snapshot it names).
//!
//! **Cleared rows keep their per-row bump in both modes.** The removal
//! inference never counts a cleared row's misses (it is RDM's own fact), so
//! nothing records whether the feed still lists one; a feed time would
//! date every cleared row that left the feed (727 of them in production on
//! 2026-10-07) to the latest poll. The feed lists a few dozen cleared rows
//! at a time (33 then), until its nightly purge.

pub mod removal;

use std::collections::HashMap;

use anyhow::Result;
use common::IncidentMessage;
use sqlx::PgPool;

use crate::freshness::{last_per_key, record_ingest};

/// Incidents are upserted in chunks of this size, each as its own
/// transaction, rather than one transaction for the whole poll batch --
/// see [`apply_snapshot`]'s doc comment for why.
pub const UPSERT_CHUNK_SIZE: usize = 50;

/// The subset of an existing `incidents` row needed to decide whether an
/// incoming `IncidentMessage` represents a real change worth recording in
/// `incident_history`.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct ExistingIncident {
    pub incident_id: String,
    pub summary: String,
    pub description: String,
    pub validity_periods: serde_json::Value,
    pub is_cleared: bool,
}

/// Pure diff check, factored out of [`apply_snapshot`] so it's testable
/// without a database: an incident is "changed" if it's new, or if its
/// summary, description, validity periods or `is_cleared` differ from
/// what's stored.
///
/// `is_cleared` joined the list on 2026-10-06: RDM often clears an incident
/// by flipping `ClearedIncident` alone, and that wrote no `incident_history`
/// row, so the history could not say when (or that) it was cleared. The
/// history consumers (the detail page's diff summary, the replay export
/// scripts) already carry `is_cleared` per row.
pub fn incident_changed(
    existing: Option<&ExistingIncident>,
    summary: &str,
    description: &str,
    validity_periods: &serde_json::Value,
    is_cleared: bool,
) -> bool {
    match existing {
        None => true,
        Some(row) => {
            row.summary != summary
                || row.description != description
                || row.validity_periods != *validity_periods
                || row.is_cleared != is_cleared
        }
    }
}

/// Narrower than `incident_changed`: true only if summary or description
/// differ from what's stored. Validity-only changes don't need
/// re-extraction -- the prose an LLM would read hasn't moved. Drives
/// whether [`apply_snapshot`] reports the id in `text_changed_ids`.
pub fn text_changed(existing: Option<&ExistingIncident>, summary: &str, description: &str) -> bool {
    match existing {
        None => true,
        Some(row) => row.summary != summary || row.description != description,
    }
}

/// Every station's name (the `stations` reference table), for resolving
/// the places an incident names (`common::station_resolver`). The same
/// query as the aggregator's `load_station_names`.
pub async fn load_station_gazetteer(
    pool: &PgPool,
) -> Result<common::station_resolver::StationGazetteer> {
    let rows: Vec<(String, String)> = sqlx::query_as("SELECT crs::text, name FROM stations")
        .fetch_all(pool)
        .await?;
    Ok(common::station_resolver::StationGazetteer::new(rows))
}

/// What one snapshot's write did: [`apply_snapshot`]'s count, then
/// [`removal::infer_removals`]'s verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IncidentSnapshotOutcome {
    pub upserted: u64,
    pub inference: removal::Inference,
}

/// What [`apply_snapshot`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedSnapshot {
    /// Incidents in the snapshot (every chunk committed).
    pub upserted: u64,
    /// The ids whose summary or description changed, or that are new: what
    /// the caller XADDs to `incident-text-changed`.
    pub text_changed_ids: Vec<String>,
}

/// How a snapshot keeps the incidents' display time current
/// (`INCIDENTS_ROW_HEARTBEAT`, plan 2c.6). See the module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RowHeartbeat {
    /// `fetched_at = NOW()` on every listed row, every snapshot (today's
    /// behaviour, the default). Keeps `incident_feed_state`'s
    /// `last_snapshot_at` NULL.
    #[default]
    On,
    /// Touch a row only when its content or listing state changes, and
    /// stamp the feed's snapshot times once per snapshot.
    Off,
}

impl RowHeartbeat {
    /// `INCIDENTS_ROW_HEARTBEAT`'s value: `true` is [`Self::On`].
    pub const fn from_flag(on: bool) -> Self {
        if on { Self::On } else { Self::Off }
    }

    /// The value as the flag.
    pub const fn is_on(self) -> bool {
        matches!(self, Self::On)
    }
}

/// An incident's display time ("Last updated from National Rail",
/// `fetchedAt`), for every reader of `incidents.fetched_at` (plan 2c.4,
/// spec §9.4): the row's own time, or, for an uncleared incident the latest
/// snapshot listed (`source_missing_polls = 0`, not ended), the feed's
/// `last_snapshot_at` if later. Use it as `{FETCHED_AT_SQL} AS fetched_at`
/// in a query over `incidents` that does not alias the table.
///
/// While [`RowHeartbeat::On`] runs, `last_snapshot_at` is NULL and
/// `GREATEST` ignores a NULL, so this is exactly `incidents.fetched_at`:
/// the readers can ship before the writer change and the writer change can
/// be reverted on its own. Cleared rows always use their own time (module
/// docs).
pub const FETCHED_AT_SQL: &str = "GREATEST(incidents.fetched_at, \
     CASE WHEN NOT incidents.is_cleared \
               AND incidents.source_missing_polls = 0 \
               AND incidents.source_removed_at IS NULL \
          THEN (SELECT s.last_snapshot_at FROM incident_feed_state s WHERE s.singleton) \
     END)";

/// Gauge: how many of the live incidents in the latest Knowledgebase poll
/// name no place the matcher resolves (and are not network-wide), so fall
/// back to every line of their operators (2026-10-06 decision 9). Set on
/// every snapshot POST. No labels: the phrase it missed goes to the debug
/// log, never into a label (unbounded cardinality).
pub const INCIDENTS_WITHOUT_PLACE_METRIC: &str = "api_incidents_without_resolved_place";

/// Sets [`INCIDENTS_WITHOUT_PLACE_METRIC`] for this poll's live (uncleared)
/// incidents and logs each one's summary at debug level, for finding the
/// phrasings the resolver misses.
#[expect(
    clippy::cast_precision_loss,
    reason = "metric gauges take f64; a poll holds a few hundred incidents"
)]
fn record_unresolved_places(
    line_matcher: &common::matcher::LineMatcher,
    incidents: &[IncidentMessage],
    gazetteer: &common::station_resolver::StationGazetteer,
) {
    let mut unresolved = 0usize;
    for incident in incidents.iter().filter(|i| !i.is_cleared) {
        if line_matcher.names_no_resolvable_place(incident, gazetteer) {
            unresolved += 1;
            tracing::debug!(
                incident_id = %incident.incident_id,
                summary = %incident.summary,
                "incident names no resolvable place; shown on every line of its operators"
            );
        }
    }
    metrics::gauge!(common::metrics::metric_name(INCIDENTS_WITHOUT_PLACE_METRIC))
        .set(unresolved as f64);
}

/// Upserts a snapshot of Knowledgebase incidents (step 1 of the module
/// docs). Each incident is inserted or updated in `incidents`; if the
/// stored `summary/description/validity_periods/is_cleared` differ from
/// what's incoming (or the incident is new), a snapshot is also appended to
/// `incident_history`.
///
/// Runs as a series of `UPSERT_CHUNK_SIZE`-sized transactions rather than one
/// transaction for the whole batch -- a full poll cycle can carry hundreds of
/// incidents, and holding row locks on all of them for the duration of one
/// giant transaction blocks unrelated single-row writers (e.g. the enricher
/// persisting extraction results) for as long as the whole batch takes.
/// Chunking bounds that lock-hold window to one chunk's worth of work. Each
/// chunk is still atomic with respect to its own `incidents`/`incident_history`
/// writes, but a failure partway through the batch no longer rolls back
/// chunks that already committed -- acceptable here because the poller
/// resends the full current feed state every cycle (see `poller-incidents`),
/// so anything not persisted this round is retried wholesale next round.
/// A chunk failure returns an error, so the caller never runs the
/// inference over a partly written snapshot.
///
/// `line_matcher` is run over each incoming incident to fill
/// `incidents.affected_lines` -- see that column's migration
/// (`20260917090000_incidents_affected_lines.sql`) and `common::matcher`'s
/// module doc. It is a pure function of the incident's own text + operator
/// list against the line catalogue, so recomputing it on every poll cycle
/// is both cheap and the mechanism by which a catalogue edit (a new
/// `match_keywords` entry, say) reaches still-live incidents: they are
/// re-sent every cycle. Incidents that have dropped out of the feed keep
/// whatever was computed when they were last seen, which is why the
/// backfill binary exists.
///
/// Every incident the batch names gets `source_missing_polls = 0` and
/// `source_removed_at = NULL`, complete snapshot or not: being listed is
/// positive evidence on its own, so a reappearing incident is un-ended at
/// once. Whether every listed row also gets `fetched_at = NOW()` is
/// `heartbeat` (module docs).
///
/// The returned `text_changed_ids` are for the caller to publish, once
/// every chunk has committed (which is when this returns) and before the
/// inference: best effort (log, do not fail), because a publish failure
/// must not fail the ingest. Redis stays out of this crate (spec §5.1).
#[expect(
    clippy::too_many_lines,
    reason = "long but linear; splitting it would scatter its shared state across helpers"
)]
pub async fn apply_snapshot(
    pool: &PgPool,
    line_matcher: &common::matcher::LineMatcher,
    incidents: &[IncidentMessage],
    heartbeat: RowHeartbeat,
) -> Result<AppliedSnapshot> {
    let mut count = 0u64;
    let mut text_changed_ids = Vec::new();

    // Matched up front, outside every transaction. This function's whole
    // chunking scheme exists to bound how long a transaction holds row
    // locks (see the doc comment above), so pure CPU work that needs no
    // database at all has no business running inside one -- even work this
    // cheap (a substring scan per catalogue line).
    //
    // The station names resolve the places each incident names (2026-10-06,
    // `common::station_resolver`), exactly as the aggregator does every
    // cycle, so `affected_lines` and the live statuses agree. Read per
    // snapshot (about 2,600 short rows every 5 minutes) rather than cached,
    // so a reference-data refresh applies at once. Fail-open: without them
    // the matcher falls back to its pre-2026-10-06 answer for this poll,
    // and the next poll recomputes every live row anyway.
    let gazetteer = load_station_gazetteer(pool).await.unwrap_or_else(|err| {
        tracing::warn!(error = ?err, "failed to load station names; incident places are not resolved this poll");
        common::station_resolver::StationGazetteer::default()
    });
    let affected_lines: Vec<Vec<String>> = incidents
        .iter()
        .map(|incident| line_matcher.affected_line_ids(incident, &gazetteer))
        .collect();
    record_unresolved_places(line_matcher, incidents, &gazetteer);

    for (chunk_index, chunk) in incidents.chunks(UPSERT_CHUNK_SIZE).enumerate() {
        let chunk_offset = chunk_index * UPSERT_CHUNK_SIZE;
        let mut tx = pool.begin().await?;

        let chunk_ids: Vec<&str> = chunk.iter().map(|i| i.incident_id.as_str()).collect();
        let existing_rows: Vec<ExistingIncident> = sqlx::query_as(
            "SELECT incident_id, summary, description, validity_periods, is_cleared \
             FROM incidents WHERE incident_id = ANY($1)",
        )
        .bind(&chunk_ids)
        .fetch_all(&mut *tx)
        .await?;
        let existing_by_id: HashMap<&str, &ExistingIncident> = existing_rows
            .iter()
            .map(|row| (row.incident_id.as_str(), row))
            .collect();

        // F2: one upsert and at most one history insert per chunk, instead
        // of one or two statements per incident. A repeated incident_id in
        // the same chunk keeps its LAST copy (a multi-row upsert cannot touch
        // a row twice); the old loop's final write was that copy too.
        let rows: Vec<(&IncidentMessage, &Vec<String>, serde_json::Value)> = chunk
            .iter()
            .enumerate()
            .map(|(offset_in_chunk, incident)| {
                serde_json::to_value(&incident.validity).map(|validity| {
                    (
                        incident,
                        &affected_lines[chunk_offset + offset_in_chunk],
                        validity,
                    )
                })
            })
            .collect::<std::result::Result<_, _>>()?;
        let rows = last_per_key(&rows, |(incident, _, _)| incident.incident_id.as_str());

        let mut changed_rows = Vec::new();
        for (incident, _, validity_json) in &rows {
            let existing = existing_by_id.get(incident.incident_id.as_str()).copied();
            if incident_changed(
                existing,
                &incident.summary,
                &incident.description,
                validity_json,
                incident.is_cleared,
            ) {
                changed_rows.push((*incident, validity_json));
            }
            if text_changed(existing, &incident.summary, &incident.description) {
                text_changed_ids.push(incident.incident_id.clone());
            }
        }

        let json_array = |values: &[String]| serde_json::Value::from(values.to_vec());
        let ids: Vec<&str> = rows
            .iter()
            .map(|(i, _, _)| i.incident_id.as_str())
            .collect();
        let summaries: Vec<&str> = rows.iter().map(|(i, _, _)| i.summary.as_str()).collect();
        let descriptions: Vec<&str> = rows
            .iter()
            .map(|(i, _, _)| i.description.as_str())
            .collect();
        let operators: Vec<serde_json::Value> = rows
            .iter()
            .map(|(i, _, _)| json_array(&i.operators))
            .collect();
        let stations: Vec<serde_json::Value> = rows
            .iter()
            .map(|(i, _, _)| json_array(&i.affected_stations))
            .collect();
        let priorities: Vec<i32> = rows.iter().map(|(i, _, _)| i.priority).collect();
        let validities: Vec<&serde_json::Value> = rows.iter().map(|(_, _, v)| v).collect();
        let planned: Vec<bool> = rows.iter().map(|(i, _, _)| i.is_planned).collect();
        let cleared: Vec<bool> = rows.iter().map(|(i, _, _)| i.is_cleared).collect();
        let lines: Vec<serde_json::Value> = rows.iter().map(|(_, l, _)| json_array(l)).collect();

        sqlx::query(
            r"
            INSERT INTO incidents (
                incident_id, summary, description, operators, affected_stations,
                priority, validity_periods, is_planned, is_cleared, fetched_at,
                first_seen_at, affected_lines, active_since
            )
            SELECT i.incident_id, i.summary, i.description,
                   ARRAY(SELECT jsonb_array_elements_text(i.operators)),
                   ARRAY(SELECT jsonb_array_elements_text(i.affected_stations)),
                   i.priority, i.validity_periods, i.is_planned, i.is_cleared, NOW(), NOW(),
                   ARRAY(SELECT jsonb_array_elements_text(i.affected_lines)), NOW()
              FROM UNNEST($1::text[], $2::text[], $3::text[], $4::jsonb[], $5::jsonb[],
                          $6::int4[], $7::jsonb[], $8::bool[], $9::bool[], $10::jsonb[])
                   AS i(incident_id, summary, description, operators, affected_stations,
                        priority, validity_periods, is_planned, is_cleared, affected_lines)
            ON CONFLICT (incident_id) DO UPDATE SET
                summary           = EXCLUDED.summary,
                description       = EXCLUDED.description,
                operators         = EXCLUDED.operators,
                affected_stations = EXCLUDED.affected_stations,
                priority          = EXCLUDED.priority,
                validity_periods  = EXCLUDED.validity_periods,
                is_planned        = EXCLUDED.is_planned,
                is_cleared        = EXCLUDED.is_cleared,
                fetched_at        = NOW(),
                affected_lines    = EXCLUDED.affected_lines,
                source_missing_polls = 0,
                source_removed_at = NULL,
                -- The rail-day cutoff's anchor (2026-10-06,
                -- `20261006130000_incidents_active_since.sql`): re-armed by
                -- a reopen (cleared -> uncleared) or by new text while
                -- uncleared. `incidents.*` here is the row as it was.
                active_since      = CASE
                    WHEN EXCLUDED.is_cleared THEN incidents.active_since
                    WHEN incidents.is_cleared
                      OR (incidents.summary, incidents.description)
                         IS DISTINCT FROM (EXCLUDED.summary, EXCLUDED.description)
                    THEN NOW()
                    ELSE incidents.active_since
                END
            WHERE (incidents.summary, incidents.description, incidents.operators,
                   incidents.affected_stations, incidents.priority,
                   incidents.validity_periods, incidents.is_planned,
                   incidents.is_cleared, incidents.affected_lines)
                  IS DISTINCT FROM
                  (EXCLUDED.summary, EXCLUDED.description, EXCLUDED.operators,
                   EXCLUDED.affected_stations, EXCLUDED.priority,
                   EXCLUDED.validity_periods, EXCLUDED.is_planned,
                   EXCLUDED.is_cleared, EXCLUDED.affected_lines)
            ",
        )
        .bind(&ids)
        .bind(&summaries)
        .bind(&descriptions)
        .bind(&operators)
        .bind(&stations)
        .bind(&priorities)
        .bind(&validities)
        .bind(&planned)
        .bind(&cleared)
        .bind(&lines)
        .execute(&mut *tx)
        .await?;

        if !changed_rows.is_empty() {
            let h_ids: Vec<&str> = changed_rows
                .iter()
                .map(|(i, _)| i.incident_id.as_str())
                .collect();
            let h_summaries: Vec<&str> = changed_rows
                .iter()
                .map(|(i, _)| i.summary.as_str())
                .collect();
            let h_descriptions: Vec<&str> = changed_rows
                .iter()
                .map(|(i, _)| i.description.as_str())
                .collect();
            let h_operators: Vec<serde_json::Value> = changed_rows
                .iter()
                .map(|(i, _)| json_array(&i.operators))
                .collect();
            let h_stations: Vec<serde_json::Value> = changed_rows
                .iter()
                .map(|(i, _)| json_array(&i.affected_stations))
                .collect();
            let h_priorities: Vec<i32> = changed_rows.iter().map(|(i, _)| i.priority).collect();
            let h_validities: Vec<&serde_json::Value> =
                changed_rows.iter().map(|(_, v)| *v).collect();
            let h_planned: Vec<bool> = changed_rows.iter().map(|(i, _)| i.is_planned).collect();
            let h_cleared: Vec<bool> = changed_rows.iter().map(|(i, _)| i.is_cleared).collect();
            sqlx::query(
                r"
                INSERT INTO incident_history (
                    incident_id, summary, description, operators, affected_stations,
                    priority, validity_periods, is_planned, is_cleared
                )
                SELECT h.incident_id, h.summary, h.description,
                       ARRAY(SELECT jsonb_array_elements_text(h.operators)),
                       ARRAY(SELECT jsonb_array_elements_text(h.affected_stations)),
                       h.priority, h.validity_periods, h.is_planned, h.is_cleared
                  FROM UNNEST($1::text[], $2::text[], $3::text[], $4::jsonb[], $5::jsonb[],
                              $6::int4[], $7::jsonb[], $8::bool[], $9::bool[])
                       WITH ORDINALITY
                       AS h(incident_id, summary, description, operators, affected_stations,
                            priority, validity_periods, is_planned, is_cleared, ord)
                 ORDER BY h.ord
                ",
            )
            .bind(&h_ids)
            .bind(&h_summaries)
            .bind(&h_descriptions)
            .bind(&h_operators)
            .bind(&h_stations)
            .bind(&h_priorities)
            .bind(&h_validities)
            .bind(&h_planned)
            .bind(&h_cleared)
            .execute(&mut *tx)
            .await?;
        }
        count += chunk.len() as u64;

        // `fetched_at` is shown per incident ("Last updated from National
        // Rail") so it must still advance for every incident in the feed,
        // changed or not. The upsert above skips an unchanged row entirely;
        // with the heartbeat on, this bumps ONLY `fetched_at` on those, in
        // one statement. Updating just that unindexed column is a HOT update
        // that reuses the row's TOASTed text/array values and touches none
        // of the GIN indexes, instead of the full-row rewrite every incident
        // got every cycle. Rows the upsert just wrote already hold this
        // transaction's NOW(). Being listed also un-ends an incident
        // (`source_missing_polls`, `source_removed_at`, see `removal`); two
        // more unindexed columns keep this a HOT update.
        //
        // With the heartbeat off, only the rows coming back (or cleared:
        // module docs) are touched; the others take the feed's time
        // (`record_snapshot_times` below, read through `FETCHED_AT_SQL`).
        let bump = match heartbeat {
            RowHeartbeat::On => {
                "UPDATE incidents SET fetched_at = NOW(), source_missing_polls = 0, \
                        source_removed_at = NULL \
                 WHERE incident_id = ANY($1) AND fetched_at <> NOW()"
            }
            RowHeartbeat::Off => {
                "UPDATE incidents SET fetched_at = NOW(), source_missing_polls = 0, \
                        source_removed_at = NULL \
                 WHERE incident_id = ANY($1) AND fetched_at <> NOW() \
                   AND (is_cleared OR source_missing_polls <> 0 \
                        OR source_removed_at IS NOT NULL)"
            }
        };
        sqlx::query(bump).bind(&chunk_ids).execute(&mut *tx).await?;
        if !chunk.is_empty() {
            record_ingest(&mut tx, "incidents", None).await?;
        }

        tx.commit().await?;
    }

    // Every chunk committed: the snapshot is written, so its time becomes
    // the feed's. An empty snapshot lists nothing, so it dates nothing.
    if !incidents.is_empty() {
        record_snapshot_times(pool, heartbeat).await?;
    }

    Ok(AppliedSnapshot {
        upserted: count,
        text_changed_ids,
    })
}

/// `incident_feed_state`'s snapshot times, once per applied non-empty
/// snapshot (plan 2c.6). Heartbeat off: `previous := last; last := now()`.
/// Heartbeat on: `previous := last; last := NULL`, so [`FETCHED_AT_SQL`] is
/// exactly the per-row time, and from the second heartbeat-on snapshot on
/// both are NULL (and this writes nothing). The one-snapshot `previous`
/// lets the first heartbeat-on snapshot after a heartbeat-off one stamp a
/// first miss correctly (see [`removal`]'s module docs).
///
/// An UPDATE only: the row is created by the first complete snapshot's
/// inference ([`removal::infer_removals`], which sets `last_snapshot_at`
/// itself when it creates the row with the heartbeat off).
async fn record_snapshot_times(pool: &PgPool, heartbeat: RowHeartbeat) -> Result<()> {
    let sql = match heartbeat {
        RowHeartbeat::Off => {
            "UPDATE incident_feed_state \
                SET previous_snapshot_at = last_snapshot_at, last_snapshot_at = now() \
              WHERE singleton"
        }
        RowHeartbeat::On => {
            "UPDATE incident_feed_state \
                SET previous_snapshot_at = last_snapshot_at, last_snapshot_at = NULL \
              WHERE singleton \
                AND (previous_snapshot_at IS NOT NULL OR last_snapshot_at IS NOT NULL)"
        }
    };
    sqlx::query(sql).execute(pool).await?;
    Ok(())
}

/// Reads either body shape `poller-incidents` has sent as a snapshot:
///
/// - since 2026-10-06, a [`common::IncidentSnapshot`] object, whose
///   `complete` lets the "Ended (no longer listed)" inference run (see
///   [`removal`]);
/// - before that, a bare `[IncidentMessage, ...]` array. Still accepted, as
///   an INCOMPLETE snapshot, so an older poller keeps ingesting during a
///   rollout but can never make an absent incident read as ended.
///
/// Deserialized from a `Value` by hand rather than through a
/// `#[serde(untagged)]` enum, so a malformed body still gets the field-level
/// error message instead of untagged's "did not match any variant". The
/// `Err` is that message, worded like axum's own `Json` data errors; the
/// api returns it as a `422`.
pub fn parse_snapshot(body: serde_json::Value) -> Result<common::IncidentSnapshot, String> {
    let parsed = if body.is_array() {
        serde_json::from_value::<Vec<IncidentMessage>>(body).map(|incidents| {
            common::IncidentSnapshot {
                incidents,
                complete: false,
                skipped: 0,
            }
        })
    } else {
        serde_json::from_value::<common::IncidentSnapshot>(body)
    };
    parsed.map_err(|err| format!("Failed to deserialize the JSON body into the target type: {err}"))
}

#[cfg(test)]
mod parse_snapshot_tests {
    use serde_json::json;

    use super::*;

    fn incident(id: &str) -> serde_json::Value {
        json!({
            "incident_id": id,
            "summary": "s",
            "description": "d",
            "operators": [],
            "affected_stations": [],
            "priority": 1,
            "validity": [],
            "is_planned": false,
            "is_cleared": false,
        })
    }

    #[test]
    fn an_older_pollers_bare_array_is_an_incomplete_snapshot() {
        let snapshot = parse_snapshot(json!([incident("A"), incident("B")]))
            .expect("the old shape is still accepted");
        assert_eq!(snapshot.incidents.len(), 2);
        assert!(
            !snapshot.complete,
            "a bare array never vouches for completeness"
        );
    }

    #[test]
    fn a_snapshot_object_carries_its_completeness() {
        let snapshot = parse_snapshot(json!({
            "incidents": [incident("A")],
            "complete": true,
            "skipped": 0,
        }))
        .expect("parses");
        assert_eq!(snapshot.incidents.len(), 1);
        assert!(snapshot.complete);
    }

    #[test]
    fn a_snapshot_without_complete_is_not_complete() {
        let snapshot = parse_snapshot(json!({"incidents": [incident("A")]})).expect("parses");
        assert!(!snapshot.complete);
        assert_eq!(snapshot.skipped, 0);
    }

    #[test]
    fn a_malformed_body_is_an_error_naming_the_problem() {
        let message = parse_snapshot(json!({"incidents": [{"incident_id": "A"}]}))
            .expect_err("missing fields");
        assert!(message.contains("summary"), "{message}");
        assert!(parse_snapshot(json!("nope")).is_err(), "not an object");
    }
}

/// The row heartbeat switch (plan 2c.6) against a real database:
/// [`apply_snapshot`] then [`removal::infer_removals`], as both writers run
/// them. Resets `incident_feed_state`, like the api's `incident_removal` DB
/// tests, so needs `--test-threads=1`. (The publish-order test moved to the
/// api's `queries`, where the order now lives, by plan task 2c.1.)
#[cfg(test)]
mod db_tests {
    use std::collections::BTreeMap;

    use chrono::{DateTime, Utc};
    use sqlx::postgres::PgPoolOptions;

    use super::*;

    const PREFIX: &str = "TEST-HEARTBEAT-";

    async fn test_pool() -> PgPool {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres")
    }

    async fn reset(pool: &PgPool) {
        for sql in [
            "DELETE FROM incident_history WHERE incident_id LIKE 'TEST-HEARTBEAT-%'",
            "DELETE FROM incidents WHERE incident_id LIKE 'TEST-HEARTBEAT-%'",
            "DELETE FROM incident_feed_state",
        ] {
            sqlx::query(sql).execute(pool).await.expect(sql);
        }
    }

    fn incident(suffix: &str) -> IncidentMessage {
        IncidentMessage {
            incident_id: format!("{PREFIX}{suffix}"),
            summary: format!("{suffix} summary"),
            description: format!("{suffix} description"),
            operators: vec!["ZZ".to_string()],
            affected_stations: vec![],
            priority: 2,
            validity: vec![],
            is_planned: false,
            is_cleared: false,
        }
    }

    fn cleared(suffix: &str) -> IncidentMessage {
        IncidentMessage {
            is_cleared: true,
            ..incident(suffix)
        }
    }

    /// One complete snapshot as both writers run it, as if
    /// `MIN_INFERENCE_GAP_SECS` had passed since the previous one (the
    /// stored baseline is backdated first; the snapshot times are not
    /// touched).
    async fn snapshot(
        pool: &PgPool,
        batch: &[IncidentMessage],
        heartbeat: RowHeartbeat,
    ) -> removal::Inference {
        sqlx::query(
            "UPDATE incident_feed_state SET last_complete_at = now() - interval '10 minutes'",
        )
        .execute(pool)
        .await
        .expect("backdate the baseline");
        let matcher = common::matcher::LineMatcher::new(&[]);
        apply_snapshot(pool, &matcher, batch, heartbeat)
            .await
            .expect("apply snapshot");
        let present: Vec<&str> = batch.iter().map(|i| i.incident_id.as_str()).collect();
        removal::infer_removals(pool, &present, true, heartbeat)
            .await
            .expect("infer removals")
    }

    /// `incident_id -> xmin` for this test's rows: an UPDATE (HOT or not)
    /// always writes a new row version with a new `xmin`, so an unchanged
    /// `xmin` proves the row was not updated. (Exact, unlike
    /// `pg_stat_*_tables.n_tup_upd`, which the writes' own pooled
    /// transactions report with a delay.)
    async fn row_versions(pool: &PgPool) -> BTreeMap<String, String> {
        sqlx::query_as::<_, (String, String)>(
            "SELECT incident_id, xmin::text FROM incidents \
              WHERE incident_id LIKE 'TEST-HEARTBEAT-%'",
        )
        .fetch_all(pool)
        .await
        .expect("row versions")
        .into_iter()
        .collect()
    }

    async fn feed_times(pool: &PgPool) -> (Option<DateTime<Utc>>, Option<DateTime<Utc>>) {
        sqlx::query_as(
            "SELECT last_snapshot_at, previous_snapshot_at FROM incident_feed_state \
              WHERE singleton",
        )
        .fetch_one(pool)
        .await
        .expect("feed state")
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                incidents::db_tests -- --ignored --test-threads=1`"]
    async fn an_identical_repeated_snapshot_updates_no_incident_with_the_heartbeat_off() {
        let pool = test_pool().await;
        reset(&pool).await;
        let batch: Vec<IncidentMessage> = (0..=UPSERT_CHUNK_SIZE)
            .map(|n| incident(&format!("{n:03}")))
            .collect();

        // Off: a baseline, then the same snapshot again.
        snapshot(&pool, &batch, RowHeartbeat::Off).await;
        let (first_feed_time, _) = feed_times(&pool).await;
        let before = row_versions(&pool).await;
        assert_eq!(before.len(), batch.len());
        let verdict = snapshot(&pool, &batch, RowHeartbeat::Off).await;
        assert_eq!(
            verdict,
            removal::Inference::Applied {
                missing: 0,
                removed: 0
            }
        );
        assert_eq!(
            row_versions(&pool).await,
            before,
            "no incidents row updated by an identical snapshot"
        );
        let (last, previous) = feed_times(&pool).await;
        assert!(last > first_feed_time, "the feed time moved on instead");
        assert_eq!(previous, first_feed_time);

        // On (today's behaviour): every row is bumped, and the feed time is
        // cleared so the readers see exactly the per-row time; the previous
        // one lasts one more snapshot (the rollback stamp, `removal`).
        snapshot(&pool, &batch, RowHeartbeat::On).await;
        let after_on = row_versions(&pool).await;
        assert!(
            before
                .iter()
                .all(|(id, xmin)| after_on.get(id) != Some(xmin)),
            "the heartbeat updates every listed row"
        );
        assert_eq!(feed_times(&pool).await, (None, last));
        snapshot(&pool, &batch, RowHeartbeat::On).await;
        assert_eq!(feed_times(&pool).await, (None, None));
        reset(&pool).await;
    }

    /// Which snapshot (1-based) a time falls in, from the
    /// `clock_timestamp()` windows measured around each one.
    fn snapshot_index(windows: &[(DateTime<Utc>, DateTime<Utc>)], at: DateTime<Utc>) -> usize {
        windows
            .iter()
            .position(|(start, end)| *start <= at && at <= *end)
            .map_or_else(
                || panic!("{at} is in no snapshot's window: {windows:?}"),
                |index| index + 1,
            )
    }

    /// Per incident suffix: (the snapshot its display time names, the
    /// snapshot its `source_removed_at` names).
    type DisplayTimes = BTreeMap<String, (usize, Option<usize>)>;

    /// Runs the 6-snapshot sequence with `modes[k]` for snapshot k and
    /// returns what the readers show after each snapshot.
    async fn run_sequence(pool: &PgPool, modes: [RowHeartbeat; 6]) -> Vec<DisplayTimes> {
        // (incident_id, display time, fetched_at, source_removed_at)
        type Row = (String, DateTime<Utc>, DateTime<Utc>, Option<DateTime<Utc>>);
        reset(pool).await;
        let (a, b, c, d) = (incident("A"), incident("B"), incident("C"), cleared("D"));
        let mut a_edited = a.clone();
        a_edited.description = "A description, updated".to_string();
        let sequence: [Vec<IncidentMessage>; 6] = [
            vec![a.clone(), b.clone(), c.clone(), d.clone()],
            // A's text changes; everything still listed.
            vec![a_edited.clone(), b.clone(), c.clone(), d.clone()],
            // C and the cleared D disappear (C: first miss).
            vec![a_edited.clone(), b.clone()],
            // C's second miss: ended.
            vec![a_edited.clone(), b.clone()],
            // C reappears.
            vec![a_edited.clone(), b.clone(), c.clone()],
            // B disappears (first miss).
            vec![a_edited.clone(), c.clone()],
        ];
        let mut windows = Vec::new();
        let mut shown = Vec::new();
        for (batch, heartbeat) in sequence.iter().zip(modes) {
            let start: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
                .fetch_one(pool)
                .await
                .expect("clock");
            snapshot(pool, batch, heartbeat).await;
            let end: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
                .fetch_one(pool)
                .await
                .expect("clock");
            windows.push((start, end));

            let rows: Vec<Row> = sqlx::query_as(&format!(
                "SELECT incident_id, {FETCHED_AT_SQL}, fetched_at, source_removed_at \
                       FROM incidents WHERE incident_id LIKE 'TEST-HEARTBEAT-%'"
            ))
            .fetch_all(pool)
            .await
            .expect("display times");
            let mut times = DisplayTimes::new();
            for (id, display, fetched_at, removed_at) in rows {
                if heartbeat == RowHeartbeat::On {
                    assert_eq!(
                        display, fetched_at,
                        "{id}: with the heartbeat on, FETCHED_AT_SQL is exactly fetched_at"
                    );
                }
                times.insert(
                    id.trim_start_matches(PREFIX).to_string(),
                    (
                        snapshot_index(&windows, display),
                        removed_at.map(|at| snapshot_index(&windows, at)),
                    ),
                );
            }
            shown.push(times);
        }
        reset(pool).await;
        shown
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                incidents::db_tests -- --ignored --test-threads=1`"]
    async fn the_display_time_is_the_same_with_the_heartbeat_on_or_off() {
        use RowHeartbeat::{Off, On};
        let pool = test_pool().await;

        let today = run_sequence(&pool, [On; 6]).await;
        // Spot-check the baseline itself: after the last snapshot.
        let last = today.last().expect("six snapshots");
        assert_eq!(last["A"], (6, None), "listed: the latest snapshot");
        assert_eq!(last["B"], (5, None), "first miss: the last one listing it");
        assert_eq!(last["C"], (6, None), "reappeared and listed");
        assert_eq!(last["D"], (2, None), "cleared, gone after snapshot 2");
        assert_eq!(today[3]["C"], (2, Some(2)), "ended: last listed in 2");

        for modes in [
            [Off; 6],
            // Turning the heartbeat off, then (rollback) back on.
            [On, On, On, Off, Off, Off],
            [Off, Off, Off, On, On, On],
            [On, Off, On, Off, On, Off],
        ] {
            assert_eq!(
                run_sequence(&pool, modes).await,
                today,
                "modes {modes:?} show the same snapshot for every incident after every snapshot"
            );
        }
    }
}

/// `incident_changed` and `text_changed`, the history and text-changed
/// guards of [`apply_snapshot`] (moved from the api's `queries` tests).
#[cfg(test)]
mod change_detection_tests {
    use super::*;

    fn existing(summary: &str, description: &str, validity: serde_json::Value) -> ExistingIncident {
        ExistingIncident {
            incident_id: "TEST123".to_string(),
            summary: summary.to_string(),
            description: description.to_string(),
            validity_periods: validity,
            is_cleared: false,
        }
    }

    #[test]
    fn new_incident_is_always_changed() {
        assert!(incident_changed(
            None,
            "summary",
            "description",
            &serde_json::json!([]),
            false
        ));
    }

    #[test]
    fn identical_incident_is_not_changed() {
        let row = existing("summary", "description", serde_json::json!([]));
        assert!(!incident_changed(
            Some(&row),
            "summary",
            "description",
            &serde_json::json!([]),
            false
        ));
    }

    #[test]
    fn changed_summary_is_detected() {
        let row = existing("old summary", "description", serde_json::json!([]));
        assert!(incident_changed(
            Some(&row),
            "new summary",
            "description",
            &serde_json::json!([]),
            false
        ));
    }

    #[test]
    fn changed_description_is_detected() {
        let row = existing("summary", "old description", serde_json::json!([]));
        assert!(incident_changed(
            Some(&row),
            "summary",
            "new description",
            &serde_json::json!([]),
            false
        ));
    }

    #[test]
    fn changed_validity_periods_is_detected() {
        let row = existing("summary", "description", serde_json::json!([]));
        let new_validity = serde_json::json!([{"from_date": "2026-01-01T00:00:00Z", "to_date": null, "is_now": true}]);
        assert!(incident_changed(
            Some(&row),
            "summary",
            "description",
            &new_validity,
            false
        ));
    }

    /// 2026-10-06: a flag-only clear used to write no history row, so the
    /// detail page's history could never show when RDM cleared an incident.
    #[test]
    fn a_flag_only_clear_is_a_change() {
        let row = existing("summary", "description", serde_json::json!([]));
        assert!(incident_changed(
            Some(&row),
            "summary",
            "description",
            &serde_json::json!([]),
            true
        ));
    }

    #[test]
    fn unrelated_operators_or_stations_changes_are_not_this_functions_concern() {
        // operators/affected_stations/priority/is_planned changes still get
        // written to `incidents` (the upsert always overwrites), they just
        // don't independently trigger a history row per the brief's spec
        // (only summary/description/validity_periods/is_cleared do).
        let row = existing("summary", "description", serde_json::json!([]));
        assert!(!incident_changed(
            Some(&row),
            "summary",
            "description",
            &serde_json::json!([]),
            false
        ));
    }

    #[test]
    fn text_changed_true_for_a_new_incident() {
        assert!(text_changed(None, "Signal failure", "Delays expected"));
    }

    #[test]
    fn text_changed_true_when_summary_differs() {
        let row = existing("Signal failure", "Delays expected", serde_json::json!([]));
        assert!(text_changed(
            Some(&row),
            "Points failure",
            "Delays expected"
        ));
    }

    #[test]
    fn text_changed_true_when_description_differs() {
        let row = existing("Signal failure", "Delays expected", serde_json::json!([]));
        assert!(text_changed(
            Some(&row),
            "Signal failure",
            "Disruption has now ended"
        ));
    }

    #[test]
    fn text_changed_false_when_only_validity_periods_would_differ() {
        // text_changed only compares summary/description -- validity is
        // deliberately excluded, since it doesn't require re-extraction of
        // prose that hasn't moved. This test simulates that by reusing the
        // same summary/description text_changed actually looks at; there's
        // no validity parameter to vary because text_changed never takes one.
        let row = existing("Signal failure", "Delays expected", serde_json::json!([]));
        assert!(!text_changed(
            Some(&row),
            "Signal failure",
            "Delays expected"
        ));
    }
}
