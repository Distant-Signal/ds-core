//! The incident snapshot: `upsert_incident_snapshot` and its helpers,
//! `incident_removal.rs` (whole, as [`removal`]) and `parse_snapshot`
//! (today's `incident_snapshot_from_body`). Redis stays out: the upsert
//! takes a publish callback, and the api passes `publish_text_changed`.
//!
//! Moved here from `api/src/data/queries.rs` by plan task 1A.8.

pub mod removal;

use std::collections::HashMap;
use std::future::Future;

use anyhow::Result;
use common::IncidentMessage;
use sqlx::PgPool;

use crate::freshness::{last_per_key, record_ingest};

/// Incidents are upserted in chunks of this size, each as its own
/// transaction, rather than one transaction for the whole poll batch --
/// see the `upsert_incidents` doc comment for why.
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

/// Pure diff check, factored out of `upsert_incidents` so it's testable
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
/// whether `upsert_incidents` publishes a `text-changed` event.
pub fn text_changed(existing: Option<&ExistingIncident>, summary: &str, description: &str) -> bool {
    match existing {
        None => true,
        Some(row) => row.summary != summary || row.description != description,
    }
}

/// Upserts a batch of Knowledgebase incidents. Each incident is inserted or
/// updated in `incidents`; if the stored `summary/description/validity_periods`
/// differ from what's incoming (or the incident is new), a snapshot is also
/// appended to `incident_history`.
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
/// Treats the batch as an INCOMPLETE snapshot: it resets the "no longer
/// listed" state of every incident it names, but never infers that an
/// absent one has left the feed. See [`upsert_incident_snapshot`], which
/// also documents `publish_text_changed`.
pub async fn upsert_incidents<F, Fut>(
    pool: &PgPool,
    line_matcher: &common::matcher::LineMatcher,
    incidents: &[IncidentMessage],
    publish_text_changed: F,
) -> Result<u64>
where
    F: FnOnce(Vec<String>) -> Fut,
    Fut: Future<Output = ()>,
{
    let outcome =
        upsert_incident_snapshot(pool, line_matcher, incidents, false, publish_text_changed)
            .await?;
    Ok(outcome.upserted)
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

/// What [`upsert_incident_snapshot`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IncidentSnapshotOutcome {
    pub upserted: u64,
    pub inference: removal::Inference,
}

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

/// [`upsert_incidents`], then -- once every chunk has committed -- the
/// "Ended (no longer listed)" inference over the incidents this snapshot
/// did NOT name, when `complete` (the poller vouches that it is the whole
/// feed) and the rest of the guard in [`removal`]
/// passes. A chunk failure returns before inference runs, so a partly
/// written snapshot never marks anything removed.
///
/// Every incident the batch names gets `source_missing_polls = 0` and
/// `source_removed_at = NULL`, complete snapshot or not: being listed is
/// positive evidence on its own, so a reappearing incident is un-ended at
/// once.
///
/// `publish_text_changed` gets the ids whose summary or description changed
/// (or that are new), once, only if there are any, after every chunk has
/// committed and before the inference runs. The api passes its Redis
/// `XADD incident-text-changed` publisher; it must be best effort (log, do
/// not fail), because a publish failure must not fail the ingest. Redis
/// stays out of this crate (spec §5.1).
#[expect(
    clippy::too_many_lines,
    reason = "long but linear; splitting it would scatter its shared state across helpers"
)]
pub async fn upsert_incident_snapshot<F, Fut>(
    pool: &PgPool,
    line_matcher: &common::matcher::LineMatcher,
    incidents: &[IncidentMessage],
    complete: bool,
    publish_text_changed: F,
) -> Result<IncidentSnapshotOutcome>
where
    F: FnOnce(Vec<String>) -> Fut,
    Fut: Future<Output = ()>,
{
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
        // this bumps ONLY `fetched_at` on those, in one statement. Updating
        // just that unindexed column is a HOT update that reuses the row's
        // TOASTed text/array values and touches none of the GIN indexes,
        // instead of the full-row rewrite every incident got every cycle.
        // Rows the upsert just wrote already hold this transaction's NOW().
        // Being listed also un-ends an incident (`source_missing_polls`,
        // `source_removed_at`, see `removal`); two more
        // unindexed columns keep this a HOT update.
        sqlx::query(
            "UPDATE incidents SET fetched_at = NOW(), source_missing_polls = 0, \
                    source_removed_at = NULL \
             WHERE incident_id = ANY($1) AND fetched_at <> NOW()",
        )
        .bind(&chunk_ids)
        .execute(&mut *tx)
        .await?;
        if !chunk.is_empty() {
            record_ingest(&mut tx, "incidents").await?;
        }

        tx.commit().await?;
    }

    // Publish only after commit: a publish before commit could announce an
    // incident that a later failure in this same batch rolls back. Publish
    // failure is logged, not propagated -- the hourly sweep (Task 5) is the
    // backstop for a missed publish, so ingestion must not fail because
    // Redis is briefly unavailable. Before the inference below, so an
    // inference failure (a 500, and a retried POST that finds no text
    // change left to publish) cannot drop these.
    if !text_changed_ids.is_empty() {
        publish_text_changed(text_changed_ids).await;
    }

    // Every chunk committed: only now is the snapshot fully written, and
    // only a fully written snapshot may say what is absent from it.
    let present_ids: Vec<&str> = incidents.iter().map(|i| i.incident_id.as_str()).collect();
    let inference = removal::infer_removals(pool, &present_ids, complete).await?;
    Ok(IncidentSnapshotOutcome {
        upserted: count,
        inference,
    })
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

/// The publish-order contract of [`upsert_incident_snapshot`] (plan task
/// 1A.8), against a real database: the callback runs once, after every
/// chunk has committed (another connection already sees the rows) and
/// before the removal inference (no feed-state baseline yet), and is not
/// called when no text changed. Resets `incident_feed_state`, like the
/// api's `incident_removal` DB tests, so needs `--test-threads=1`.
#[cfg(test)]
mod db_tests {
    use std::sync::Mutex;

    use sqlx::postgres::PgPoolOptions;

    use super::*;

    const PREFIX: &str = "TEST-PUBLISH-ORDER-";

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
            "DELETE FROM incident_history WHERE incident_id LIKE 'TEST-PUBLISH-ORDER-%'",
            "DELETE FROM incidents WHERE incident_id LIKE 'TEST-PUBLISH-ORDER-%'",
            "DELETE FROM incident_feed_state",
        ] {
            sqlx::query(sql).execute(pool).await.expect(sql);
        }
    }

    fn incident(suffix: &str, summary: &str) -> IncidentMessage {
        IncidentMessage {
            incident_id: format!("{PREFIX}{suffix}"),
            summary: summary.to_string(),
            description: format!("{suffix} description"),
            operators: vec!["ZZ".to_string()],
            affected_stations: vec![],
            priority: 2,
            validity: vec![],
            is_planned: false,
            is_cleared: false,
        }
    }

    /// What the callback saw when it ran.
    #[derive(Debug, PartialEq, Eq)]
    struct AtPublish {
        ids: Vec<String>,
        /// This test's rows visible to ANOTHER connection: committed ones.
        committed_rows: i64,
        /// `incident_feed_state` rows: 0 until the inference has run.
        feed_state_rows: i64,
    }

    async fn feed_state_rows(pool: &PgPool) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM incident_feed_state")
            .fetch_one(pool)
            .await
            .expect("count feed state")
    }

    /// One complete snapshot; returns what the callback saw, if it ran.
    async fn snapshot(
        pool: &PgPool,
        batch: &[IncidentMessage],
    ) -> (Option<AtPublish>, IncidentSnapshotOutcome) {
        let seen: Mutex<Vec<AtPublish>> = Mutex::new(Vec::new());
        let matcher = common::matcher::LineMatcher::new(&[]);
        let outcome = upsert_incident_snapshot(pool, &matcher, batch, true, |ids| async {
            let committed_rows: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM incidents WHERE incident_id LIKE 'TEST-PUBLISH-ORDER-%'",
            )
            .fetch_one(pool)
            .await
            .expect("count committed rows");
            let feed_state_rows = feed_state_rows(pool).await;
            seen.lock().expect("not poisoned").push(AtPublish {
                ids,
                committed_rows,
                feed_state_rows,
            });
        })
        .await
        .expect("upsert snapshot");
        let mut seen = seen.into_inner().expect("not poisoned");
        assert!(
            seen.len() <= 1,
            "published at most once per snapshot: {seen:?}"
        );
        (seen.pop(), outcome)
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                incidents::db_tests -- --ignored --test-threads=1`"]
    async fn text_changes_are_published_after_commit_and_before_inference() {
        let pool = test_pool().await;
        reset(&pool).await;

        // More than one chunk, so "after commit" means after the LAST chunk.
        let batch: Vec<IncidentMessage> = (0..=UPSERT_CHUNK_SIZE)
            .map(|n| incident(&format!("{n:03}"), "Signal failure"))
            .collect();
        let (published, outcome) = snapshot(&pool, &batch).await;
        let expected_ids: Vec<String> = batch.iter().map(|i| i.incident_id.clone()).collect();
        assert_eq!(
            published,
            Some(AtPublish {
                ids: expected_ids,
                committed_rows: i64::try_from(batch.len()).expect("small"),
                feed_state_rows: 0,
            }),
            "every chunk committed, inference not yet run"
        );
        assert_eq!(outcome.inference, removal::Inference::NoBaseline);
        assert_eq!(
            feed_state_rows(&pool).await,
            1,
            "the inference ran after the publish"
        );

        // Unchanged text: no publish at all.
        let (published, _) = snapshot(&pool, &batch).await;
        assert_eq!(published, None);

        // One summary edited: only that id.
        let mut edited = batch.clone();
        edited[1].summary = "Signal failure (updated)".to_string();
        let (published, _) = snapshot(&pool, &edited).await;
        assert_eq!(
            published.map(|at| at.ids),
            Some(vec![edited[1].incident_id.clone()])
        );
        reset(&pool).await;
    }
}

/// `incident_changed` and `text_changed`, the history and text-changed
/// guards of [`upsert_incidents`] (moved from the api's `queries` tests).
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
