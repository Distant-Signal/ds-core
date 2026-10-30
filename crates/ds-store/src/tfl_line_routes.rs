//! `tfl_line_routes` (DS-151; docs/superpowers/specs/
//! 2026-10-23-trips-plan-tfl-stop-ends-design.md §3): the ingest-writer's
//! `tfl-line-routes/1` handler replaces it from one snapshot.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use chrono::{DateTime, Utc};
use common::tfl_routes::{
    DIRECTIONS, MAX_STOPS, NAME_MAX_CHARS, TflLineRoute, is_line_id, step_seconds_fit,
};
use common::tfl_stops::is_stop_id;
use serde::Serialize;
use sqlx::PgConnection;

use crate::freshness::record_ingest;

/// What [`replace`] did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Replaced {
    /// Routes written (every valid route of the snapshot).
    pub written: u64,
    /// Rows of the previous snapshot deleted.
    pub deleted: u64,
    /// Routes refused before writing, as `line/direction`: a line id that
    /// is not a `TfL` slug, an unknown direction, a stop id that is not a
    /// `NaPTAN` id, or fewer than 2 or more than
    /// [`common::tfl_routes::MAX_STOPS`] stops.
    pub invalid: Vec<String>,
    /// The stored routes are from a newer snapshot: nothing was written.
    pub skipped_older: bool,
    /// Routes written without their running times (DS-206), as
    /// `line/direction`: `step_seconds` that do not fit the stops
    /// ([`common::tfl_routes::step_seconds_fit`]).
    pub untimed: Vec<String>,
}

/// One row as the insert's `jsonb_to_recordset` reads it.
#[derive(Serialize)]
struct Row<'a> {
    line_id: &'a str,
    direction: &'a str,
    route_index: i16,
    name: String,
    stop_ids: &'a [String],
    step_seconds: Option<&'a [Option<u32>]>,
}

fn valid(route: &TflLineRoute) -> bool {
    is_line_id(&route.line_id)
        && DIRECTIONS.contains(&route.direction.as_str())
        && (2..=MAX_STOPS).contains(&route.stop_ids.len())
        && route.stop_ids.iter().all(|id| is_stop_id(id))
}

/// Replaces `tfl_line_routes` with `routes` (a whole snapshot fetched at
/// `observed_at`) inside the caller's transaction:
///
/// - an empty snapshot writes nothing (the producer never sends one; it
///   would read as "`TfL` has no lines");
/// - a snapshot older than the stored routes is skipped
///   ([`Replaced::skipped_older`]);
/// - an invalid route is left out ([`Replaced::invalid`]), an exact repeat
///   is kept once, and the rest is written, numbered in snapshot order
///   within its line and direction;
/// - freshness `tfl-line-routes` is `observed_at`.
pub async fn replace(
    conn: &mut PgConnection,
    routes: &[TflLineRoute],
    observed_at: DateTime<Utc>,
) -> Result<Replaced> {
    let mut outcome = Replaced::default();
    if routes.is_empty() {
        return Ok(outcome);
    }
    let newest: Option<DateTime<Utc>> =
        sqlx::query_scalar("SELECT max(fetched_at) FROM tfl_line_routes")
            .fetch_one(&mut *conn)
            .await?;
    if newest.is_some_and(|newest| newest > observed_at) {
        outcome.skipped_older = true;
        return Ok(outcome);
    }

    let mut rows = Vec::with_capacity(routes.len());
    let mut next_index: BTreeMap<(&str, &str), i16> = BTreeMap::new();
    let mut seen = BTreeSet::new();
    for route in routes {
        if !valid(route) {
            outcome
                .invalid
                .push(format!("{}/{}", route.line_id, route.direction));
            continue;
        }
        if !seen.insert((&route.line_id, &route.direction, &route.stop_ids)) {
            continue;
        }
        let index = next_index
            .entry((&route.line_id, &route.direction))
            .or_default();
        let step_seconds = route.step_seconds.as_deref();
        let step_seconds = match step_seconds {
            Some(steps) if !step_seconds_fit(steps, route.stop_ids.len()) => {
                outcome
                    .untimed
                    .push(format!("{}/{}", route.line_id, route.direction));
                None
            }
            steps => steps,
        };
        rows.push(Row {
            line_id: &route.line_id,
            direction: &route.direction,
            route_index: *index,
            name: route.name.trim().chars().take(NAME_MAX_CHARS).collect(),
            stop_ids: &route.stop_ids,
            step_seconds,
        });
        *index = index.saturating_add(1);
    }
    if rows.is_empty() {
        return Ok(outcome);
    }

    outcome.deleted = sqlx::query("DELETE FROM tfl_line_routes")
        .execute(&mut *conn)
        .await?
        .rows_affected();
    outcome.written = sqlx::query(
        r"
        INSERT INTO tfl_line_routes
            (line_id, direction, route_index, name, stop_ids, step_seconds, fetched_at)
        SELECT r.line_id, r.direction, r.route_index, r.name,
               ARRAY(SELECT jsonb_array_elements_text(r.stop_ids)),
               CASE WHEN jsonb_typeof(r.step_seconds) = 'array' THEN
                   ARRAY(SELECT NULLIF(step.value, 'null'::jsonb)::INTEGER
                         FROM jsonb_array_elements(r.step_seconds) WITH ORDINALITY AS step(value, n)
                         ORDER BY step.n)
               END,
               $2
        FROM jsonb_to_recordset($1::jsonb) AS r(
            line_id TEXT, direction TEXT, route_index SMALLINT, name TEXT, stop_ids JSONB,
            step_seconds JSONB)
        ",
    )
    .bind(sqlx::types::Json(&rows))
    .bind(observed_at)
    .execute(&mut *conn)
    .await?
    .rows_affected();

    record_ingest(
        conn,
        common::tfl_routes::FRESHNESS_SOURCE,
        Some(observed_at),
    )
    .await?;
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(line: &str, direction: &str, stops: &[&str]) -> TflLineRoute {
        TflLineRoute {
            line_id: line.into(),
            direction: direction.into(),
            name: "Test".into(),
            stop_ids: stops.iter().map(|s| (*s).to_owned()).collect(),
            step_seconds: None,
        }
    }

    #[test]
    fn only_well_formed_routes_are_valid() {
        let ok = ["940GZZLUXAA", "940GZZLUXAB"];
        assert!(valid(&route("testline", "inbound", &ok)));
        assert!(!valid(&route("Test Line", "inbound", &ok)));
        assert!(!valid(&route("testline", "sideways", &ok)));
        assert!(!valid(&route("testline", "inbound", &ok[..1])));
        assert!(!valid(&route("testline", "inbound", &["940GZZLUXAA", "x"])));
    }
}
