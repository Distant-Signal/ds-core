//! Schedule-first resolution of a tracked-train pin's `train_uid`, per
//! docs/superpowers/specs/2026-09-05-schedule-first-train-tracking-design.md.
//! Attempted once at pin-creation time (`routes::train::post_track`) and
//! again periodically for every still-`pending` row
//! (`run_schedule_match_sweep`, `main.rs`'s new background loop) -- both
//! paths funnel through `attempt_schedule_match`, the only place this
//! crate ever calls `schedule_query::match_pin`.

use std::collections::HashMap;

use chrono::{DateTime, NaiveDate, Utc};
use common::LineDefinition;
use schedule_query::LinePopulationEntry;
use serde::Serialize;
use sqlx::PgPool;

use crate::data::eta_blend::london_to_utc;
use crate::data::{queries, train_tracking};

/// `CRS -> Vec<line_id>` (Decision 2 of the design spec), built from the
/// static `lines/*.toml` catalogue -- mirrors
/// `crates/schedule-reference/src/main.rs`'s own `lines_to_publish`
/// predicate exactly (a line qualifies if it has at least one
/// `tiploc`-bearing station), then further filters to only the
/// TIPLOC-bearing stations themselves, since a station with no TIPLOC has
/// no way to ever appear in a CIF calling-point list anyway. Built once
/// at `AppState::init` from `app.config.lines` (already loaded there for
/// `full_coverage_enabled_for`'s own use -- this is a pure re-keying of
/// data already in memory, no new I/O).
pub fn crs_to_line_ids(lines: &[LineDefinition]) -> HashMap<String, Vec<String>> {
    let mut index: HashMap<String, Vec<String>> = HashMap::new();
    for line in lines {
        if !line.stations.iter().any(|s| s.tiploc.is_some()) {
            continue;
        }
        for station in &line.stations {
            if station.tiploc.is_none() {
                continue;
            }
            let crs = station.crs.to_uppercase();
            let ids = index.entry(crs).or_default();
            if !ids.contains(&line.id) {
                ids.push(line.id.clone());
            }
        }
    }
    index
}

/// camelCase wire shape for one calling point, converted from
/// `schedule_query::CallingPoint` (whose own JSON keys are snake_case --
/// see this task's own note) BEFORE storage, so `schedule_calling_points`
/// is stored already camelCase and the read path can relay it verbatim.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ScheduleCallingPointDto {
    tiploc: String,
    kind: schedule_query::CallingPointKind,
    booked_arrival: Option<chrono::NaiveTime>,
    booked_departure: Option<chrono::NaiveTime>,
    is_half_minute_arrival: bool,
    is_half_minute_departure: bool,
}

impl From<&schedule_query::CallingPoint> for ScheduleCallingPointDto {
    fn from(cp: &schedule_query::CallingPoint) -> Self {
        Self {
            tiploc: cp.tiploc.clone(),
            kind: cp.kind,
            booked_arrival: cp.booked_arrival,
            booked_departure: cp.booked_departure,
            is_half_minute_arrival: cp.is_half_minute_arrival,
            is_half_minute_departure: cp.is_half_minute_departure,
        }
    }
}

/// One pin's schedule-match attempt (Decision 3 steps 1-5), called both
/// synchronously at creation (`routes::train::post_track`, Task 7) and
/// periodically for every still-`pending` row (`run_schedule_match_sweep`,
/// Task 8). Iterates `crs_line_index`'s candidate lines for
/// `pin_origin_crs` IN A FIXED ORDER and returns on the FIRST candidate
/// line whose own population yields any match at all (this plan's Open
/// Question 3 resolution: trusts that a second candidate line, if any,
/// would resolve the same UID/date identically, so there is nothing to
/// gain from fetching every candidate and reconciling).
///
/// Returns `Ok(true)` only if a match was found AND actually written
/// (i.e. the row was still eligible -- see `apply_schedule_match`'s own
/// guard). `Ok(false)` covers every other honest "still pending" outcome
/// uniformly: no candidate line, no `stanox_crs` rows for this CRS, no
/// `schedule_line_population` published yet for any candidate, or no
/// calling point within tolerance.
pub async fn attempt_schedule_match(
    pool: &PgPool,
    tracked_train_id: i64,
    pin_origin_crs: &str,
    pin_scheduled_departure: DateTime<Utc>,
    service_date: NaiveDate,
    crs_line_index: &HashMap<String, Vec<String>>,
) -> anyhow::Result<bool> {
    let Some(candidate_lines) = crs_line_index.get(&pin_origin_crs.to_uppercase()) else {
        return Ok(false);
    };

    let origin_tiplocs = queries::list_stanox_crs_for_crs(pool, pin_origin_crs).await?;
    if origin_tiplocs.is_empty() {
        return Ok(false);
    }
    let tiplocs: Vec<&str> = origin_tiplocs.iter().map(|r| r.tiploc.as_str()).collect();

    for line_id in candidate_lines {
        let Some(json) = queries::get_schedule_line_population(pool, line_id, service_date).await?
        else {
            continue;
        };
        let entries: Vec<LinePopulationEntry> = serde_json::from_value(json)?;

        let Some(matched) = schedule_query::match_pin(
            &entries,
            &tiplocs,
            pin_scheduled_departure,
            common::MATCH_TOLERANCE,
            |t| london_to_utc(service_date.and_time(t)),
        ) else {
            continue;
        };

        let calling_points: Vec<ScheduleCallingPointDto> = matched
            .calling_points
            .iter()
            .map(ScheduleCallingPointDto::from)
            .collect();
        let calling_points_json = serde_json::to_value(&calling_points)?;

        let destination_crs = match matched.calling_points.last() {
            Some(cp) => {
                queries::crs_for_tiploc(pool, schedule_query::normalize_tiploc(&cp.tiploc)).await?
            }
            None => None,
        };

        return train_tracking::apply_schedule_match(
            pool,
            tracked_train_id,
            &matched.uid,
            line_id,
            &calling_points_json,
            destination_crs.as_deref(),
        )
        .await;
    }

    Ok(false)
}

/// The periodic sweep's own entry point (Decision 3's "also run this same
/// attempt periodically"): re-runs `attempt_schedule_match` against every
/// still-`pending`, never-matched row. A single row's failure (e.g. a
/// malformed `schedule_line_population` JSONB for one line) is logged and
/// skipped, not propagated -- one bad row must never stop the sweep from
/// making progress on every other row. Returns the count of rows this
/// call actually matched, for the caller's own logging.
pub async fn run_schedule_match_sweep(
    pool: &PgPool,
    crs_line_index: &HashMap<String, Vec<String>>,
) -> anyhow::Result<u64> {
    let rows = train_tracking::list_pending_pins_for_schedule_match(pool).await?;
    let mut matched = 0u64;
    for row in rows {
        match attempt_schedule_match(
            pool,
            row.id,
            &row.pin_origin_crs,
            row.pin_scheduled_departure,
            row.service_date,
            crs_line_index,
        )
        .await
        {
            Ok(true) => matched += 1,
            Ok(false) => {}
            Err(err) => {
                tracing::warn!(
                    error = ?err,
                    tracked_train_id = row.id,
                    "schedule match attempt failed for this pin; will retry next sweep"
                );
            }
        }
    }
    Ok(matched)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(id: &str, stations: Vec<(&str, Option<&str>)>) -> LineDefinition {
        LineDefinition {
            id: id.to_string(),
            name: id.to_string(),
            mode: "rail".to_string(),
            category: "national-rail".to_string(),
            operators: vec![],
            stations: stations
                .into_iter()
                .map(|(crs, tiploc)| common::Station {
                    crs: crs.to_string(),
                    tiploc: tiploc.map(str::to_string),
                    role: "minor".to_string(),
                    segment: None,
                })
                .collect(),
            sample_stations: vec![],
            match_keywords: vec![],
            excluded_keywords: vec![],
            severity_overrides: std::collections::HashMap::new(),
            exclusive_segments: vec![],
            destination_crs_filter: vec![],
            headcode_prefixes: vec![],
            full_coverage_enabled: false,
        }
    }

    #[test]
    fn a_tiploc_bearing_station_maps_its_crs_to_its_line() {
        let lines = vec![line("wcml", vec![("EUS", Some("EUSTON"))])];
        let index = crs_to_line_ids(&lines);
        assert_eq!(index.get("EUS"), Some(&vec!["wcml".to_string()]));
    }

    #[test]
    fn a_crs_with_no_tiploc_on_its_station_entry_is_not_indexed() {
        let lines = vec![line("wcml", vec![("EUS", Some("EUSTON")), ("ZZZ", None)])];
        let index = crs_to_line_ids(&lines);
        assert_eq!(index.get("ZZZ"), None);
    }

    #[test]
    fn a_crs_shared_by_two_lines_maps_to_both() {
        let lines = vec![
            line("line-a", vec![("EUS", Some("EUSTON"))]),
            line("line-b", vec![("EUS", Some("EUSTON"))]),
        ];
        let index = crs_to_line_ids(&lines);
        let mut ids = index.get("EUS").cloned().unwrap_or_default();
        ids.sort();
        assert_eq!(ids, vec!["line-a".to_string(), "line-b".to_string()]);
    }

    #[test]
    fn a_line_with_no_tiploc_bearing_station_at_all_is_excluded_entirely() {
        let lines = vec![line("no-tiploc-line", vec![("ZZA", None), ("ZZB", None)])];
        let index = crs_to_line_ids(&lines);
        assert!(index.is_empty());
    }

    #[test]
    fn a_lowercase_crs_on_a_station_entry_is_indexed_uppercased() {
        let lines = vec![line("wcml", vec![("eus", Some("EUSTON"))])];
        let index = crs_to_line_ids(&lines);
        assert_eq!(index.get("EUS"), Some(&vec!["wcml".to_string()]));
    }
}

#[cfg(test)]
mod db_tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    async fn connect() -> PgPool {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres")
    }

    fn population_json(uid: &str, tiploc: &str, departure: &str) -> serde_json::Value {
        serde_json::json!([{
            "uid": uid,
            "calling_points": [{
                "tiploc": tiploc,
                "kind": "Origin",
                "booked_arrival": null,
                "booked_departure": departure,
                "is_half_minute_arrival": false,
                "is_half_minute_departure": false
            }]
        }])
    }

    #[tokio::test]
    #[ignore = "requires a live database; see this plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p api \
                attempt_schedule_match -- --ignored --test-threads=1`"]
    async fn attempt_schedule_match_reproduces_the_eus_bug_and_now_resolves_it() {
        let pool = connect().await;
        let user_id = "TEST-SCHEDULE-MATCH-EUS";
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind("schedule-match@example.com")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");

        sqlx::query(
            "INSERT INTO stanox_crs (stanox, crs, tiploc, station_name, source_sequence) \
             VALUES ('TEST-EUS-STANOX', 'EUS', 'EUSTON', 'LONDON EUSTON', 1) \
             ON CONFLICT (stanox) DO NOTHING",
        )
        .execute(&pool)
        .await
        .expect("seed stanox_crs");

        let service_date: chrono::NaiveDate = "2026-09-05".parse().unwrap();
        sqlx::query(
            "INSERT INTO schedule_line_population (line_id, service_date, population) \
             VALUES ('west-coast-main-line', $1, $2) \
             ON CONFLICT (line_id, service_date) DO UPDATE SET population = EXCLUDED.population",
        )
        .bind(service_date)
        .bind(population_json("C99999", "EUSTON ", "19:15"))
        .execute(&pool)
        .await
        .expect("seed schedule_line_population");

        // The exact reported bug: a pin created more than an hour after
        // its train's own origin-departure window (the pin's own
        // scheduled_departure is still 19:15 -- what changes is that no
        // live TRUST Movement for it will ever arrive within this
        // process's test window, exactly mirroring "pinned an hour late,
        // TRUST's own ±20-minute window already closed").
        let scheduled_departure: chrono::DateTime<chrono::Utc> =
            "2026-09-05T19:15:00+01:00".parse().unwrap(); // BST -> 18:15 UTC
        let (tracked_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO tracked_trains (user_id, service_date, pin_origin_crs, pin_scheduled_departure) \
             VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(user_id)
        .bind(service_date)
        .bind("EUS")
        .bind(scheduled_departure)
        .fetch_one(&pool)
        .await
        .expect("seed fixture tracked_trains row");

        let mut crs_line_index = HashMap::new();
        crs_line_index.insert("EUS".to_string(), vec!["west-coast-main-line".to_string()]);

        let matched = attempt_schedule_match(
            &pool,
            tracked_train_id,
            "EUS",
            scheduled_departure,
            service_date,
            &crs_line_index,
        )
        .await
        .expect("attempt schedule match");
        assert!(matched, "the pin should schedule-match against C99999");

        let state = train_tracking::get_by_tracking_id(&pool, tracked_train_id)
            .await
            .expect("read tracked train")
            .expect("tracked train exists");
        assert_eq!(state.resolution_status, "schedule_matched");
        assert_eq!(state.train_uid, Some("C99999".to_string()));
        assert_eq!(state.train_id, None, "train_id must stay TRUST-exclusive");

        sqlx::query("DELETE FROM schedule_line_population WHERE line_id = 'west-coast-main-line' AND service_date = $1")
            .bind(service_date)
            .execute(&pool)
            .await
            .expect("cleanup population");
        sqlx::query("DELETE FROM stanox_crs WHERE stanox = 'TEST-EUS-STANOX'")
            .execute(&pool)
            .await
            .expect("cleanup stanox_crs");
        sqlx::query("DELETE FROM tracked_trains WHERE user_id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("cleanup tracked_trains");
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("cleanup user");
    }

    #[tokio::test]
    #[ignore = "requires a live database; see this plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p api \
                attempt_schedule_match -- --ignored --test-threads=1`"]
    async fn attempt_schedule_match_with_no_candidate_line_leaves_the_row_pending() {
        let pool = connect().await;
        let user_id = "TEST-SCHEDULE-MATCH-NO-CANDIDATE";
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind("no-candidate@example.com")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");

        let service_date: chrono::NaiveDate = "2026-09-05".parse().unwrap();
        let (tracked_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO tracked_trains (user_id, service_date, pin_origin_crs, pin_scheduled_departure) \
             VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(user_id)
        .bind(service_date)
        .bind("ZZZ")
        .bind("2026-09-05T19:15:00Z".parse::<chrono::DateTime<chrono::Utc>>().unwrap())
        .fetch_one(&pool)
        .await
        .expect("seed fixture tracked_trains row");

        let matched = attempt_schedule_match(
            &pool,
            tracked_train_id,
            "ZZZ",
            "2026-09-05T19:15:00Z".parse().unwrap(),
            service_date,
            &HashMap::new(), // no candidate lines at all
        )
        .await
        .expect("attempt schedule match");
        assert!(!matched);

        let state = train_tracking::get_by_tracking_id(&pool, tracked_train_id)
            .await
            .expect("read tracked train")
            .expect("tracked train exists");
        assert_eq!(state.resolution_status, "pending");
        assert_eq!(state.train_uid, None);

        sqlx::query("DELETE FROM tracked_trains WHERE user_id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("cleanup tracked_trains");
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("cleanup user");
    }
}
