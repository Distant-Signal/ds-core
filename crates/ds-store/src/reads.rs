//! The internal readers (ingest architecture spec R2 and §11, plan 4.2):
//! what full-coverage-consumer, trust-consumer and poller-ldbws read
//! straight from Postgres once their `*_SOURCE` is `db`, instead of the
//! api's `GET /private/{schedule-line-population,tracked-trains,
//! sample-stations,stanox-crs}`.
//!
//! Each reader connects as a read-only role (`full_coverage_ro`,
//! `trust_consumer`, `ldbws_ro` in `files/db-grants.yaml`) that can read
//! exactly what its functions here touch:
//!
//! | Reader | Function | Reads |
//! |---|---|---|
//! | full-coverage-consumer | [`list_population_versions`], [`get_schedule_line_population_conditional`] | `schedule_line_population` |
//! | full-coverage-consumer, trust-consumer | [`list_stanox_crs`] | `stanox_crs` (plus `tiploc_crs`, `corpus_stanox_crs` with the CORPUS fallback) |
//! | trust-consumer | [`list_active_tracked_trains`] | the view `ingest_active_tracked_trains` |
//! | poller-ldbws | [`count_sample_station_pins`], [`list_custom_line_stations`], [`sample_stations::select_sample_stations`] | the views `ingest_sample_station_pins`, `ingest_custom_line_stations` |
//!
//! The views (migration `20261009144000_ingest_read_views.sql`) read
//! personal tables but expose no user id or owner, so the two reader roles
//! that need them never see whose pin, subscription or custom line a row is.

use std::collections::HashMap;

use anyhow::Result;
use chrono::{DateTime, NaiveDate, Utc};
use common::{LineDefinition, TrackedTrainRef};
use sqlx::PgPool;

pub mod sample_stations;

pub use crate::reference::{list_stanox_crs, list_stanox_crs_with};
pub use crate::schedule::{ConditionalPopulation, get_schedule_line_population_conditional};

/// One published `(line_id, service_date)` population and its version.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct PopulationVersion {
    pub line_id: String,
    pub service_date: NaiveDate,
    /// The row's `updated_at`, which the publish only moves when the
    /// population changed: the same version the api's `ETag` carries.
    pub updated_at: DateTime<Utc>,
}

/// The version of every published population among `line_ids` x `dates`,
/// in one query (spec §11.1, step 1): full-coverage-consumer's direct
/// replacement for one conditional GET per pair. A pair with no row is
/// simply absent (nothing published yet). Fetch the changed pairs with
/// [`get_schedule_line_population_conditional`], passing the version held,
/// so a publish between the two queries degrades to "not modified" or a
/// fresh body, never a torn read.
pub async fn list_population_versions(
    pool: &PgPool,
    line_ids: &[String],
    dates: &[NaiveDate],
) -> Result<Vec<PopulationVersion>> {
    if line_ids.is_empty() || dates.is_empty() {
        return Ok(Vec::new());
    }
    Ok(sqlx::query_as::<_, PopulationVersion>(
        "SELECT line_id, service_date, updated_at FROM schedule_line_population \
         WHERE line_id = ANY($1) AND service_date = ANY($2) \
         ORDER BY line_id, service_date",
    )
    .bind(line_ids)
    .bind(dates)
    .fetch_all(pool)
    .await?)
}

/// The tag a reader holds for a population version: exactly the api's
/// `ETag` for it (`"slp-<updated_at in µs>"`, `routes/ingest.rs`'s
/// `population_etag`), so full-coverage-consumer keeps one kind of
/// validator whichever source it reads from.
pub fn population_version_tag(updated_at: DateTime<Utc>) -> String {
    format!("\"slp-{}\"", updated_at.timestamp_micros())
}

/// The version a [`population_version_tag`] (or the api's `ETag`, weak or
/// not) names; `None` for anything else.
pub fn parse_population_version_tag(tag: &str) -> Option<DateTime<Utc>> {
    let tag = tag.trim();
    let tag = tag.strip_prefix("W/").unwrap_or(tag);
    let micros = tag
        .strip_prefix('"')?
        .strip_suffix('"')?
        .strip_prefix("slp-")?
        .parse::<i64>()
        .ok()?;
    DateTime::from_timestamp_micros(micros)
}

/// trust-consumer's reference set, from the view
/// `ingest_active_tracked_trains`: the same rows as
/// [`crate::tracking::list_active_tracked_trains`] (the api's
/// `GET /private/tracked-trains`), whose SELECT the view carries unchanged.
pub async fn list_active_tracked_trains(pool: &PgPool) -> Result<Vec<TrackedTrainRef>> {
    let rows = sqlx::query_as::<_, crate::tracking::TrackedTrainRow>(
        "SELECT id, service_date, pin_origin_crs, pin_scheduled_departure, \
                resolution_status, train_uid, train_id, trains_id, destination_crs \
         FROM ingest_active_tracked_trains",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(TrackedTrainRef::from).collect())
}

/// How many users pinned each line (only lines with a pin appear), from
/// the view `ingest_sample_station_pins`: the api's
/// `preferences::count_pins_per_line`, as counts only.
pub async fn count_sample_station_pins(pool: &PgPool) -> Result<HashMap<String, i64>> {
    let rows: Vec<(String, i64)> =
        sqlx::query_as("SELECT line_id, pins FROM ingest_sample_station_pins")
            .fetch_all(pool)
            .await?;
    Ok(rows.into_iter().collect())
}

/// One custom line's id and stations, from the view
/// `ingest_custom_line_stations` (no owner, no name).
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct CustomLineStations {
    pub id: String,
    /// Ordered CRS codes: every one is also a sample station
    /// (`common::CustomLine::stations`).
    pub stations: Vec<String>,
}

impl From<CustomLineStations> for LineDefinition {
    /// The line as the api's `GET /private/sample-stations` sees it
    /// (`LineDefinition::from(CustomLine)`): only `id` and the sample
    /// stations matter to [`sample_stations::select_sample_stations`]; the
    /// view carries no name, so the id stands in for it.
    fn from(line: CustomLineStations) -> Self {
        LineDefinition::from(common::CustomLine {
            name: line.id.clone(),
            id: line.id,
            operators: Vec::new(),
            stations: line.stations,
            headcode_prefixes: Vec::new(),
            destination_crs_filter: Vec::new(),
        })
    }
}

/// Every custom line's id and stations, from the view
/// `ingest_custom_line_stations`, ordered by id.
pub async fn list_custom_line_stations(pool: &PgPool) -> Result<Vec<CustomLineStations>> {
    Ok(sqlx::query_as::<_, CustomLineStations>(
        "SELECT id, stations FROM ingest_custom_line_stations ORDER BY id",
    )
    .fetch_all(pool)
    .await?)
}

/// What `GET /private/sample-stations` answers for `selection`, computed
/// by the reader itself (spec §11.2): `catalogue` (the line catalogue in
/// the reader's image, `LINES_DIR`) plus the custom lines, narrowed by
/// [`sample_stations::select_sample_stations`]. The pin counts are read
/// only when `selection` needs them, as the api does.
pub async fn select_sample_stations_from(
    pool: &PgPool,
    catalogue: &[LineDefinition],
    selection: sample_stations::SampleSelection,
) -> Result<Vec<String>> {
    let mut lines: Vec<LineDefinition> = catalogue.to_vec();
    lines.extend(
        list_custom_line_stations(pool)
            .await?
            .into_iter()
            .map(LineDefinition::from),
    );
    let pin_counts = if selection.is_unrestricted() {
        HashMap::new()
    } else {
        count_sample_station_pins(pool).await?
    };
    Ok(sample_stations::select_sample_stations(
        &lines,
        &pin_counts,
        selection,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_population_version_tag_round_trips_and_matches_the_api_etag() {
        let at = DateTime::from_timestamp_micros(1_790_000_000_123_456).unwrap();
        let tag = population_version_tag(at);
        assert_eq!(tag, "\"slp-1790000000123456\"");
        assert_eq!(parse_population_version_tag(&tag), Some(at));
        assert_eq!(parse_population_version_tag(&format!("W/{tag}")), Some(at));
        for other in ["*", "\"abc\"", "slp-1", "\"slp-x\"", ""] {
            assert_eq!(parse_population_version_tag(other), None, "{other}");
        }
    }
}

/// Database-gated: each test gets its own throwaway database
/// (`#[sqlx::test]`, migrated from `crates/ds-store/migrations`). The
/// reader-role tests create a NOLOGIN role for the duration of the test
/// (roles are cluster-wide, so each has a unique name and is dropped at
/// the end) and `SET ROLE` to it, so the `DATABASE_URL` role must be able
/// to create roles and databases (CI's superuser).
#[cfg(test)]
mod db_tests {
    use super::*;

    const VIEWS: [&str; 3] = [
        "ingest_active_tracked_trains",
        "ingest_sample_station_pins",
        "ingest_custom_line_stations",
    ];

    async fn seed_user(pool: &PgPool, id: &str) {
        sqlx::query("INSERT INTO users (id) VALUES ($1)")
            .bind(id)
            .execute(pool)
            .await
            .unwrap();
    }

    /// A reader role holding exactly `grants` (`GRANT SELECT ON <each>`),
    /// unique to this test. Dropped by [`drop_role`].
    async fn reader_role(pool: &PgPool, grants: &[&str]) -> String {
        let role = format!(
            "ds_reads_test_{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        );
        sqlx::query(&format!("CREATE ROLE {role} NOLOGIN"))
            .execute(pool)
            .await
            .unwrap();
        for object in grants {
            sqlx::query(&format!("GRANT SELECT ON {object} TO {role}"))
                .execute(pool)
                .await
                .unwrap();
        }
        role
    }

    async fn drop_role(pool: &PgPool, role: &str) {
        sqlx::query(&format!("DROP OWNED BY {role}"))
            .execute(pool)
            .await
            .unwrap();
        sqlx::query(&format!("DROP ROLE {role}"))
            .execute(pool)
            .await
            .unwrap();
    }

    /// A pool whose every connection runs as `role` (`SET ROLE` on
    /// connect), over the same database as `pool`.
    async fn pool_as(pool: &PgPool, role: &str) -> PgPool {
        let role = role.to_string();
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .after_connect(move |conn, _| {
                let role = role.clone();
                Box::pin(async move {
                    sqlx::query(&format!("SET ROLE {role}"))
                        .execute(&mut *conn)
                        .await?;
                    Ok(())
                })
            })
            .connect_with((*pool.connect_options()).clone())
            .await
            .unwrap()
    }

    /// Spec §16 phase 4: the views expose no user id or owner column.
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn the_views_expose_no_user_or_owner_column(pool: PgPool) {
        for view in VIEWS {
            let columns: Vec<String> = sqlx::query_scalar(
                "SELECT column_name::text FROM information_schema.columns \
                 WHERE table_schema = 'public' AND table_name = $1 ORDER BY ordinal_position",
            )
            .bind(view)
            .fetch_all(&pool)
            .await
            .unwrap();
            assert!(!columns.is_empty(), "{view} exists");
            for column in &columns {
                assert!(
                    !["user_id", "owner", "owner_id", "email", "name"].contains(&column.as_str()),
                    "{view} exposes {column}"
                );
            }
        }
    }

    /// The pin-count and custom-line views give what the api's
    /// `preferences::count_pins_per_line` and `custom_lines::list_custom_lines`
    /// queries give, and the reader role reads them with SELECT on the
    /// views alone, never the personal tables.
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a superuser: it creates a role)"]
    async fn the_ldbws_views_match_the_api_queries_as_the_reader_role(pool: PgPool) {
        seed_user(&pool, "u-reads-1").await;
        seed_user(&pool, "u-reads-2").await;
        for (user, line) in [
            ("u-reads-1", "wcml"),
            ("u-reads-2", "wcml"),
            ("u-reads-1", "anglia"),
        ] {
            sqlx::query("INSERT INTO pinned_lines (user_id, line_id) VALUES ($1, $2)")
                .bind(user)
                .bind(line)
                .execute(&pool)
                .await
                .unwrap();
        }
        for (id, user, stations) in [
            ("custom-commute", "u-reads-1", vec!["WOK", "WAT"]),
            ("custom-coast", "u-reads-2", vec!["BTN"]),
        ] {
            sqlx::query(
                "INSERT INTO custom_lines (id, name, operators, stations, user_id) \
                 VALUES ($1, $1, '{}', $2, $3)",
            )
            .bind(id)
            .bind(&stations)
            .bind(user)
            .execute(&pool)
            .await
            .unwrap();
        }

        // The api's own queries, as the owner.
        let api_pins: HashMap<String, i64> = sqlx::query_as::<_, (String, i64)>(
            "SELECT line_id, COUNT(*) AS pins FROM pinned_lines GROUP BY line_id",
        )
        .fetch_all(&pool)
        .await
        .unwrap()
        .into_iter()
        .collect();
        let mut api_custom: Vec<(String, Vec<String>)> =
            sqlx::query_as("SELECT id, stations FROM custom_lines ORDER BY created_at")
                .fetch_all(&pool)
                .await
                .unwrap();
        api_custom.sort();

        let role = reader_role(
            &pool,
            &["ingest_sample_station_pins", "ingest_custom_line_stations"],
        )
        .await;
        let reader = pool_as(&pool, &role).await;
        assert_eq!(count_sample_station_pins(&reader).await.unwrap(), api_pins);
        let custom: Vec<(String, Vec<String>)> = list_custom_line_stations(&reader)
            .await
            .unwrap()
            .into_iter()
            .map(|line| (line.id, line.stations))
            .collect();
        assert_eq!(custom, api_custom);
        let denied = sqlx::query("SELECT 1 FROM pinned_lines")
            .execute(&reader)
            .await
            .unwrap_err();
        assert_eq!(
            denied.as_database_error().and_then(|e| e.code()).as_deref(),
            Some("42501"),
            "the reader role must not read pinned_lines itself"
        );

        // The whole selection: the catalogue plus the custom lines, capped
        // with the pins, as GET /private/sample-stations computes it.
        let catalogue = vec![LineDefinition::from(common::CustomLine {
            id: "wcml".to_string(),
            name: "wcml".to_string(),
            operators: Vec::new(),
            stations: vec!["EUS".to_string(), "MKC".to_string()],
            headcode_prefixes: Vec::new(),
            destination_crs_filter: Vec::new(),
        })];
        let all = select_sample_stations_from(
            &reader,
            &catalogue,
            sample_stations::SampleSelection::default(),
        )
        .await
        .unwrap();
        assert_eq!(all, vec!["BTN", "EUS", "MKC", "WAT", "WOK"]);
        let pinned_only = select_sample_stations_from(
            &reader,
            &catalogue,
            sample_stations::SampleSelection {
                pinned_lines_only: true,
                max_stations: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(pinned_only, vec!["EUS", "MKC"]);
        reader.close().await;
        drop_role(&pool, &role).await;
    }

    /// The tracked-trains view returns what `tracking::list_active_tracked_trains`
    /// (the api's `GET /private/tracked-trains`) returns, read as a role
    /// with SELECT on the view only.
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a superuser: it creates a role)"]
    async fn the_tracked_trains_view_matches_the_api_query_as_the_reader_role(pool: PgPool) {
        seed_user(&pool, "u-reads-3").await;
        let today: NaiveDate = sqlx::query_scalar("SELECT CURRENT_DATE")
            .fetch_one(&pool)
            .await
            .unwrap();
        let trains_id: i64 = sqlx::query_scalar(
            "INSERT INTO trains (train_uid, service_date, train_id, destination_crs) \
             VALUES ('Y12345', $1, '1A23', 'EDB') RETURNING id",
        )
        .bind(today)
        .fetch_one(&pool)
        .await
        .unwrap();
        for (date, status, trains) in [
            (today, "resolved", Some(trains_id)),
            (today, "pending", None),
            (today, "unresolved", None),
            (
                today - chrono::Duration::days(5),
                "resolved",
                Some(trains_id),
            ),
        ] {
            sqlx::query(
                "INSERT INTO train_subscriptions \
                     (user_id, service_date, pin_origin_crs, resolution_status, trains_id) \
                 VALUES ('u-reads-3', $1, 'KGX', $2, $3)",
            )
            .bind(date)
            .bind(status)
            .bind(trains)
            .execute(&pool)
            .await
            .unwrap();
        }

        let mut expected = crate::tracking::list_active_tracked_trains(&pool)
            .await
            .unwrap();
        expected.sort_by_key(|r| r.id);
        assert_eq!(expected.len(), 2, "the resolved and the pending one");

        let role = reader_role(&pool, &["ingest_active_tracked_trains"]).await;
        let reader = pool_as(&pool, &role).await;
        let mut got = list_active_tracked_trains(&reader).await.unwrap();
        got.sort_by_key(|r| r.id);
        assert_eq!(
            serde_json::to_value(&got).unwrap(),
            serde_json::to_value(&expected).unwrap()
        );
        assert!(
            sqlx::query("SELECT 1 FROM train_subscriptions")
                .execute(&reader)
                .await
                .is_err(),
            "the reader role must not read train_subscriptions itself"
        );
        reader.close().await;
        drop_role(&pool, &role).await;
    }

    /// `list_population_versions` lists exactly the published pairs asked
    /// for, with the version the conditional fetch compares; a changed
    /// population moves only its own pair's version.
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a superuser: it creates a role)"]
    async fn population_versions_list_the_published_pairs_as_the_reader_role(pool: PgPool) {
        let day: NaiveDate = "2050-01-10".parse().unwrap();
        let next = day + chrono::Duration::days(1);
        for (line, date) in [
            ("wcml", day),
            ("wcml", next),
            ("anglia", day),
            ("other", day),
        ] {
            crate::schedule::upsert_schedule_line_population(
                &pool,
                line,
                date,
                r#"[{"uid":"C1"}]"#,
            )
            .await
            .unwrap();
        }
        let role = reader_role(&pool, &["schedule_line_population"]).await;
        let reader = pool_as(&pool, &role).await;
        let lines = vec!["wcml".to_string(), "anglia".to_string()];
        let before = list_population_versions(&reader, &lines, &[day, next])
            .await
            .unwrap();
        let keys: Vec<(&str, NaiveDate)> = before
            .iter()
            .map(|v| (v.line_id.as_str(), v.service_date))
            .collect();
        assert_eq!(keys, vec![("anglia", day), ("wcml", day), ("wcml", next)]);
        assert!(
            list_population_versions(&reader, &[], &[day])
                .await
                .unwrap()
                .is_empty()
        );

        // A held version that still matches reads nothing; a stale one
        // gets the body.
        let wcml_day = before[1].updated_at;
        assert_eq!(
            get_schedule_line_population_conditional(&reader, "wcml", day, false, &[wcml_day])
                .await
                .unwrap(),
            Some(ConditionalPopulation::NotModified {
                updated_at: wcml_day
            })
        );

        crate::schedule::upsert_schedule_line_population(&pool, "wcml", day, r#"[{"uid":"C2"}]"#)
            .await
            .unwrap();
        let after = list_population_versions(&reader, &lines, &[day, next])
            .await
            .unwrap();
        let moved: Vec<&str> = before
            .iter()
            .zip(&after)
            .filter(|(b, a)| b.updated_at != a.updated_at)
            .map(|(b, _)| b.line_id.as_str())
            .collect();
        assert_eq!(moved, vec!["wcml"], "only the republished pair moved");
        assert!(matches!(
            get_schedule_line_population_conditional(&reader, "wcml", day, false, &[wcml_day])
                .await
                .unwrap(),
            Some(ConditionalPopulation::Modified { .. })
        ));
        reader.close().await;
        drop_role(&pool, &role).await;
    }
}
