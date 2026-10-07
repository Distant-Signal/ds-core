//! Shared fixtures for the database-gated (`#[ignore]`) tests of this crate
//! and of the crates that use it -- DB review 2026-09-27 B1/B3, decision
//! DQ16. Built for this crate's own tests, and for other crates' tests
//! through the `test-support` feature, which only dev-dependencies turn on:
//! no binary is built with it.
//!
//! Those tests share ONE database (CI runs them against a single migrated
//! database with `--test-threads=1`; a developer runs them against a local
//! one that may also hold real reference data). The rules this module backs:
//!
//! * clean up BEFORE seeding, not only after, so a run that panicked
//!   half-way (and so never reached its trailing cleanup) cannot poison the
//!   next run;
//! * clean up again on `Drop`, so a failing assertion no longer leaks its
//!   fixtures into every later test;
//! * only ever delete synthetic data: every statement must carry a `WHERE`
//!   clause naming the test's own fixture keys (digit-bearing CRS codes,
//!   `TEST-`/fixture uid prefixes, far-future dates), never a whole table or
//!   a whole real day.
//!
//! New test modules should prefer `#[sqlx::test]` (a throwaway database per
//! test) instead; this is for the existing shared-database modules.
//!
//! Moved from the api's `test_support` and from the fixtures the api's and
//! this crate's test modules each kept a copy of (ingest architecture plan
//! 1A, unit F).

// Test fixtures: a failed seed or cleanup must fail the calling test. Not
// `expect`: under `cfg(test)` clippy's allow-expect-in-tests already
// silences these, so an `#[expect]` would be unfulfilled there.
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    reason = "test fixtures: a failed seed or cleanup must fail the calling test"
)]

use chrono::{DateTime, NaiveDate, Utc};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, PgConnection, PgPool};

/// A pool on `DATABASE_URL`, for a database-gated test.
pub async fn connect() -> PgPool {
    let database_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
    PgPoolOptions::new()
        .connect(&database_url)
        .await
        .expect("connect to postgres")
}

/// The database's own `CURRENT_DATE`, which is what every query under
/// test compares `service_date` against. Seeding from
/// `Utc::now().date_naive()` instead only agreed with it because the
/// server happened to run in UTC (DB review 2026-09-27 B4); this holds
/// whatever the server's or the session's `TimeZone` is.
pub async fn db_today(pool: &PgPool) -> NaiveDate {
    sqlx::query_scalar("SELECT CURRENT_DATE")
        .fetch_one(pool)
        .await
        .expect("read CURRENT_DATE")
}

/// Runs `sql` (a single-row, single-text-column query, typically
/// `SELECT xmin::text FROM ...`) and returns the value: an unchanged
/// `xmin` shows a no-op guard left the row physically untouched.
pub async fn xmin(pool: &PgPool, sql: &str) -> String {
    sqlx::query_scalar(sql)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|err| panic!("{sql}: {err}"))
}

/// Fixture dates must be at least this far in the future, so a date-scoped
/// delete can never touch a real published day. See [`assert_synthetic_date`].
pub const FIRST_SYNTHETIC_YEAR: i32 = 2050;

/// Panics unless `date` is a far-future fixture date. Every test helper that
/// deletes "everything on `date`" calls this first: the tables involved
/// (`schedule_destination_departures`, `schedule_calling_points_full`, ...)
/// are replaced a whole `service_date` at a time by the code under test, so
/// such a test necessarily owns its whole day -- which is only safe when
/// that day is synthetic.
pub fn assert_synthetic_date(date: NaiveDate) {
    use chrono::Datelike;
    assert!(
        date.year() >= FIRST_SYNTHETIC_YEAR,
        "refusing a date-scoped test delete on {date}: fixture dates must be in \
         {FIRST_SYNTHETIC_YEAR} or later so they can never be real data"
    );
}

/// A set of scoped `DELETE`s run once when created (before the test seeds
/// anything) and again when dropped (after the test, pass or fail).
///
/// Hold it in a named binding for the whole test (`let _cleanup = ...`, not
/// `let _ = ...`, which drops it immediately).
pub struct FixtureCleanup {
    database_url: String,
    statements: Vec<String>,
}

impl FixtureCleanup {
    /// Runs `statements` against `pool` now and remembers them for `Drop`.
    ///
    /// Each statement must be a single scoped `DELETE ... WHERE ...` with
    /// its fixture keys inlined as literals (they are test constants).
    pub async fn new<S: Into<String>>(
        pool: &PgPool,
        statements: impl IntoIterator<Item = S>,
    ) -> Self {
        let statements: Vec<String> = statements.into_iter().map(Into::into).collect();
        for statement in &statements {
            let upper = statement.to_ascii_uppercase();
            assert!(
                upper.trim_start().starts_with("DELETE FROM ") && upper.contains(" WHERE "),
                "fixture cleanup must be a scoped DELETE ... WHERE, never a whole table: \
                 {statement}"
            );
        }
        run_all(pool, &statements).await;
        Self {
            database_url: std::env::var("DATABASE_URL")
                .expect("DATABASE_URL must be set to run this test"),
            statements,
        }
    }
}

async fn run_all(pool: &PgPool, statements: &[String]) {
    for statement in statements {
        sqlx::query(statement)
            .execute(pool)
            .await
            .unwrap_or_else(|err| panic!("fixture cleanup failed: {statement}: {err}"));
    }
}

impl Drop for FixtureCleanup {
    fn drop(&mut self) {
        // `Drop` cannot await, and the test's own runtime may be the
        // current-thread flavour that is busy dropping us, so run the
        // deletes on a fresh thread with its own runtime and connection.
        let database_url = std::mem::take(&mut self.database_url);
        let statements = std::mem::take(&mut self.statements);
        let outcome = std::thread::spawn(move || -> Result<(), String> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|err| err.to_string())?;
            runtime.block_on(async move {
                let mut conn = PgConnection::connect(&database_url)
                    .await
                    .map_err(|err| err.to_string())?;
                for statement in &statements {
                    sqlx::query(statement)
                        .execute(&mut conn)
                        .await
                        .map_err(|err| format!("{statement}: {err}"))?;
                }
                conn.close().await.map_err(|err| err.to_string())
            })
        })
        .join();
        let problem = match outcome {
            Ok(Ok(())) => return,
            Ok(Err(err)) => err,
            Err(_) => "cleanup thread panicked".to_string(),
        };
        // Never panic while the test itself is already unwinding (that
        // aborts the whole test binary); the next run's up-front cleanup
        // covers anything left behind. A passing test, though, should not
        // silently leave fixtures behind.
        if std::thread::panicking() {
            eprintln!("fixture cleanup on drop failed: {problem}");
        } else {
            panic!("fixture cleanup on drop failed: {problem}");
        }
    }
}

/// A fixture `users` row (`{user_id}@example.com`), if not already there.
pub async fn seed_user(pool: &PgPool, user_id: &str) {
    sqlx::query(
        "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
    )
    .bind(user_id)
    .bind(format!("{user_id}@example.com"))
    .bind(user_id)
    .execute(pool)
    .await
    .expect("seed fixture user");
}

/// Deletes a fixture user with its tickets and train subscriptions.
pub async fn cleanup_user(pool: &PgPool, user_id: &str) {
    sqlx::query("DELETE FROM tracked_train_tickets WHERE user_id = $1")
        .bind(user_id)
        .execute(pool)
        .await
        .expect("cleanup fixture tickets");
    sqlx::query("DELETE FROM train_subscriptions WHERE user_id = $1")
        .bind(user_id)
        .execute(pool)
        .await
        .expect("cleanup fixture tracked_trains");
    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(user_id)
        .execute(pool)
        .await
        .expect("cleanup fixture user");
}

/// Minimal fixture row -- only the `NOT NULL` columns
/// (`crates/ds-store/migrations/20260828120000_train_tracking.sql:40-76`).
pub async fn seed_tracked_train(pool: &PgPool, user_id: &str) -> i64 {
    let (id,): (i64,) = sqlx::query_as(
        "INSERT INTO train_subscriptions (user_id, service_date, pin_origin_crs, pin_scheduled_departure) \
         VALUES ($1, $2, $3, $4) RETURNING id",
    )
    .bind(user_id)
    .bind("2026-09-02".parse::<NaiveDate>().unwrap())
    .bind("KGX")
    .bind("2026-09-02T09:00:00Z".parse::<DateTime<Utc>>().unwrap())
    .fetch_one(pool)
    .await
    .expect("insert fixture tracked_trains row");
    id
}

/// Fixture for `list_pending_pins_for_backlog_match`'s own tests and the
/// sweeps': a `train_subscriptions` row with every column that predicate
/// cares about under direct caller control, rather than only what
/// [`seed_tracked_train`]'s fixed-value insert offers.
pub async fn seed_backlog_candidate_pin(
    pool: &PgPool,
    user_id: &str,
    service_date: NaiveDate,
    pin_origin_crs: Option<&str>,
    pin_scheduled_departure: Option<DateTime<Utc>>,
    resolution_status: &str,
    trains_id: Option<i64>,
) -> i64 {
    let (id,): (i64,) = sqlx::query_as(
        "INSERT INTO train_subscriptions \
            (user_id, service_date, pin_origin_crs, pin_scheduled_departure, \
             resolution_status, trains_id) \
         VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
    )
    .bind(user_id)
    .bind(service_date)
    .bind(pin_origin_crs)
    .bind(pin_scheduled_departure)
    .bind(resolution_status)
    .bind(trains_id)
    .fetch_one(pool)
    .await
    .expect("insert fixture backlog-candidate row");
    id
}

/// An on-time Euston departure for subscription `tracked_train_id`, with no
/// resolution of its own (set `resolved_train_uid`/`resolved_train_id` to
/// resolve).
pub fn fixture_event(tracked_train_id: i64, dedup_key: &str) -> common::TrainMovementEventMessage {
    common::TrainMovementEventMessage {
        tracked_train_id,
        resolved_train_uid: None,
        resolved_train_id: None,
        identity_date: None,
        dedup_key: dedup_key.to_string(),
        msg_type: "0003".to_string(),
        event_type: Some("DEPARTURE".to_string()),
        loc_stanox: Some("72410".to_string()),
        loc_crs: Some("EUS".to_string()),
        planned_timestamp: Some("2026-09-05T18:15:00Z".parse().unwrap()),
        gbtt_timestamp: None,
        actual_timestamp: Some("2026-09-05T18:15:00Z".parse().unwrap()),
        variation_status: Some("ON TIME".to_string()),
        raw_body: serde_json::json!({}),
        status: "en_route".to_string(),
        last_reported_location: Some("EUS".to_string()),
        last_event_type: Some("DEPARTURE".to_string()),
        delay_minutes: Some(0),
        next_calling_point: Some("CRE".to_string()),
        eta_next: None,
        eta_source: None,
    }
}

/// One line's `schedule_line_population` holding a single schedule that
/// originates at `tiploc` at `departure` (`HH:MM`).
pub fn population_json(uid: &str, tiploc: &str, departure: &str) -> serde_json::Value {
    population_json_multi(&[(uid, tiploc, departure)])
}

/// [`population_json`]'s many-schedule sibling: ONE line's population
/// carrying several schedules, in the given order. Needed because the
/// real `Y80908` bug lives entirely INSIDE one line's population (two
/// services departing the same station in the same minute), not across
/// two lines -- see `find_schedule_match`'s own doc comment.
pub fn population_json_multi(entries: &[(&str, &str, &str)]) -> serde_json::Value {
    serde_json::Value::Array(
        entries
            .iter()
            .map(|(uid, tiploc, departure)| {
                serde_json::json!({
                    "uid": uid,
                    "calling_points": [{
                        "tiploc": tiploc,
                        "kind": "Origin",
                        "booked_arrival": null,
                        "booked_departure": departure,
                        "is_half_minute_arrival": false,
                        "is_half_minute_departure": false
                    }]
                })
            })
            .collect(),
    )
}

/// A TRUST cancellation (`0002`) or change-of-origin (`0006`) reason for
/// headcode `9TR0000Q26` on 2031-05-06, at `at` (RFC 3339).
pub fn train_reason_message(
    uid: Option<&str>,
    msg_type: &str,
    code: &str,
    at: &str,
) -> common::TrainReasonMessage {
    common::TrainReasonMessage {
        train_id: "9TR0000Q26".to_string(),
        train_uid: uid.map(str::to_string),
        service_date: "2031-05-06".parse().unwrap(),
        msg_type: msg_type.to_string(),
        reason_code: code.to_string(),
        canx_type: (msg_type == "0002").then(|| "AT ORIGIN".to_string()),
        loc_stanox: Some("87701".to_string()),
        event_at: Some(at.parse().unwrap()),
    }
}

/// A Knowledgebase station row with every optional field set.
pub fn station_reference(crs: &str, name: &str) -> common::StationReference {
    common::StationReference {
        crs: crs.to_string(),
        name: name.to_string(),
        latitude: Some(51.5),
        longitude: Some(-0.1),
        station_operator: Some("ZZ".to_string()),
        accessibility: serde_json::json!({"stepFree": true}),
    }
}

/// A full-coverage `Recent` window row for 2026-09-27 10:50-11:50 with
/// `total` trains all on time.
pub fn full_coverage_window_row(
    line_id: &str,
    computed_at: &str,
    total: u32,
) -> common::FullCoverageWindowStatsRow {
    common::FullCoverageWindowStatsRow {
        line_id: line_id.to_string(),
        window_kind: common::FullCoverageWindowKind::Recent,
        service_date: "2026-09-27".parse().unwrap(),
        window_start: "2026-09-27T10:50:00Z".parse().unwrap(),
        window_end: "2026-09-27T11:50:00Z".parse().unwrap(),
        computed_at: computed_at.parse().unwrap(),
        counts: common::FullCoverageWindowCounts {
            total,
            on_time: total,
            ..Default::default()
        },
        relevance: "full".to_string(),
        presumed_enabled: true,
        partial: false,
        feed_stale: false,
        stats_version: 2,
    }
}

/// `schedule_destination_departures` fixtures, shared by the upsert tests
/// (here, in `schedule::publish`) and the api's search tests over the same
/// rows. Each test takes its own far-future day.
pub mod destination_departures {
    use sqlx::PgPool;

    use crate::schedule::{
        ScheduleDestinationDeparturesRow, upsert_schedule_destination_departures,
    };

    /// A distinct, far-future fixture date per test: far-future so a
    /// whole-day delete can never touch real data, distinct so no two tests
    /// share a day.
    pub fn fixture_date(day: u32) -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(2099, 1, day).expect("valid fixture date")
    }

    pub fn time(h: u32, m: u32) -> chrono::NaiveTime {
        chrono::NaiveTime::from_hms_opt(h, m, 0).expect("valid fixture time")
    }

    /// Whole-day, because the upsert under test replaces whole days -- so
    /// only ever on a synthetic 2050+ fixture date (asserted).
    pub async fn delete_day(pool: &PgPool, service_date: chrono::NaiveDate) {
        super::assert_synthetic_date(service_date);
        sqlx::query("DELETE FROM schedule_destination_departures WHERE service_date = $1")
            .bind(service_date)
            .execute(pool)
            .await
            .expect("cleanup fixture schedule_destination_departures rows");
    }

    pub fn row(
        service_date: chrono::NaiveDate,
        destination_crs: &str,
        scheduled: chrono::NaiveTime,
        train_uid: &str,
        origin_crs: &str,
        true_origin_crs: Option<&str>,
        destination_arrival: Option<chrono::NaiveTime>,
    ) -> ScheduleDestinationDeparturesRow {
        row_with_calling_point_arrival(
            service_date,
            destination_crs,
            scheduled,
            train_uid,
            origin_crs,
            true_origin_crs,
            destination_arrival,
            None,
        )
    }

    /// `row`'s sibling for the tests that actually need to control
    /// `calling_point_arrival` -- kept as a separate function rather than
    /// adding an 8th positional argument to `row` itself, so every existing
    /// `row(...)` call site (which is about something else entirely) does
    /// not need to grow a trailing `None`.
    #[expect(
        clippy::too_many_arguments,
        reason = "test fixture: one positional argument per column it seeds"
    )]
    pub fn row_with_calling_point_arrival(
        service_date: chrono::NaiveDate,
        destination_crs: &str,
        scheduled: chrono::NaiveTime,
        train_uid: &str,
        origin_crs: &str,
        true_origin_crs: Option<&str>,
        destination_arrival: Option<chrono::NaiveTime>,
        calling_point_arrival: Option<chrono::NaiveTime>,
    ) -> ScheduleDestinationDeparturesRow {
        ScheduleDestinationDeparturesRow {
            service_date,
            destination_crs: destination_crs.to_string(),
            scheduled,
            day_offset: 0,
            train_uid: train_uid.to_string(),
            origin_crs: origin_crs.to_string(),
            destination_arrival,
            destination_arrival_day_offset: 0,
            true_origin_crs: true_origin_crs.map(str::to_string),
            calling_point_arrival,
            operator_atoc: None,
            headcode: None,
            rsid: None,
            ..Default::default()
        }
    }

    /// Three trains to ZRD from two origins at three times -- enough to
    /// discriminate the origin filter, the time bounds and the ordering
    /// independently. The flat-shape equivalent of the original plan's
    /// single three-element JSONB bucket.
    pub fn fixture_rows(service_date: chrono::NaiveDate) -> Vec<ScheduleDestinationDeparturesRow> {
        vec![
            row(
                service_date,
                "ZRD",
                time(8, 22),
                "C10001",
                "EUS",
                Some("PAD"),
                None,
            ),
            row(
                service_date,
                "ZRD",
                time(10, 5),
                "C10002",
                "CRE",
                Some("SWA"),
                None,
            ),
            row(
                service_date,
                "ZRD",
                time(18, 40),
                "C10003",
                "EUS",
                Some("PAD"),
                None,
            ),
        ]
    }

    pub async fn seed(pool: &PgPool, service_date: chrono::NaiveDate) {
        delete_day(pool, service_date).await;
        upsert_schedule_destination_departures(pool, &fixture_rows(service_date))
            .await
            .expect("seed fixture rows");
    }
}
