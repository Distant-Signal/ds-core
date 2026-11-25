//! The aggregator's prune guard against a real Postgres, as the aggregator
//! role (DS-638): a `trains` row that a journey leg still needs (a leg whose
//! `final_snapshot_at` is NULL) must not be pruned, and the role that
//! evaluates the guard must read nothing else of `journey_legs`.
//!
//! ```text
//! DATABASE_URL=postgres://... cargo test -p ds-store --test aggregator_prune_guard \
//!     -- --ignored --test-threads=1
//! ```
//!
//! Fixtures are seeded through `MIGRATION_DATABASE_URL` (the schema owner)
//! when it is set, as CI's per-service passes provide, else `DATABASE_URL`.
//! The refusals are checked only when `DS_TEST_NARROWED_ROLES` lists
//! `aggregator` (`scripts/test-postgres-roles.py --narrow`).
//!
//! The same role also runs the loss alarm's two `bound_legs_without_train`
//! gauge queries (DS-658), which read the subscription's `service_date` and
//! `resolution_status` and nothing else of it.

#![expect(
    clippy::expect_used,
    reason = "test helpers outside #[test] fns: a panic is the failure"
)]

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const USER: &str = "TEST-DS638-GUARD-USER";
const KEPT_UID: &str = "C99638";
const SNAPSHOTTED_UID: &str = "C99639";
const UNREFERENCED_UID: &str = "C99640";

/// The prune guard as ds-workers' aggregator runs it.
const GUARD: &str = "SELECT t.train_uid FROM trains t \
     WHERE t.train_uid = ANY($1) \
       AND NOT EXISTS (SELECT 1 FROM train_subscriptions ts \
                       JOIN journey_legs jl ON jl.train_subscription_id = ts.id \
                       WHERE ts.trains_id = t.id AND jl.final_snapshot_at IS NULL) \
     ORDER BY t.train_uid";

/// The loss alarm's gauges as ds-workers' aggregator runs them: the legs
/// whose subscription is matched but has lost its train, from `$1` on, split
/// by whether the leg was snapshotted (`final_snapshot_at`).
const BOUND_LEGS_WITHOUT_TRAIN: &str = "SELECT \
       COUNT(*) FILTER (WHERE jl.final_snapshot_at IS NULL), \
       COUNT(*) FILTER (WHERE jl.final_snapshot_at IS NOT NULL) \
     FROM train_subscriptions ts \
     JOIN journey_legs jl ON jl.train_subscription_id = ts.id \
     WHERE ts.trains_id IS NULL \
       AND ts.resolution_status IN ('resolved', 'schedule_matched') \
       AND ts.service_date >= $1";

async fn pool(var: &str) -> Option<PgPool> {
    let url = std::env::var(var).ok().filter(|url| !url.is_empty())?;
    Some(
        PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("connect to postgres"),
    )
}

async fn cleanup(owner: &PgPool) {
    for sql in [
        "DELETE FROM journeys WHERE user_id = $1",
        "DELETE FROM train_subscriptions WHERE user_id = $1",
        "DELETE FROM users WHERE id = $1",
    ] {
        sqlx::query(sql)
            .bind(USER)
            .execute(owner)
            .await
            .expect("clean up fixtures");
    }
    sqlx::query("DELETE FROM trains WHERE train_uid = ANY($1) AND service_date = '2026-11-01'")
        .bind(vec![KEPT_UID, SNAPSHOTTED_UID, UNREFERENCED_UID])
        .execute(owner)
        .await
        .expect("clean up trains");
}

/// A train, optionally with a subscription on it and a leg bound to that
/// subscription, whose `final_snapshot_at` is set when `snapshotted`.
async fn seed_train(owner: &PgPool, uid: &str, leg: Option<bool>) {
    let (train,): (i64,) = sqlx::query_as(
        "INSERT INTO trains (train_uid, service_date) VALUES ($1, '2026-11-01') RETURNING id",
    )
    .bind(uid)
    .fetch_one(owner)
    .await
    .expect("seed train");
    let Some(snapshotted) = leg else { return };
    let (subscription,): (i64,) = sqlx::query_as(
        "INSERT INTO train_subscriptions (user_id, service_date, trains_id) \
         VALUES ($1, '2026-11-01', $2) RETURNING id",
    )
    .bind(USER)
    .bind(train)
    .fetch_one(owner)
    .await
    .expect("seed subscription");
    let (journey,): (i64,) =
        sqlx::query_as("INSERT INTO journeys (user_id) VALUES ($1) RETURNING id")
            .bind(USER)
            .fetch_one(owner)
            .await
            .expect("seed journey");
    sqlx::query(
        "INSERT INTO journey_legs \
            (journey_id, leg_order, service_date, train_subscription_id, \
             final_train_uid, final_snapshot, final_snapshot_at) \
         VALUES ($1, 1, '2026-11-01', $2, \
                 $3, CASE WHEN $4 THEN '{}'::jsonb END, CASE WHEN $4 THEN now() END)",
    )
    .bind(journey)
    .bind(subscription)
    .bind(snapshotted.then_some(uid))
    .bind(snapshotted)
    .execute(owner)
    .await
    .expect("seed leg");
}

async fn denied(pool: &PgPool, sql: &str) {
    let err = sqlx::query(sql)
        .execute(pool)
        .await
        .expect_err(&format!("{sql} must be refused"));
    let code = err
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .map(std::borrow::Cow::into_owned);
    assert_eq!(code.as_deref(), Some("42501"), "{sql}: {err}");
}

#[tokio::test]
#[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
            --test aggregator_prune_guard -- --ignored --test-threads=1`"]
async fn the_aggregator_runs_the_prune_guard_and_reads_nothing_else_of_legs() {
    let role = pool("DATABASE_URL")
        .await
        .expect("DATABASE_URL must be set to run this test");
    let owner = pool("MIGRATION_DATABASE_URL")
        .await
        .unwrap_or_else(|| role.clone());
    cleanup(&owner).await;
    sqlx::query("INSERT INTO users (id, name) VALUES ($1, $1)")
        .bind(USER)
        .execute(&owner)
        .await
        .expect("seed user");
    seed_train(&owner, KEPT_UID, Some(false)).await;
    seed_train(&owner, SNAPSHOTTED_UID, Some(true)).await;
    seed_train(&owner, UNREFERENCED_UID, None).await;

    let prunable: Vec<(String,)> = sqlx::query_as(GUARD)
        .bind(vec![KEPT_UID, SNAPSHOTTED_UID, UNREFERENCED_UID])
        .fetch_all(&role)
        .await
        .expect("the aggregator runs the guard");
    let prunable: Vec<&str> = prunable.iter().map(|(uid,)| uid.as_str()).collect();
    assert_eq!(
        prunable,
        [SNAPSHOTTED_UID, UNREFERENCED_UID],
        "the train a leg still needs is not prunable"
    );

    if std::env::var("DS_TEST_NARROWED_ROLES")
        .is_ok_and(|roles| roles.split(',').any(|role| role.trim() == "aggregator"))
    {
        denied(&role, "SELECT final_snapshot FROM journey_legs").await;
        denied(&role, "SELECT final_train_uid FROM journey_legs").await;
        denied(&role, "SELECT id FROM journey_legs").await;
        denied(&role, "SELECT * FROM journey_legs").await;
        denied(
            &role,
            "UPDATE journey_legs SET final_snapshot_at = now() WHERE false",
        )
        .await;
        denied(&role, "DELETE FROM journey_legs WHERE false").await;
        denied(
            &role,
            "INSERT INTO journey_legs (journey_id, leg_order, service_date) \
             SELECT 0, 1, CURRENT_DATE WHERE false",
        )
        .await;
        denied(&role, "SELECT user_id FROM train_subscriptions").await;
    }

    cleanup(&owner).await;
}

/// Seeds a leg on a matched subscription with no train, snapshotted or not.
async fn seed_trainless_leg(owner: &PgPool, status: &str, snapshotted: bool) {
    let (subscription,): (i64,) = sqlx::query_as(
        "INSERT INTO train_subscriptions (user_id, service_date, resolution_status) \
         VALUES ($1, '2026-11-23', $2) RETURNING id",
    )
    .bind(USER)
    .bind(status)
    .fetch_one(owner)
    .await
    .expect("seed subscription");
    let (journey,): (i64,) =
        sqlx::query_as("INSERT INTO journeys (user_id) VALUES ($1) RETURNING id")
            .bind(USER)
            .fetch_one(owner)
            .await
            .expect("seed journey");
    sqlx::query(
        "INSERT INTO journey_legs \
            (journey_id, leg_order, service_date, train_subscription_id, \
             final_train_uid, final_snapshot, final_snapshot_at) \
         VALUES ($1, 1, '2026-11-23', $2, \
                 $3, CASE WHEN $4 THEN '{}'::jsonb END, CASE WHEN $4 THEN now() END)",
    )
    .bind(journey)
    .bind(subscription)
    .bind(snapshotted.then_some("C99641"))
    .bind(snapshotted)
    .execute(owner)
    .await
    .expect("seed leg");
}

async fn gauges(role: &PgPool, since: &str) -> (i64, i64) {
    sqlx::query_as(BOUND_LEGS_WITHOUT_TRAIN)
        .bind(chrono::NaiveDate::parse_from_str(since, "%Y-%m-%d").expect("date"))
        .fetch_one(role)
        .await
        .expect("the aggregator runs the bound-legs gauge")
}

#[tokio::test]
#[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
            --test aggregator_prune_guard -- --ignored --test-threads=1`"]
async fn the_aggregator_runs_the_bound_legs_gauges_and_reads_no_more_of_subscriptions() {
    let role = pool("DATABASE_URL")
        .await
        .expect("DATABASE_URL must be set to run this test");
    let owner = pool("MIGRATION_DATABASE_URL")
        .await
        .unwrap_or_else(|| role.clone());
    cleanup(&owner).await;
    sqlx::query("INSERT INTO users (id, name) VALUES ($1, $1)")
        .bind(USER)
        .execute(&owner)
        .await
        .expect("seed user");
    let (unsnapshotted, snapshotted) = gauges(&role, "2026-11-22").await;

    seed_trainless_leg(&owner, "resolved", false).await;
    seed_trainless_leg(&owner, "schedule_matched", false).await;
    seed_trainless_leg(&owner, "resolved", true).await;
    // Not matched: not counted.
    seed_trainless_leg(&owner, "unresolved", false).await;

    let (u, s) = gauges(&role, "2026-11-22").await;
    assert_eq!(
        (u - unsnapshotted, s - snapshotted),
        (2, 1),
        "matched trainless legs count by snapshot state; an unmatched one does not"
    );
    let (u, s) = gauges(&role, "2026-11-24").await;
    assert!(
        (u, s) <= (unsnapshotted, snapshotted),
        "legs from before the date floor are not counted"
    );

    if std::env::var("DS_TEST_NARROWED_ROLES")
        .is_ok_and(|roles| roles.split(',').any(|role| role.trim() == "aggregator"))
    {
        for column in [
            "user_id",
            "public_id",
            "notifications_enabled",
            "pin_origin_crs",
            "unresolved_from",
            "tracked_at",
        ] {
            denied(&role, &format!("SELECT {column} FROM train_subscriptions")).await;
        }
        denied(&role, "SELECT * FROM train_subscriptions").await;
        denied(
            &role,
            "UPDATE train_subscriptions SET resolution_status = 'resolved' WHERE false",
        )
        .await;
        denied(&role, "DELETE FROM train_subscriptions WHERE false").await;
    }

    cleanup(&owner).await;
}
