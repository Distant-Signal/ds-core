//! `train_service_labels` (DS-513) against a migrated Postgres: the class
//! check, the cascade with `trains`, and that no role that serves users can
//! read it. The grants half runs only where the roles exist (the roles
//! pass); it skips otherwise.
//!
//! ```text
//! DATABASE_URL=postgres://... cargo test -p ds-store --test train_service_labels \
//!     -- --ignored --test-threads=1
//! ```

#![expect(
    clippy::expect_used,
    reason = "test helpers outside #[test] fns: a panic is the failure"
)]

use sqlx::PgPool;

async fn pool() -> PgPool {
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("connect to postgres")
}

async fn train(pool: &PgPool, uid: &str) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO trains (train_uid, service_date) VALUES ($1, '2099-06-06') RETURNING id",
    )
    .bind(uid)
    .fetch_one(pool)
    .await
    .expect("insert a train")
}

#[tokio::test]
#[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
            --test train_service_labels -- --ignored --test-threads=1`"]
async fn a_label_takes_a_known_class_and_goes_with_its_train() {
    let pool = pool().await;
    let can_write: bool = sqlx::query_scalar(
        "SELECT has_table_privilege(current_user, 'train_service_labels', 'INSERT')",
    )
    .fetch_one(&pool)
    .await
    .expect("check the privilege");
    if !can_write {
        eprintln!("skipped: this role cannot write labels (only trust_backlog and the owner can)");
        return;
    }
    let id = train(&pool, "DS513CASCADE").await;
    sqlx::query(
        "INSERT INTO train_service_labels (trains_id, service_class, toc_id) \
         VALUES ($1, 'freight', '55')",
    )
    .bind(id)
    .execute(&pool)
    .await
    .expect("a known class and a toc_id are accepted");

    let refused = sqlx::query(
        "INSERT INTO train_service_labels (trains_id, service_class) VALUES ($1, 'express')",
    )
    .bind(id)
    .execute(&pool)
    .await;
    assert!(
        refused.is_err(),
        "an unknown class (or a second row) is refused"
    );

    let too_long =
        sqlx::query("UPDATE train_service_labels SET toc_id = '12345' WHERE trains_id = $1")
            .bind(id)
            .execute(&pool)
            .await;
    assert!(
        too_long.is_err(),
        "a toc_id over four characters is refused"
    );

    sqlx::query("DELETE FROM trains WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await
        .expect("delete the train");
    let left: i64 =
        sqlx::query_scalar("SELECT count(*) FROM train_service_labels WHERE trains_id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .expect("count labels");
    assert_eq!(left, 0, "the label is deleted with its train");
}

#[tokio::test]
#[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
            --test train_service_labels -- --ignored --test-threads=1`"]
async fn only_the_label_writer_can_reach_the_table() {
    // `scripts/test-postgres-roles.py --mode per-service` gives every role's
    // login as DATABASE_URL_<ROLE>; with none set there is nothing to prove.
    let roles: Vec<(String, String)> = std::env::vars()
        .filter(|(name, _)| name.starts_with("DATABASE_URL_"))
        .collect();
    if roles.is_empty() {
        eprintln!("skipped: no DATABASE_URL_<ROLE> (not the per-service roles pass)");
        return;
    }
    for (name, url) in roles {
        // The per-service pass gives these four table-wide DML (`--observe`),
        // for suites that seed fixtures through them: nothing to prove there.
        // The generated grants test below covers them.
        if ["API", "AGGREGATOR", "ENRICHER", "NOTIFIER"]
            .iter()
            .any(|observed| name == format!("DATABASE_URL_{observed}"))
        {
            continue;
        }
        let role_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("connect as the role");
        let read = sqlx::query("SELECT trains_id, service_class, toc_id FROM train_service_labels")
            .fetch_all(&role_pool)
            .await;
        let read_trains_id_only = sqlx::query("SELECT trains_id FROM train_service_labels")
            .fetch_all(&role_pool)
            .await;
        assert!(read.is_err(), "{name} must not read toc_id");
        if name == "DATABASE_URL_TRUST_BACKLOG" {
            assert!(
                read_trains_id_only.is_ok(),
                "the label writer reads the conflict target"
            );
            let update = sqlx::query("UPDATE train_service_labels SET toc_id = NULL")
                .execute(&role_pool)
                .await;
            assert!(update.is_err(), "the label writer never updates");
        } else {
            assert!(
                read_trains_id_only.is_err(),
                "{name} must not read train_service_labels at all"
            );
        }
    }
}

/// No database needed: the rendered grants give the table to
/// `trust_backlog` alone, INSERT and a SELECT on the conflict target only.
#[test]
fn the_rendered_grants_give_the_table_to_the_label_writer_alone() {
    let grants = include_str!("../../../access/postgres-grants.sql");
    let rows: Vec<&str> = grants
        .lines()
        .filter(|line| line.contains("('train_service_labels',"))
        .collect();
    assert!(!rows.is_empty(), "the table is classified and granted");
    for row in &rows {
        assert!(
            row.contains("'trust_backlog'"),
            "only trust_backlog is granted the table: {row}"
        );
        assert!(
            row.contains("'INSERT'") || (row.contains("'SELECT'") && row.contains("trains_id")),
            "INSERT, or SELECT on trains_id only: {row}"
        );
    }
}
