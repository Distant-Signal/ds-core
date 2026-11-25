//! `webhook_destinations.secret_nonce` after the stage 2 migrations (DS-615,
//! docs/secret-box-aad.md): a v1 nonce (12 bytes) and a v2 nonce (13 bytes)
//! are accepted, any other length is refused, and the constraint is
//! validated. Every write runs in a transaction that is rolled back.
//!
//! ```text
//! DATABASE_URL=postgres://... cargo test -p ds-store --test webhook_secret_nonce \
//!     -- --ignored --test-threads=1
//! ```

#![expect(
    clippy::expect_used,
    reason = "test code: a panic is the right failure in a test"
)]

use sqlx::{PgPool, Postgres, Transaction};

async fn pool() -> PgPool {
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("connect to postgres")
}

/// Inserts a destination with `nonce_len` bytes of nonce (and a plausible
/// ciphertext and key id) inside a savepoint, and says whether it was accepted.
async fn accepts(tx: &mut Transaction<'_, Postgres>, url: &str, nonce_len: usize) -> bool {
    let mut sp = sqlx::Acquire::begin(&mut **tx).await.expect("savepoint");
    let result = sqlx::query(
        "INSERT INTO webhook_destinations \
             (user_id, kind, name, url, secret_ciphertext, secret_nonce, secret_key_id) \
         VALUES ('ds615-nonce-test', 'generic', 'n', $1, $2, $3, 'k1')",
    )
    .bind(url)
    .bind(vec![7_u8; 32])
    .bind(vec![9_u8; nonce_len])
    .execute(&mut *sp)
    .await;
    let ok = result.is_ok();
    if ok {
        sp.commit().await.expect("release savepoint");
    } else {
        sp.rollback().await.expect("roll back to savepoint");
    }
    ok
}

#[tokio::test]
#[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
            --test webhook_secret_nonce -- --ignored --test-threads=1`"]
async fn the_nonce_check_takes_12_and_13_bytes_and_is_validated() {
    let pool = pool().await;
    let can_write: bool = sqlx::query_scalar(
        "SELECT has_table_privilege(current_user, 'webhook_destinations', 'INSERT') \
            AND has_table_privilege(current_user, 'users', 'INSERT')",
    )
    .fetch_one(&pool)
    .await
    .expect("check the privilege");
    if !can_write {
        eprintln!("skipped: this role cannot insert users and destinations");
        return;
    }

    let validated: bool = sqlx::query_scalar(
        "SELECT convalidated FROM pg_constraint \
         WHERE conrelid = 'webhook_destinations'::regclass \
           AND conname = 'webhook_destinations_secret_nonce_check'",
    )
    .fetch_one(&pool)
    .await
    .expect("the constraint exists under its default name");
    assert!(validated, "the nonce check is validated");

    let mut tx = pool.begin().await.expect("begin");
    sqlx::query("INSERT INTO users (id) VALUES ('ds615-nonce-test')")
        .execute(&mut *tx)
        .await
        .expect("insert a user");

    assert!(accepts(&mut tx, "https://a.example/", 12).await, "v1 nonce");
    assert!(accepts(&mut tx, "https://b.example/", 13).await, "v2 nonce");
    for (i, len) in [0_usize, 1, 11, 14, 24].into_iter().enumerate() {
        assert!(
            !accepts(&mut tx, &format!("https://bad{i}.example/"), len).await,
            "a {len}-byte nonce is refused"
        );
    }

    // No secret at all is still allowed (all three columns null).
    sqlx::query(
        "INSERT INTO webhook_destinations (user_id, kind, name, url) \
         VALUES ('ds615-nonce-test', 'ntfy', 'n', 'https://none.example/')",
    )
    .execute(&mut *tx)
    .await
    .expect("a destination without a secret");

    // A 12-byte row can be rewritten to 13 bytes (the backfill's UPDATE).
    let updated = sqlx::query(
        "UPDATE webhook_destinations SET secret_nonce = $1 \
         WHERE url = 'https://a.example/' AND octet_length(secret_nonce) = 12",
    )
    .bind(vec![2_u8; 13])
    .execute(&mut *tx)
    .await
    .expect("update a v1 row to a 13-byte nonce");
    assert_eq!(updated.rows_affected(), 1);

    tx.rollback().await.expect("roll back");
}
