// crates/api/src/data/notifier_forward_queue.rs
//! Write side of the notifier-forwarding queue (Task 17). Read/poll side
//! lives in `crates/notifier` (Task 18).

use common::TrainForwardSignalMessage;
use sqlx::PgPool;

pub async fn insert_forward_signals(
    pool: &PgPool,
    signals: &[TrainForwardSignalMessage],
) -> anyhow::Result<u64> {
    let mut inserted = 0u64;
    for signal in signals {
        sqlx::query(
            "INSERT INTO notifier_forward_queue (trains_id, event_summary) VALUES ($1, $2)",
        )
        .bind(signal.trains_id)
        .bind(&signal.event_summary)
        .execute(pool)
        .await?;
        inserted += 1;
    }
    Ok(inserted)
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

    /// A real `trains` row to satisfy `notifier_forward_queue.trains_id`'s
    /// `REFERENCES trains(id) ON DELETE CASCADE` foreign key -- deleting it
    /// at the end of a test also cleans up any queue rows this test wrote
    /// (cascade), same posture as `trust_event_backlog.rs`'s own db_tests
    /// cleaning up by deleting the `trains` row they created.
    async fn fixture_train(pool: &PgPool, train_uid: &str) -> i64 {
        crate::data::trains::find_or_create_train(pool, train_uid, "2026-09-06".parse().unwrap())
            .await
            .expect("find_or_create_train")
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                insert_forward_signals_inserts_one_row_per_signal -- --ignored`"]
    async fn insert_forward_signals_inserts_one_row_per_signal() {
        let pool = connect().await;
        let trains_id = fixture_train(&pool, "TEST-FORWARD-QUEUE-UID-1").await;

        let signals = vec![
            TrainForwardSignalMessage {
                trains_id,
                event_summary: "en_route at WAT".to_string(),
            },
            TrainForwardSignalMessage {
                trains_id,
                event_summary: "en_route at CLJ".to_string(),
            },
        ];

        let inserted = insert_forward_signals(&pool, &signals)
            .await
            .expect("insert_forward_signals");
        assert_eq!(inserted, 2);

        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM notifier_forward_queue WHERE trains_id = $1")
                .bind(trains_id)
                .fetch_one(&pool)
                .await
                .expect("count");
        assert_eq!(count, 2);

        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                insert_forward_signals_with_an_empty_slice_inserts_nothing -- --ignored`"]
    async fn insert_forward_signals_with_an_empty_slice_inserts_nothing() {
        let pool = connect().await;

        let inserted = insert_forward_signals(&pool, &[])
            .await
            .expect("insert_forward_signals");
        assert_eq!(inserted, 0);
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                insert_forward_signals_called_twice_inserts_a_row_each_time_no_dedup \
                -- --ignored --test-threads=1`"]
    async fn insert_forward_signals_called_twice_inserts_a_row_each_time_no_dedup() {
        // Same real function called twice against non-reset state (not a
        // duplicated inline copy, no reset in between): this table has no
        // dedup_key -- it's a forwarding signal, not an at-least-once ingest
        // log -- so, unlike train_movement_events/trust_event_backlog, two
        // calls MUST produce two rows, not one. Proves insert_forward_signals
        // is a plain append, never accidentally deduped.
        let pool = connect().await;
        let trains_id = fixture_train(&pool, "TEST-FORWARD-QUEUE-UID-2").await;
        let signals = vec![TrainForwardSignalMessage {
            trains_id,
            event_summary: "en_route at WAT".to_string(),
        }];

        insert_forward_signals(&pool, &signals)
            .await
            .expect("first insert_forward_signals");
        insert_forward_signals(&pool, &signals)
            .await
            .expect("second insert_forward_signals, same signal, non-reset state");

        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM notifier_forward_queue WHERE trains_id = $1")
                .bind(trains_id)
                .fetch_one(&pool)
                .await
                .expect("count");
        assert_eq!(
            count, 2,
            "no dedup_key on this table -- two calls append two rows"
        );

        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
    }
}
