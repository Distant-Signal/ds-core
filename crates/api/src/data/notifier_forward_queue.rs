// crates/api/src/data/notifier_forward_queue.rs
//! Write side of the notifier-forwarding queue (Task 17). Read/poll side
//! lives in `crates/notifier` (Task 18).

use common::TrainForwardSignalMessage;
use sqlx::PgPool;

/// Appends every signal in ONE statement (DB2-29): all rows or none. The
/// old per-row autocommit loop could commit part of a batch and then 500,
/// so the consumer's retry re-inserted the committed prefix. Rows get their
/// `id`s in input order (`WITH ORDINALITY ... ORDER BY`), which is the order
/// the notifier polls them in.
pub async fn insert_forward_signals(
    pool: &PgPool,
    signals: &[TrainForwardSignalMessage],
) -> anyhow::Result<u64> {
    if signals.is_empty() {
        return Ok(0);
    }
    let trains_ids: Vec<i64> = signals.iter().map(|s| s.trains_id).collect();
    let summaries: Vec<&str> = signals.iter().map(|s| s.event_summary.as_str()).collect();
    let result = sqlx::query(
        "INSERT INTO notifier_forward_queue (trains_id, event_summary) \
         SELECT trains_id, event_summary \
           FROM UNNEST($1::bigint[], $2::text[]) WITH ORDINALITY AS s(trains_id, event_summary, ord) \
          ORDER BY ord",
    )
    .bind(&trains_ids)
    .bind(&summaries)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
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
    /// (cascade), same posture as `trust_event_backlog.rs`'s own `db_tests`
    /// cleaning up by deleting the `trains` row they created.
    async fn fixture_train(pool: &PgPool, train_uid: &str) -> i64 {
        crate::data::trains::find_or_create_train(pool, train_uid, "2026-09-06".parse().unwrap())
            .await
            .expect("find_or_create_train")
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                insert_forward_signals_inserts_one_row_per_signal -- --ignored --test-threads=1`"]
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
                insert_forward_signals_with_an_empty_slice_inserts_nothing -- --ignored --test-threads=1`"]
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

    /// DB2-29: one bad signal (an unknown `trains_id`, refused by the
    /// foreign key) rolls back the whole batch, and a good batch keeps its
    /// input order in `id`.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                insert_forward_signals_is_all_or_nothing -- --ignored --test-threads=1`"]
    async fn insert_forward_signals_is_all_or_nothing_and_keeps_input_order() {
        let pool = connect().await;
        sqlx::query("DELETE FROM trains WHERE train_uid = 'TEST-FORWARD-QUEUE-UID-3'")
            .execute(&pool)
            .await
            .ok();
        let trains_id = fixture_train(&pool, "TEST-FORWARD-QUEUE-UID-3").await;
        let signal = |trains_id: i64, summary: &str| TrainForwardSignalMessage {
            trains_id,
            event_summary: summary.to_string(),
        };

        let bad = vec![signal(trains_id, "first"), signal(-1, "no such train")];
        assert!(insert_forward_signals(&pool, &bad).await.is_err());
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM notifier_forward_queue WHERE trains_id = $1")
                .bind(trains_id)
                .fetch_one(&pool)
                .await
                .expect("count");
        assert_eq!(count, 0, "a failed batch must commit nothing");

        let good = vec![
            signal(trains_id, "a"),
            signal(trains_id, "b"),
            signal(trains_id, "c"),
        ];
        assert_eq!(insert_forward_signals(&pool, &good).await.unwrap(), 3);
        let order: Vec<String> = sqlx::query_scalar(
            "SELECT event_summary FROM notifier_forward_queue WHERE trains_id = $1 ORDER BY id",
        )
        .bind(trains_id)
        .fetch_all(&pool)
        .await
        .expect("order");
        assert_eq!(order, vec!["a", "b", "c"]);

        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
    }
}
