// Moved to ds_store::backlog::matching (ingest architecture plan 1A.10)
pub use ds_store::backlog::matching::{
    BacklogReplayOutcome, attempt_backlog_match, attempt_backlog_match_by_uid,
    find_train_id_by_uid, run_backlog_match_sweep,
};

// These two stay in the api until ingest architecture plan unit F: they
// run schedule_matching's sweep, which has not moved yet.
#[cfg(test)]
#[expect(
    clippy::similar_names,
    clippy::too_many_lines,
    reason = "test code: paired test values share names; scenario tests read top to bottom"
)]
mod db_tests {
    use super::*;
    use chrono::{DateTime, NaiveDate, Utc};
    use sqlx::PgPool;
    use sqlx::postgres::PgPoolOptions;

    async fn connect() -> PgPool {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set to run this test");
        PgPoolOptions::new()
            .connect(&database_url)
            .await
            .expect("connect to postgres")
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                a_full_activation_plus_movement_backlog_resolves_the_pin_to_resolved -- --ignored --test-threads=1`"]
    async fn a_full_activation_plus_movement_backlog_resolves_the_pin_to_resolved() {
        let pool = connect().await;
        let user_id = "TEST-BACKLOG-MATCH-USER";
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind("backlog-match@example.com")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");

        let service_date: NaiveDate = "2026-09-05".parse().unwrap();
        let scheduled: DateTime<Utc> = "2026-09-05T18:15:00Z".parse().unwrap();
        // The public time, a minute before the working one: the replay must
        // carry it onto train_movement_events (2026-10-07).
        let gbtt: DateTime<Utc> = "2026-09-05T18:14:00Z".parse().unwrap();

        // Faithful to Task 9's real producer behavior, NOT a shortcut:
        // the Activation row (msg_type '0001') is the ONLY row that ever
        // carries a real `train_uid` and the ONLY row with `crs = NULL`;
        // the Movement row (msg_type '0003') carries the real `crs` +
        // timing data but `train_uid = NULL` -- Task 9's own consumer
        // never correlates the two in-process, `attempt_backlog_match`
        // does that at read time instead (see `find_backlog_match`'s own
        // doc comment). An earlier draft of this test set `train_uid` on
        // the Movement row directly, which papered over a real bug in
        // this plan's own backfill query -- caught and fixed during this
        // plan's second review pass (see Task 1's migration and this
        // module's `fetch_backlog_history`).
        sqlx::query(
            "INSERT INTO trust_event_backlog \
                (crs, train_uid, train_id, service_date, msg_type, event_type, \
                 planned_timestamp, actual_timestamp, variation_status, dedup_key, gbtt_timestamp) \
             VALUES (NULL, $1, $2, $3, '0001', NULL, NULL, NULL, NULL, $4, NULL), \
                    ($5, NULL, $2, $3, '0003', 'DEPARTURE', $6, $6, 'ON TIME', $7, $8)",
        )
        .bind("C99999")
        .bind("TEST-BACKLOG-TRAIN-ID")
        .bind(service_date)
        .bind("test-backlog-dedup-activation")
        .bind("EUS")
        .bind(scheduled)
        .bind("test-backlog-dedup-movement")
        .bind(gbtt)
        .execute(&pool)
        .await
        .expect("seed backlog rows");

        let (tracked_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions (user_id, service_date, pin_origin_crs, pin_scheduled_departure) \
             VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(user_id)
        .bind(service_date)
        .bind("EUS")
        .bind(scheduled)
        .fetch_one(&pool)
        .await
        .expect("seed tracked_trains row");

        let matched = attempt_backlog_match(&pool, tracked_train_id, "EUS", scheduled)
            .await
            .expect("attempt_backlog_match");
        assert!(matched);

        // `tracked_trains` no longer has its own `train_uid` column (Task
        // 22 dropped it) -- the resolved identity now lives exclusively on
        // the shared `trains` row, joined via `trains_id`.
        let (resolution_status, train_uid): (String, Option<String>) = sqlx::query_as(
            "SELECT tt.resolution_status, tr.train_uid \
             FROM train_subscriptions tt LEFT JOIN trains tr ON tr.id = tt.trains_id \
             WHERE tt.id = $1",
        )
        .bind(tracked_train_id)
        .fetch_one(&pool)
        .await
        .expect("read back tracked_trains joined to its resolved trains row");
        assert_eq!(resolution_status, "resolved");
        assert_eq!(train_uid, Some("C99999".to_string()));
        let replayed_gbtt: Vec<Option<DateTime<Utc>>> = sqlx::query_scalar(
            "SELECT m.gbtt_timestamp FROM train_movement_events m \
             JOIN train_subscriptions tt ON tt.trains_id = m.trains_id \
             WHERE tt.id = $1 AND m.msg_type = '0003'",
        )
        .bind(tracked_train_id)
        .fetch_all(&pool)
        .await
        .expect("read back the replayed movement");
        assert_eq!(replayed_gbtt, vec![Some(gbtt)]);

        // Real bug caught while running this plan's own end-to-end
        // verification (Task 13): this test's own fixture cleanup, as
        // specced, deleted the trust_event_backlog rows but never the
        // tracked_trains row it inserted above. tracked_trains has a real
        // UNIQUE(train_uid, service_date) WHERE train_uid IS NOT NULL
        // constraint (tracked_trains_resolved_identity, added by the
        // schedule-first design) -- re-running this test without deleting
        // that row made the SECOND run's own INSERT INTO tracked_trains
        // violate that constraint against the FIRST run's leftover row
        // (both resolve to the same train_uid=C99999/service_date). Delete
        // it here too so this test is idempotent across repeated runs, not
        // just its own single first execution.
        sqlx::query("DELETE FROM train_subscriptions WHERE id = $1")
            .bind(tracked_train_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trust_event_backlog WHERE train_id = 'TEST-BACKLOG-TRAIN-ID'")
            .execute(&pool)
            .await
            .ok();
        // Same class of leak as the tracked_trains one documented just
        // above, discovered the same way (Task 8's own end-to-end
        // verification): `attempt_backlog_match`'s own Step A dual-write
        // creates a shared `trains` row for this C99999/2026-09-05 identity
        // too, and this test never cleaned it up. That identity is also
        // used by `schedule_matching::db_tests`'s own EUS fixture -- an
        // uncleaned row here corrupted that unrelated test's `train_id`
        // assertion once Step C started reading `train_id` through the
        // joined `trains` row instead of `tracked_trains`' own column.
        sqlx::query("DELETE FROM trains WHERE train_uid = 'C99999' AND service_date = $1")
            .bind(service_date)
            .execute(&pool)
            .await
            .ok();
    }

    /// Finding #4's own regression test: an implausible row and a genuinely
    /// plausible one both fall inside the same CRS+time window. Before this
    /// fix, `find_backlog_match`'s `ORDER BY planned_timestamp LIMIT 1`
    /// selected whichever row sorted first REGARDLESS of plausibility,
    /// rejected it in Rust, and returned `Ok(None)` -- so a later sweep
    /// would deterministically re-select and re-reject that exact same row
    /// forever, never reaching the plausible second candidate sitting right
    /// next to it. Excluding the implausible row directly in the SQL means
    /// the plausible candidate is found and resolves the pin on the very
    /// first attempt, not merely "eventually, once the implausible row ages
    /// out of retention."
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p api \
                a_plausible_candidate_is_found_even_when_a_more_favorably_sorted_implausible_row_exists \
                -- --ignored --test-threads=1`"]
    async fn a_plausible_candidate_is_found_even_when_a_more_favorably_sorted_implausible_row_exists()
     {
        let pool = connect().await;
        let user_id = "TEST-BACKLOG-FALLTHROUGH-USER";
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind("backlog-fallthrough@example.com")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");

        let service_date: NaiveDate = "2026-09-05".parse().unwrap();
        let scheduled: DateTime<Utc> = "2026-09-05T18:15:00Z".parse().unwrap();

        // Candidate 1: sorts FIRST by planned_timestamp (exactly on time),
        // but its actual_timestamp is implausibly ahead of its own
        // received_at -- must be excluded from the query entirely, not just
        // rejected after being selected.
        let received_at_implausible: DateTime<Utc> = "2026-09-05T17:16:00Z".parse().unwrap();
        // Candidate 2: sorts SECOND (4 minutes after scheduled, comfortably
        // within the M9-tightened SCHEDULED_DEPARTURE_TOLERANCE of 5), but is
        // genuinely plausible -- this is the row that must actually resolve
        // the pin.
        let plausible_planned: DateTime<Utc> = "2026-09-05T18:19:00Z".parse().unwrap();
        let received_at_plausible: DateTime<Utc> = "2026-09-05T18:20:00Z".parse().unwrap();

        sqlx::query(
            "INSERT INTO trust_event_backlog \
                (crs, train_uid, train_id, service_date, msg_type, event_type, \
                 planned_timestamp, actual_timestamp, variation_status, dedup_key, received_at) \
             VALUES \
                ($1, NULL, $2, $3, '0003', 'DEPARTURE', $4, $4, 'ON TIME', $5, $6), \
                ($1, NULL, $7, $3, '0003', 'DEPARTURE', $8, $8, 'ON TIME', $9, $10), \
                (NULL, $11, $7, $3, '0001', NULL, NULL, NULL, NULL, $12, $10)",
        )
        .bind("EUS")
        .bind("TEST-BACKLOG-FALLTHROUGH-IMPLAUSIBLE-TRAIN-ID")
        .bind(service_date)
        .bind(scheduled)
        .bind("test-backlog-fallthrough-dedup-implausible")
        .bind(received_at_implausible)
        .bind("TEST-BACKLOG-FALLTHROUGH-PLAUSIBLE-TRAIN-ID")
        .bind(plausible_planned)
        .bind("test-backlog-fallthrough-dedup-plausible")
        .bind(received_at_plausible)
        // An Activation row for the PLAUSIBLE train_id only -- so
        // find_backlog_match's own Activation lookup discovers a train_uid
        // and the Step A dual-write sets trains_id, letting this test
        // verify identity via the joined `trains` row (same pattern as
        // `a_full_activation_plus_movement_backlog_resolves_the_pin_to_resolved`
        // above). The implausible train_id deliberately has NO Activation
        // row -- it's excluded from the CRS+time query before an Activation
        // lookup would even run for it.
        .bind("TEST-DW-FALLTHROUGH-UID")
        .bind("test-backlog-fallthrough-dedup-activation")
        .execute(&pool)
        .await
        .expect("seed both backlog rows plus an Activation for the plausible one");

        let (tracked_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions (user_id, service_date, pin_origin_crs, pin_scheduled_departure) \
             VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(user_id)
        .bind(service_date)
        .bind("EUS")
        .bind(scheduled)
        .fetch_one(&pool)
        .await
        .expect("seed tracked_trains row");

        let matched = attempt_backlog_match(&pool, tracked_train_id, "EUS", scheduled)
            .await
            .expect("attempt_backlog_match");
        assert!(
            matched,
            "the plausible second candidate must be found even though the implausible row \
             would otherwise have sorted first"
        );

        let (resolution_status,): (String,) =
            sqlx::query_as("SELECT resolution_status FROM train_subscriptions WHERE id = $1")
                .bind(tracked_train_id)
                .fetch_one(&pool)
                .await
                .expect("read back tracked_trains");
        assert_eq!(resolution_status, "resolved");

        let (trains_id, train_id): (i64, String) = sqlx::query_as(
            "SELECT tr.id, tr.train_id FROM train_subscriptions tt \
             JOIN trains tr ON tr.id = tt.trains_id \
             WHERE tt.id = $1",
        )
        .bind(tracked_train_id)
        .fetch_one(&pool)
        .await
        .expect("read back the resolved train_id");
        assert_eq!(
            train_id, "TEST-BACKLOG-FALLTHROUGH-PLAUSIBLE-TRAIN-ID",
            "must resolve to the plausible candidate, never the implausible one"
        );

        sqlx::query("DELETE FROM train_subscriptions WHERE id = $1")
            .bind(tracked_train_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query(
            "DELETE FROM trust_event_backlog WHERE train_id IN \
             ('TEST-BACKLOG-FALLTHROUGH-IMPLAUSIBLE-TRAIN-ID', 'TEST-BACKLOG-FALLTHROUGH-PLAUSIBLE-TRAIN-ID')",
        )
        .execute(&pool)
        .await
        .ok();
        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .ok();
    }
}
