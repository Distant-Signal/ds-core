//! Moving `train_current_state.status` to `completed` after the fact.
//!
//! `completed` is otherwise decided once, when a movement is ingested:
//! `trust_schema::journey::apply_movement` marks an `ARRIVAL` at the
//! train's known `trains.destination_crs` as the end of the journey. Two
//! gaps left trains reading `en_route` for good:
//!
//! 1. **The destination was learned after the arrival.** A schedule match
//!    that runs after the terminus `ARRIVAL` was ingested (the reconciliation
//!    sweep's 30-minute grace, `enrich_shared_train`'s replay-then-match
//!    order), or a backlog replay that could not look the destination up
//!    (no Activation in the backlog, so no `train_uid` to key the lookup
//!    on), wrote the arrival as `en_route`, and nothing derived it again.
//!    [`complete_on_known_destination_arrival`] closes that. A direct
//!    backlog replay calls it once it is done; a replay queued in the
//!    train-event outbox (the api's, DS-118) is completed by the outbox
//!    loop once applied ([`complete_subscription_train_on_known_destination_arrival`]);
//!    and a schedule match (`trains::find_or_create_train_with_schedule_match`,
//!    which the api runs too, and the api's role may not write
//!    `train_current_state`) is followed by the writer's
//!    [`complete_tracked_trains_on_known_destination_arrival`], after its
//!    own sweeps' matches and on every reconciliation tick.
//! 2. **No terminating evidence ever arrives.** A train with no known
//!    destination at all (most of the national population the TRUST backlog
//!    consumer writes: only tracked trains are schedule-matched), one that
//!    terminated short, or one whose last reports TRUST never sent, has
//!    nothing that could ever complete it. [`finish_stale_en_route_trains`]
//!    is the safety net: once a train's service day plus a margin has
//!    passed with no further write to its state, it is finished. It runs in
//!    the reconciliation sweep, so the first tick after a deploy also
//!    finishes every row already stuck.
//!
//! Both move only `en_route` rows. `cancelled` and `awaiting_activation`
//! (a train TRUST never reported running) are left alone, and the schema
//! has no other terminal state, so a stale train is `completed`: what the
//! readers (`travelled`, a journey's finished legs, trust-consumer's active
//! set) take it to mean is "no longer running", which it is.

use chrono::{DateTime, Duration, NaiveDate, Utc};
use sqlx::PgPool;

/// A train is stale once its `service_date` is at least this many days
/// before today's London date. A service runs into the next calendar day
/// at most (the rail day ends at 02:00 and an overnight sleeper arrives in
/// the morning), so by the start of `service_date + 2` it has finished,
/// however late it ran.
pub const STALE_EN_ROUTE_AFTER_DAYS: i64 = 2;

/// ...and only once nothing has written its state for this long, so a
/// train still being reported (an engineering run, a recycled `train_id`
/// TRUST is still talking about) is not finished under the writers' feet.
pub const STALE_EN_ROUTE_QUIET: Duration = Duration::hours(6);

/// If `trains_id`'s current state is `en_route` but its last event is an
/// `ARRIVAL` at the train's now-known `destination_crs`, marks it
/// `completed` -- the result `apply_movement` would have given had the
/// destination been known when that arrival was ingested. Returns whether
/// it changed the row; a no-op (already `completed`, not at the
/// destination, no state yet) returns `false`, so calling it repeatedly is
/// harmless.
///
/// Compares the stored `last_reported_location`, which is the translated
/// CRS whenever the event had one (else the raw STANOX, which can never
/// equal a CRS), case-insensitively, as `apply_movement` does. Like that
/// function it cannot tell a circular service's final call from an earlier
/// call at the same CRS; the read side's position-based check
/// (`api::data::journey::confirmed_final_arrival`) still backs that up.
pub async fn complete_on_known_destination_arrival<'c, E>(
    executor: E,
    trains_id: i64,
) -> anyhow::Result<bool>
where
    E: sqlx::PgExecutor<'c>,
{
    let result = sqlx::query(
        "UPDATE train_current_state cs \
         SET status = 'completed', updated_at = NOW() \
         FROM trains t \
         WHERE t.id = $1 \
           AND cs.trains_id = t.id \
           AND cs.status = 'en_route' \
           AND cs.last_event_type = 'ARRIVAL' \
           AND upper(cs.last_reported_location) = upper(t.destination_crs)",
    )
    .bind(trains_id)
    .execute(executor)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// [`complete_on_known_destination_arrival`] for the train subscription
/// `subscription_id` is bound to, if any: the backlog replay knows its
/// subscription, and the `trains` row it was bound to during the replay.
/// One statement, so it runs on the outbox loop's transaction too.
pub async fn complete_subscription_train_on_known_destination_arrival<'c, E>(
    executor: E,
    subscription_id: i64,
) -> anyhow::Result<bool>
where
    E: sqlx::PgExecutor<'c>,
{
    let result = sqlx::query(
        "UPDATE train_current_state cs \
         SET status = 'completed', updated_at = NOW() \
         FROM train_subscriptions s \
         JOIN trains t ON t.id = s.trains_id \
         WHERE s.id = $1 \
           AND cs.trains_id = t.id \
           AND cs.status = 'en_route' \
           AND cs.last_event_type = 'ARRIVAL' \
           AND upper(cs.last_reported_location) = upper(t.destination_crs)",
    )
    .bind(subscription_id)
    .execute(executor)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// [`complete_on_known_destination_arrival`] for every tracked train of a
/// service date not yet past [`last_stale_service_date`] (older ones are
/// [`finish_stale_en_route_trains`]'s): the writer's sweeps run it after
/// their schedule matches. A schedule match does not complete the train
/// itself any more (DS-118): the api's role may not write
/// `train_current_state`, so a match the api makes after the terminus
/// `ARRIVAL` was stored is completed here, by the writer's next
/// reconciliation tick (5 minutes by default). Returns the rows changed.
pub async fn complete_tracked_trains_on_known_destination_arrival(
    pool: &PgPool,
    now: DateTime<Utc>,
) -> anyhow::Result<u64> {
    let result = sqlx::query(
        "UPDATE train_current_state cs \
         SET status = 'completed', updated_at = NOW() \
         FROM trains t \
         WHERE cs.trains_id = t.id \
           AND t.id IN (SELECT s.trains_id FROM train_subscriptions s \
                        WHERE s.trains_id IS NOT NULL AND s.service_date > $1) \
           AND cs.status = 'en_route' \
           AND cs.last_event_type = 'ARRIVAL' \
           AND upper(cs.last_reported_location) = upper(t.destination_crs)",
    )
    .bind(last_stale_service_date(now))
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// The last `service_date` [`finish_stale_en_route_trains`] treats as
/// finished at `now`: today's London date minus
/// [`STALE_EN_ROUTE_AFTER_DAYS`].
pub fn last_stale_service_date(now: DateTime<Utc>) -> NaiveDate {
    now.with_timezone(&chrono_tz::Europe::London).date_naive()
        - Duration::days(STALE_EN_ROUTE_AFTER_DAYS)
}

/// The safety net: marks `completed` every `en_route` train whose
/// `service_date` is on or before [`last_stale_service_date`] and whose
/// state has not been written for [`STALE_EN_ROUTE_QUIET`]. Returns how
/// many rows it changed. Idempotent: a second run at the same `now` finds
/// nothing.
///
/// One `UPDATE` per stale `service_date`, so the first run after a deploy
/// (hundreds of thousands of rows in production, about one day's national
/// population each) commits in day-sized pieces rather than one long
/// transaction.
pub async fn finish_stale_en_route_trains(
    pool: &PgPool,
    now: DateTime<Utc>,
) -> anyhow::Result<u64> {
    let last_date = last_stale_service_date(now);
    let quiet_since = now - STALE_EN_ROUTE_QUIET;
    let dates: Vec<NaiveDate> = sqlx::query_scalar(
        "SELECT DISTINCT t.service_date \
         FROM train_current_state cs \
         JOIN trains t ON t.id = cs.trains_id \
         WHERE cs.status = 'en_route' \
           AND t.service_date <= $1 \
           AND cs.updated_at < $2 \
         ORDER BY t.service_date",
    )
    .bind(last_date)
    .bind(quiet_since)
    .fetch_all(pool)
    .await?;

    let mut finished = 0;
    for date in dates {
        finished += sqlx::query(
            "UPDATE train_current_state cs \
             SET status = 'completed', updated_at = NOW() \
             FROM trains t \
             WHERE t.id = cs.trains_id \
               AND t.service_date = $1 \
               AND cs.status = 'en_route' \
               AND cs.updated_at < $2",
        )
        .bind(date)
        .bind(quiet_since)
        .execute(pool)
        .await?
        .rows_affected();
    }
    Ok(finished)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(rfc3339: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339).unwrap().to_utc()
    }

    fn date(s: &str) -> NaiveDate {
        s.parse().unwrap()
    }

    #[test]
    fn a_service_day_is_stale_from_the_start_of_the_day_after_next() {
        // 2026-09-27 00:30 BST is still 2026-09-26 in UTC: London's date
        // decides, so the 25th is already stale.
        assert_eq!(
            last_stale_service_date(at("2026-09-26T23:30:00Z")),
            date("2026-09-25")
        );
        assert_eq!(
            last_stale_service_date(at("2026-09-26T22:59:59Z")),
            date("2026-09-24")
        );
        // GMT: London's date is UTC's.
        assert_eq!(
            last_stale_service_date(at("2026-12-03T00:00:00Z")),
            date("2026-12-01")
        );
    }

    #[test]
    fn the_margin_outlasts_an_overnight_service() {
        // A sleeper dated the 25th that arrives at 08:00 on the 26th is not
        // touched until the 27th.
        let arrival = at("2026-09-26T07:00:00Z");
        assert!(last_stale_service_date(arrival) < date("2026-09-25"));
        assert!(last_stale_service_date(arrival + STALE_EN_ROUTE_QUIET) < date("2026-09-25"));
    }
}

#[cfg(test)]
mod db_tests {
    use super::*;
    use crate::test_support::{FixtureCleanup, connect};

    const UID_PREFIX: &str = "TEST-COMPLETION-";

    async fn cleanup(pool: &PgPool) -> FixtureCleanup {
        FixtureCleanup::new(
            pool,
            [format!(
                "DELETE FROM trains WHERE train_uid LIKE '{UID_PREFIX}%'"
            )],
        )
        .await
    }

    async fn seed_train(
        pool: &PgPool,
        suffix: &str,
        service_date: NaiveDate,
        destination_crs: Option<&str>,
    ) -> i64 {
        sqlx::query_scalar(
            "INSERT INTO trains (train_uid, service_date, destination_crs) \
             VALUES ($1, $2, $3) RETURNING id",
        )
        .bind(format!("{UID_PREFIX}{suffix}"))
        .bind(service_date)
        .bind(destination_crs)
        .fetch_one(pool)
        .await
        .expect("seed trains row")
    }

    async fn seed_state(
        pool: &PgPool,
        trains_id: i64,
        status: &str,
        last_event_type: Option<&str>,
        last_reported_location: Option<&str>,
        updated_at: DateTime<Utc>,
    ) {
        sqlx::query(
            "INSERT INTO train_current_state \
                (trains_id, status, last_event_type, last_reported_location, event_time, \
                 updated_at) \
             VALUES ($1, $2, $3, $4, $5, $5)",
        )
        .bind(trains_id)
        .bind(status)
        .bind(last_event_type)
        .bind(last_reported_location)
        .bind(updated_at)
        .execute(pool)
        .await
        .expect("seed train_current_state");
    }

    async fn status(pool: &PgPool, trains_id: i64) -> String {
        sqlx::query_scalar("SELECT status FROM train_current_state WHERE trains_id = $1")
            .bind(trains_id)
            .fetch_one(pool)
            .await
            .expect("read status")
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                train_completion::db_tests -- --ignored --test-threads=1`"]
    async fn an_arrival_at_a_destination_learned_later_completes_the_train() {
        let pool = connect().await;
        let _cleanup = cleanup(&pool).await;
        let now = Utc::now();
        let today = now.date_naive();

        let arrived = seed_train(&pool, "ARRIVED", today, Some("BHM")).await;
        seed_state(
            &pool,
            arrived,
            "en_route",
            Some("ARRIVAL"),
            Some("bhm"),
            now,
        )
        .await;
        let intermediate = seed_train(&pool, "INTERMEDIATE", today, Some("BHM")).await;
        seed_state(
            &pool,
            intermediate,
            "en_route",
            Some("ARRIVAL"),
            Some("COV"),
            now,
        )
        .await;
        let departed = seed_train(&pool, "DEPARTED", today, Some("BHM")).await;
        seed_state(
            &pool,
            departed,
            "en_route",
            Some("DEPARTURE"),
            Some("BHM"),
            now,
        )
        .await;
        let unknown = seed_train(&pool, "UNKNOWN", today, None).await;
        seed_state(
            &pool,
            unknown,
            "en_route",
            Some("ARRIVAL"),
            Some("BHM"),
            now,
        )
        .await;
        let cancelled = seed_train(&pool, "CANCELLED", today, Some("BHM")).await;
        seed_state(
            &pool,
            cancelled,
            "cancelled",
            Some("ARRIVAL"),
            Some("BHM"),
            now,
        )
        .await;

        assert!(
            complete_on_known_destination_arrival(&pool, arrived)
                .await
                .unwrap()
        );
        assert_eq!(status(&pool, arrived).await, "completed");
        // Idempotent: the second call changes nothing.
        assert!(
            !complete_on_known_destination_arrival(&pool, arrived)
                .await
                .unwrap()
        );
        assert_eq!(status(&pool, arrived).await, "completed");

        for (trains_id, expected) in [
            (intermediate, "en_route"),
            (departed, "en_route"),
            (unknown, "en_route"),
            (cancelled, "cancelled"),
        ] {
            assert!(
                !complete_on_known_destination_arrival(&pool, trains_id)
                    .await
                    .unwrap()
            );
            assert_eq!(status(&pool, trains_id).await, expected, "{trains_id}");
        }
    }

    /// The production shape of the bug (train subscription 74, 2026-09-25):
    /// a backlog replay wrote the terminus arrival while the destination was
    /// unknown, and the schedule match that supplied it came afterwards.
    /// The match itself writes no shared train state (the api runs it, and
    /// its role may not; DS-118); the writer's sweep step completes the
    /// tracked train.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                train_completion::db_tests -- --ignored --test-threads=1`"]
    async fn a_schedule_match_after_the_terminus_arrival_completes_the_train() {
        let pool = connect().await;
        let _cleanup = cleanup(&pool).await;
        let now = Utc::now();
        let service_date = now.date_naive();
        let uid = format!("{UID_PREFIX}LATE-MATCH");

        let trains_id = crate::trains::find_or_create_train(&pool, &uid, service_date)
            .await
            .unwrap();
        seed_state(
            &pool,
            trains_id,
            "en_route",
            Some("ARRIVAL"),
            Some("BHM"),
            now,
        )
        .await;

        let user_id = "TEST-COMPLETION-LATE-MATCH-USER";
        crate::test_support::seed_user(&pool, user_id).await;
        crate::test_support::seed_backlog_candidate_pin(
            &pool,
            user_id,
            service_date,
            Some("EUS"),
            Some(now),
            "pending",
            Some(trains_id),
        )
        .await;

        let matched = crate::trains::find_or_create_train_with_schedule_match(
            &pool,
            &uid,
            service_date,
            Some("EUS"),
            Some(now),
            Some("BHM"),
            "test-line",
            &serde_json::json!([]),
            &[],
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(matched, trains_id);
        assert_eq!(status(&pool, trains_id).await, "en_route");

        assert!(
            complete_tracked_trains_on_known_destination_arrival(&pool, now)
                .await
                .unwrap()
                >= 1
        );
        assert_eq!(status(&pool, trains_id).await, "completed");
        // Idempotent.
        complete_tracked_trains_on_known_destination_arrival(&pool, now)
            .await
            .unwrap();
        assert_eq!(status(&pool, trains_id).await, "completed");
        crate::test_support::cleanup_user(&pool, user_id).await;
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                train_completion::db_tests -- --ignored --test-threads=1`"]
    async fn stale_en_route_trains_are_finished_once_and_nothing_else_is_touched() {
        let pool = connect().await;
        let _cleanup = cleanup(&pool).await;
        let now = Utc::now();
        let last_stale = last_stale_service_date(now);
        let quiet = now - STALE_EN_ROUTE_QUIET - Duration::minutes(1);

        // Stale: an old service day, quiet for long enough. Covers the
        // no-destination population and a train that stopped short.
        let stale = seed_train(&pool, "STALE", last_stale, None).await;
        seed_state(
            &pool,
            stale,
            "en_route",
            Some("ARRIVAL"),
            Some("COV"),
            quiet,
        )
        .await;
        let older = seed_train(&pool, "OLDER", last_stale - Duration::days(20), Some("BHM")).await;
        seed_state(
            &pool,
            older,
            "en_route",
            Some("DEPARTURE"),
            Some("RUG"),
            quiet,
        )
        .await;

        // Not stale: the service day is too recent...
        let recent = seed_train(&pool, "RECENT", last_stale + Duration::days(1), None).await;
        seed_state(
            &pool,
            recent,
            "en_route",
            Some("ARRIVAL"),
            Some("COV"),
            quiet,
        )
        .await;
        // ...the state was written too recently...
        let busy = seed_train(&pool, "BUSY", last_stale, None).await;
        seed_state(&pool, busy, "en_route", Some("ARRIVAL"), Some("COV"), now).await;
        // ...or it is not en_route at all.
        let cancelled = seed_train(&pool, "CANCELLED", last_stale, None).await;
        seed_state(&pool, cancelled, "cancelled", None, None, quiet).await;
        let never_ran = seed_train(&pool, "NEVER-RAN", last_stale, None).await;
        seed_state(&pool, never_ran, "awaiting_activation", None, None, quiet).await;

        let first = finish_stale_en_route_trains(&pool, now).await.unwrap();
        assert!(
            first >= 2,
            "finished {first}, expected at least the two fixtures"
        );
        assert_eq!(status(&pool, stale).await, "completed");
        assert_eq!(status(&pool, older).await, "completed");
        assert_eq!(status(&pool, recent).await, "en_route");
        assert_eq!(status(&pool, busy).await, "en_route");
        assert_eq!(status(&pool, cancelled).await, "cancelled");
        assert_eq!(status(&pool, never_ran).await, "awaiting_activation");

        // Idempotent: nothing is left for a second run at the same instant.
        assert_eq!(finish_stale_en_route_trains(&pool, now).await.unwrap(), 0);
        assert_eq!(status(&pool, stale).await, "completed");
    }
}
