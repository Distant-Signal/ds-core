//! What poller-ldbws's arrivals lane polls (pre-departure prediction phase
//! 3, DS-152): the view `ingest_tracked_stop_arrival_targets`, every station
//! a user alights at on a tracked train where that train has a booked
//! arrival, with the booked public arrival as an instant. Departure boards
//! never list a terminating or set-down-only call, so these stops need the
//! arrival board. Read as the `ldbws_ro` role, whose only grants are SELECT
//! on its `ingest_*` views (no user ids).

use chrono::{DateTime, Duration, NaiveDate, Utc};
use sqlx::PgPool;

/// One alighting stop to poll.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct TrackedStopArrivalTarget {
    pub train_uid: String,
    pub service_date: NaiveDate,
    /// Upper-case CRS.
    pub crs: String,
    /// The train's booked public arrival there (else the working one).
    pub arrives_at: DateTime<Utc>,
}

/// The targets whose booked arrival is between `now - behind` and
/// `now + ahead`, soonest first.
pub async fn tracked_stop_arrival_targets(
    pool: &PgPool,
    now: DateTime<Utc>,
    behind: Duration,
    ahead: Duration,
) -> anyhow::Result<Vec<TrackedStopArrivalTarget>> {
    Ok(sqlx::query_as(
        "SELECT train_uid, service_date, crs, arrives_at \
         FROM ingest_tracked_stop_arrival_targets \
         WHERE arrives_at >= $1 AND arrives_at <= $2 \
         ORDER BY arrives_at, train_uid, crs",
    )
    .bind(now - behind)
    .bind(now + ahead)
    .fetch_all(pool)
    .await?)
}

#[cfg(test)]
mod db_tests {
    use super::*;

    async fn cleanup(pool: &PgPool, user: &str) {
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user)
            .execute(pool)
            .await
            .expect("cleanup");
        for sql in [
            "DELETE FROM trains WHERE train_uid = 'TSARR01'",
            "DELETE FROM schedule_calling_points_full WHERE uid = 'TSARR01'",
            "DELETE FROM tiploc_crs WHERE tiploc IN ('TSARRA', 'TSARRB')",
        ] {
            sqlx::query(sql).execute(pool).await.expect("cleanup");
        }
    }

    /// The view joins the subscription's pin destination to the train's
    /// calling points with a booked arrival: the terminating stop is a
    /// target (the departure lane never polls it), the origin is not, and a
    /// cancelled train has none.
    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL=... cargo test -p ds-store \
                tracked_stop_arrival_targets -- --ignored --test-threads=1"]
    async fn the_view_lists_each_alighting_stop_with_an_arrival() {
        let pool = crate::test_support::connect().await;
        let now = Utc::now();
        // Departs QYA in about an hour, terminates at QYB 30 minutes later
        // (dated by the departure's local day).
        let departs = (now + Duration::hours(1)).with_timezone(&chrono_tz::Europe::London);
        let today = departs.date_naive();
        let local = departs.time();
        let user = "TEST-TRACKED-STOP-ARRIVAL-TARGETS";
        cleanup(&pool, user).await;
        crate::test_support::seed_user(&pool, user).await;
        for (tiploc, crs) in [("TSARRA", "QYA"), ("TSARRB", "QYB")] {
            sqlx::query(
                "INSERT INTO tiploc_crs (tiploc, crs, station_name, stanox, source_sequence) \
                 VALUES ($1, $2, 'Test', '99998', 1) ON CONFLICT DO NOTHING",
            )
            .bind(tiploc)
            .bind(crs)
            .execute(&pool)
            .await
            .expect("crosswalk");
        }
        let arrive = local + Duration::minutes(30);
        // An arrival after midnight would carry day_offset 1; the fixture
        // keeps it simple and skips the minute-boundary edge by dating the
        // arrival with the same offset rule the view uses.
        let day_offset = i16::from(arrive < local);
        for (seq, tiploc, kind, arrival, departure, offset) in [
            (1_i16, "TSARRA ", "origin", None, Some(local), 0_i16),
            (2, "TSARRB ", "terminate", Some(arrive), None, day_offset),
        ] {
            sqlx::query(
                "INSERT INTO schedule_calling_points_full (service_date, uid, seq, tiploc, kind, \
                 booked_arrival, booked_departure, day_offset, public_arrival, public_departure) \
                 VALUES ($1, 'TSARR01', $2, $3, $4, $5, $6, $7, $5, $6)",
            )
            .bind(today)
            .bind(seq)
            .bind(tiploc)
            .bind(kind)
            .bind(arrival)
            .bind(departure)
            .bind(offset)
            .execute(&pool)
            .await
            .expect("calling point");
        }
        let trains_id: i64 = sqlx::query_scalar(
            "INSERT INTO trains (train_uid, service_date) VALUES ('TSARR01', $1) RETURNING id",
        )
        .bind(today)
        .fetch_one(&pool)
        .await
        .expect("trains");
        sqlx::query(
            "INSERT INTO train_subscriptions (user_id, service_date, resolution_status, \
             trains_id, pin_origin_crs, pin_destination_crs) \
             VALUES ($1, $2, 'schedule_matched', $3, 'QYA', 'qyb')",
        )
        .bind(user)
        .bind(today)
        .bind(trains_id)
        .execute(&pool)
        .await
        .expect("subscription");

        let targets =
            tracked_stop_arrival_targets(&pool, now, Duration::minutes(5), Duration::hours(3))
                .await
                .expect("read");
        let ours: Vec<&TrackedStopArrivalTarget> = targets
            .iter()
            .filter(|t| t.train_uid == "TSARR01")
            .collect();
        assert_eq!(ours.len(), 1, "{targets:?}");
        assert_eq!(ours[0].crs, "QYB");
        assert!(
            (ours[0].arrives_at - (now + Duration::minutes(90)))
                .num_minutes()
                .abs()
                <= 1,
            "{:?}",
            ours[0]
        );

        // A cancelled train is no longer polled.
        sqlx::query("INSERT INTO train_current_state (trains_id, status) VALUES ($1, 'cancelled')")
            .bind(trains_id)
            .execute(&pool)
            .await
            .expect("state");
        let targets =
            tracked_stop_arrival_targets(&pool, now, Duration::minutes(5), Duration::hours(3))
                .await
                .expect("read");
        assert!(targets.iter().all(|t| t.train_uid != "TSARR01"));

        cleanup(&pool, user).await;
    }
}
