//! What poller-ldbws's tracked-stop lane polls (pre-departure prediction
//! phase 1, DS-127): the view `ingest_tracked_stop_targets`, every station a
//! user boards, alights or changes at on a tracked train where that train
//! has a booked departure, with the booked public departure as an instant.
//! Read as the `ldbws_ro` role, whose only grants are SELECT on its
//! `ingest_*` views (no user ids).

use chrono::{DateTime, Duration, NaiveDate, Utc};
use sqlx::PgPool;

/// One stop to poll.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct TrackedStopTarget {
    pub train_uid: String,
    pub service_date: NaiveDate,
    /// Upper-case CRS.
    pub crs: String,
    /// The train's booked public departure there.
    pub departs_at: DateTime<Utc>,
}

/// The targets whose booked departure is between `now - behind` and
/// `now + ahead`, soonest first.
pub async fn tracked_stop_targets(
    pool: &PgPool,
    now: DateTime<Utc>,
    behind: Duration,
    ahead: Duration,
) -> anyhow::Result<Vec<TrackedStopTarget>> {
    Ok(sqlx::query_as(
        "SELECT train_uid, service_date, crs, departs_at \
         FROM ingest_tracked_stop_targets \
         WHERE departs_at >= $1 AND departs_at <= $2 \
         ORDER BY departs_at, train_uid, crs",
    )
    .bind(now - behind)
    .bind(now + ahead)
    .fetch_all(pool)
    .await?)
}

#[cfg(test)]
mod db_tests {
    use super::*;

    /// The view joins a journey leg's ends and the subscription's pin to
    /// the train's calling points with a booked departure: an alighting
    /// stop where the train terminates is not a target, a cancelled train
    /// has none.
    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL=... cargo test -p ds-store \
                tracked_stop_targets -- --ignored --test-threads=1"]
    #[expect(
        clippy::too_many_lines,
        reason = "one fixture-heavy scenario, read top to bottom"
    )]
    async fn the_view_lists_each_tracked_stop_with_a_departure() {
        let pool = crate::test_support::connect().await;
        let now = Utc::now();
        // Departs QZA in about an hour (dated by that local day, so the
        // test also holds late in the evening), terminates at QZB.
        let departs = (now + Duration::hours(1)).with_timezone(&chrono_tz::Europe::London);
        let today = departs.date_naive();
        let local = departs.time();
        let user = "TEST-TRACKED-STOP-TARGETS";
        sqlx::query("DELETE FROM trains WHERE train_uid = 'TSTGT01'")
            .execute(&pool)
            .await
            .expect("cleanup");
        sqlx::query("DELETE FROM schedule_calling_points_full WHERE uid = 'TSTGT01'")
            .execute(&pool)
            .await
            .expect("cleanup");
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user)
            .execute(&pool)
            .await
            .expect("cleanup");
        crate::test_support::seed_user(&pool, user).await;
        for (tiploc, crs) in [("TSTGTA", "QZA"), ("TSTGTB", "QZB")] {
            sqlx::query(
                "INSERT INTO tiploc_crs (tiploc, crs, station_name, stanox, source_sequence) \
                 VALUES ($1, $2, 'Test', '99999', 1) ON CONFLICT DO NOTHING",
            )
            .bind(tiploc)
            .bind(crs)
            .execute(&pool)
            .await
            .expect("crosswalk");
        }
        let arrive = local + Duration::minutes(30);
        for (seq, tiploc, kind, arrival, departure) in [
            (1_i16, "TSTGTA ", "origin", None, Some(local)),
            (2, "TSTGTB ", "terminate", Some(arrive), None),
        ] {
            sqlx::query(
                "INSERT INTO schedule_calling_points_full (service_date, uid, seq, tiploc, kind, \
                 booked_arrival, booked_departure, day_offset, public_departure) \
                 VALUES ($1, 'TSTGT01', $2, $3, $4, $5, $6, 0, $6)",
            )
            .bind(today)
            .bind(seq)
            .bind(tiploc)
            .bind(kind)
            .bind(arrival)
            .bind(departure)
            .execute(&pool)
            .await
            .expect("calling point");
        }
        let trains_id: i64 = sqlx::query_scalar(
            "INSERT INTO trains (train_uid, service_date) VALUES ('TSTGT01', $1) RETURNING id",
        )
        .bind(today)
        .fetch_one(&pool)
        .await
        .expect("trains");
        sqlx::query(
            "INSERT INTO train_subscriptions (user_id, service_date, resolution_status, \
             trains_id, pin_origin_crs, pin_destination_crs) \
             VALUES ($1, $2, 'schedule_matched', $3, 'qza', 'QZB')",
        )
        .bind(user)
        .bind(today)
        .bind(trains_id)
        .execute(&pool)
        .await
        .expect("subscription");

        let targets = tracked_stop_targets(&pool, now, Duration::minutes(5), Duration::hours(3))
            .await
            .expect("read");
        let ours: Vec<&TrackedStopTarget> = targets
            .iter()
            .filter(|t| t.train_uid == "TSTGT01")
            .collect();
        assert_eq!(ours.len(), 1, "{targets:?}");
        assert_eq!(ours[0].crs, "QZA");
        assert!(
            (ours[0].departs_at - (now + Duration::hours(1)))
                .num_minutes()
                .abs()
                <= 1
        );

        // A cancelled train is no longer polled.
        sqlx::query("INSERT INTO train_current_state (trains_id, status) VALUES ($1, 'cancelled')")
            .bind(trains_id)
            .execute(&pool)
            .await
            .expect("state");
        let targets = tracked_stop_targets(&pool, now, Duration::minutes(5), Duration::hours(3))
            .await
            .expect("read");
        assert!(targets.iter().all(|t| t.train_uid != "TSTGT01"));

        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user)
            .execute(&pool)
            .await
            .expect("cleanup");
        sqlx::query("DELETE FROM trains WHERE train_uid = 'TSTGT01'")
            .execute(&pool)
            .await
            .expect("cleanup");
        sqlx::query("DELETE FROM schedule_calling_points_full WHERE uid = 'TSTGT01'")
            .execute(&pool)
            .await
            .expect("cleanup");
        sqlx::query("DELETE FROM tiploc_crs WHERE tiploc IN ('TSTGTA', 'TSTGTB')")
            .execute(&pool)
            .await
            .expect("cleanup");
    }
}
