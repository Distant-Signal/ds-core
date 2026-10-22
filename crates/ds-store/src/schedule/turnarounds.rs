//! `schedule_turnarounds`: each passenger train's previous working, for the
//! pre-departure late-start prediction
//! (docs/superpowers/specs/2026-10-21-pre-departure-prediction-design.md,
//! section 4 and decision Q1).
//!
//! Phase 1 infers the link from the platform sequence ("option A"): for a
//! passenger train's origin call with a booked platform, take the
//! timetable event just before its departure at the same station and
//! platform. The link holds when that event is a terminating arrival of a
//! passenger train run by the same operator, 2 to 60 minutes before the
//! departure. Any other event in between (another train's departure, a
//! call, a pass at that platform) breaks the chain, so no link is made.
//! Over 2026-10-15..21 this paired 78% of passenger origins with a booked
//! platform, and 92% of the trains whose incoming train had eaten the whole
//! turnaround started at least 3 minutes late (spec section 3a).
//!
//! [`replace_inferred_for_date`] rebuilds one service date's inferred rows
//! in one statement, from `schedule_calling_points_full` and
//! `schedule_services` for that date and the day before (an overnight
//! arrival can form a train the next morning). schedule-reference runs it
//! after publishing the date's calling points and services. Rows with
//! `link = 'association'` (phase 2) are never touched by it.

use anyhow::Result;
use chrono::NaiveDate;
use sqlx::PgPool;

/// The inference's window: the incoming train must arrive this long before
/// the departure at least ...
pub const MIN_TURNAROUND_MINUTES: i32 = 2;
/// ... and at most this long.
pub const MAX_TURNAROUND_MINUTES: i32 = 60;

/// CIF train categories counted as passenger trains (`schedule_services.
/// train_category`): ordinary (`OL`, `OO`, `OS`, `OU`, `OW`) and express
/// (`XC`, `XD`, `XI`, `XR`, `XU`, `XX`, `XZ`) passenger services.
pub const PASSENGER_CATEGORIES: &[&str] = &[
    "OL", "OO", "OS", "OU", "OW", "XC", "XD", "XI", "XR", "XU", "XX", "XZ",
];

/// `statement_timeout` for one date's rebuild. Two days of calling points
/// are about a million rows; the window sort takes a few seconds.
const STATEMENT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

/// Rebuilds `service_date`'s inferred previous-working links (see the
/// module docs), in one transaction: the date's old inferred rows are
/// deleted and the new ones inserted, so a reader sees the old set or the
/// new one. Returns the number of links written.
pub async fn replace_inferred_for_date(pool: &PgPool, service_date: NaiveDate) -> Result<u64> {
    let mut tx = pool.begin().await?;
    common::pg::set_local_statement_timeout(&mut tx, STATEMENT_TIMEOUT).await?;
    sqlx::query("DELETE FROM schedule_turnarounds WHERE service_date = $1 AND link = 'inferred'")
        .bind(service_date)
        .execute(&mut *tx)
        .await?;
    // One event per calling point at a booked platform: a terminating
    // call's arrival, else the departure (or, for a call with none, its
    // arrival). Times are working times, as local timestamps; `kind DESC`
    // orders a terminate before an origin at the same minute, so a train
    // formed from an arrival that very minute still pairs.
    let written = sqlx::query(
        "WITH svc AS ( \
             SELECT service_date, uid, operator_atoc FROM schedule_services \
             WHERE service_date IN ($1::date - 1, $1::date) AND mode = 'train' \
               AND train_category = ANY($2::text[]) \
         ), ev AS ( \
             SELECT c.service_date, c.uid, c.kind, UPPER(TRIM(c.tiploc)) AS tiploc, \
                    c.platform, s.operator_atoc, \
                    (c.service_date + c.day_offset + CASE WHEN c.kind = 'terminate' \
                        THEN COALESCE(c.working_arrival, c.booked_arrival) \
                        ELSE COALESCE(c.working_departure, c.booked_departure, \
                                      c.working_arrival, c.booked_arrival) END) AS t \
             FROM schedule_calling_points_full c \
             JOIN svc s ON s.service_date = c.service_date AND s.uid = c.uid \
             WHERE c.service_date IN ($1::date - 1, $1::date) \
               AND NULLIF(TRIM(c.platform), '') IS NOT NULL \
         ), seq AS ( \
             SELECT ev.*, \
                    lag(kind) OVER w AS p_kind, lag(uid) OVER w AS p_uid, \
                    lag(service_date) OVER w AS p_date, lag(operator_atoc) OVER w AS p_op, \
                    lag(t) OVER w AS p_t \
             FROM ev \
             WINDOW w AS (PARTITION BY tiploc, platform ORDER BY t, kind DESC, uid) \
         ) \
         INSERT INTO schedule_turnarounds \
             (service_date, uid, prev_service_date, prev_uid, booked_turnaround_minutes, link, \
              origin_tiploc, platform, departs_at) \
         SELECT DISTINCT ON (service_date, uid) \
                service_date, uid, p_date, p_uid, \
                (EXTRACT(EPOCH FROM t - p_t) / 60)::int::smallint, 'inferred', tiploc, platform, \
                t AT TIME ZONE 'Europe/London' \
         FROM seq \
         WHERE service_date = $1::date AND kind = 'origin' AND p_kind = 'terminate' \
           AND p_op IS NOT DISTINCT FROM operator_atoc AND operator_atoc IS NOT NULL \
           AND t - p_t >= make_interval(mins => $3) \
           AND t - p_t <= make_interval(mins => $4) \
         ORDER BY service_date, uid \
         ON CONFLICT (service_date, uid) DO NOTHING",
    )
    .bind(service_date)
    .bind(PASSENGER_CATEGORIES)
    .bind(MIN_TURNAROUND_MINUTES)
    .bind(MAX_TURNAROUND_MINUTES)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    tx.commit().await?;
    Ok(written)
}

#[cfg(test)]
mod db_tests {
    use super::*;

    /// The owner's pool when set (the rebuild is schedule-reference's
    /// write; a per-service run as the api role may not make it).
    async fn pool() -> PgPool {
        crate::test_support::schedule_fixture_pool(&crate::test_support::connect().await).await
    }

    /// Fixture dates in 2099 (`test_support::assert_synthetic_date`), so
    /// nothing here can collide with a real publish.
    fn day(d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(2099, 7, d).expect("valid date")
    }

    async fn service(pool: &PgPool, date: NaiveDate, uid: &str, op: &str, category: &str) {
        sqlx::query(
            "INSERT INTO schedule_services (service_date, uid, mode, train_category, \
             operator_atoc, stp) VALUES ($1, $2, 'train', $3, $4, 'P') \
             ON CONFLICT (service_date, uid) DO UPDATE SET operator_atoc = EXCLUDED.operator_atoc",
        )
        .bind(date)
        .bind(uid)
        .bind(category)
        .bind(op)
        .execute(pool)
        .await
        .expect("insert service");
    }

    #[expect(clippy::too_many_arguments, reason = "one fixture row's columns")]
    async fn call(
        pool: &PgPool,
        date: NaiveDate,
        uid: &str,
        seq: i16,
        tiploc: &str,
        kind: &str,
        arrival: Option<&str>,
        departure: Option<&str>,
        platform: Option<&str>,
    ) {
        let time = |t: Option<&str>| {
            t.map(|t| chrono::NaiveTime::parse_from_str(t, "%H:%M").expect("time"))
        };
        sqlx::query(
            "INSERT INTO schedule_calling_points_full (service_date, uid, seq, tiploc, kind, \
             booked_arrival, booked_departure, day_offset, platform, working_arrival, \
             working_departure) VALUES ($1, $2, $3, $4, $5, $6, $7, 0, $8, $6, $7)",
        )
        .bind(date)
        .bind(uid)
        .bind(seq)
        .bind(tiploc)
        .bind(kind)
        .bind(time(arrival))
        .bind(time(departure))
        .bind(platform)
        .execute(pool)
        .await
        .expect("insert calling point");
    }

    async fn cleanup(pool: &PgPool, dates: &[NaiveDate]) {
        for table in [
            "schedule_turnarounds",
            "schedule_calling_points_full",
            "schedule_services",
        ] {
            sqlx::query(&format!("DELETE FROM {table} WHERE service_date = ANY($1)"))
                .bind(dates)
                .execute(pool)
                .await
                .expect("cleanup");
        }
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL=... cargo test -p ds-store \
                turnarounds -- --ignored --test-threads=1"]
    #[expect(
        clippy::too_many_lines,
        reason = "one fixture-heavy scenario, read top to bottom"
    )]
    async fn infers_the_previous_working_from_the_platform_sequence() {
        let pool = pool().await;
        let (d0, d1) = (day(9), day(10));
        cleanup(&pool, &[d0, d1]).await;

        // P1 terminates at YORK platform 3 at 10:00; T1 starts there at
        // 10:12: a 12-minute turnaround.
        service(&pool, d1, "P00001", "NT", "OO").await;
        call(
            &pool,
            d1,
            "P00001",
            1,
            "LEEDS",
            "origin",
            None,
            Some("09:00"),
            Some("1"),
        )
        .await;
        call(
            &pool,
            d1,
            "P00001",
            2,
            "YORK",
            "terminate",
            Some("10:00"),
            None,
            Some("3"),
        )
        .await;
        service(&pool, d1, "T00001", "NT", "OO").await;
        call(
            &pool,
            d1,
            "T00001",
            1,
            "YORK",
            "origin",
            None,
            Some("10:12"),
            Some("3"),
        )
        .await;
        call(
            &pool,
            d1,
            "T00001",
            2,
            "LEEDS",
            "terminate",
            Some("11:00"),
            None,
            Some("2"),
        )
        .await;

        // T2 starts at YORK platform 5, but X2 departs from platform 5
        // between P2's arrival and T2: X2 is P2's next working, T2 has
        // none.
        service(&pool, d1, "P00002", "NT", "OO").await;
        call(
            &pool,
            d1,
            "P00002",
            1,
            "LEEDS",
            "origin",
            None,
            Some("09:10"),
            Some("1"),
        )
        .await;
        call(
            &pool,
            d1,
            "P00002",
            2,
            "YORK",
            "terminate",
            Some("10:00"),
            None,
            Some("5"),
        )
        .await;
        service(&pool, d1, "X00002", "NT", "OO").await;
        call(
            &pool,
            d1,
            "X00002",
            1,
            "YORK",
            "origin",
            None,
            Some("10:05"),
            Some("5"),
        )
        .await;
        call(
            &pool,
            d1,
            "X00002",
            2,
            "LEEDS",
            "terminate",
            Some("11:00"),
            None,
            Some("1"),
        )
        .await;
        service(&pool, d1, "T00002", "NT", "OO").await;
        call(
            &pool,
            d1,
            "T00002",
            1,
            "YORK",
            "origin",
            None,
            Some("10:20"),
            Some("5"),
        )
        .await;
        call(
            &pool,
            d1,
            "T00002",
            2,
            "LEEDS",
            "terminate",
            Some("11:10"),
            None,
            Some("1"),
        )
        .await;

        // T3: the arrival on its platform is another operator's: no link.
        service(&pool, d1, "P00003", "TP", "XX").await;
        call(
            &pool,
            d1,
            "P00003",
            1,
            "LEEDS",
            "origin",
            None,
            Some("09:20"),
            Some("1"),
        )
        .await;
        call(
            &pool,
            d1,
            "P00003",
            2,
            "YORK",
            "terminate",
            Some("10:00"),
            None,
            Some("7"),
        )
        .await;
        service(&pool, d1, "T00003", "NT", "OO").await;
        call(
            &pool,
            d1,
            "T00003",
            1,
            "YORK",
            "origin",
            None,
            Some("10:10"),
            Some("7"),
        )
        .await;

        // T4: the arrival is 90 minutes before: outside the window.
        service(&pool, d1, "P00004", "NT", "OO").await;
        call(
            &pool,
            d1,
            "P00004",
            2,
            "YORK",
            "terminate",
            Some("08:00"),
            None,
            Some("8"),
        )
        .await;
        service(&pool, d1, "T00004", "NT", "OO").await;
        call(
            &pool,
            d1,
            "T00004",
            1,
            "YORK",
            "origin",
            None,
            Some("09:30"),
            Some("8"),
        )
        .await;

        // T5 starts just after midnight from a train that arrived the
        // evening before (the previous service date).
        service(&pool, d0, "P00005", "NT", "OO").await;
        call(
            &pool,
            d0,
            "P00005",
            2,
            "YORK",
            "terminate",
            Some("23:50"),
            None,
            Some("9"),
        )
        .await;
        sqlx::query(
            "INSERT INTO schedule_calling_points_full (service_date, uid, seq, tiploc, kind, \
             booked_departure, day_offset, platform, working_departure) \
             VALUES ($1, 'T00005', 1, 'YORK', 'origin', '00:05', 0, '9', '00:05')",
        )
        .bind(d1)
        .execute(&pool)
        .await
        .expect("insert");
        service(&pool, d1, "T00005", "NT", "OO").await;

        let written = replace_inferred_for_date(&pool, d1).await.expect("replace");
        let rows: Vec<(String, String, NaiveDate, i16, String)> = sqlx::query_as(
            "SELECT uid, prev_uid, prev_service_date, booked_turnaround_minutes, platform \
             FROM schedule_turnarounds WHERE service_date = $1 ORDER BY uid",
        )
        .bind(d1)
        .fetch_all(&pool)
        .await
        .expect("read");
        assert_eq!(written, 3);
        assert_eq!(
            rows,
            vec![
                ("T00001".into(), "P00001".into(), d1, 12, "3".into()),
                ("T00005".into(), "P00005".into(), d0, 15, "9".into()),
                ("X00002".into(), "P00002".into(), d1, 5, "5".into()),
            ]
        );

        // A rerun replaces, never duplicates.
        assert_eq!(
            replace_inferred_for_date(&pool, d1).await.expect("rerun"),
            3
        );
        cleanup(&pool, &[d0, d1]).await;
    }
}
