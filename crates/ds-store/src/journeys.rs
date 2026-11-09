//! Journey reads shared by the api and the notifier.
//!
//! [`leave_by_sql`] is the one definition of a journey's leave-by time
//! (distant-signal's docs/superpowers/specs/2026-11-06-regular-journeys-design.md
//! §5.1): the api serves it on the journey page and the home cards, and the
//! notifier's leave-by push names it, so the two never disagree.

use crate::trains::{LegEnd, leg_crs_sql};

/// What a leave-by is timed from ([`leave_by_sql`]'s `basis` column).
pub const BASIS_BOOKED: &str = "booked";
/// See [`BASIS_BOOKED`].
pub const BASIS_SUGGESTION: &str = "suggestion";
/// See [`BASIS_BOOKED`]: a remind-me occurrence's usual train, not yet
/// picked ([`leave_by_sql`]'s "unpicked train" rule).
pub const BASIS_USUAL: &str = "usual";
/// See [`BASIS_BOOKED`]: a remind-me occurrence's suggested train that is
/// the usual operator's nearest one, not the usual train itself (a retimed
/// train with a new UID); not yet picked.
pub const BASIS_CLOSEST: &str = "closest";

/// SQL for one journey's leave-by, a subquery to use as
/// `LEFT JOIN LATERAL ({leave_by_sql("j")}) lb ON TRUE`, where `journey`
/// is the outer query's alias for a `journeys` row. It yields one row, or
/// none when the journey has no leave-by:
///
/// | column | type | |
/// |---|---|---|
/// | `leave_by_at` | `timestamptz` | when to leave: `departure` minus `station_buffer_minutes` |
/// | `departure` | `timestamptz` | the departure it is timed from |
/// | `basis` | `text` | [`BASIS_BOOKED`], [`BASIS_SUGGESTION`], [`BASIS_USUAL`] or [`BASIS_CLOSEST`] |
/// | `leg_id` | `bigint` | the `journey_legs` id of the first leg with a time |
///
/// The rules (§5.1):
///
/// - No `station_buffer_minutes` on the journey: no leave-by.
/// - The plan is the journey's current one: `journey_legs` in `leg_order`,
///   with each leg's booked time from the journey's latest revision
///   (`journey_revision_legs` by `source_leg_id`), so an accepted replan
///   moves it with no extra code. A leg the revision doesn't hold with the
///   same train (no revision yet, as for an occurrence the notifier minted,
///   or a train committed after the snapshot) falls back to what a
///   snapshot would book: the train's timetabled departure from the leg's
///   own origin, else the subscription's pinned departure.
/// - The first leg with a time sets it (decision 15): a train leg's booked
///   departure, or an untracked leg's (Tube, bus, walk) planned departure.
///   A train leg with no train yet has no time and is passed over,
///   unless the usual-train rule below gives it one.
/// - A train leg with no train yet (a remind-me occurrence nobody has
///   picked a train for) takes the departure of the journey's
///   `regular_plan` suggestion when the plan is `suggested`, not yet
///   accepted and chose either the usual train (reason `usual`, basis
///   [`BASIS_USUAL`]) or the usual operator's nearest train within the
///   planner's window (reason `same_operator`, basis [`BASIS_CLOSEST`]: a
///   retimed train with a new UID). A `best` suggestion, the target's own
///   pick when the usual train doesn't run, has no leave-by: it may be far
///   from the time the user is used to, and the pick reminder is how they
///   hear of it. The time is read from the suggested train's timetable
///   (`schedule_destination_departures`, as a bound train's is), so a
///   retime the plan hasn't caught up with yet still moves it; the plan's
///   stored departure is the fallback. A suggested train that is cancelled
///   in `train_current_state` gives none. Accepting or tracking binds the
///   leg (and moves the plan off `suggested`), so the time then comes from
///   the train (basis [`BASIS_BOOKED`]) and the same leave-by is not
///   repeated.
/// - Lateness never moves it (decision 5): no live time is read.
/// - The journey's first train is cancelled (`train_current_state`): with
///   an open break whose best suggestion is `covered`, it is that
///   suggestion's departure minus the buffer, basis [`BASIS_SUGGESTION`];
///   without one there is no leave-by.
///
/// Reads `journeys` (`id`, `station_buffer_minutes`, `regular_plan`,
/// `regular_plan_status`), `journey_legs`,
/// `train_subscriptions` (`id`, `trains_id`, `pin_origin_crs`,
/// `pin_destination_crs`, `pin_scheduled_departure`), `trains`,
/// `train_current_state`, `journey_revisions` (`id`, `journey_id`,
/// `revision_no`), `journey_revision_legs` (`revision_id`, `position`,
/// `source_leg_id`, `kind`, `trains_id`, `booked_departure`,
/// `planned_departure`), `journey_breaks` and
/// `schedule_destination_departures` (db-grants.yaml).
#[expect(clippy::too_many_lines, reason = "one SQL text, kept whole")]
pub fn leave_by_sql(journey: &str) -> String {
    let origin = leg_crs_sql(LegEnd::Origin, "lb_jl", "lb_ts", "lb_tr");
    format!(
        "SELECT CASE WHEN lb_ft.cancelled THEN lb_sg.departure ELSE lb_fl.departure END \
                    - make_interval(mins => {journey}.station_buffer_minutes::INT) AS leave_by_at, \
                CASE WHEN lb_ft.cancelled THEN lb_sg.departure ELSE lb_fl.departure END \
                    AS departure, \
                CASE WHEN lb_ft.cancelled THEN '{BASIS_SUGGESTION}' \
                     WHEN lb_fl.fallback_reason = 'usual' THEN '{BASIS_USUAL}' \
                     WHEN lb_fl.fallback_reason IS NOT NULL THEN '{BASIS_CLOSEST}' \
                     ELSE '{BASIS_BOOKED}' END AS basis, \
                lb_fl.leg_id \
         FROM ( \
             SELECT lb_leg.leg_id, lb_leg.departure, lb_leg.fallback_reason FROM ( \
                 SELECT lb_jl.id AS leg_id, lb_jl.leg_order, \
                        CASE WHEN lb_jl.kind = 'train' AND lb_ts.id IS NULL \
                             THEN lb_pl.reason END AS fallback_reason, \
                        CASE WHEN lb_jl.kind = 'train' AND lb_ts.id IS NULL THEN \
                             CASE WHEN lb_pl.reason IS NULL THEN NULL \
                                  WHEN EXISTS ( \
                                      SELECT 1 FROM trains lb_ut \
                                        JOIN train_current_state lb_uc ON lb_uc.trains_id = lb_ut.id \
                                       WHERE lb_ut.train_uid = lb_pl.first_train ->> 'trainUid' \
                                         AND lb_ut.service_date \
                                                 = (lb_pl.first_train ->> 'serviceDate')::date \
                                         AND lb_uc.status = 'cancelled') THEN NULL \
                                  ELSE COALESCE( \
                                      (SELECT ((lb_ud.service_date + lb_ud.day_offset) \
                                               + COALESCE(lb_ud.public_departure, lb_ud.scheduled)) \
                                              AT TIME ZONE 'Europe/London' \
                                         FROM schedule_destination_departures lb_ud \
                                        WHERE lb_pl.first_leg ->> 'kind' = 'train' \
                                          AND lb_ud.train_uid = lb_pl.first_leg ->> 'trainUid' \
                                          AND lb_ud.service_date \
                                                  = (lb_pl.first_leg ->> 'serviceDate')::date \
                                          AND lb_ud.origin_crs = UPPER(lb_pl.first_leg ->> 'originCrs') \
                                        ORDER BY lb_ud.day_offset, lb_ud.scheduled \
                                        LIMIT 1), \
                                      lb_pl.departure) END \
                             WHEN lb_jl.kind = 'train' THEN COALESCE( \
                                 (SELECT lb_rl.booked_departure \
                                    FROM journey_revision_legs lb_rl \
                                   WHERE lb_rl.revision_id = lb_rev.id \
                                     AND lb_rl.source_leg_id = lb_jl.id \
                                     AND lb_rl.kind = 'train' \
                                     AND lb_rl.trains_id = lb_ts.trains_id \
                                   LIMIT 1), \
                                 (SELECT ((lb_sdd.service_date + lb_sdd.day_offset) \
                                          + COALESCE(lb_sdd.public_departure, lb_sdd.scheduled)) \
                                         AT TIME ZONE 'Europe/London' \
                                    FROM schedule_destination_departures lb_sdd \
                                   WHERE lb_sdd.train_uid = lb_tr.train_uid \
                                     AND lb_sdd.service_date = lb_tr.service_date \
                                     AND lb_sdd.origin_crs = UPPER({origin}) \
                                   ORDER BY lb_sdd.day_offset, lb_sdd.scheduled \
                                   LIMIT 1), \
                                 lb_ts.pin_scheduled_departure) \
                             ELSE COALESCE( \
                                 (SELECT lb_rl.planned_departure \
                                    FROM journey_revision_legs lb_rl \
                                   WHERE lb_rl.revision_id = lb_rev.id \
                                     AND lb_rl.source_leg_id = lb_jl.id \
                                     AND lb_rl.kind = lb_jl.kind \
                                   LIMIT 1), \
                                 lb_jl.planned_departure) \
                        END AS departure \
                 FROM journey_legs lb_jl \
                 LEFT JOIN train_subscriptions lb_ts \
                        ON lb_ts.id = lb_jl.train_subscription_id AND lb_jl.kind = 'train' \
                 LEFT JOIN trains lb_tr ON lb_tr.id = lb_ts.trains_id \
                 LEFT JOIN LATERAL ( \
                     SELECT lb_pj.regular_plan ->> 'reason' AS reason, \
                            (lb_pj.regular_plan -> 'choice' ->> 'departure')::timestamptz \
                                AS departure, \
                            lb_pj.regular_plan -> 'choice' -> 'legs' -> 0 AS first_leg, \
                            jsonb_path_query_first(lb_pj.regular_plan, \
                                '$.choice.legs[*] ? (@.kind == \"train\")') AS first_train \
                       FROM journeys lb_pj \
                      WHERE lb_pj.id = {journey}.id \
                        AND lb_pj.regular_plan_status = 'suggested' \
                        AND lb_pj.regular_plan ->> 'acceptedIndex' IS NULL \
                        AND lb_pj.regular_plan ->> 'reason' IN ('usual', 'same_operator') \
                 ) lb_pl ON lb_jl.kind = 'train' AND lb_ts.id IS NULL \
                 LEFT JOIN LATERAL ( \
                     SELECT lb_r.id FROM journey_revisions lb_r \
                      WHERE lb_r.journey_id = {journey}.id \
                      ORDER BY lb_r.revision_no DESC LIMIT 1 \
                 ) lb_rev ON TRUE \
                 WHERE lb_jl.journey_id = {journey}.id \
                   AND (lb_jl.kind <> 'train' OR lb_ts.trains_id IS NOT NULL \
                        OR lb_ts.id IS NULL) \
             ) lb_leg \
             WHERE lb_leg.departure IS NOT NULL \
             ORDER BY lb_leg.leg_order \
             LIMIT 1 \
         ) lb_fl \
         CROSS JOIN LATERAL ( \
             SELECT COALESCE(( \
                 SELECT lb_cs.status = 'cancelled' \
                   FROM journey_legs lb_tl \
                   JOIN train_subscriptions lb_tts ON lb_tts.id = lb_tl.train_subscription_id \
                   LEFT JOIN train_current_state lb_cs ON lb_cs.trains_id = lb_tts.trains_id \
                  WHERE lb_tl.journey_id = {journey}.id AND lb_tl.kind = 'train' \
                    AND lb_tts.trains_id IS NOT NULL \
                  ORDER BY lb_tl.leg_order \
                  LIMIT 1), FALSE) AS cancelled \
         ) lb_ft \
         LEFT JOIN LATERAL ( \
             SELECT (lb_jb.suggestions -> 'suggestions' -> 0 ->> 'departure')::timestamptz \
                        AS departure \
               FROM journey_breaks lb_jb \
              WHERE lb_jb.journey_id = {journey}.id AND lb_jb.status = 'open' \
                AND lb_jb.suggestions -> 'suggestions' -> 0 ->> 'fit' = 'covered' \
              LIMIT 1 \
         ) lb_sg ON lb_ft.cancelled \
         WHERE {journey}.station_buffer_minutes IS NOT NULL \
           AND (NOT lb_ft.cancelled OR lb_sg.departure IS NOT NULL)"
    )
}

#[cfg(test)]
mod db_tests {
    use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
    use chrono_tz::Europe::London;
    use sqlx::PgPool;

    use super::*;

    #[test]
    fn the_sql_names_the_outer_alias_only_through_its_columns() {
        let sql = leave_by_sql("jx");
        assert!(sql.contains("jx.id"));
        assert!(sql.contains("jx.station_buffer_minutes"));
        assert!(!sql.contains(" j."), "no stray alias: {sql}");
    }

    const USER: &str = "leave-by-test-user";

    fn london(date: NaiveDate, h: u32, m: u32) -> DateTime<Utc> {
        London
            .from_local_datetime(&date.and_hms_opt(h, m, 0).unwrap())
            .earliest()
            .unwrap()
            .with_timezone(&Utc)
    }

    fn date() -> NaiveDate {
        NaiveDate::from_ymd_opt(2050, 3, 14).unwrap()
    }

    async fn journey(pool: &PgPool, buffer: Option<i16>) -> i64 {
        crate::test_support::seed_user(pool, USER).await;
        sqlx::query_scalar(
            "INSERT INTO journeys (user_id, station_buffer_minutes) VALUES ($1, $2) RETURNING id",
        )
        .bind(USER)
        .bind(buffer)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// A train leg bound to a fresh `trains` row departing `departure`
    /// (its subscription's pin). Returns `(leg id, trains id)`.
    async fn train_leg(
        pool: &PgPool,
        journey_id: i64,
        order: i32,
        uid: &str,
        departure: DateTime<Utc>,
    ) -> (i64, i64) {
        let trains_id: i64 = sqlx::query_scalar(
            "INSERT INTO trains (train_uid, service_date, origin_crs, destination_crs, \
                                 scheduled_departure) \
             VALUES ($1, $2, 'QWA', 'QWB', $3) RETURNING id",
        )
        .bind(uid)
        .bind(date())
        .bind(departure)
        .fetch_one(pool)
        .await
        .unwrap();
        let sub: i64 = sqlx::query_scalar(
            "INSERT INTO train_subscriptions (user_id, trains_id, service_date, pin_origin_crs, \
                                              pin_destination_crs, pin_scheduled_departure) \
             VALUES ($1, $2, $3, 'QWA', 'QWB', $4) RETURNING id",
        )
        .bind(USER)
        .bind(trains_id)
        .bind(date())
        .bind(departure)
        .fetch_one(pool)
        .await
        .unwrap();
        let leg: i64 = sqlx::query_scalar(
            "INSERT INTO journey_legs (journey_id, leg_order, origin_crs, destination_crs, \
                                       service_date, train_subscription_id) \
             VALUES ($1, $2, 'QWA', 'QWB', $3, $4) RETURNING id",
        )
        .bind(journey_id)
        .bind(order)
        .bind(date())
        .bind(sub)
        .fetch_one(pool)
        .await
        .unwrap();
        (leg, trains_id)
    }

    async fn unmatched_leg(pool: &PgPool, journey_id: i64, order: i32) -> i64 {
        sqlx::query_scalar(
            "INSERT INTO journey_legs (journey_id, leg_order, origin_crs, destination_crs, \
                                       service_date) \
             VALUES ($1, $2, 'QWA', 'QWB', $3) RETURNING id",
        )
        .bind(journey_id)
        .bind(order)
        .bind(date())
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn untracked_leg(
        pool: &PgPool,
        journey_id: i64,
        order: i32,
        departure: DateTime<Utc>,
    ) -> i64 {
        sqlx::query_scalar(
            "INSERT INTO journey_legs (journey_id, leg_order, service_date, kind, match_mode, transit_mode, \
                                       origin_place, destination_place, planned_departure, \
                                       planned_arrival) \
             VALUES ($1, $2, $3, 'untracked', 'manual', 'walk', 'Home', 'Station', $4, $5) RETURNING id",
        )
        .bind(journey_id)
        .bind(order)
        .bind(date())
        .bind(departure)
        .bind(departure + Duration::minutes(10))
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// A revision `no` of the journey holding `legs` as
    /// `(source leg, kind, trains id, booked or planned departure)`.
    async fn revision(
        pool: &PgPool,
        journey_id: i64,
        no: i32,
        legs: &[(i64, &str, Option<i64>, DateTime<Utc>)],
    ) {
        let revision_id: i64 = sqlx::query_scalar(
            "INSERT INTO journey_revisions (journey_id, revision_no, reason, actor) \
             VALUES ($1, $2, CASE WHEN $2 = 1 THEN 'booked' ELSE 'replace' END, 'owner') \
             RETURNING id",
        )
        .bind(journey_id)
        .bind(no)
        .fetch_one(pool)
        .await
        .unwrap();
        for (position, (leg, kind, trains_id, at)) in legs.iter().enumerate() {
            let train = *kind == "train";
            sqlx::query(
                "INSERT INTO journey_revision_legs (revision_id, position, source_leg_id, kind, \
                                                    service_date, trains_id, booked_departure, \
                                                    planned_departure) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            )
            .bind(revision_id)
            .bind(i32::try_from(position).unwrap() + 1)
            .bind(leg)
            .bind(kind)
            .bind(date())
            .bind(trains_id)
            .bind(train.then_some(*at))
            .bind((!train).then_some(*at))
            .execute(pool)
            .await
            .unwrap();
        }
    }

    async fn cancel(pool: &PgPool, trains_id: i64) {
        sqlx::query("INSERT INTO train_current_state (trains_id, status) VALUES ($1, 'cancelled')")
            .bind(trains_id)
            .execute(pool)
            .await
            .unwrap();
    }

    async fn open_break(pool: &PgPool, journey_id: i64, fit: &str, departure: DateTime<Utc>) {
        let suggestions = serde_json::json!({
            "suggestions": [{"id": "s1", "departure": departure, "fit": fit}],
        });
        sqlx::query(
            "INSERT INTO journey_breaks (journey_id, user_id, cause, fault, severity, \
                                         break_position, fingerprint, suggestions) \
             VALUES ($1, $2, 'cancelled', 'operator', 3, 1, 'f', $3)",
        )
        .bind(journey_id)
        .bind(USER)
        .bind(suggestions)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn leave_by(
        pool: &PgPool,
        journey_id: i64,
    ) -> Option<(DateTime<Utc>, DateTime<Utc>, String, i64)> {
        sqlx::query_as(&format!(
            "SELECT lb.leave_by_at, lb.departure, lb.basis, lb.leg_id FROM journeys j \
             LEFT JOIN LATERAL ({}) lb ON TRUE WHERE j.id = $1 AND lb.leave_by_at IS NOT NULL",
            leave_by_sql("j")
        ))
        .bind(journey_id)
        .fetch_optional(pool)
        .await
        .unwrap()
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn a_train_first_leg_is_its_booked_departure_less_the_buffer(pool: PgPool) {
        let j = journey(&pool, Some(22)).await;
        let booked = london(date(), 8, 42);
        let (leg, trains_id) = train_leg(&pool, j, 1, "Q10001", booked).await;
        train_leg(&pool, j, 2, "Q10002", london(date(), 9, 30)).await;
        // No revision yet (an occurrence the notifier minted): the pin.
        assert_eq!(
            leave_by(&pool, j).await,
            Some((london(date(), 8, 20), booked, BASIS_BOOKED.to_string(), leg))
        );
        // The revision's booked time wins over the pin.
        revision(
            &pool,
            j,
            1,
            &[(leg, "train", Some(trains_id), london(date(), 8, 40))],
        )
        .await;
        assert_eq!(leave_by(&pool, j).await.unwrap().0, london(date(), 8, 18));
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn no_buffer_or_no_timed_leg_has_no_leave_by(pool: PgPool) {
        let none = journey(&pool, None).await;
        train_leg(&pool, none, 1, "Q10003", london(date(), 8, 42)).await;
        assert_eq!(leave_by(&pool, none).await, None);

        let untimed = journey(&pool, Some(10)).await;
        unmatched_leg(&pool, untimed, 1).await;
        assert_eq!(leave_by(&pool, untimed).await, None);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn an_untracked_first_leg_sets_it(pool: PgPool) {
        let j = journey(&pool, Some(5)).await;
        let walk = untracked_leg(&pool, j, 1, london(date(), 8, 10)).await;
        train_leg(&pool, j, 2, "Q10004", london(date(), 8, 42)).await;
        assert_eq!(
            leave_by(&pool, j).await,
            Some((
                london(date(), 8, 5),
                london(date(), 8, 10),
                BASIS_BOOKED.to_string(),
                walk
            ))
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn an_unmatched_first_leg_is_passed_over(pool: PgPool) {
        let j = journey(&pool, Some(10)).await;
        unmatched_leg(&pool, j, 1).await;
        let (leg, _) = train_leg(&pool, j, 2, "Q10005", london(date(), 9, 0)).await;
        let lb = leave_by(&pool, j).await.unwrap();
        assert_eq!((lb.0, lb.3), (london(date(), 8, 50), leg));
    }

    async fn set_plan(
        pool: &PgPool,
        journey_id: i64,
        (status, reason, accepted): (&str, &str, Option<u8>),
        departure: DateTime<Utc>,
        train_uid: &str,
    ) {
        let plan = serde_json::json!({
            "reason": reason,
            "choice": {
                "departure": departure, "arrival": departure, "changeCount": 0,
                "legs": [{"kind": "train", "trainUid": train_uid, "serviceDate": date(),
                          "originCrs": "QWA", "departure": departure,
                          "arrival": departure}],
            },
            "alternatives": [],
            "sweepLegIds": [],
            "acceptedIndex": accepted,
        });
        sqlx::query(
            "UPDATE journeys SET regular_plan = $2, regular_plan_status = $3 WHERE id = $1",
        )
        .bind(journey_id)
        .bind(plan)
        .bind(status)
        .execute(pool)
        .await
        .unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn an_unpicked_train_takes_the_suggested_usual_trains_departure(pool: PgPool) {
        let j = journey(&pool, Some(15)).await;
        let leg = unmatched_leg(&pool, j, 1).await;
        let usual = london(date(), 7, 50);
        // No plan yet: nothing.
        assert_eq!(leave_by(&pool, j).await, None);
        set_plan(&pool, j, ("suggested", "usual", None), usual, "Q20001").await;
        assert_eq!(
            leave_by(&pool, j).await,
            Some((london(date(), 7, 35), usual, BASIS_USUAL.to_string(), leg))
        );
        // The same operator a few minutes off is the closest, not the usual.
        set_plan(
            &pool,
            j,
            ("suggested", "same_operator", None),
            usual,
            "Q20001",
        )
        .await;
        assert_eq!(leave_by(&pool, j).await.unwrap().2, BASIS_CLOSEST);
        // The sweep refreshed the suggestion to a moved train: it follows.
        let moved = london(date(), 7, 40);
        set_plan(&pool, j, ("suggested", "usual", None), moved, "Q20001").await;
        assert_eq!(leave_by(&pool, j).await.unwrap().0, london(date(), 7, 25));
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn only_a_suggested_usual_train_gives_a_usual_leave_by(pool: PgPool) {
        let j = journey(&pool, Some(15)).await;
        unmatched_leg(&pool, j, 1).await;
        let at = london(date(), 7, 50);
        // The day's best (the usual train doesn't run, and nothing is near
        // it), pending, no service, committed or accepted: none.
        set_plan(&pool, j, ("suggested", "best", None), at, "Q20002").await;
        assert_eq!(leave_by(&pool, j).await, None);
        for status in ["pending", "no_service", "committed"] {
            set_plan(&pool, j, (status, "usual", None), at, "Q20002").await;
            assert_eq!(leave_by(&pool, j).await, None, "{status}");
        }
        set_plan(&pool, j, ("suggested", "usual", Some(0)), at, "Q20002").await;
        assert_eq!(leave_by(&pool, j).await, None);
        // No buffer: none.
        let unbuffered = journey(&pool, None).await;
        unmatched_leg(&pool, unbuffered, 1).await;
        set_plan(
            &pool,
            unbuffered,
            ("suggested", "usual", None),
            at,
            "Q20002",
        )
        .await;
        assert_eq!(leave_by(&pool, unbuffered).await, None);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn a_retimed_suggested_train_uses_its_timetabled_departure(pool: PgPool) {
        let j = journey(&pool, Some(15)).await;
        unmatched_leg(&pool, j, 1).await;
        // The plan was made against the old 07:50; the timetable now says
        // 07:53 (a retime inside the planner's window) before the sweep
        // has re-planned.
        set_plan(
            &pool,
            j,
            ("suggested", "usual", None),
            london(date(), 7, 50),
            "Q20006",
        )
        .await;
        assert_eq!(leave_by(&pool, j).await.unwrap().1, london(date(), 7, 50));
        sqlx::query(
            "INSERT INTO schedule_destination_departures (service_date, destination_crs, \
                 scheduled, train_uid, origin_crs, public_departure) \
             VALUES ($1, 'QWB', '07:52:30', 'Q20006', 'QWA', '07:53')",
        )
        .bind(date())
        .execute(&pool)
        .await
        .unwrap();
        let lb = leave_by(&pool, j).await.unwrap();
        assert_eq!(
            (lb.0, lb.1, lb.2),
            (
                london(date(), 7, 38),
                london(date(), 7, 53),
                BASIS_USUAL.to_string()
            )
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn a_cancelled_usual_train_has_no_usual_leave_by(pool: PgPool) {
        let j = journey(&pool, Some(15)).await;
        unmatched_leg(&pool, j, 1).await;
        let at = london(date(), 7, 50);
        set_plan(&pool, j, ("suggested", "usual", None), at, "Q20003").await;
        assert!(leave_by(&pool, j).await.is_some());
        let trains_id: i64 = sqlx::query_scalar(
            "INSERT INTO trains (train_uid, service_date, origin_crs, destination_crs, \
                                 scheduled_departure) \
             VALUES ('Q20003', $1, 'QWA', 'QWB', $2) RETURNING id",
        )
        .bind(date())
        .bind(at)
        .fetch_one(&pool)
        .await
        .unwrap();
        cancel(&pool, trains_id).await;
        assert_eq!(leave_by(&pool, j).await, None);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn tracking_a_train_replaces_the_usual_leave_by_with_the_booked_one(pool: PgPool) {
        let j = journey(&pool, Some(15)).await;
        let unmatched = unmatched_leg(&pool, j, 1).await;
        set_plan(
            &pool,
            j,
            ("suggested", "usual", None),
            london(date(), 7, 50),
            "Q20004",
        )
        .await;
        assert_eq!(leave_by(&pool, j).await.unwrap().2, BASIS_USUAL);
        // The owner tracks a different train: its own time, basis booked,
        // even while the plan is still marked suggested.
        sqlx::query("DELETE FROM journey_legs WHERE id = $1")
            .bind(unmatched)
            .execute(&pool)
            .await
            .unwrap();
        let (leg, _) = train_leg(&pool, j, 1, "Q20005", london(date(), 8, 5)).await;
        let lb = leave_by(&pool, j).await.unwrap();
        assert_eq!(
            (lb.0, lb.2, lb.3),
            (london(date(), 7, 50), BASIS_BOOKED.to_string(), leg)
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn the_latest_revision_is_the_plan(pool: PgPool) {
        let j = journey(&pool, Some(20)).await;
        // Revision 1 booked the 08:42; an accepted replan (revision 2) put
        // the 08:27 on a new leg, which is now the journey's only leg.
        let (old_leg, old_train) = train_leg(&pool, j, 1, "Q10006", london(date(), 8, 42)).await;
        revision(
            &pool,
            j,
            1,
            &[(old_leg, "train", Some(old_train), london(date(), 8, 42))],
        )
        .await;
        sqlx::query("DELETE FROM journey_legs WHERE id = $1")
            .bind(old_leg)
            .execute(&pool)
            .await
            .unwrap();
        let (new_leg, new_train) = train_leg(&pool, j, 1, "Q10007", london(date(), 8, 27)).await;
        revision(
            &pool,
            j,
            2,
            &[(new_leg, "train", Some(new_train), london(date(), 8, 27))],
        )
        .await;
        let lb = leave_by(&pool, j).await.unwrap();
        assert_eq!((lb.0, lb.3), (london(date(), 8, 7), new_leg));
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn a_revision_for_another_train_is_not_used(pool: PgPool) {
        let j = journey(&pool, Some(10)).await;
        let (leg, _) = train_leg(&pool, j, 1, "Q10008", london(date(), 9, 0)).await;
        // A snapshot taken before the leg's train was changed outside a
        // planning edit: it names another train, so the pin is used.
        revision(
            &pool,
            j,
            1,
            &[(leg, "train", Some(-1), london(date(), 7, 0))],
        )
        .await;
        assert_eq!(leave_by(&pool, j).await.unwrap().0, london(date(), 8, 50));
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn lateness_never_moves_it(pool: PgPool) {
        let j = journey(&pool, Some(22)).await;
        let (_, trains_id) = train_leg(&pool, j, 1, "Q10009", london(date(), 8, 42)).await;
        sqlx::query(
            "INSERT INTO train_current_state (trains_id, status, delay_minutes) \
             VALUES ($1, 'en_route', 6)",
        )
        .bind(trains_id)
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(leave_by(&pool, j).await.unwrap().0, london(date(), 8, 20));
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn a_cancelled_first_train_needs_a_covered_suggestion(pool: PgPool) {
        let j = journey(&pool, Some(22)).await;
        let (leg, trains_id) = train_leg(&pool, j, 1, "Q10010", london(date(), 8, 42)).await;
        cancel(&pool, trains_id).await;
        assert_eq!(leave_by(&pool, j).await, None, "nothing suggested");

        open_break(&pool, j, "check", london(date(), 8, 57)).await;
        assert_eq!(leave_by(&pool, j).await, None, "suggestion not covered");

        sqlx::query("UPDATE journey_breaks SET status = 'dismissed' WHERE journey_id = $1")
            .bind(j)
            .execute(&pool)
            .await
            .unwrap();
        open_break(&pool, j, "covered", london(date(), 8, 57)).await;
        assert_eq!(
            leave_by(&pool, j).await,
            Some((
                london(date(), 8, 35),
                london(date(), 8, 57),
                BASIS_SUGGESTION.to_string(),
                leg
            ))
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn the_timetable_is_read_in_london_time_across_the_clock_changes(pool: PgPool) {
        // The last Sundays in March and October 2050. With no revision the
        // train's 08:42 comes from the timetable (the pin is wrong on
        // purpose), read as London time on each side of the change.
        for (n, day) in [(1, (2050, 3, 27)), (2, (2050, 10, 30))] {
            let day = NaiveDate::from_ymd_opt(day.0, day.1, day.2).unwrap();
            let uid = format!("Q2000{n}");
            let j = journey(&pool, Some(22)).await;
            let (leg, trains_id) = train_leg(
                &pool,
                j,
                1,
                &uid,
                Utc.with_ymd_and_hms(2050, 1, 1, 0, 0, 0).unwrap(),
            )
            .await;
            sqlx::query("UPDATE trains SET service_date = $1 WHERE id = $2")
                .bind(day)
                .bind(trains_id)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("UPDATE journey_legs SET service_date = $1 WHERE id = $2")
                .bind(day)
                .bind(leg)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query(
                "INSERT INTO schedule_destination_departures (service_date, destination_crs, \
                     scheduled, train_uid, origin_crs, public_departure) \
                 VALUES ($1, 'QWB', '08:41:30', $2, 'QWA', '08:42')",
            )
            .bind(day)
            .bind(&uid)
            .execute(&pool)
            .await
            .unwrap();
            assert_eq!(
                leave_by(&pool, j).await.unwrap().0,
                london(day, 8, 20),
                "{day}"
            );
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn the_buffer_and_the_lead_are_range_checked(pool: PgPool) {
        crate::test_support::seed_user(&pool, USER).await;
        for sql in [
            "INSERT INTO journeys (user_id, station_buffer_minutes) VALUES ($1, 121)",
            "INSERT INTO journeys (user_id, station_buffer_minutes) VALUES ($1, -1)",
            "INSERT INTO journey_templates (user_id, days_of_week, station_buffer_minutes) \
             VALUES ($1, 31, 121)",
            "INSERT INTO notification_preferences (user_id, leave_by_lead_minutes) VALUES ($1, 6)",
        ] {
            let err = sqlx::query(sql)
                .bind(USER)
                .execute(&pool)
                .await
                .unwrap_err();
            assert!(err.to_string().contains("check constraint"), "{sql}: {err}");
        }
        sqlx::query("INSERT INTO journeys (user_id, station_buffer_minutes) VALUES ($1, 120)")
            .bind(USER)
            .execute(&pool)
            .await
            .unwrap();
        let prefs: (bool, bool, i16, bool) = sqlx::query_as(
            "INSERT INTO notification_preferences (user_id) VALUES ($1) \
             RETURNING leave_by, leave_by_quiet_hours, leave_by_lead_minutes, morning_summary",
        )
        .bind(USER)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(prefs, (true, true, 0, false));
    }
}
