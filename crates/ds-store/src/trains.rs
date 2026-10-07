//! Train identity and per-stop state: `trains.rs`'s shared functions
//! (`find_or_create_train*`, `mark_train*_resolved*`,
//! `destination_crs_for_train*`, `bind_subscription_unless_other_train`),
//! `stop_delay.rs`, `stop_live_status.rs`, `eta_blend::london_to_utc`,
//! and the `JourneyStop`/`StopStatus`/`StopTimetable` types (which
//! `api`'s `journey.rs` re-exports).

pub mod stop_delay;
pub mod stop_live_status;
pub mod types;

use std::collections::HashMap;

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use sqlx::PgPool;

/// Finds or creates the `trains` row for `(train_uid, service_date)`,
/// returning its surrogate `id`. `DO UPDATE` (never `DO NOTHING`) is what
/// makes `RETURNING id` reliable on a re-run against a row that already
/// exists -- safe to call more than once for the same identity.
///
/// Generic over `E: PgExecutor` (rather than `&PgPool`) -- same reason as
/// `train_tracking::create_pin`'s own doc comment: it lets
/// `train_tracking::flip_legacy_resolution` (2026-09-26 review, Medium
/// finding 6) call this with `&mut *tx` from inside its own transaction, so
/// a downstream `mark_train_resolved` failure (e.g. a
/// `trains_train_id_service_date` unique-index collision) rolls back
/// alongside the `resolution_status = 'resolved'` write that transaction
/// also makes, rather than leaving that write committed on its own with
/// nothing to show for it. Every standalone caller keeps passing a bare
/// `&PgPool`/`&pool` unchanged -- `&PgPool` implements `PgExecutor<'_>` too.
pub async fn find_or_create_train<'c, E>(
    executor: E,
    train_uid: &str,
    service_date: NaiveDate,
) -> anyhow::Result<i64>
where
    E: sqlx::PgExecutor<'c>,
{
    // Read first, insert only when the row is not visible (DB review part 2,
    // DB2-7): the old unconditional `ON CONFLICT DO UPDATE SET train_uid =
    // EXCLUDED.train_uid` rewrote the row (a dead tuple and a row lock) on
    // every call, i.e. for every train in every TRUST batch. The `DO UPDATE`
    // is kept on the insert branch only for the race where another
    // transaction commits the row after this statement's snapshot: it then
    // locks and returns that row rather than returning nothing.
    let row: (i64,) = sqlx::query_as(
        "WITH existing AS ( \
             SELECT id FROM trains WHERE train_uid = $1 AND service_date = $2 \
         ), inserted AS ( \
             INSERT INTO trains (train_uid, service_date) \
             SELECT $1, $2 WHERE NOT EXISTS (SELECT 1 FROM existing) \
             ON CONFLICT (train_uid, service_date) DO UPDATE SET train_uid = EXCLUDED.train_uid \
             RETURNING id \
         ) \
         SELECT id FROM existing UNION ALL SELECT id FROM inserted",
    )
    .bind(train_uid)
    .bind(service_date)
    .fetch_one(executor)
    .await?;
    Ok(row.0)
}

/// Batch-shaped sibling of [`find_or_create_train`] -- one multi-row
/// `INSERT ... SELECT * FROM UNNEST(...) ... ON CONFLICT DO UPDATE
/// ... RETURNING` covering every DISTINCT `(train_uid, service_date)` pair
/// in `pairs`, instead of one single-row `INSERT` per pair. Existing pairs
/// come back from the read half, new ones from the insert's `RETURNING`
/// (whose `DO UPDATE` also covers a concurrently-committed row), exactly
/// like the single-row version -- callers can rely on the returned map
/// having exactly one entry per element of `pairs` (never fewer).
///
/// `pairs` MUST already be deduplicated by the caller: passing the same
/// `(train_uid, service_date)` pair twice in one call is a caller bug,
/// not something this function guards against -- `ingest_shared_movement`'s
/// whole reason for calling this at all is to have already collapsed a
/// batch's repeated identities down to their distinct pairs before this
/// point.
pub async fn find_or_create_trains_batch(
    pool: &PgPool,
    pairs: &[(String, NaiveDate)],
) -> anyhow::Result<HashMap<(String, NaiveDate), i64>> {
    if pairs.is_empty() {
        return Ok(HashMap::new());
    }
    let train_uids: Vec<&str> = pairs.iter().map(|(uid, _)| uid.as_str()).collect();
    let service_dates: Vec<NaiveDate> = pairs.iter().map(|(_, date)| *date).collect();

    // Same read-first shape as `find_or_create_train`: only pairs not
    // already visible are inserted, so a batch of known trains writes
    // nothing.
    let rows: Vec<(String, NaiveDate, i64)> = sqlx::query_as(
        "WITH input AS ( \
             SELECT * FROM UNNEST($1::text[], $2::date[]) AS u(train_uid, service_date) \
         ), existing AS ( \
             SELECT t.train_uid, t.service_date, t.id FROM trains t \
             JOIN input i ON i.train_uid = t.train_uid AND i.service_date = t.service_date \
         ), inserted AS ( \
             INSERT INTO trains (train_uid, service_date) \
             SELECT i.train_uid, i.service_date FROM input i \
             WHERE NOT EXISTS ( \
                 SELECT 1 FROM existing e \
                 WHERE e.train_uid = i.train_uid AND e.service_date = i.service_date \
             ) \
             ON CONFLICT (train_uid, service_date) DO UPDATE SET train_uid = EXCLUDED.train_uid \
             RETURNING train_uid, service_date, id \
         ) \
         SELECT train_uid, service_date, id FROM existing \
         UNION ALL SELECT train_uid, service_date, id FROM inserted",
    )
    .bind(&train_uids)
    .bind(&service_dates)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(train_uid, service_date, id)| ((train_uid, service_date), id))
        .collect())
}

/// Same idempotent shape as [`find_or_create_train`], but also mirrors a
/// successful schedule match's own result onto the shared row (Step A's
/// "`attempt_schedule_match`... mirror their result onto the shared trains
/// row too"). `COALESCE`d against the existing value on every schedule
/// column so a second subscriber's independent match against the same
/// physical train never clobbers data an earlier one already wrote.
#[expect(
    clippy::too_many_arguments,
    reason = "each argument is an independent input from the single caller; a struct would only wrap them"
)]
pub async fn find_or_create_train_with_schedule_match(
    pool: &PgPool,
    train_uid: &str,
    service_date: NaiveDate,
    // The SCHEDULE's own origin (first calling point), never a pin's
    // boarding station (DB2-8). `None` leaves the column for a later writer.
    origin_crs: Option<&str>,
    scheduled_departure: Option<DateTime<Utc>>,
    destination_crs: Option<&str>,
    matched_line_id: &str,
    calling_points: &serde_json::Value,
    // Darwin/LDBWS's own explicit per-calling-point skip snapshot for THIS
    // schedule match's caller (`common::TrackPinRequest.skipped_stations`,
    // threaded via `schedule_matching::attempt_schedule_match`'s own
    // `pin_skipped_stations` param) -- feeds
    // `journey::build_journey_stops`' Darwin-explicit `SkipSource::Darwin`/
    // `Both` signal (see that module's `StopStatus`). Merged onto the
    // shared row the same "never clobber a real value already written"
    // way every other schedule column here is, EXCEPT this one has no
    // `NULL` to `COALESCE` against (`trains.skipped_stations` is `NOT
    // NULL DEFAULT '{}'`) -- so an empty incoming array explicitly keeps
    // whatever the row already has instead, via the `CASE` below, rather
    // than an unconditional overwrite that would let a second subscriber
    // with no departure-board pin at all (e.g. the NR-primary path,
    // `attempt_schedule_match_for_shared_train`, which always passes `&[]`
    // here) silently erase a first subscriber's real Darwin snapshot.
    skipped_stations: &[String],
    // Darwin/LDBWS's own origin-platform snapshot for THIS schedule match's
    // caller (`common::TrackPinRequest.platform`/`planned_platform`,
    // threaded via `schedule_matching::attempt_schedule_match`'s own
    // `pin_platform`/`pin_planned_platform` params) -- feeds the origin
    // `JourneyStop`'s `platform`/`plannedPlatform`/`platformChanged`
    // (`journey::build_journey_stops`). Merged onto the shared row via a
    // plain `COALESCE` (unlike `skipped_stations`' `CASE`): these are
    // nullable scalars, so `COALESCE(existing, new)` already has the same
    // "never clobber a real value already written" property the `CASE`
    // exists to give the non-nullable array a NULL-free default for.
    platform: Option<&str>,
    planned_platform: Option<&str>,
) -> anyhow::Result<i64> {
    let row: (i64,) = sqlx::query_as(
        "INSERT INTO trains \
            (train_uid, service_date, origin_crs, scheduled_departure, destination_crs, \
             matched_line_id, calling_points, schedule_matched_at, skipped_stations, \
             platform, planned_platform) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), $8, $9, $10) \
         ON CONFLICT (train_uid, service_date) DO UPDATE SET \
            train_uid            = EXCLUDED.train_uid, \
            origin_crs           = COALESCE(trains.origin_crs, EXCLUDED.origin_crs), \
            scheduled_departure  = COALESCE(trains.scheduled_departure, EXCLUDED.scheduled_departure), \
            destination_crs      = COALESCE(trains.destination_crs, EXCLUDED.destination_crs), \
            matched_line_id      = COALESCE(trains.matched_line_id, EXCLUDED.matched_line_id), \
            calling_points       = COALESCE(trains.calling_points, EXCLUDED.calling_points), \
            schedule_matched_at  = COALESCE(trains.schedule_matched_at, EXCLUDED.schedule_matched_at), \
            skipped_stations     = CASE WHEN cardinality(trains.skipped_stations) > 0 \
                                        THEN trains.skipped_stations \
                                        ELSE EXCLUDED.skipped_stations END, \
            platform              = COALESCE(trains.platform, EXCLUDED.platform), \
            planned_platform      = COALESCE(trains.planned_platform, EXCLUDED.planned_platform) \
         RETURNING id",
    )
    .bind(train_uid)
    .bind(service_date)
    .bind(origin_crs)
    .bind(scheduled_departure)
    .bind(destination_crs)
    .bind(matched_line_id)
    .bind(calling_points)
    .bind(skipped_stations)
    .bind(platform)
    .bind(planned_platform)
    .fetch_one(pool)
    .await?;
    Ok(row.0)
}

/// Points subscription `subscription_id` at `trains_id` for the backlog
/// CRS+time heuristic (DB2-5), but only when that cannot overwrite a
/// DIFFERENT train: the subscription must be unbound, already bound to
/// `trains_id`, or bound to a row with the same `train_uid` (a same-uid,
/// other-date row, which the heuristic is allowed to correct). The check is
/// in the UPDATE itself, so a schedule match that binds the subscription
/// between the caller's own contradiction precheck and this write is seen
/// (READ COMMITTED re-evaluates the WHERE on the new row version).
///
/// Returns `false` when nothing was written: the subscription is gone, or it
/// is now bound to another train. The caller must then stop rather than
/// replay another train's history onto it.
pub async fn bind_subscription_unless_other_train(
    pool: &PgPool,
    subscription_id: i64,
    trains_id: i64,
) -> anyhow::Result<bool> {
    let result = sqlx::query(
        "UPDATE train_subscriptions ts SET trains_id = $2 \
         WHERE ts.id = $1 \
           AND (ts.trains_id IS NULL \
                OR ts.trains_id = $2 \
                OR EXISTS (SELECT 1 FROM trains cur, trains new \
                            WHERE cur.id = ts.trains_id AND new.id = $2 \
                              AND UPPER(cur.train_uid) = UPPER(new.train_uid)))",
    )
    .bind(subscription_id)
    .bind(trains_id)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Mirrors a live-TRUST resolution onto the shared row's own
/// Live-TRUST-derived columns (`train_id`, `resolved_at`) -- never
/// clobbers `train_uid`/schedule columns, which this function doesn't
/// touch at all. Safe to call more than once for the same `trains_id`
/// (a later Movement re-supplying the same `train_id` is a harmless
/// no-op overwrite of identical values).
///
/// Generic over `E: PgExecutor` (rather than `&PgPool`) -- see
/// [`find_or_create_train`]'s own doc comment: this is the write whose
/// possible `trains_train_id_service_date` unique-index collision must roll
/// back together with `flip_legacy_resolution`'s `resolution_status`
/// write, not land (or fail) on its own.
#[expect(
    clippy::similar_names,
    reason = "the similar names are distinct domain terms"
)]
pub async fn mark_train_resolved<'c, E>(
    executor: E,
    trains_id: i64,
    train_id: &str,
) -> anyhow::Result<()>
where
    E: sqlx::PgExecutor<'c>,
{
    // No-op when `train_id` is already this value (DB2-7): every movement
    // for a resolved train used to rewrite the row, and `resolved_at` now
    // records when the row was resolved to this `train_id`, not the time of
    // its latest movement.
    sqlx::query(
        "UPDATE trains SET train_id = $2, resolved_at = NOW() \
         WHERE id = $1 AND train_id IS DISTINCT FROM $2",
    )
    .bind(trains_id)
    .bind(train_id)
    .execute(executor)
    .await?;
    Ok(())
}

/// Batch-shaped sibling of [`mark_train_resolved`] -- one `UPDATE ...
/// FROM UNNEST(...)` covering every `(trains_id, train_id)` pair in
/// `pairs`, instead of one single-row `UPDATE` per pair.
///
/// `pairs` MUST carry at most one entry per `trains_id` (the caller's
/// responsibility, same as [`find_or_create_trains_batch`]'s dedup
/// contract) -- if a batch has more than one event resolving to the same
/// `trains_id`, the caller must already have collapsed those down to the
/// LAST one in event order, matching what a sequential loop of
/// [`mark_train_resolved`] calls would leave behind (each call plainly
/// overwrites `train_id`, so only the final call's value survives).
#[expect(
    clippy::similar_names,
    reason = "the similar names are distinct domain terms"
)]
pub async fn mark_trains_resolved_batch(
    pool: &PgPool,
    pairs: &[(i64, String)],
) -> anyhow::Result<()> {
    if pairs.is_empty() {
        return Ok(());
    }
    let trains_ids: Vec<i64> = pairs.iter().map(|(id, _)| *id).collect();
    let train_ids: Vec<&str> = pairs
        .iter()
        .map(|(_, train_id)| train_id.as_str())
        .collect();

    sqlx::query(
        "UPDATE trains AS t SET train_id = u.train_id, resolved_at = NOW() \
         FROM UNNEST($1::bigint[], $2::text[]) AS u(trains_id, train_id) \
         WHERE t.id = u.trains_id AND t.train_id IS DISTINCT FROM u.train_id",
    )
    .bind(&trains_ids)
    .bind(&train_ids)
    .execute(pool)
    .await?;
    Ok(())
}

// Step B's one-off backfill job (`backfill_trains_id_for_resolved_rows`,
// which used to live here) already ran, exactly once, against every real
// environment -- Task 22's own Step 1 dry-run count of 0 confirms nothing
// was left for it to do. Its own `SELECT id, train_uid, service_date FROM
// tracked_trains WHERE train_uid IS NOT NULL ...` read a column
// (`tracked_trains.train_uid`) Task 22's migration drops entirely, so it
// can never run again; removed here, alongside its own one-off
// `run_step_b_backfill_of_existing_resolved_rows` test below, rather than
// left as permanently-broken dead code.

/// The shared `trains` row's own known final/terminus CRS
/// (`trains.destination_crs`) for one `trains_id`, or `None` if the row
/// doesn't exist yet or no schedule has ever matched it. Feeds
/// `trust_event_backlog_match::replay_backlog_history`'s confirmed-terminus-
/// ARRIVAL detection (`trust_schema::journey::apply_movement`'s
/// `destination_crs` param) -- a read-only precheck, same posture as
/// `shared_train_enrichment_state` just below: it never creates a row, so a
/// train with no `trains` row at all simply reports "unknown" rather than
/// conjuring one into existence.
pub async fn destination_crs_for_train(
    pool: &PgPool,
    train_uid: &str,
    service_date: NaiveDate,
) -> anyhow::Result<Option<String>> {
    let row: Option<(Option<String>,)> = sqlx::query_as(
        "SELECT destination_crs FROM trains WHERE train_uid = $1 AND service_date = $2",
    )
    .bind(train_uid)
    .bind(service_date)
    .fetch_optional(pool)
    .await?;
    Ok(row.and_then(|(destination_crs,)| destination_crs))
}

/// Batch-shaped sibling of [`destination_crs_for_train`] -- one SELECT
/// covering every DISTINCT `trains_id` in `trains_ids`, rather than one
/// SELECT per id, mirroring `trust_event_backlog::fetch_previous_derived_states_batch`'s
/// own shape. A `trains_id` with no known `destination_crs` (no schedule
/// match yet, or the row doesn't exist) is simply absent from the returned
/// map -- callers must treat a missing key the same as `None`.
pub async fn destination_crs_for_trains_batch(
    pool: &PgPool,
    trains_ids: &[i64],
) -> anyhow::Result<HashMap<i64, String>> {
    if trains_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let rows: Vec<(i64, Option<String>)> =
        sqlx::query_as("SELECT id, destination_crs FROM trains WHERE id = ANY($1)")
            .bind(trains_ids)
            .fetch_all(pool)
            .await?;
    Ok(rows
        .into_iter()
        .filter_map(|(id, destination_crs)| destination_crs.map(|crs| (id, crs)))
        .collect())
}

/// Resolves a Darwin wall-clock time to the instant it names. Darwin
/// publishes Europe/London local times, not UTC -- building the
/// `DateTime<Utc>` directly from `HH:MM` made every `darwin-estimated` ETA
/// exactly an hour late for the ~7 months of British Summer Time, i.e. most
/// of the year, and the error was invisible in winter.
///
/// Same `LocalResult` handling as
/// `crates/poller-tfl/src/dlr/timetable.rs::london_to_utc`, and for the same
/// reason: a departure board really does carry 01:00-01:59 times, which are
/// the ones that occur twice on the autumn clock change and not at all on
/// the spring one. The ambiguous hour takes the first (BST) occurrence, and
/// a nonexistent local time yields `None` so the caller simply leaves TRUST's
/// own ETA in place -- this whole overlay is best-effort, so declining to
/// guess costs nothing. (The aggregator's variant panics on those cases
/// instead, but it only ever resolves local 02:00, which is never ambiguous.)
///
/// **Not used for [`resolve_london_time_near`]'s own ambiguous case as of
/// the L12 fix (2026-09-26 review)** -- that function has a real `anchor`
/// to disambiguate against and picks the genuinely nearer candidate itself
/// rather than reaching for this function's fixed "always BST" rule (see
/// its own doc comment). This function's fixed rule remains exactly right
/// for every OTHER caller in this crate (`journey.rs`, `reconciliation.rs`,
/// `schedule_matching.rs`, `routes::train`), none of which have an anchor
/// instant of their own to pick a nearer candidate against -- they resolve
/// a bare CIF/schedule date+time with no better information available,
/// same posture this function has always taken.
pub fn london_to_utc(naive: chrono::NaiveDateTime) -> Option<DateTime<Utc>> {
    match chrono_tz::Europe::London.from_local_datetime(&naive) {
        chrono::LocalResult::Single(dt) => Some(dt.with_timezone(&Utc)),
        chrono::LocalResult::Ambiguous(earliest, _) => Some(earliest.with_timezone(&Utc)),
        chrono::LocalResult::None => None,
    }
}

#[cfg(test)]
#[expect(
    clippy::similar_names,
    reason = "test code: paired test values share names"
)]
mod db_tests {
    use super::*;
    use crate::test_support::{connect, xmin};

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                find_or_create_train_returns_the_same_id_on_a_repeat_call -- --ignored --test-threads=1`"]
    async fn find_or_create_train_returns_the_same_id_on_a_repeat_call() {
        let pool = connect().await;
        let service_date: NaiveDate = "2026-09-06".parse().unwrap();

        let first = find_or_create_train(&pool, "TEST-TRAINS-UID-1", service_date)
            .await
            .expect("first find_or_create_train");
        let second = find_or_create_train(&pool, "TEST-TRAINS-UID-1", service_date)
            .await
            .expect("second find_or_create_train");
        assert_eq!(
            first, second,
            "the same (train_uid, service_date) must resolve to one row"
        );

        sqlx::query("DELETE FROM trains WHERE train_uid = 'TEST-TRAINS-UID-1'")
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                find_or_create_trains_batch_dedups_and_resolves_every_distinct_pair \
                -- --ignored --test-threads=1`"]
    async fn find_or_create_trains_batch_dedups_and_resolves_every_distinct_pair() {
        let pool = connect().await;
        let service_date: NaiveDate = "2026-09-06".parse().unwrap();

        // Seed one of the two identities up front, so this also proves the
        // batched call resolves a PRE-EXISTING row via its ON CONFLICT
        // branch, not just fresh inserts.
        let pre_existing = find_or_create_train(&pool, "TEST-TRAINS-BATCH-UID-1", service_date)
            .await
            .expect("seed one identity");

        let pairs = vec![
            ("TEST-TRAINS-BATCH-UID-1".to_string(), service_date),
            ("TEST-TRAINS-BATCH-UID-2".to_string(), service_date),
        ];
        let map = find_or_create_trains_batch(&pool, &pairs)
            .await
            .expect("find_or_create_trains_batch");

        assert_eq!(map.len(), 2, "one entry per distinct pair");
        assert_eq!(
            map[&("TEST-TRAINS-BATCH-UID-1".to_string(), service_date)],
            pre_existing,
            "a pre-existing identity must resolve to its EXISTING id, not a new row"
        );
        let second_id = map[&("TEST-TRAINS-BATCH-UID-2".to_string(), service_date)];
        assert_ne!(second_id, pre_existing);

        sqlx::query("DELETE FROM trains WHERE train_uid IN ($1, $2)")
            .bind("TEST-TRAINS-BATCH-UID-1")
            .bind("TEST-TRAINS-BATCH-UID-2")
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                mark_trains_resolved_batch_sets_train_id_for_every_pair -- --ignored --test-threads=1`"]
    async fn mark_trains_resolved_batch_sets_train_id_for_every_pair() {
        let pool = connect().await;
        let service_date: NaiveDate = "2026-09-06".parse().unwrap();
        let id_1 = find_or_create_train(&pool, "TEST-TRAINS-BATCH-RESOLVE-1", service_date)
            .await
            .expect("find_or_create_train");
        let id_2 = find_or_create_train(&pool, "TEST-TRAINS-BATCH-RESOLVE-2", service_date)
            .await
            .expect("find_or_create_train");

        mark_trains_resolved_batch(
            &pool,
            &[
                (id_1, "221800001".to_string()),
                (id_2, "221800002".to_string()),
            ],
        )
        .await
        .expect("mark_trains_resolved_batch");

        let (train_id_1,): (Option<String>,) =
            sqlx::query_as("SELECT train_id FROM trains WHERE id = $1")
                .bind(id_1)
                .fetch_one(&pool)
                .await
                .expect("read back trains row 1");
        assert_eq!(train_id_1, Some("221800001".to_string()));

        let (train_id_2,): (Option<String>,) =
            sqlx::query_as("SELECT train_id FROM trains WHERE id = $1")
                .bind(id_2)
                .fetch_one(&pool)
                .await
                .expect("read back trains row 2");
        assert_eq!(train_id_2, Some("221800002".to_string()));

        sqlx::query("DELETE FROM trains WHERE id IN ($1, $2)")
            .bind(id_1)
            .bind(id_2)
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                mark_train_resolved_sets_train_id_and_resolved_at -- --ignored --test-threads=1`"]
    async fn mark_train_resolved_sets_train_id_and_resolved_at() {
        let pool = connect().await;
        let service_date: NaiveDate = "2026-09-06".parse().unwrap();
        let trains_id = find_or_create_train(&pool, "TEST-TRAINS-UID-2", service_date)
            .await
            .expect("find_or_create_train");

        mark_train_resolved(&pool, trains_id, "221832406")
            .await
            .expect("mark_train_resolved");

        let (train_id, resolved_at): (Option<String>, Option<DateTime<Utc>>) =
            sqlx::query_as("SELECT train_id, resolved_at FROM trains WHERE id = $1")
                .bind(trains_id)
                .fetch_one(&pool)
                .await
                .expect("read back trains row");
        assert_eq!(train_id, Some("221832406".to_string()));
        assert!(resolved_at.is_some());

        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                find_or_create_train_with_schedule_match_never_clobbers_an_earlier_match -- --ignored --test-threads=1`"]
    async fn find_or_create_train_with_schedule_match_never_clobbers_an_earlier_match() {
        let pool = connect().await;
        let service_date: NaiveDate = "2026-09-06".parse().unwrap();
        let scheduled_departure: DateTime<Utc> = "2026-09-06T12:00:00Z".parse().unwrap();
        let calling_points = serde_json::json!(["PAD", "RDG"]);

        let first_id = find_or_create_train_with_schedule_match(
            &pool,
            "TEST-TRAINS-UID-3",
            service_date,
            Some("PAD"),
            Some(scheduled_departure),
            None,
            "line-a",
            &calling_points,
            &[],
            None,
            None,
        )
        .await
        .expect("first find_or_create_train_with_schedule_match");

        let second_id = find_or_create_train_with_schedule_match(
            &pool,
            "TEST-TRAINS-UID-3",
            service_date,
            Some("ZZZ"),
            Some(scheduled_departure),
            None,
            "line-b",
            &calling_points,
            &[],
            None,
            None,
        )
        .await
        .expect("second find_or_create_train_with_schedule_match");

        assert_eq!(
            first_id, second_id,
            "the same (train_uid, service_date) must resolve to one row"
        );

        let (origin_crs, matched_line_id): (Option<String>, Option<String>) =
            sqlx::query_as("SELECT origin_crs, matched_line_id FROM trains WHERE id = $1")
                .bind(first_id)
                .fetch_one(&pool)
                .await
                .expect("read back trains row");
        assert_eq!(
            origin_crs,
            Some("PAD".to_string()),
            "a later independent match must not clobber the first match's origin_crs"
        );
        assert_eq!(
            matched_line_id,
            Some("line-a".to_string()),
            "a later independent match must not clobber the first match's matched_line_id"
        );

        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(first_id)
            .execute(&pool)
            .await
            .ok();
    }

    /// Structural regression test for the `trains_train_id_service_date`
    /// partial unique index
    /// (`20260925222000_trains_train_id_service_date_unique.sql`), the
    /// schema-level backstop for today's (2026-09-25) CRS+time
    /// mismatching incident: two DIFFERENT `trains` rows (i.e. two
    /// different `train_uid`s) must never be able to share one TRUST
    /// `train_id` for the same `service_date` once both are resolved --
    /// that is exactly the bug class that let one subscription silently
    /// read another train's movements. Bypasses `mark_train_resolved` and
    /// writes the second row's `train_id` directly, so this test exercises
    /// the CONSTRAINT itself rather than any application-level guard (the
    /// `is_provable_identity_contradiction` veto in
    /// `trust_event_backlog_match.rs` / `trust-consumer::matching` is
    /// covered by its own tests already; this one proves the database
    /// still refuses the write even if every application-level guard were
    /// ever removed or had a bug).
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                two_trains_rows_cannot_share_a_train_id_and_service_date_once_resolved \
                -- --ignored --test-threads=1`"]
    async fn two_trains_rows_cannot_share_a_train_id_and_service_date_once_resolved() {
        let pool = connect().await;
        let service_date: NaiveDate = "2026-09-25".parse().unwrap();

        let first_id = find_or_create_train(&pool, "TEST-TRAINS-UNIQUE-UID-1", service_date)
            .await
            .expect("find_or_create_train (first)");
        let second_id = find_or_create_train(&pool, "TEST-TRAINS-UNIQUE-UID-2", service_date)
            .await
            .expect("find_or_create_train (second)");

        mark_train_resolved(&pool, first_id, "TEST-SHARED-TRAIN-ID")
            .await
            .expect("mark_train_resolved (first) must succeed -- no collision yet");

        // The second, DIFFERENT trains row claiming the SAME train_id for
        // the SAME service_date is exactly today's incident shape (a
        // London Northwestern pin's `trains` row and an Avanti service's
        // `trains` row both carrying one TRUST train_id) -- this must be
        // rejected by the database itself, not merely by application code.
        let result =
            sqlx::query("UPDATE trains SET train_id = $2, resolved_at = NOW() WHERE id = $1")
                .bind(second_id)
                .bind("TEST-SHARED-TRAIN-ID")
                .execute(&pool)
                .await;

        assert!(
            result.is_err(),
            "a second trains row must not be able to claim an already-resolved train_id for the \
             same service_date; the trains_train_id_service_date unique index should have \
             rejected this write, but it succeeded"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("trains_train_id_service_date"),
            "expected the unique index name in the constraint-violation error, got: {err}"
        );

        sqlx::query("DELETE FROM trains WHERE id IN ($1, $2)")
            .bind(first_id)
            .bind(second_id)
            .execute(&pool)
            .await
            .ok();
    }

    /// The other half of the same constraint: multiple schedule-matched-
    /// but-not-yet-TRUST-resolved `trains` rows (`train_id IS NULL`) for
    /// the SAME `service_date` must remain unaffected -- these are
    /// perfectly ordinary, expected rows (every service scheduled for a
    /// rail day that TRUST hasn't activated yet), and the partial index's
    /// whole point (`WHERE train_id IS NOT NULL`) is to never block them.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                multiple_null_train_id_rows_for_the_same_service_date_are_allowed -- --ignored --test-threads=1`"]
    async fn multiple_null_train_id_rows_for_the_same_service_date_are_allowed() {
        let pool = connect().await;
        let service_date: NaiveDate = "2026-09-25".parse().unwrap();

        let first_id = find_or_create_train(&pool, "TEST-TRAINS-NULL-UID-1", service_date)
            .await
            .expect("find_or_create_train (first, still unresolved)");
        let second_id = find_or_create_train(&pool, "TEST-TRAINS-NULL-UID-2", service_date)
            .await
            .expect("find_or_create_train (second, still unresolved)");

        assert_ne!(first_id, second_id);

        let (train_id_1,): (Option<String>,) =
            sqlx::query_as("SELECT train_id FROM trains WHERE id = $1")
                .bind(first_id)
                .fetch_one(&pool)
                .await
                .expect("read back trains row 1");
        let (train_id_2,): (Option<String>,) =
            sqlx::query_as("SELECT train_id FROM trains WHERE id = $1")
                .bind(second_id)
                .fetch_one(&pool)
                .await
                .expect("read back trains row 2");
        assert_eq!(train_id_1, None, "neither row has been TRUST-resolved yet");
        assert_eq!(train_id_2, None, "neither row has been TRUST-resolved yet");

        sqlx::query("DELETE FROM trains WHERE id IN ($1, $2)")
            .bind(first_id)
            .bind(second_id)
            .execute(&pool)
            .await
            .ok();
    }

    /// DB2-5: the backlog heuristic's bind never moves a subscription off a
    /// DIFFERENT train, but may bind an unbound one or move it between two
    /// rows of the same uid.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                bind_subscription_unless_other_train -- --ignored --test-threads=1`"]
    async fn bind_subscription_unless_other_train_never_repoints_to_another_uid() {
        let pool = connect().await;
        let user_id = "TEST-DB2-5-BIND-USER";
        let cleanup = || async {
            sqlx::query("DELETE FROM train_subscriptions WHERE user_id = $1")
                .bind(user_id)
                .execute(&pool)
                .await
                .ok();
            sqlx::query("DELETE FROM trains WHERE train_uid LIKE 'TEST-DB2-5-%'")
                .execute(&pool)
                .await
                .ok();
            sqlx::query("DELETE FROM users WHERE id = $1")
                .bind(user_id)
                .execute(&pool)
                .await
                .ok();
        };
        cleanup().await;
        sqlx::query("INSERT INTO users (id, email, name) VALUES ($1, 'db2-5@example.com', $1)")
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("seed user");

        let day: NaiveDate = "2026-09-08".parse().unwrap();
        let a_today = find_or_create_train(&pool, "TEST-DB2-5-A", day)
            .await
            .unwrap();
        let a_tomorrow = find_or_create_train(&pool, "TEST-DB2-5-A", day.succ_opt().unwrap())
            .await
            .unwrap();
        let b_today = find_or_create_train(&pool, "TEST-DB2-5-B", day)
            .await
            .unwrap();
        let (subscription,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions (user_id, service_date) VALUES ($1, $2) RETURNING id",
        )
        .bind(user_id)
        .bind(day)
        .fetch_one(&pool)
        .await
        .expect("seed subscription");
        let bound = |pool: PgPool| async move {
            let (id,): (Option<i64>,) =
                sqlx::query_as("SELECT trains_id FROM train_subscriptions WHERE id = $1")
                    .bind(subscription)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            id
        };

        assert!(
            bind_subscription_unless_other_train(&pool, subscription, a_tomorrow)
                .await
                .unwrap()
        );
        assert_eq!(
            bound(pool.clone()).await,
            Some(a_tomorrow),
            "an unbound pin binds"
        );
        assert!(
            bind_subscription_unless_other_train(&pool, subscription, a_today)
                .await
                .unwrap()
        );
        assert_eq!(
            bound(pool.clone()).await,
            Some(a_today),
            "same uid, other date may move"
        );
        assert!(
            bind_subscription_unless_other_train(&pool, subscription, a_today)
                .await
                .unwrap()
        );
        assert!(
            !bind_subscription_unless_other_train(&pool, subscription, b_today)
                .await
                .unwrap(),
            "a different uid must not be written"
        );
        assert_eq!(bound(pool.clone()).await, Some(a_today));
        assert!(
            !bind_subscription_unless_other_train(&pool, -1, a_today)
                .await
                .unwrap()
        );

        cleanup().await;
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                trains_identity_writes_leave_an_unchanged_row_alone -- --ignored --test-threads=1`"]
    async fn trains_identity_writes_leave_an_unchanged_row_alone() {
        let pool = connect().await;
        let date = NaiveDate::from_ymd_opt(2099, 3, 2).unwrap();
        sqlx::query("DELETE FROM trains WHERE train_uid LIKE 'TEST-GUARD-T%'")
            .execute(&pool)
            .await
            .unwrap();
        let id = find_or_create_train(&pool, "TEST-GUARD-T1", date)
            .await
            .unwrap();
        let sql = format!("SELECT xmin::text FROM trains WHERE id = {id}");
        let first = xmin(&pool, &sql).await;
        assert_eq!(
            find_or_create_train(&pool, "TEST-GUARD-T1", date)
                .await
                .unwrap(),
            id
        );
        assert_eq!(
            xmin(&pool, &sql).await,
            first,
            "a known train is not rewritten"
        );

        let pairs = vec![
            ("TEST-GUARD-T1".to_string(), date),
            ("TEST-GUARD-T2".to_string(), date),
        ];
        let ids = find_or_create_trains_batch(&pool, &pairs).await.unwrap();
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[&pairs[0]], id);
        assert_eq!(xmin(&pool, &sql).await, first);
        let again = find_or_create_trains_batch(&pool, &pairs).await.unwrap();
        assert_eq!(again, ids);

        mark_train_resolved(&pool, id, "9Z99").await.unwrap();
        let resolved = xmin(&pool, &sql).await;
        assert_ne!(resolved, first);
        mark_train_resolved(&pool, id, "9Z99").await.unwrap();
        mark_trains_resolved_batch(&pool, &[(id, "9Z99".to_string())])
            .await
            .unwrap();
        assert_eq!(
            xmin(&pool, &sql).await,
            resolved,
            "re-resolving to the same train_id is a no-op"
        );

        sqlx::query("DELETE FROM trains WHERE train_uid LIKE 'TEST-GUARD-T%'")
            .execute(&pool)
            .await
            .unwrap();
    }
}
