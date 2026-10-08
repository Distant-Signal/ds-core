//! Periodic reconciliation for tracked-train state that a one-shot,
//! in-process-only attempt failed to advance. See
//! docs/superpowers/specs/2026-09-08-tracked-train-reconciliation-design.md.

use std::collections::HashMap;

use chrono::{DateTime, NaiveDate, Utc};
use sqlx::PgPool;

use super::schedule_matching;
use crate::trains::london_to_utc;

/// Stall 1's fix: a `train_subscriptions` row can stay `'pending'` forever
/// even after `train_movement_events` proves the train it points at was
/// genuinely tracked -- see the design doc's §1.1 for the two independent,
/// confirmed ways this happens (a redeployed `trust-consumer` that never
/// observed the Activation in-process, and a partial-batch-post-failure
/// race in the multi-subscriber fan-out). Writes only `resolution_status`,
/// replicating `train_tracking::flip_legacy_resolution`'s own
/// post-Task-22 invariant exactly -- every other column that function used
/// to also write now lives on the shared `trains` row and is already
/// correctly set by whichever subscriber's write succeeded first, since
/// `mark_train_resolved` operates per-`trains_id`, not per-subscription.
pub async fn reconcile_stuck_resolution_status(pool: &PgPool) -> anyhow::Result<u64> {
    let result = sqlx::query(
        "UPDATE train_subscriptions \
         SET resolution_status = 'resolved' \
         WHERE resolution_status = 'pending' \
           AND trains_id IS NOT NULL \
           AND EXISTS ( \
               SELECT 1 FROM train_movement_events tme \
               WHERE tme.trains_id = train_subscriptions.trains_id \
           )",
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// One `trains` row that has at least one subscriber but no schedule data
/// yet -- Stall 2's candidate set (design doc Decision 2). Scoped to
/// subscriber-referenced rows only: a `trains` row can exist with zero
/// subscribers (broad ingestion elsewhere in this codebase), and this
/// sweep's whole purpose is fixing what a TRACKED train's page shows, not
/// backfilling schedule data for the whole network. A zero-subscriber row
/// gets its own, narrower, read-triggered equivalent instead --
/// `routes::train::enrich_public_train_schedule`, run once per public
/// `GET /Train/by-uid/{uid}/{date}` view rather than proactively swept --
/// see that function's own doc comment for the bug this closes (real live
/// TRUST data, permanently null schedule, for any train nobody has ever
/// tracked).
#[derive(Debug, Clone, sqlx::FromRow)]
struct EnrichmentCandidate {
    id: i64,
    train_uid: String,
    service_date: NaiveDate,
}

/// Bounded to a `service_date` within `schedule_destination_departures`'
/// own retention window (2 days by default, `crates/aggregator`'s
/// `schedule_destination_departures_retention_days`) -- the only source
/// [`true_origin_departure`] can ever read from. Without this bound, an
/// older subscribed-but-unmatched `trains` row (nothing left to enrich it
/// from at all, CIF data long pruned) would be re-selected as a candidate,
/// and re-fail `true_origin_departure`, on every single sweep tick for as
/// long as `trains_retention_days` (30 days) keeps the row alive --
/// guaranteed-futile repeated work. Found by this plan's own final
/// whole-branch review.
///
/// And bounded above at `CURRENT_DATE + 1` (2026-10-08, when tracking by
/// uid went from 7 to 28 days ahead): [`retry_schedule_enrichment_for_nr_primary_trains`]
/// skips a train until its origin departure has passed, and a train dated
/// two or more UTC days ahead cannot have departed (its date starts at
/// London midnight, at most an hour before UTC midnight). The `+ 1`, not
/// `+ 0`, covers London's date running ahead of the database's UTC
/// `CURRENT_DATE` between midnight and 01:00 BST. Without it each far
/// tracked train cost a `true_origin_departure` read every tick for up to
/// four weeks, only to be skipped.
///
/// Buses and ferries (`schedule_services.mode <> 'train'`) are excluded
/// (2026-10-06): they are timetable-only, enriched once at track time
/// (`routes::train::enrich_shared_train`) with no grace period to wait out,
/// and a sweep that cannot match one would otherwise re-select it every
/// tick. Their page needs no match anyway: journey stops fall back to the
/// published calling points.
async fn list_trains_needing_schedule_enrichment(
    pool: &PgPool,
) -> anyhow::Result<Vec<EnrichmentCandidate>> {
    let rows = sqlx::query_as::<_, EnrichmentCandidate>(
        "SELECT DISTINCT tr.id, tr.train_uid, tr.service_date \
         FROM trains tr \
         JOIN train_subscriptions ts ON ts.trains_id = tr.id \
         WHERE tr.schedule_matched_at IS NULL \
           AND tr.service_date >= CURRENT_DATE - INTERVAL '2 days' \
           AND tr.service_date <= CURRENT_DATE + 1 \
           AND NOT EXISTS ( \
               SELECT 1 FROM schedule_services ss \
               WHERE ss.service_date = tr.service_date AND ss.uid = tr.train_uid \
                 AND ss.mode <> 'train')",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// The ids [`list_trains_needing_schedule_enrichment`] selects, for the
/// api `train_tracking`'s sweep-exclusion test (built only for tests: the
/// api's dev-dependency turns on the `test-support` feature).
#[cfg(any(test, feature = "test-support"))]
#[expect(
    clippy::expect_used,
    reason = "a test helper: a failed query should fail the calling test"
)]
pub async fn enrichment_candidate_ids_for_tests(pool: &PgPool) -> Vec<i64> {
    list_trains_needing_schedule_enrichment(pool)
        .await
        .expect("list enrichment candidates")
        .into_iter()
        .map(|candidate| candidate.id)
        .collect()
}

/// The one row of `schedule_destination_departures` for `(train_uid,
/// service_date)` that represents the schedule's OWN origin departure --
/// `origin_crs = true_origin_crs` is what picks that row out from the
/// several this train may have (one per departure-bearing calling point).
/// See the design doc §3 Decision 2.
///
/// `pub`, not private: the api's `routes::train::enrich_public_train_schedule`
/// reuses this exact lookup for the untracked-train counterpart of
/// [`retry_schedule_enrichment_for_nr_primary_trains`] below -- see that
/// function's own doc comment for why a `trains` row with zero subscribers
/// needs a schedule-enrichment path of its own too.
pub async fn true_origin_departure(
    pool: &PgPool,
    train_uid: &str,
    service_date: NaiveDate,
) -> anyhow::Result<Option<(String, chrono::NaiveTime)>> {
    let row: Option<(String, chrono::NaiveTime)> = sqlx::query_as(
        "SELECT origin_crs, scheduled FROM schedule_destination_departures \
         WHERE train_uid = $1 AND service_date = $2 AND origin_crs = true_origin_crs \
         ORDER BY scheduled LIMIT 1",
    )
    .bind(train_uid)
    .bind(service_date)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Stall 2's fix: an NR-primary tracked train
/// (`train_tracking::create_subscription_for_train`) never acquires
/// schedule data (origin, destination, calling points) unless TRUST backlog
/// happened to have it at track-creation time
/// (`routes::train::enrich_shared_train`). CIF SCHEDULE data is available
/// independent of TRUST, in `schedule_destination_departures` -- this
/// retries against it for every still-unmatched, subscriber-referenced
/// `trains` row whose true origin departure has passed by `grace_period`
/// (a courtesy to the live/backlog paths' own normal resolution window, not
/// a data-availability requirement -- see the design doc §3 Decision 3).
/// `now` is injected, not read from the clock, so this stays testable with
/// fixed timestamps -- the same convention
/// `train_tracking::validate_pin(pin, now)` already establishes.
///
/// **Final-review fix**: a successful match here used to only write the
/// shared `trains` row via `attempt_schedule_match_for_shared_train`,
/// leaving every `train_subscriptions.resolution_status` still `'pending'`
/// -- nothing else in this codebase can ever advance an NR-primary row's
/// own status column, since `apply_schedule_match`'s gate (`WHERE trains_id
/// IS NULL AND resolution_status = 'pending'`, `train_tracking.rs`) is
/// false by construction the instant `create_subscription_for_train` sets
/// `trains_id`. This now also bulk-`UPDATE`s `train_subscriptions`, scoped
/// by `trains_id` rather than a single subscription id, because a shared
/// `trains` row can have more than one subscriber pointed at it (the same
/// "every subscriber sharing one physical train" fan-out
/// `list_active_tracked_trains`/`by_train_uid` already use elsewhere) --
/// every subscriber sharing this identity should be advanced together,
/// since the schedule data now exists for all of them, not just whichever
/// one happened to trigger this candidate row.
#[expect(
    clippy::implicit_hasher,
    reason = "callers always use the default hasher"
)]
pub async fn retry_schedule_enrichment_for_nr_primary_trains(
    pool: &PgPool,
    crs_line_index: &HashMap<String, Vec<String>>,
    grace_period: chrono::Duration,
    now: DateTime<Utc>,
) -> anyhow::Result<u64> {
    let candidates = list_trains_needing_schedule_enrichment(pool).await?;
    let mut matched = 0u64;

    for candidate in candidates {
        let origin =
            match true_origin_departure(pool, &candidate.train_uid, candidate.service_date).await {
                Ok(Some(origin)) => origin,
                Ok(None) => continue,
                Err(err) => {
                    tracing::warn!(
                        error = ?err,
                        trains_id = candidate.id,
                        "schedule-enrichment lookup failed for this train; will retry next sweep"
                    );
                    continue;
                }
            };
        let (origin_crs, scheduled) = origin;

        let Some(scheduled_departure) = london_to_utc(candidate.service_date.and_time(scheduled))
        else {
            tracing::warn!(
                trains_id = candidate.id,
                "scheduled departure did not resolve to a real London local time; skipping"
            );
            continue;
        };

        if now - scheduled_departure < grace_period {
            continue;
        }

        match schedule_matching::attempt_schedule_match_for_shared_train(
            pool,
            &candidate.train_uid,
            &origin_crs,
            scheduled_departure,
            candidate.service_date,
            crs_line_index,
        )
        .await
        {
            Ok(true) => {
                matched += 1;
                if let Err(err) = sqlx::query(
                    "UPDATE train_subscriptions \
                     SET resolution_status = 'schedule_matched' \
                     WHERE trains_id = $1 AND resolution_status = 'pending'",
                )
                .bind(candidate.id)
                .execute(pool)
                .await
                {
                    tracing::warn!(
                        error = ?err,
                        trains_id = candidate.id,
                        "schedule enrichment succeeded but failed to advance subscriber resolution_status; the shared trains row is enriched but subscriber-facing pages may show stale status until the next successful attempt for a DIFFERENT trigger, since this trains_id will no longer be a sweep candidate"
                    );
                }
            }
            Ok(false) => {}
            Err(err) => {
                tracing::warn!(
                    error = ?err,
                    trains_id = candidate.id,
                    "schedule-enrichment retry failed for this train; will retry next sweep"
                );
            }
        }
    }

    Ok(matched)
}

/// Both halves of the reconciliation sweep, combined for
/// `main.rs`'s single background loop. See the design doc §3 Decision 5
/// for why one shared interval governs both.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReconciliationSweepResult {
    pub resolution_status_reconciled: u64,
    pub schedule_enrichment_matched: u64,
}

#[expect(
    clippy::implicit_hasher,
    reason = "callers always use the default hasher"
)]
pub async fn run_reconciliation_sweep(
    pool: &PgPool,
    crs_line_index: &HashMap<String, Vec<String>>,
    grace_period: chrono::Duration,
) -> anyhow::Result<ReconciliationSweepResult> {
    let resolution_status_reconciled = reconcile_stuck_resolution_status(pool).await?;
    let schedule_enrichment_matched = retry_schedule_enrichment_for_nr_primary_trains(
        pool,
        crs_line_index,
        grace_period,
        Utc::now(),
    )
    .await?;
    Ok(ReconciliationSweepResult {
        resolution_status_reconciled,
        schedule_enrichment_matched,
    })
}

#[cfg(test)]
mod db_tests {
    use super::*;
    use crate::test_support::{connect, population_json, seed_user};

    async fn seed_train(pool: &PgPool, train_uid: &str, service_date: NaiveDate) -> i64 {
        let (id,): (i64,) = sqlx::query_as(
            "INSERT INTO trains (train_uid, service_date) VALUES ($1, $2) RETURNING id",
        )
        .bind(train_uid)
        .bind(service_date)
        .fetch_one(pool)
        .await
        .expect("seed fixture trains row");
        id
    }

    async fn seed_pending_subscription(
        pool: &PgPool,
        user_id: &str,
        service_date: NaiveDate,
        trains_id: i64,
    ) -> i64 {
        let (id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions (user_id, service_date, trains_id, resolution_status) \
             VALUES ($1, $2, $3, 'pending') RETURNING id",
        )
        .bind(user_id)
        .bind(service_date)
        .bind(trains_id)
        .fetch_one(pool)
        .await
        .expect("seed fixture pending subscription");
        id
    }

    async fn seed_movement_event(pool: &PgPool, trains_id: i64, dedup_key: &str) {
        sqlx::query(
            "INSERT INTO train_movement_events (trains_id, dedup_key, msg_type, raw_body) \
             VALUES ($1, $2, '0003', '{}'::jsonb)",
        )
        .bind(trains_id)
        .bind(dedup_key)
        .execute(pool)
        .await
        .expect("seed fixture movement event");
    }

    async fn read_resolution_status(pool: &PgPool, id: i64) -> String {
        let status: String =
            sqlx::query_scalar("SELECT resolution_status FROM train_subscriptions WHERE id = $1")
                .bind(id)
                .fetch_one(pool)
                .await
                .expect("read back resolution_status");
        status
    }

    async fn cleanup(pool: &PgPool, user_id: &str, train_uid: &str) {
        sqlx::query("DELETE FROM train_subscriptions WHERE user_id = $1")
            .bind(user_id)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trains WHERE train_uid = $1")
            .bind(train_uid)
            .execute(pool)
            .await
            .ok();
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(pool)
            .await
            .ok();
    }

    /// A `service_date` guaranteed to fall inside
    /// `list_trains_needing_schedule_enrichment`'s `CURRENT_DATE - INTERVAL
    /// '2 days'` bound regardless of when this test runs -- a fixed
    /// historical date (this module's own tests used to hardcode
    /// `2020-01-01`) would otherwise be silently excluded by that bound the
    /// moment it landed, turning every "skips" test green for the wrong
    /// reason instead of red for the right one.
    fn recent_service_date() -> NaiveDate {
        Utc::now().date_naive() - chrono::Duration::days(1)
    }

    /// The real UTC instant a `service_date`'s local (`Europe/London`)
    /// wall-clock time converts to -- same conversion
    /// `retry_schedule_enrichment_for_nr_primary_trains` itself applies
    /// internally, so a test's `now` fixture stays correct year-round
    /// (BST vs GMT) without the test author having to reason about the
    /// offset by hand.
    fn london_departure_utc(service_date: NaiveDate, hour: u32, minute: u32) -> DateTime<Utc> {
        london_to_utc(
            service_date
                .and_hms_opt(hour, minute, 0)
                .expect("valid local time"),
        )
        .expect("a real London local time")
    }

    /// Fixture for Stall 2's tests: a subscribed, unmatched `trains` row,
    /// plus everything `attempt_schedule_match_for_shared_train` needs to
    /// successfully match it (a `stanox_crs` translation, a
    /// `schedule_line_population` entry) and a `schedule_destination_departures`
    /// row for `true_origin_departure` to find. Returns
    /// `(trains_id, subscription_id, crs_line_index)`.
    #[expect(
        clippy::too_many_arguments,
        reason = "test fixture: one positional argument per column it seeds"
    )]
    async fn seed_enrichment_fixture(
        pool: &PgPool,
        user_id: &str,
        train_uid: &str,
        service_date: NaiveDate,
        origin_crs: &str,
        stanox: &str,
        line_id: &str,
        scheduled: &str,
    ) -> (i64, i64, HashMap<String, Vec<String>>) {
        seed_user(pool, user_id).await;
        sqlx::query(
            "INSERT INTO stanox_crs (stanox, crs, tiploc, station_name, source_sequence) \
             VALUES ($1, $2, 'EUSTON', 'LONDON EUSTON', 1) ON CONFLICT (stanox) DO NOTHING",
        )
        .bind(stanox)
        .bind(origin_crs)
        .execute(pool)
        .await
        .expect("seed stanox_crs");
        sqlx::query(
            "INSERT INTO schedule_line_population (line_id, service_date, population) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (line_id, service_date) DO UPDATE SET population = EXCLUDED.population",
        )
        .bind(line_id)
        .bind(service_date)
        .bind(population_json(train_uid, "EUSTON ", scheduled))
        .execute(pool)
        .await
        .expect("seed schedule_line_population");
        sqlx::query(
            "INSERT INTO schedule_destination_departures \
                (service_date, destination_crs, scheduled, train_uid, origin_crs, true_origin_crs) \
             VALUES ($1, 'WAT', $2::time, $3, $4, $4)",
        )
        .bind(service_date)
        .bind(scheduled)
        .bind(train_uid)
        .bind(origin_crs)
        .execute(pool)
        .await
        .expect("seed schedule_destination_departures");

        let trains_id = seed_train(pool, train_uid, service_date).await;
        let subscription_id =
            seed_pending_subscription(pool, user_id, service_date, trains_id).await;

        let mut crs_line_index = HashMap::new();
        crs_line_index.insert(origin_crs.to_string(), vec![line_id.to_string()]);

        (trains_id, subscription_id, crs_line_index)
    }

    async fn read_schedule_matched_at(pool: &PgPool, trains_id: i64) -> Option<DateTime<Utc>> {
        let schedule_matched_at: Option<DateTime<Utc>> =
            sqlx::query_scalar("SELECT schedule_matched_at FROM trains WHERE id = $1")
                .bind(trains_id)
                .fetch_one(pool)
                .await
                .expect("read back schedule_matched_at");
        schedule_matched_at
    }

    async fn cleanup_enrichment_fixture(
        pool: &PgPool,
        user_id: &str,
        train_uid: &str,
        stanox: &str,
        line_id: &str,
        service_date: NaiveDate,
    ) {
        cleanup(pool, user_id, train_uid).await;
        sqlx::query("DELETE FROM schedule_destination_departures WHERE train_uid = $1")
            .bind(train_uid)
            .execute(pool)
            .await
            .ok();
        sqlx::query(
            "DELETE FROM schedule_line_population WHERE line_id = $1 AND service_date = $2",
        )
        .bind(line_id)
        .bind(service_date)
        .execute(pool)
        .await
        .ok();
        sqlx::query("DELETE FROM stanox_crs WHERE stanox = $1")
            .bind(stanox)
            .execute(pool)
            .await
            .ok();
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                reconcile_stuck_resolution_status_flips_a_pending_row_with_movement_events_to_resolved \
                -- --ignored --test-threads=1`"]
    async fn reconcile_stuck_resolution_status_flips_a_pending_row_with_movement_events_to_resolved()
     {
        let pool = connect().await;
        let user_id = "TEST-RECON-STALL1-A";
        let train_uid = "TEST-RECON-STALL1-UID-A";
        let service_date: NaiveDate = "2026-09-08".parse().unwrap();
        seed_user(&pool, user_id).await;
        let trains_id = seed_train(&pool, train_uid, service_date).await;
        let subscription_id =
            seed_pending_subscription(&pool, user_id, service_date, trains_id).await;
        seed_movement_event(&pool, trains_id, "TEST-RECON-DEDUP-A").await;

        let flipped = reconcile_stuck_resolution_status(&pool)
            .await
            .expect("reconcile_stuck_resolution_status");
        assert!(flipped >= 1, "at least this fixture's row must be counted");

        let status = read_resolution_status(&pool, subscription_id).await;
        assert_eq!(
            status, "resolved",
            "movement events exist for this trains_id, so the stuck subscription must be resolved"
        );

        cleanup(&pool, user_id, train_uid).await;
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                reconcile_stuck_resolution_status_leaves_a_pending_row_with_no_movement_events_untouched \
                -- --ignored --test-threads=1`"]
    async fn reconcile_stuck_resolution_status_leaves_a_pending_row_with_no_movement_events_untouched()
     {
        // The L78659 live-example variant: a genuinely missed Activation,
        // zero train_movement_events rows. This must NOT be flipped --
        // there is no honest evidence the train ever ran (design doc
        // Decision 1).
        let pool = connect().await;
        let user_id = "TEST-RECON-STALL1-B";
        let train_uid = "TEST-RECON-STALL1-UID-B";
        let service_date: NaiveDate = "2026-09-08".parse().unwrap();
        seed_user(&pool, user_id).await;
        let trains_id = seed_train(&pool, train_uid, service_date).await;
        let subscription_id =
            seed_pending_subscription(&pool, user_id, service_date, trains_id).await;

        reconcile_stuck_resolution_status(&pool)
            .await
            .expect("reconcile_stuck_resolution_status");

        let status = read_resolution_status(&pool, subscription_id).await;
        assert_eq!(
            status, "pending",
            "no movement events exist for this trains_id, so the row must stay pending"
        );

        cleanup(&pool, user_id, train_uid).await;
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                reconcile_stuck_resolution_status_never_touches_a_row_that_isnt_pending \
                -- --ignored --test-threads=1`"]
    async fn reconcile_stuck_resolution_status_never_touches_a_row_that_isnt_pending() {
        let pool = connect().await;
        let user_id = "TEST-RECON-STALL1-C";
        let train_uid = "TEST-RECON-STALL1-UID-C";
        let service_date: NaiveDate = "2026-09-08".parse().unwrap();
        seed_user(&pool, user_id).await;
        let trains_id = seed_train(&pool, train_uid, service_date).await;
        let (subscription_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions (user_id, service_date, trains_id, resolution_status) \
             VALUES ($1, $2, $3, 'schedule_matched') RETURNING id",
        )
        .bind(user_id)
        .bind(service_date)
        .bind(trains_id)
        .fetch_one(&pool)
        .await
        .expect("seed fixture schedule_matched subscription");
        seed_movement_event(&pool, trains_id, "TEST-RECON-DEDUP-C").await;

        reconcile_stuck_resolution_status(&pool)
            .await
            .expect("reconcile_stuck_resolution_status");

        let status = read_resolution_status(&pool, subscription_id).await;
        assert_eq!(
            status, "schedule_matched",
            "only a 'pending' row may be reconciled by this function"
        );

        cleanup(&pool, user_id, train_uid).await;
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                retry_schedule_enrichment_matches_a_subscribed_trains_row_past_the_grace_period \
                -- --ignored --test-threads=1`"]
    async fn retry_schedule_enrichment_matches_a_subscribed_trains_row_past_the_grace_period() {
        let pool = connect().await;
        let user_id = "TEST-RECON-STALL2-A";
        let train_uid = "TEST-RECON-STALL2-UID-A";
        let service_date = recent_service_date();
        let (trains_id, _subscription_id, crs_line_index) = seed_enrichment_fixture(
            &pool,
            user_id,
            train_uid,
            service_date,
            "EUS",
            "TEST-RECON-STANOX-A",
            "test-recon-line-a",
            "09:00",
        )
        .await;

        // 1 hour after the 09:00 scheduled departure -- comfortably past
        // the 30-minute grace period.
        let now = london_departure_utc(service_date, 9, 0) + chrono::Duration::hours(1);
        let matched = retry_schedule_enrichment_for_nr_primary_trains(
            &pool,
            &crs_line_index,
            chrono::Duration::minutes(30),
            now,
        )
        .await
        .expect("retry_schedule_enrichment_for_nr_primary_trains");
        assert!(matched >= 1, "at least this fixture's row must be matched");

        assert!(
            read_schedule_matched_at(&pool, trains_id).await.is_some(),
            "the trains row must now carry schedule data"
        );

        cleanup_enrichment_fixture(
            &pool,
            user_id,
            train_uid,
            "TEST-RECON-STANOX-A",
            "test-recon-line-a",
            service_date,
        )
        .await;
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                retry_schedule_enrichment_skips_a_row_still_inside_the_grace_period \
                -- --ignored --test-threads=1`"]
    async fn retry_schedule_enrichment_skips_a_row_still_inside_the_grace_period() {
        let pool = connect().await;
        let user_id = "TEST-RECON-STALL2-B";
        let train_uid = "TEST-RECON-STALL2-UID-B";
        let service_date = recent_service_date();
        let (trains_id, _subscription_id, crs_line_index) = seed_enrichment_fixture(
            &pool,
            user_id,
            train_uid,
            service_date,
            "EUS",
            "TEST-RECON-STANOX-B",
            "test-recon-line-b",
            "09:00",
        )
        .await;

        // Only 10 minutes after the 09:00 scheduled departure -- inside a
        // 30-minute grace period.
        let now = london_departure_utc(service_date, 9, 0) + chrono::Duration::minutes(10);
        let matched = retry_schedule_enrichment_for_nr_primary_trains(
            &pool,
            &crs_line_index,
            chrono::Duration::minutes(30),
            now,
        )
        .await
        .expect("retry_schedule_enrichment_for_nr_primary_trains");
        assert_eq!(
            matched, 0,
            "still inside the grace period; must not be touched yet"
        );

        assert!(read_schedule_matched_at(&pool, trains_id).await.is_none());

        cleanup_enrichment_fixture(
            &pool,
            user_id,
            train_uid,
            "TEST-RECON-STANOX-B",
            "test-recon-line-b",
            service_date,
        )
        .await;
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                retry_schedule_enrichment_skips_a_trains_row_with_no_subscriber \
                -- --ignored --test-threads=1`"]
    async fn retry_schedule_enrichment_skips_a_trains_row_with_no_subscriber() {
        let pool = connect().await;
        let train_uid = "TEST-RECON-STALL2-UID-C";
        let service_date = recent_service_date();
        // A trains row with real schedule data available, but no
        // train_subscriptions row referencing it at all -- Decision 2's
        // scoping must exclude it.
        let trains_id = seed_train(&pool, train_uid, service_date).await;
        sqlx::query(
            "INSERT INTO schedule_destination_departures \
                (service_date, destination_crs, scheduled, train_uid, origin_crs, true_origin_crs) \
             VALUES ($1, 'WAT', '09:00'::time, $2, 'EUS', 'EUS')",
        )
        .bind(service_date)
        .bind(train_uid)
        .execute(&pool)
        .await
        .expect("seed schedule_destination_departures");

        let now = london_departure_utc(service_date, 9, 0) + chrono::Duration::hours(3);
        let matched = retry_schedule_enrichment_for_nr_primary_trains(
            &pool,
            &HashMap::new(),
            chrono::Duration::minutes(30),
            now,
        )
        .await
        .expect("retry_schedule_enrichment_for_nr_primary_trains");
        assert_eq!(
            matched, 0,
            "an unsubscribed trains row must never be a candidate"
        );

        assert!(read_schedule_matched_at(&pool, trains_id).await.is_none());

        sqlx::query("DELETE FROM schedule_destination_departures WHERE train_uid = $1")
            .bind(train_uid)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM trains WHERE train_uid = $1")
            .bind(train_uid)
            .execute(&pool)
            .await
            .ok();
    }

    /// Tracking by uid reaches 28 days ahead: a far tracked train is not an
    /// enrichment candidate (it cannot have departed), tomorrow's still is.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                schedule_enrichment_candidates_stop_at_tomorrow \
                -- --ignored --test-threads=1`"]
    async fn schedule_enrichment_candidates_stop_at_tomorrow() {
        let pool = connect().await;
        let user_id = "TEST-RECON-FAR";
        let near_uid = "TEST-RECON-FAR-NEAR";
        let far_uid = "TEST-RECON-FAR-FAR";
        cleanup(&pool, user_id, near_uid).await;
        cleanup(&pool, user_id, far_uid).await;
        seed_user(&pool, user_id).await;
        let today: NaiveDate = sqlx::query_scalar("SELECT CURRENT_DATE")
            .fetch_one(&pool)
            .await
            .expect("CURRENT_DATE");
        let near_date = today + chrono::Duration::days(1);
        let far_date = today + chrono::Duration::days(20);
        let near = seed_train(&pool, near_uid, near_date).await;
        let far = seed_train(&pool, far_uid, far_date).await;
        seed_pending_subscription(&pool, user_id, near_date, near).await;
        seed_pending_subscription(&pool, user_id, far_date, far).await;

        let candidates = enrichment_candidate_ids_for_tests(&pool).await;
        assert!(
            candidates.contains(&near),
            "tomorrow's train is a candidate"
        );
        assert!(
            !candidates.contains(&far),
            "a train 20 days ahead cannot have departed, so it is not re-read every tick"
        );

        cleanup(&pool, user_id, near_uid).await;
        cleanup(&pool, user_id, far_uid).await;
    }

    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                retry_schedule_enrichment_skips_a_trains_row_with_no_schedule_destination_departures_data \
                -- --ignored --test-threads=1`"]
    async fn retry_schedule_enrichment_skips_a_trains_row_with_no_schedule_destination_departures_data()
     {
        let pool = connect().await;
        let user_id = "TEST-RECON-STALL2-D";
        let train_uid = "TEST-RECON-STALL2-UID-D";
        let service_date = recent_service_date();
        seed_user(&pool, user_id).await;
        let trains_id = seed_train(&pool, train_uid, service_date).await;
        seed_pending_subscription(&pool, user_id, service_date, trains_id).await;

        let now = london_departure_utc(service_date, 12, 0);
        let matched = retry_schedule_enrichment_for_nr_primary_trains(
            &pool,
            &HashMap::new(),
            chrono::Duration::minutes(30),
            now,
        )
        .await
        .expect("retry_schedule_enrichment_for_nr_primary_trains");
        assert_eq!(
            matched, 0,
            "no CIF data exists for this train; nothing to enrich from"
        );

        assert!(read_schedule_matched_at(&pool, trains_id).await.is_none());

        cleanup(&pool, user_id, train_uid).await;
    }

    /// Finding 1 of this plan's final whole-branch review: schedule
    /// enrichment writing the shared `trains` row alone is not enough --
    /// the subscriber's own `train_subscriptions.resolution_status` must
    /// also advance, or `/train/by-id/{trackingId}` (the page a just-
    /// tracked NR-primary user is actually sent to) keeps rendering
    /// "Waiting to hear from Network Rail" forever even after the shared
    /// row is fully enriched.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                retry_schedule_enrichment_advances_the_subscribers_own_resolution_status \
                -- --ignored --test-threads=1`"]
    async fn retry_schedule_enrichment_advances_the_subscribers_own_resolution_status() {
        let pool = connect().await;
        let user_id = "TEST-RECON-STALL2-E";
        let train_uid = "TEST-RECON-STALL2-UID-E";
        let service_date = recent_service_date();
        let (_trains_id, subscription_id, crs_line_index) = seed_enrichment_fixture(
            &pool,
            user_id,
            train_uid,
            service_date,
            "EUS",
            "TEST-RECON-STANOX-E",
            "test-recon-line-e",
            "09:00",
        )
        .await;

        let now = london_departure_utc(service_date, 9, 0) + chrono::Duration::hours(1);
        let matched = retry_schedule_enrichment_for_nr_primary_trains(
            &pool,
            &crs_line_index,
            chrono::Duration::minutes(30),
            now,
        )
        .await
        .expect("retry_schedule_enrichment_for_nr_primary_trains");
        assert!(matched >= 1, "at least this fixture's row must be matched");

        let status = read_resolution_status(&pool, subscription_id).await;
        assert_eq!(
            status, "schedule_matched",
            "the subscriber's own resolution_status must advance once the shared trains row is \
             enriched, not just the trains row itself -- otherwise /train/by-id/{{trackingId}} \
             stays stuck on 'Waiting to hear from Network Rail' forever"
        );

        cleanup_enrichment_fixture(
            &pool,
            user_id,
            train_uid,
            "TEST-RECON-STANOX-E",
            "test-recon-line-e",
            service_date,
        )
        .await;
    }
}
