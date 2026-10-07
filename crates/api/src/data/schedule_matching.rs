//! Moved to `ds_store::sweeps::schedule_matching` (ingest architecture plan
//! 1A.10). The re-exports keep every `data::schedule_matching::…` call site
//! unchanged; the DB tests below read the pin back through the api's user
//! read model (`train_tracking::get_by_tracking_id`), so they stay here, on
//! `ds_store::test_support`'s shared fixtures.

// Moved to ds_store::sweeps::schedule_matching (ingest architecture plan 1A.10).
pub use ds_store::sweeps::schedule_matching::{
    ScheduleMatch, attempt_schedule_match, attempt_schedule_match_for_shared_train,
    crs_to_line_ids, find_schedule_match_for_known_train, run_schedule_match_sweep,
};

#[cfg(test)]
#[expect(
    clippy::too_many_lines,
    reason = "test code: scenario tests read top to bottom"
)]
mod db_tests {
    use std::collections::HashMap;

    use chrono::{DateTime, NaiveDate, Utc};
    use common::LineDefinition;

    use super::*;
    use crate::data::train_tracking;
    use ds_store::test_support::{connect, population_json};

    #[tokio::test]
    #[ignore = "requires a live database; see this plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p api \
                attempt_schedule_match -- --ignored --test-threads=1`"]
    async fn attempt_schedule_match_reproduces_the_eus_bug_and_now_resolves_it() {
        let pool = connect().await;
        let user_id = "TEST-SCHEDULE-MATCH-EUS";
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind("schedule-match@example.com")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");

        sqlx::query(
            "INSERT INTO stanox_crs (stanox, crs, tiploc, station_name, source_sequence) \
             VALUES ('TEST-EUS-STANOX', 'EUS', 'EUSTON', 'LONDON EUSTON', 1) \
             ON CONFLICT (stanox) DO NOTHING",
        )
        .execute(&pool)
        .await
        .expect("seed stanox_crs");

        let service_date: NaiveDate = "2026-09-05".parse().unwrap();
        sqlx::query(
            "INSERT INTO schedule_line_population (line_id, service_date, population) \
             VALUES ('west-coast-main-line', $1, $2) \
             ON CONFLICT (line_id, service_date) DO UPDATE SET population = EXCLUDED.population",
        )
        .bind(service_date)
        .bind(population_json("C99999", "EUSTON ", "19:15"))
        .execute(&pool)
        .await
        .expect("seed schedule_line_population");

        // The exact reported bug: a pin created more than an hour after
        // its train's own origin-departure window (the pin's own
        // scheduled_departure is still 19:15 -- what changes is that no
        // live TRUST Movement for it will ever arrive within this
        // process's test window, exactly mirroring "pinned an hour late,
        // TRUST's own ±20-minute window already closed").
        let scheduled_departure: DateTime<Utc> = "2026-09-05T19:15:00+01:00".parse().unwrap(); // BST -> 18:15 UTC
        let (tracked_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions (user_id, service_date, pin_origin_crs, pin_scheduled_departure) \
             VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(user_id)
        .bind(service_date)
        .bind("EUS")
        .bind(scheduled_departure)
        .fetch_one(&pool)
        .await
        .expect("seed fixture tracked_trains row");

        let mut crs_line_index = HashMap::new();
        crs_line_index.insert("EUS".to_string(), vec!["west-coast-main-line".to_string()]);

        let matched = attempt_schedule_match(
            &pool,
            tracked_train_id,
            "EUS",
            scheduled_departure,
            None, // no pinned destination in this fixture
            service_date,
            &crs_line_index,
            &[],
            None,
            None,
        )
        .await
        .expect("attempt schedule match");
        assert!(matched, "the pin should schedule-match against C99999");

        let state = train_tracking::get_by_tracking_id(&pool, tracked_train_id)
            .await
            .expect("read tracked train")
            .expect("tracked train exists");
        assert_eq!(state.resolution_status, "schedule_matched");
        assert_eq!(state.train_uid, Some("C99999".to_string()));
        assert_eq!(state.train_id, None, "train_id must stay TRUST-exclusive");

        sqlx::query("DELETE FROM schedule_line_population WHERE line_id = 'west-coast-main-line' AND service_date = $1")
            .bind(service_date)
            .execute(&pool)
            .await
            .expect("cleanup population");
        sqlx::query("DELETE FROM stanox_crs WHERE stanox = 'TEST-EUS-STANOX'")
            .execute(&pool)
            .await
            .expect("cleanup stanox_crs");
        sqlx::query("DELETE FROM train_subscriptions WHERE user_id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("cleanup tracked_trains");
        // Step A's dual-write (attempt_schedule_match's own
        // find_or_create_train_with_schedule_match call) creates a shared
        // `trains` row for this identity too -- discovered as a real
        // cross-test leak during Task 8's own end-to-end verification: this
        // C99999/2026-09-05 identity is shared with
        // `trust_event_backlog_match::db_tests`'s own EUS fixture, and
        // neither test used to clean up its `trains` row, so whichever ran
        // second inherited the first's leftover `train_id`. Now that Step C
        // reads `train_id` through this row, an uncleaned leftover silently
        // corrupts an unrelated test's assertion. See the same fix applied
        // to `trust_event_backlog_match.rs`.
        sqlx::query("DELETE FROM trains WHERE train_uid = 'C99999' AND service_date = $1")
            .bind(service_date)
            .execute(&pool)
            .await
            .expect("cleanup trains");
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("cleanup user");
    }

    /// The exact live-confirmed midnight-crossing bug (2026-09-09
    /// investigation: c2c UID F49687, `service_date` 2026-09-05, Liverpool
    /// Street 23:48 -> Stratford 23:54/55 -> Barking 00:06/00:07 -> ... ->
    /// Shoeburyness 01:01 -- every calling point from Barking onward is
    /// really 2026-09-06 wall-clock), reproduced end to end through
    /// `attempt_schedule_match`: a pin dated with Barking's REAL actual
    /// calendar day (2026-09-06) must schedule-match against a population
    /// entry whose Barking calling point carries `day_offset: 1` relative
    /// to the schedule's own `service_date` (2026-09-05). Before this fix,
    /// `find_schedule_match`'s `to_utc` closure ignored `day_offset`
    /// entirely, so this pin -- correctly dated a full day after
    /// `service_date` -- would never land within `MATCH_TOLERANCE` of a
    /// candidate silently mis-stamped a day earlier, permanently stuck
    /// "Waiting to hear from Network Rail".
    #[tokio::test]
    #[ignore = "requires a live database; see this plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p api \
                attempt_schedule_match -- --ignored --test-threads=1`"]
    async fn attempt_schedule_match_matches_a_post_midnight_calling_point_via_its_day_offset() {
        let pool = connect().await;
        let user_id = "TEST-SCHEDULE-MATCH-MIDNIGHT";
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind("schedule-match-midnight@example.com")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");

        sqlx::query(
            "INSERT INTO stanox_crs (stanox, crs, tiploc, station_name, source_sequence) \
             VALUES ('TEST-BKG-STANOX', 'ZBK', 'BARKING', 'BARKING', 1) \
             ON CONFLICT (stanox) DO NOTHING",
        )
        .execute(&pool)
        .await
        .expect("seed stanox_crs");

        let service_date: NaiveDate = "2026-09-05".parse().unwrap();
        let population = serde_json::json!([{
            "uid": "TEST-F49687",
            "calling_points": [
                {
                    "tiploc": "LIVST  ",
                    "kind": "Origin",
                    "booked_arrival": null,
                    "booked_departure": "23:48:00",
                    "is_half_minute_arrival": false,
                    "is_half_minute_departure": false,
                    "day_offset": 0
                },
                {
                    "tiploc": "BARKING",
                    "kind": "Intermediate",
                    "booked_arrival": "00:06:00",
                    "booked_departure": "00:07:00",
                    "is_half_minute_arrival": false,
                    "is_half_minute_departure": false,
                    "day_offset": 1
                }
            ]
        }]);
        sqlx::query(
            "INSERT INTO schedule_line_population (line_id, service_date, population) \
             VALUES ('test-c2c-line', $1, $2) \
             ON CONFLICT (line_id, service_date) DO UPDATE SET population = EXCLUDED.population",
        )
        .bind(service_date)
        .bind(&population)
        .execute(&pool)
        .await
        .expect("seed schedule_line_population");

        // Barking's REAL booked_departure is 2026-09-06 00:07 Europe/London
        // (BST) = 2026-09-05T23:07:00Z -- a full calendar day after the
        // schedule's own service_date (2026-09-05), which is exactly what
        // day_offset: 1 says. The pin is dated with this REAL, correct
        // instant, as a genuine tracked-train pin would be.
        let scheduled_departure: DateTime<Utc> = "2026-09-06T00:07:00+01:00".parse().unwrap();
        let (tracked_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions (user_id, service_date, pin_origin_crs, pin_scheduled_departure) \
             VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(user_id)
        .bind(service_date)
        .bind("ZBK")
        .bind(scheduled_departure)
        .fetch_one(&pool)
        .await
        .expect("seed fixture tracked_trains row");

        let mut crs_line_index = HashMap::new();
        crs_line_index.insert("ZBK".to_string(), vec!["test-c2c-line".to_string()]);

        let matched = attempt_schedule_match(
            &pool,
            tracked_train_id,
            "ZBK",
            scheduled_departure,
            None, // no pinned destination in this fixture
            service_date,
            &crs_line_index,
            &[],
            None,
            None,
        )
        .await
        .expect("attempt schedule match");
        assert!(
            matched,
            "a pin correctly dated on Barking's REAL calendar day must schedule-match against \
             TEST-F49687's day_offset: 1 calling point"
        );

        let state = train_tracking::get_by_tracking_id(&pool, tracked_train_id)
            .await
            .expect("read tracked train")
            .expect("tracked train exists");
        assert_eq!(state.resolution_status, "schedule_matched");
        assert_eq!(state.train_uid, Some("TEST-F49687".to_string()));

        sqlx::query("DELETE FROM schedule_line_population WHERE line_id = 'test-c2c-line' AND service_date = $1")
            .bind(service_date)
            .execute(&pool)
            .await
            .expect("cleanup population");
        sqlx::query("DELETE FROM stanox_crs WHERE stanox = 'TEST-BKG-STANOX'")
            .execute(&pool)
            .await
            .expect("cleanup stanox_crs");
        sqlx::query("DELETE FROM train_subscriptions WHERE user_id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("cleanup tracked_trains");
        sqlx::query("DELETE FROM trains WHERE train_uid = 'TEST-F49687' AND service_date = $1")
            .bind(service_date)
            .execute(&pool)
            .await
            .expect("cleanup trains");
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("cleanup user");
    }

    fn fixture_line_with_no_toml_tiploc(id: &str, crs: &str) -> LineDefinition {
        LineDefinition {
            id: id.to_string(),
            name: id.to_string(),
            mode: "rail".to_string(),
            category: "national-rail".to_string(),
            operators: vec![],
            stations: vec![common::Station {
                crs: crs.to_string(),
                tiploc: None, // the exact scenario the 2026-09-09 fix covers
                role: "minor".to_string(),
                segment: None,
            }],
            sample_stations: vec![],
            match_keywords: vec![],
            excluded_keywords: vec![],
            severity_overrides: HashMap::new(),
            destination_crs_filter: vec![],
            headcode_prefixes: vec![],
            full_coverage_enabled: false,
            pass_through: Vec::new(),
            crs_aliases: std::collections::BTreeMap::new(),
            trunk_for: Vec::new(),
        }
    }

    /// The actual regression test for the tiploc-schedule-matching-gap bug
    /// (2026-09-09): a station whose TOML entry carries no `tiploc` at all
    /// -- exactly the ~83% CRS-code case the live-production investigation
    /// found -- must still schedule-match, because its `crs_line_index`
    /// entry now comes from `crs_to_line_ids` itself (not hand-built, unlike
    /// the sibling tests above) and the real TIPLOC is resolved separately
    /// from the CIF-derived `stanox_crs` table. Before this fix,
    /// `crs_to_line_ids` would have produced an EMPTY index for this line
    /// (no station has a TOML `tiploc`), so `find_schedule_match` would
    /// have returned `Ok(None)` immediately, without ever touching
    /// `stanox_crs` -- permanently stuck "Waiting to hear from Network
    /// Rail" for any pin at this CRS.
    #[tokio::test]
    #[ignore = "requires a live database; see this plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p api \
                attempt_schedule_match -- --ignored --test-threads=1`"]
    async fn attempt_schedule_match_matches_a_station_with_no_toml_tiploc_via_real_stanox_crs_data()
    {
        let pool = connect().await;
        let user_id = "TEST-SCHEDULE-MATCH-NO-TOML-TIPLOC";
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind("no-toml-tiploc@example.com")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");

        // Real, CIF-derived data -- entirely independent of the TOML
        // catalogue below, and the only place a real TIPLOC comes from now.
        sqlx::query(
            "INSERT INTO stanox_crs (stanox, crs, tiploc, station_name, source_sequence) \
             VALUES ('TEST-NTT-STANOX', 'ZNT', 'ZNOTIPLOC', 'TEST NO TIPLOC STATION', 1) \
             ON CONFLICT (stanox) DO NOTHING",
        )
        .execute(&pool)
        .await
        .expect("seed stanox_crs");

        let service_date: NaiveDate = "2026-09-09".parse().unwrap();
        sqlx::query(
            "INSERT INTO schedule_line_population (line_id, service_date, population) \
             VALUES ('test-no-toml-tiploc-line', $1, $2) \
             ON CONFLICT (line_id, service_date) DO UPDATE SET population = EXCLUDED.population",
        )
        .bind(service_date)
        .bind(population_json("C88888", "ZNOTIPLOC", "19:15"))
        .execute(&pool)
        .await
        .expect("seed schedule_line_population");

        let scheduled_departure: DateTime<Utc> = "2026-09-09T19:15:00+01:00".parse().unwrap();
        let (tracked_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions (user_id, service_date, pin_origin_crs, pin_scheduled_departure) \
             VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(user_id)
        .bind(service_date)
        .bind("ZNT")
        .bind(scheduled_departure)
        .fetch_one(&pool)
        .await
        .expect("seed fixture tracked_trains row");

        // The load-bearing bit: this line's ONLY station has no TOML
        // `tiploc` set, and the index is built via the real
        // `crs_to_line_ids` function under test -- not hand-constructed
        // like the sibling tests above -- so this genuinely exercises the
        // fixed indexing behavior end to end.
        let lines = vec![fixture_line_with_no_toml_tiploc(
            "test-no-toml-tiploc-line",
            "ZNT",
        )];
        let crs_line_index = crs_to_line_ids(&lines);
        assert_eq!(
            crs_line_index.get("ZNT"),
            Some(&vec!["test-no-toml-tiploc-line".to_string()]),
            "sanity check: the fixed crs_to_line_ids must index a no-toml-tiploc station"
        );

        let matched = attempt_schedule_match(
            &pool,
            tracked_train_id,
            "ZNT",
            scheduled_departure,
            None, // no pinned destination in this fixture
            service_date,
            &crs_line_index,
            &[],
            None,
            None,
        )
        .await
        .expect("attempt schedule match");
        assert!(
            matched,
            "a station with no TOML tiploc must still schedule-match via real stanox_crs data"
        );

        let state = train_tracking::get_by_tracking_id(&pool, tracked_train_id)
            .await
            .expect("read tracked train")
            .expect("tracked train exists");
        assert_eq!(state.resolution_status, "schedule_matched");
        assert_eq!(state.train_uid, Some("C88888".to_string()));

        sqlx::query("DELETE FROM schedule_line_population WHERE line_id = 'test-no-toml-tiploc-line' AND service_date = $1")
            .bind(service_date)
            .execute(&pool)
            .await
            .expect("cleanup population");
        sqlx::query("DELETE FROM stanox_crs WHERE stanox = 'TEST-NTT-STANOX'")
            .execute(&pool)
            .await
            .expect("cleanup stanox_crs");
        sqlx::query("DELETE FROM train_subscriptions WHERE user_id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("cleanup tracked_trains");
        sqlx::query("DELETE FROM trains WHERE train_uid = 'C88888' AND service_date = $1")
            .bind(service_date)
            .execute(&pool)
            .await
            .expect("cleanup trains");
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("cleanup user");
    }

    #[tokio::test]
    #[ignore = "requires a live database; see this plan's Global Constraints for the \
                DATABASE_URL incantation, then run with `cargo test -p api \
                attempt_schedule_match -- --ignored --test-threads=1`"]
    async fn attempt_schedule_match_with_no_candidate_line_leaves_the_row_pending() {
        let pool = connect().await;
        let user_id = "TEST-SCHEDULE-MATCH-NO-CANDIDATE";
        sqlx::query(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        )
        .bind(user_id)
        .bind("no-candidate@example.com")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed fixture user");

        let service_date: NaiveDate = "2026-09-05".parse().unwrap();
        let (tracked_train_id,): (i64,) = sqlx::query_as(
            "INSERT INTO train_subscriptions (user_id, service_date, pin_origin_crs, pin_scheduled_departure) \
             VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(user_id)
        .bind(service_date)
        .bind("ZZZ")
        .bind("2026-09-05T19:15:00Z".parse::<DateTime<Utc>>().unwrap())
        .fetch_one(&pool)
        .await
        .expect("seed fixture tracked_trains row");

        let matched = attempt_schedule_match(
            &pool,
            tracked_train_id,
            "ZZZ",
            "2026-09-05T19:15:00Z".parse().unwrap(),
            None, // no pinned destination in this fixture
            service_date,
            &HashMap::new(), // no candidate lines at all
            &[],
            None,
            None,
        )
        .await
        .expect("attempt schedule match");
        assert!(!matched);

        let state = train_tracking::get_by_tracking_id(&pool, tracked_train_id)
            .await
            .expect("read tracked train")
            .expect("tracked train exists");
        assert_eq!(state.resolution_status, "pending");
        assert_eq!(state.train_uid, None);

        sqlx::query("DELETE FROM train_subscriptions WHERE user_id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("cleanup tracked_trains");
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("cleanup user");
    }
}
