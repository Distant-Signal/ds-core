//! The live state of tracked Iarnród Éireann trains, derived from the
//! board history (distant-signal's
//! docs/superpowers/specs/2026-11-05-irish-rail-train-tracking-design.md
//! §4.2 and §6).
//!
//! A tracked Irish train is a `trains` row whose `train_uid` is
//! `ie:<train_code>` and whose `service_date` is the train's `Traindate`
//! (the api's `POST /Journeys/irish-train` makes it). After each history
//! write the writer calls [`derive_for_samples`], which upserts the
//! `train_current_state` of every such train the samples showed, and queues
//! a `notifier_forward_queue` row whenever the status or the delay changes,
//! so the notifier (which wakes only on new movement events or queue rows)
//! sees it.
//!
//! This path never goes through `upsert_train_event_on` or the train event
//! outbox: their cancellation and resolution side effects are GB-only, and
//! it writes no `train_movement_events`. A train we haven't seen has no
//! state row, never an "on time" one, and a train that went quiet keeps the
//! state it had ([`common::island_of_ireland::train_state::current_state`]).

use anyhow::Result;
use chrono::{DateTime, NaiveDate, Utc};
use common::island_of_ireland::train_state::{CurrentState, Observation, current_state, interpret};
use common::island_of_ireland::{IslandOfIrelandNetwork, IslandOfIrelandStationSample, tracking};
use sqlx::PgConnection;

use super::network_wire;

/// The network whose trains have `ie:` uids.
const NETWORK: IslandOfIrelandNetwork = IslandOfIrelandNetwork::RepublicOfIreland;

/// What [`derive_for_trains`] wrote.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TrackedStateWrite {
    /// `train_current_state` rows inserted or changed.
    pub states: u64,
    /// `notifier_forward_queue` rows queued (a status or delay not queued
    /// before for that train).
    pub signals: u64,
}

/// [`derive_for_trains`] for every Iarnród Éireann train in `samples` with
/// a `train_date` and a valid code: the writer's `ioi-station-samples/1`
/// apply, after [`super::record_train_history`], in the same transaction.
pub async fn derive_for_samples(
    conn: &mut PgConnection,
    samples: &[IslandOfIrelandStationSample],
    now: DateTime<Utc>,
) -> Result<TrackedStateWrite> {
    let mut keys: Vec<(String, NaiveDate)> = samples
        .iter()
        .filter(|sample| sample.network == NETWORK)
        .flat_map(|sample| &sample.departures)
        .filter_map(|departure| {
            let date = departure.train_date?;
            tracking::is_train_code(&departure.train_code)
                .then(|| (departure.train_code.clone(), date))
        })
        .collect();
    keys.sort_unstable();
    keys.dedup();
    derive_for_trains(conn, &keys, now).await
}

/// One tracked train's newest history row.
#[derive(sqlx::FromRow)]
struct TrackedRow {
    trains_id: i64,
    train_code: String,
    train_date: NaiveDate,
    destination: String,
    status: String,
    late_minutes: Option<i32>,
    last_location: Option<String>,
    last_seen_at: DateTime<Utc>,
}

/// Upserts `train_current_state` for each `(train_code, train_date)` in
/// `keys` that has both a history row and a tracked `trains` row, read at
/// `now` (design §6.2). Trains in neither are skipped: the writer passes
/// every train on the boards, and most aren't tracked.
///
/// The state's `event_time` is the history's `last_seen_at` (when a board
/// last showed the train, not when it moved), and an older observation
/// never overwrites a newer one. A changed row whose status and delay have
/// not been queued for that train before queues one forward signal, dedup
/// key `ie:<code>:<date>:<status>:<delay>`.
pub async fn derive_for_trains(
    conn: &mut PgConnection,
    keys: &[(String, NaiveDate)],
    now: DateTime<Utc>,
) -> Result<TrackedStateWrite> {
    if keys.is_empty() {
        return Ok(TrackedStateWrite::default());
    }
    let rows: Vec<TrackedRow> = sqlx::query_as(
        r"
        SELECT tr.id AS trains_id, h.train_code, h.train_date, h.destination,
               h.status, h.late_minutes, h.last_location, h.last_seen_at
        FROM UNNEST($1::text[], $2::date[]) AS k(train_code, train_date)
        JOIN island_of_ireland_trains h
          ON h.network = $3 AND h.train_code = k.train_code AND h.train_date = k.train_date
        JOIN trains tr
          ON tr.train_uid = $4 || h.train_code AND tr.service_date = h.train_date
        ORDER BY tr.id
        ",
    )
    .bind(
        keys.iter()
            .map(|(code, _)| code.as_str())
            .collect::<Vec<_>>(),
    )
    .bind(keys.iter().map(|(_, date)| *date).collect::<Vec<_>>())
    .bind(network_wire(NETWORK))
    .bind(tracking::TRAIN_UID_PREFIX)
    .fetch_all(&mut *conn)
    .await?;

    let derived = derive_states(&rows, now);
    if derived.is_empty() {
        return Ok(TrackedStateWrite::default());
    }
    let changed = upsert_states(conn, &derived).await?;
    let signals: Vec<common::TrainForwardSignalMessage> = derived
        .into_iter()
        .filter(|d| changed.contains(&d.trains_id))
        .map(|d| common::TrainForwardSignalMessage {
            trains_id: d.trains_id,
            event_summary: d.summary,
            dedup_key: Some(d.dedup_key),
        })
        .collect();
    let queued =
        crate::tracking::forward_queue::insert_forward_signals_on(&mut *conn, &signals).await?;
    Ok(TrackedStateWrite {
        states: changed.len() as u64,
        signals: queued,
    })
}

/// One tracked train's state, ready to write, with the forward signal its
/// change would queue.
struct DerivedState {
    trains_id: i64,
    state: CurrentState,
    event_time: DateTime<Utc>,
    summary: String,
    dedup_key: String,
}

/// Design §6.2 for each row at `now`; a train not seen recently is left
/// out (no write).
fn derive_states(rows: &[TrackedRow], now: DateTime<Utc>) -> Vec<DerivedState> {
    rows.iter()
        .filter_map(|row| {
            let read = interpret(
                &Observation {
                    status: &row.status,
                    late_minutes: row.late_minutes,
                    last_location: row.last_location.as_deref(),
                    destination: &row.destination,
                    last_seen_at: row.last_seen_at,
                },
                now,
            );
            let state = current_state(&read)?;
            let delay = state
                .delay_minutes
                .map_or_else(|| "-".to_owned(), |d| d.to_string());
            let dedup_key = format!(
                "{}{}:{}:{}:{delay}",
                tracking::TRAIN_UID_PREFIX,
                row.train_code,
                row.train_date,
                state.status,
            );
            let summary = match &state.last_reported_location {
                Some(place) => format!("{} at {place} (board)", state.status),
                None => format!("{} (board)", state.status),
            };
            Some(DerivedState {
                trains_id: row.trains_id,
                state,
                event_time: row.last_seen_at,
                summary,
                dedup_key,
            })
        })
        .collect()
}

/// Upserts `derived` into `train_current_state`, never over a newer
/// `event_time` and never when nothing changed. Returns the `trains_id`s
/// written. `next_calling_point` and the ETA are TRUST's; an Irish train
/// has none, so they are cleared rather than left from nothing.
async fn upsert_states(conn: &mut PgConnection, derived: &[DerivedState]) -> Result<Vec<i64>> {
    let changed: Vec<i64> = sqlx::query_scalar(
        r"
        INSERT INTO train_current_state AS cs
            (trains_id, status, last_reported_location, last_event_type,
             delay_minutes, event_time, updated_at)
        SELECT trains_id, status, location, event_type, delay, event_time, now()
        FROM UNNEST($1::int8[], $2::text[], $3::text[], $4::text[], $5::int4[],
                    $6::timestamptz[])
            AS i(trains_id, status, location, event_type, delay, event_time)
        ON CONFLICT (trains_id) WHERE trains_id IS NOT NULL DO UPDATE SET
            status                 = EXCLUDED.status,
            last_reported_location = EXCLUDED.last_reported_location,
            last_event_type        = EXCLUDED.last_event_type,
            delay_minutes          = EXCLUDED.delay_minutes,
            next_calling_point     = NULL,
            eta_next               = NULL,
            eta_source             = NULL,
            event_time             = EXCLUDED.event_time,
            updated_at             = now()
        WHERE (cs.event_time IS NULL OR EXCLUDED.event_time >= cs.event_time)
          AND (cs.status, cs.last_reported_location, cs.last_event_type,
               cs.delay_minutes, cs.next_calling_point, cs.eta_next, cs.eta_source)
              IS DISTINCT FROM
              (EXCLUDED.status, EXCLUDED.last_reported_location,
               EXCLUDED.last_event_type, EXCLUDED.delay_minutes,
               NULL::text, NULL::timestamptz, NULL::text)
        RETURNING cs.trains_id
        ",
    )
    .bind(derived.iter().map(|d| d.trains_id).collect::<Vec<_>>())
    .bind(derived.iter().map(|d| d.state.status).collect::<Vec<_>>())
    .bind(
        derived
            .iter()
            .map(|d| d.state.last_reported_location.as_deref())
            .collect::<Vec<_>>(),
    )
    .bind(
        derived
            .iter()
            .map(|d| d.state.last_event_type)
            .collect::<Vec<_>>(),
    )
    .bind(
        derived
            .iter()
            .map(|d| d.state.delay_minutes)
            .collect::<Vec<_>>(),
    )
    .bind(derived.iter().map(|d| d.event_time).collect::<Vec<_>>())
    .fetch_all(&mut *conn)
    .await?;
    Ok(changed)
}

#[cfg(test)]
mod db_tests {
    use super::*;
    use crate::samples::island_of_ireland::{prune_train_history, record_train_history};
    use crate::test_support::{cleanup_user, connect, seed_user};
    use common::island_of_ireland::IslandOfIrelandDeparture;
    use sqlx::PgPool;

    const USER: &str = "test-ioi-tracked-user";

    fn day(d: u32) -> NaiveDate {
        let date = NaiveDate::from_ymd_opt(2099, 1, d).unwrap();
        crate::test_support::assert_synthetic_date(date);
        date
    }

    /// The fixtures' base time, an hour ago, fixed once per process (the
    /// history's guards treat a time more than 2 minutes ahead as skew).
    fn base() -> DateTime<Utc> {
        static BASE: std::sync::LazyLock<DateTime<Utc>> = std::sync::LazyLock::new(|| {
            use chrono::{DurationRound, TimeDelta};
            (Utc::now() - chrono::Duration::hours(1))
                .duration_trunc(TimeDelta::seconds(1))
                .unwrap()
        });
        *BASE
    }

    fn at(seconds: i64) -> DateTime<Utc> {
        base() + chrono::Duration::seconds(seconds)
    }

    fn departure(
        code: &str,
        date: NaiveDate,
        status: &str,
        late: i32,
        last_location: Option<&str>,
    ) -> IslandOfIrelandDeparture {
        IslandOfIrelandDeparture {
            train_code: code.into(),
            origin: "Zesttown".into(),
            destination: "Zorough".into(),
            scheduled_arrival: Some("10:50".into()),
            scheduled_departure: Some("10:51".into()),
            expected_arrival: Some("10:50".into()),
            expected_departure: Some("10:51".into()),
            late_minutes: late,
            status: status.into(),
            due_in_minutes: Some(5),
            train_date: Some(date),
            origin_time: Some("10:00".into()),
            destination_time: Some("12:00".into()),
            last_location: last_location.map(Into::into),
        }
    }

    fn board(
        station: &str,
        seconds: i64,
        departures: Vec<IslandOfIrelandDeparture>,
    ) -> IslandOfIrelandStationSample {
        IslandOfIrelandStationSample {
            station_id: station.into(),
            network: NETWORK,
            polled_at: at(seconds),
            departures,
        }
    }

    async fn clean(pool: &PgPool) {
        for sql in [
            "DELETE FROM island_of_ireland_train_calls WHERE train_code LIKE 'ZT%'",
            "DELETE FROM island_of_ireland_trains WHERE train_code LIKE 'ZT%'",
            "DELETE FROM trains WHERE train_uid LIKE 'ie:ZT%' OR train_uid LIKE 'ZT%'",
        ] {
            sqlx::query(sql).execute(pool).await.unwrap();
        }
        cleanup_user(pool, USER).await;
    }

    /// Records `samples` and derives their tracked states, as the writer's
    /// apply does, read a minute after the newest poll.
    async fn apply(
        conn: &mut PgConnection,
        samples: &[IslandOfIrelandStationSample],
    ) -> TrackedStateWrite {
        record_train_history(conn, samples).await.unwrap();
        let newest = samples.iter().map(|s| s.polled_at).max().unwrap();
        derive_for_samples(conn, samples, newest + chrono::Duration::minutes(1))
            .await
            .unwrap()
    }

    type StateRow = (String, Option<i32>, Option<String>, Option<String>);

    async fn state(pool: &PgPool, trains_id: i64) -> Option<StateRow> {
        sqlx::query_as(
            "SELECT status, delay_minutes, last_reported_location, last_event_type \
             FROM train_current_state WHERE trains_id = $1",
        )
        .bind(trains_id)
        .fetch_optional(pool)
        .await
        .unwrap()
    }

    async fn signal_keys(pool: &PgPool, trains_id: i64) -> Vec<String> {
        sqlx::query_scalar(
            "SELECT dedup_key FROM notifier_forward_queue WHERE trains_id = $1 ORDER BY id",
        )
        .bind(trains_id)
        .fetch_all(pool)
        .await
        .unwrap()
    }

    /// Board by board: an untracked train gets no state; a tracked one
    /// gets design §6.2's state, and one signal per new status and delay
    /// (a moved location alone queues nothing); a late, older poll
    /// changes nothing; a GB uid that happens to equal the code is never
    /// touched.
    #[tokio::test]
    #[ignore = "requires a live database; run with `cargo test -p ds-store \
                island_of_ireland -- --ignored --test-threads=1`"]
    #[expect(clippy::too_many_lines, reason = "one scenario, board by board")]
    async fn tracked_trains_get_state_and_signals_from_the_boards() {
        let pool = connect().await;
        clean(&pool).await;
        let mut conn = pool.acquire().await.unwrap();
        let date = day(5);

        // Not tracked yet: the history is written, no state.
        let quiet = [board(
            "ZTAAA",
            0,
            vec![departure("ZT01", date, "No Information", 0, None)],
        )];
        assert_eq!(apply(&mut conn, &quiet).await, TrackedStateWrite::default());

        let trains_id = crate::trains::find_or_create_train(&pool, "ie:ZT01", date)
            .await
            .unwrap();
        let gb_id = crate::trains::find_or_create_train(&pool, "ZT01", date)
            .await
            .unwrap();

        // No report: awaiting activation with no delay, never "on time".
        let wrote = derive_for_trains(&mut conn, &[("ZT01".into(), date)], at(60))
            .await
            .unwrap();
        assert_eq!(
            wrote,
            TrackedStateWrite {
                states: 1,
                signals: 1
            }
        );
        assert_eq!(
            state(&pool, trains_id).await,
            Some(("awaiting_activation".into(), None, None, None))
        );

        let running = [board(
            "ZTAAA",
            300,
            vec![departure(
                "ZT01",
                date,
                "En Route",
                14,
                Some("Departed Zesttown"),
            )],
        )];
        assert_eq!(
            apply(&mut conn, &running).await,
            TrackedStateWrite {
                states: 1,
                signals: 1
            }
        );
        assert_eq!(
            state(&pool, trains_id).await,
            Some((
                "en_route".into(),
                Some(14),
                Some("Zesttown".into()),
                Some("DEPARTURE".into())
            ))
        );

        // Same delay, new place: the state moves, no new signal.
        let moved = [board(
            "ZTBBB",
            600,
            vec![departure(
                "ZT01",
                date,
                "En Route",
                14,
                Some("Arrived Zmid"),
            )],
        )];
        assert_eq!(
            apply(&mut conn, &moved).await,
            TrackedStateWrite {
                states: 1,
                signals: 0
            }
        );

        let later = [board(
            "ZTBBB",
            900,
            vec![departure(
                "ZT01",
                date,
                "En Route",
                16,
                Some("Departed Zmid"),
            )],
        )];
        assert_eq!(
            apply(&mut conn, &later).await,
            TrackedStateWrite {
                states: 1,
                signals: 1
            }
        );

        // An older poll arriving late: the history keeps the newer row, and
        // the state is unchanged.
        let stale = [board(
            "ZTAAA",
            450,
            vec![departure("ZT01", date, "No Information", 0, None)],
        )];
        assert_eq!(apply(&mut conn, &stale).await, TrackedStateWrite::default());
        assert_eq!(state(&pool, trains_id).await.unwrap().1, Some(16));

        let arrived = [board(
            "ZTCCC",
            1200,
            vec![departure(
                "ZT01",
                date,
                "En Route",
                16,
                Some("Arrived Zorough"),
            )],
        )];
        assert_eq!(
            apply(&mut conn, &arrived).await,
            TrackedStateWrite {
                states: 1,
                signals: 1
            }
        );
        assert_eq!(
            state(&pool, trains_id).await,
            Some((
                "completed".into(),
                Some(16),
                Some("Zorough".into()),
                Some("ARRIVAL".into())
            ))
        );

        let key = |rest: &str| format!("ie:ZT01:{date}:{rest}");
        assert_eq!(
            signal_keys(&pool, trains_id).await,
            [
                key("awaiting_activation:-"),
                key("en_route:14"),
                key("en_route:16"),
                key("completed:16"),
            ]
        );
        assert_eq!(state(&pool, gb_id).await, None);
        assert_eq!(signal_keys(&pool, gb_id).await, Vec::<String>::new());

        // Read 30 minutes after the train's last board, nothing is written:
        // a quiet train keeps the state it had.
        sqlx::query("DELETE FROM train_current_state WHERE trains_id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE island_of_ireland_trains SET last_location = 'Departed Zmid' \
             WHERE train_code = 'ZT01'",
        )
        .execute(&pool)
        .await
        .unwrap();
        let wrote = derive_for_trains(&mut conn, &[("ZT01".into(), date)], at(1200 + 31 * 60))
            .await
            .unwrap();
        assert_eq!(wrote, TrackedStateWrite::default());
        assert_eq!(state(&pool, trains_id).await, None);

        drop(conn);
        clean(&pool).await;
    }

    /// The prune's two tiers: a subscribed train's history stays until the
    /// tracked cutoff; a train with a `trains` row but no subscription, or
    /// none at all, goes at the untracked one.
    #[tokio::test]
    #[ignore = "requires a live database; run with `cargo test -p ds-store \
                island_of_ireland -- --ignored --test-threads=1`"]
    async fn the_prune_keeps_tracked_trains_longer() {
        let pool = connect().await;
        clean(&pool).await;
        seed_user(&pool, USER).await;
        let mut conn = pool.acquire().await.unwrap();
        let boards = [board(
            "ZTAAA",
            0,
            vec![
                departure("ZT21", day(1), "En Route", 0, None),
                departure("ZT22", day(1), "En Route", 0, None),
                departure("ZT23", day(1), "En Route", 0, None),
            ],
        )];
        record_train_history(&mut conn, &boards).await.unwrap();
        let tracked = crate::trains::find_or_create_train(&pool, "ie:ZT21", day(1))
            .await
            .unwrap();
        crate::trains::find_or_create_train(&pool, "ie:ZT22", day(1))
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO train_subscriptions \
                 (user_id, service_date, trains_id, resolution_status, pin_origin_ref) \
             VALUES ($1, $2, $3, 'resolved', 'ie:ZTAAA')",
        )
        .bind(USER)
        .bind(day(1))
        .bind(tracked)
        .execute(&pool)
        .await
        .unwrap();

        let left = || async {
            sqlx::query_scalar::<_, String>(
                "SELECT train_code FROM island_of_ireland_trains WHERE train_code LIKE 'ZT%' \
                 UNION ALL SELECT train_code FROM island_of_ireland_train_calls \
                 WHERE train_code LIKE 'ZT%' ORDER BY 1",
            )
            .fetch_all(&pool)
            .await
            .unwrap()
        };
        // Untracked cutoff past day 1, tracked cutoff not: ZT22 and ZT23
        // go (a call and a train each), ZT21 stays.
        assert_eq!(
            prune_train_history(&mut conn, day(3), day(1), 100)
                .await
                .unwrap(),
            4
        );
        assert_eq!(left().await, ["ZT21", "ZT21"]);
        // A tracked cutoff later than the untracked one counts as it.
        assert_eq!(
            prune_train_history(&mut conn, day(1), day(9), 100)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            prune_train_history(&mut conn, day(3), day(2), 100)
                .await
                .unwrap(),
            2
        );
        assert_eq!(left().await, Vec::<String>::new());

        drop(conn);
        clean(&pool).await;
    }
}
