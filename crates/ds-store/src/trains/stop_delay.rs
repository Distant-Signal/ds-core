//! A train's `delayMinutes` as a passenger sees it: measured against the
//! PUBLIC timetable at the passenger's own stop (design doc §9 decision 2).
//!
//! `train_current_state.delay_minutes` is TRUST's running delay, from the
//! train's latest report anywhere (a pass included), against the working
//! timetable. It stays internal: it is the input to the forecasts here and
//! to the per-stop estimates (`journey::apply_delay_estimates`). Every route
//! that serves a train-level `delayMinutes` replaces it through
//! [`apply_public_delays`] with:
//!
//! * the delay at the passenger's own stop (where they get off: a tracked
//!   train's pin destination, a journey leg's destination), measured once
//!   the train has reported there and forecast before then
//!   (`delayProvisional: true`); or
//! * with no stop of their own (the public train page, a line's trains),
//!   the delay at the latest call the train reported at.
//!
//! See `common::public_delay` for the arithmetic and the fallbacks, and
//! `delayBasis` for which baseline was used.

use sqlx::PgPool;

pub use common::public_delay::db::StopDelayTarget;
pub use common::public_delay::{DelayBasis, StopDelay};

/// [`common::public_delay::db::stop_delays`] with this process's CORPUS
/// crosswalk setting.
pub async fn stop_delays(
    pool: &PgPool,
    targets: &[StopDelayTarget],
) -> anyhow::Result<Vec<Option<StopDelay>>> {
    common::public_delay::db::stop_delays(pool, targets, crate::corpus::fallback_enabled()).await
}

/// A served row carrying a train-level delay.
pub trait PublicDelayFields {
    /// The train and the passenger's stop on it; `None` when the row has no
    /// matched train (its delay is left alone).
    fn delay_target(&self) -> Option<StopDelayTarget>;
    /// Stores the delay at the target's stop (`None`: not known).
    fn set_public_delay(&mut self, delay: Option<StopDelay>);
}

/// Replaces each row's delay with the public one at its own stop, in one
/// batched read for the whole slice (two queries; none when no row has a
/// matched train).
pub async fn apply_public_delays<T: PublicDelayFields>(
    pool: &PgPool,
    rows: &mut [T],
) -> anyhow::Result<()> {
    let mut positions = Vec::new();
    let mut targets = Vec::new();
    for (index, row) in rows.iter().enumerate() {
        if let Some(target) = row.delay_target() {
            positions.push(index);
            targets.push(target);
        }
    }
    if targets.is_empty() {
        return Ok(());
    }
    let delays = stop_delays(pool, &targets).await?;
    for (index, delay) in positions.into_iter().zip(delays) {
        rows[index].set_public_delay(delay);
    }
    Ok(())
}

/// [`StopDelayTarget`] from a row's usual columns: `None` without a matched
/// train (no `trains` row or no UID yet).
pub fn target(
    trains_id: Option<i64>,
    train_uid: Option<&str>,
    service_date: chrono::NaiveDate,
    stop_crs: Option<&str>,
    working_delay_minutes: Option<i32>,
) -> Option<StopDelayTarget> {
    Some(StopDelayTarget {
        trains_id: trains_id?,
        train_uid: train_uid?.to_string(),
        service_date,
        stop_crs: stop_crs
            .map(str::trim)
            .filter(|crs| !crs.is_empty())
            .map(str::to_uppercase),
        working_delay_minutes,
    })
}

/// The three wire fields a delay-carrying row stores, in one place so every
/// row type spells them the same way.
pub fn split(delay: Option<StopDelay>) -> (Option<i32>, Option<DelayBasis>, bool) {
    match delay {
        Some(delay) => (Some(delay.minutes), Some(delay.basis), delay.provisional),
        None => (None, None, false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_row_without_a_matched_train_has_no_target() {
        let date = chrono::NaiveDate::from_ymd_opt(2026, 10, 1).unwrap();
        assert_eq!(
            target(None, Some("C01372"), date, Some("MTH"), Some(3)),
            None
        );
        assert_eq!(target(Some(1), None, date, Some("MTH"), Some(3)), None);
        let target = target(Some(1), Some("C01372"), date, Some(" mth "), Some(3)).unwrap();
        assert_eq!(target.stop_crs.as_deref(), Some("MTH"));
        let blank = super::target(Some(1), Some("C01372"), date, Some(" "), None).unwrap();
        assert_eq!(blank.stop_crs, None);
    }

    #[test]
    fn split_spells_the_three_fields() {
        assert_eq!(split(None), (None, None, false));
        assert_eq!(
            split(Some(StopDelay {
                minutes: 4,
                basis: DelayBasis::PublicSchedule,
                provisional: true
            })),
            (Some(4), Some(DelayBasis::PublicSchedule), true)
        );
    }
}

#[cfg(test)]
mod db_tests {
    use super::*;
    use crate::test_support::connect;

    /// The shared read end to end, on the design doc's Avanti 9G44 shape:
    /// Watford Jn (public departure 20:31) to Milton Keynes (working 20:50H,
    /// public 20:51). 2026-10-02 is BST, so 20:31 local is 19:31Z.
    #[tokio::test]
    #[ignore = "requires a live database; run with `DATABASE_URL=... cargo test -p ds-store \
                stop_delays_measure_and_forecast_on_public_times -- --ignored --test-threads=1`"]
    #[expect(
        clippy::too_many_lines,
        reason = "one end-to-end fixture: its seed rows are most of the length"
    )]
    async fn stop_delays_measure_and_forecast_on_public_times() {
        use crate::{reference, schedule};
        let pool = connect().await;
        let date: chrono::NaiveDate = "2026-10-02".parse().unwrap();
        let uid = "TEST-SDLY";
        let trains_id = crate::trains::find_or_create_train(&pool, uid, date)
            .await
            .expect("find_or_create_train");
        sqlx::query("DELETE FROM train_movement_events WHERE trains_id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .expect("clear events");
        let records: Vec<common::StanoxCrsRecord> = ["EUS", "WFJ", "MKC"]
            .iter()
            .enumerate()
            .map(|(i, crs)| common::StanoxCrsRecord {
                stanox: format!("{uid}-{i}"),
                crs: (*crs).to_string(),
                tiploc: format!("{uid}-{crs}"),
                station_name: (*crs).to_string(),
                source_sequence: 1,
                change_time_minutes: None,
            })
            .collect();
        reference::upsert_stanox_crs(&pool, &records)
            .await
            .expect("seed stanox_crs");
        let t = |s: &str| Some(s.parse::<chrono::NaiveTime>().unwrap());
        let row = |seq: i16, crs: &str, kind: &str| schedule::ScheduleCallingPointsFullRow {
            service_date: date,
            uid: uid.to_string(),
            seq,
            tiploc: format!("{uid}-{crs}"),
            kind: kind.to_string(),
            day_offset: 0,
            ..Default::default()
        };
        let rows = vec![
            schedule::ScheduleCallingPointsFullRow {
                booked_departure: t("20:16:00"),
                public_departure: t("20:16:00"),
                working_departure: t("20:16:00"),
                ..row(0, "EUS", "origin")
            },
            schedule::ScheduleCallingPointsFullRow {
                booked_arrival: t("20:29:00"),
                booked_departure: t("20:31:00"),
                public_departure: t("20:31:00"),
                working_arrival: t("20:29:00"),
                working_departure: t("20:31:00"),
                ..row(1, "WFJ", "intermediate")
            },
            schedule::ScheduleCallingPointsFullRow {
                booked_arrival: t("20:50:00"),
                public_arrival: t("20:51:00"),
                working_arrival: t("20:50:30"),
                ..row(2, "MKC", "terminate")
            },
        ];
        schedule::upsert_schedule_calling_points_full(&pool, &rows)
            .await
            .expect("seed schedule_calling_points_full");
        // Departed Watford 4 late against TRUST's own public time.
        sqlx::query(
            "INSERT INTO train_movement_events \
                (trains_id, dedup_key, msg_type, event_type, loc_crs, planned_timestamp, \
                 actual_timestamp, gbtt_timestamp, variation_status, raw_body) \
             VALUES ($1, 'sdly-1', '0003', 'DEPARTURE', 'WFJ', '2026-10-02T19:31:00Z', \
                     '2026-10-02T19:35:00Z', '2026-10-02T19:31:00Z', 'LATE', '{}'::jsonb)",
        )
        .bind(trains_id)
        .execute(&pool)
        .await
        .expect("seed departure");

        let target = |stop: Option<&str>| StopDelayTarget {
            trains_id,
            train_uid: uid.to_string(),
            service_date: date,
            stop_crs: stop.map(str::to_string),
            working_delay_minutes: Some(4),
        };
        let delays = stop_delays(&pool, &[target(Some("MKC")), target(None)])
            .await
            .expect("stop_delays");
        assert_eq!(
            delays,
            vec![
                // Forecast: 20:50:30 + 4 = 20:54:30 against 20:51.
                Some(StopDelay {
                    minutes: 3,
                    basis: DelayBasis::PublicSchedule,
                    provisional: true
                }),
                // No stop of the user's own: the latest call, Watford.
                Some(StopDelay {
                    minutes: 4,
                    basis: DelayBasis::Public,
                    provisional: false
                }),
            ]
        );

        // Arrived at Milton Keynes, matched from the backlog (no gbtt): the
        // public arrival is TRUST's planned time plus the schedule's 30 s.
        sqlx::query(
            "INSERT INTO train_movement_events \
                (trains_id, dedup_key, msg_type, event_type, loc_crs, planned_timestamp, \
                 actual_timestamp, variation_status, raw_body) \
             VALUES ($1, 'sdly-2', '0003', 'ARRIVAL', 'MKC', '2026-10-02T19:50:30Z', \
                     '2026-10-02T19:56:00Z', 'LATE', '{}'::jsonb)",
        )
        .bind(trains_id)
        .execute(&pool)
        .await
        .expect("seed arrival");
        let delays = stop_delays(&pool, &[target(Some("mkc"))])
            .await
            .expect("stop_delays");
        assert_eq!(
            delays,
            vec![Some(StopDelay {
                minutes: 5,
                basis: DelayBasis::PublicSchedule,
                provisional: false
            })]
        );

        sqlx::query("DELETE FROM trains WHERE id = $1")
            .bind(trains_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM schedule_calling_points_full WHERE uid = $1")
            .bind(uid)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM stanox_crs WHERE stanox LIKE 'TEST-SDLY-%'")
            .execute(&pool)
            .await
            .ok();
    }
}
