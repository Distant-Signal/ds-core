//! Delay measured against the PUBLIC timetable, the time a passenger is sold
//! and the one Delay Repay and the National Rail performance measures use.
//! See docs/superpowers/specs/2026-10-01-working-vs-public-times-design.md
//! §9 (decision 2) and §11.
//!
//! # Baselines
//!
//! A reported TRUST movement is measured against the first of these that is
//! known ([`DelayBasis`] says which one was used):
//!
//! 1. [`DelayBasis::Public`]: TRUST's own public time for the movement,
//!    `gbtt_timestamp`. It comes off the same message as `actual_timestamp`
//!    and went through the same timestamp correction, so the two are on one
//!    time base (see `trust_timestamp`'s module doc for why that matters).
//! 2. [`DelayBasis::PublicSchedule`]: the public time from the CIF schedule
//!    at that call. TRUST's `gbtt_timestamp` is empty for a stop with no
//!    public time and for every row stored before 2026-10-07: until then
//!    trust-backlog-consumer, which writes almost every movement, dropped
//!    it (see migration 20261007210000). The CIF public time is NOT diffed
//!    against the TRUST actual directly, which would mix two time bases
//!    again; instead TRUST's own `planned_timestamp` (the working time) is
//!    moved by the schedule's `public - working` gap at that call. Both
//!    halves of that gap come from the same CIF record.
//! 3. [`DelayBasis::Working`]: the working timetable, `actual - planned`, when
//!    the call has no public time in that direction (a pass, or the
//!    departure of a set-down-only stop) or there is no schedule row.
//!
//! # Forecasts
//!
//! A call the train has not reported yet gets a forecast: the call's working
//! time plus the train's current running delay (TRUST's own, measured on
//! the working timetable, which is what the train is actually being run to),
//! compared with the call's public time. So the recovery margin the public
//! timetable builds in at a terminus counts, exactly as it will when the
//! train arrives: running 3 late into a terminus whose public arrival is 3
//! minutes after the working one is a forecast of 0. Every number here is a
//! difference between CIF times, so no TRUST instant is compared with a CIF
//! one.
//!
//! The arithmetic is pure; the [`db`] half (behind the `postgres` feature)
//! is the one read both `api` and `notifier` use to find the inputs.

use chrono::{DateTime, Duration, NaiveTime, Timelike, Utc};
use serde::Serialize;

use crate::trust_timestamp::{MAX_PLAUSIBLE_DELAY_MINUTES, plausible_delay_minutes};

/// Which baseline a delay was measured against. See the module doc.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum DelayBasis {
    /// TRUST's own public-timetable time (`gbtt_timestamp`).
    Public,
    /// The public time from the CIF schedule at that call.
    PublicSchedule,
    /// The working timetable: no public time was known.
    Working,
}

impl DelayBasis {
    /// The wire spelling (`public`, `publicSchedule`, `working`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::PublicSchedule => "publicSchedule",
            Self::Working => "working",
        }
    }

    /// Whether the baseline was a public time (either source).
    pub fn is_public(self) -> bool {
        !matches!(self, Self::Working)
    }
}

/// One direction (arrival or departure) of one call in the CIF schedule:
/// its public time and its exact working time (with `:30` seconds for a
/// half-minute). London local wall-clock times, as CIF carries them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScheduledSide {
    pub public: Option<NaiveTime>,
    pub working: Option<NaiveTime>,
}

impl ScheduledSide {
    /// `public - working`, the two read as the nearest pair across midnight
    /// (a 23:59:30 working arrival is a 00:00 public one, +30 s).
    pub fn public_minus_working(self) -> Option<Duration> {
        Some(nearest_gap(self.public?, self.working?))
    }
}

/// `later - earlier` for two clock times known to be close together, taking
/// whichever reading across midnight is under 12 hours.
fn nearest_gap(later: NaiveTime, earlier: NaiveTime) -> Duration {
    let mut seconds = i64::from(later.num_seconds_from_midnight())
        - i64::from(earlier.num_seconds_from_midnight());
    if seconds > 12 * 3600 {
        seconds -= 24 * 3600;
    } else if seconds < -12 * 3600 {
        seconds += 24 * 3600;
    }
    Duration::seconds(seconds)
}

/// The three TRUST times of one reported movement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reported {
    pub actual: DateTime<Utc>,
    /// TRUST's working (WTT) time for the movement.
    pub planned: Option<DateTime<Utc>>,
    /// TRUST's public time for the movement; `None` for a pass, a non-public
    /// event, or a movement matched from the backlog.
    pub gbtt: Option<DateTime<Utc>>,
}

/// A delay in whole minutes (truncated toward zero, like every other TRUST
/// delay in this codebase; positive is late) and the baseline it used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MeasuredDelay {
    pub minutes: i32,
    pub basis: DelayBasis,
}

/// The delay of a reported movement, measured against the first baseline
/// known (see the module doc). `scheduled` is the CIF schedule's same side
/// of the same call, for the [`DelayBasis::PublicSchedule`] fallback; pass
/// `ScheduledSide::default()` when there is no schedule row. `None` when
/// there is no baseline at all or the gap is implausible (a corrupt
/// timestamp more than a day out), like every other TRUST delay.
pub fn reported_delay(reported: Reported, scheduled: ScheduledSide) -> Option<MeasuredDelay> {
    reported_delay_with_gap(reported, scheduled.public_minus_working())
}

/// [`reported_delay`] given the schedule's `public - working` gap at the
/// call directly (from two instants of the same call, say), or `None` when
/// the call has no public time in that direction.
pub fn reported_delay_with_gap(
    reported: Reported,
    public_minus_working: Option<Duration>,
) -> Option<MeasuredDelay> {
    if let Some(gbtt) = reported.gbtt {
        return plausible_delay_minutes(reported.actual, gbtt).map(|minutes| MeasuredDelay {
            minutes,
            basis: DelayBasis::Public,
        });
    }
    let planned = reported.planned?;
    match public_minus_working {
        Some(gap) => {
            plausible_delay_minutes(reported.actual, planned + gap).map(|minutes| MeasuredDelay {
                minutes,
                basis: DelayBasis::PublicSchedule,
            })
        }
        None => plausible_delay_minutes(reported.actual, planned).map(|minutes| MeasuredDelay {
            minutes,
            basis: DelayBasis::Working,
        }),
    }
}

/// The forecast delay at a call not reported yet: the working time plus
/// `working_delay_minutes` (the train's current running delay), compared
/// with the public time. With no public time (or no working time to project
/// from) the running delay itself, on [`DelayBasis::Working`]. `None` for an
/// implausible input.
#[expect(
    clippy::cast_possible_truncation,
    reason = "bounded by MAX_PLAUSIBLE_DELAY_MINUTES just above"
)]
pub fn forecast_delay(
    scheduled: ScheduledSide,
    working_delay_minutes: i32,
) -> Option<MeasuredDelay> {
    if i64::from(working_delay_minutes).unsigned_abs() > MAX_PLAUSIBLE_DELAY_MINUTES.unsigned_abs()
    {
        return None;
    }
    let Some(gap) = scheduled.public_minus_working() else {
        return Some(MeasuredDelay {
            minutes: working_delay_minutes,
            basis: DelayBasis::Working,
        });
    };
    // Seconds, then truncated toward zero, so a half-minute gap behaves like
    // `num_minutes` does on a TRUST pair.
    let seconds = i64::from(working_delay_minutes) * 60 - gap.num_seconds();
    Some(MeasuredDelay {
        minutes: (seconds / 60) as i32,
        basis: DelayBasis::PublicSchedule,
    })
}

/// The kind of a TRUST movement, as far as a call's delay is concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MovementKind {
    Arrival,
    Departure,
    /// A pass (or anything else): never a call the passenger used.
    Other,
}

impl MovementKind {
    /// From `train_movement_events.event_type`.
    pub fn from_event_type(event_type: Option<&str>) -> Self {
        match event_type {
            Some("ARRIVAL") => Self::Arrival,
            Some("DEPARTURE") => Self::Departure,
            _ => Self::Other,
        }
    }
}

/// One TRUST movement at a call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CallMovement {
    pub kind: MovementKind,
    pub reported: Reported,
}

/// The delay at one stop, for a passenger using it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StopDelay {
    pub minutes: i32,
    pub basis: DelayBasis,
    /// `true` while the train has not reported at the stop yet, so this is
    /// a forecast ([`forecast_delay`]); `false` once it is a measurement.
    pub provisional: bool,
}

impl StopDelay {
    fn measured(delay: MeasuredDelay) -> Self {
        Self {
            minutes: delay.minutes,
            basis: delay.basis,
            provisional: false,
        }
    }

    fn forecast(delay: MeasuredDelay) -> Self {
        Self {
            minutes: delay.minutes,
            basis: delay.basis,
            provisional: true,
        }
    }
}

/// The delay at the stop a passenger gets off at (or, failing an arrival
/// time there, gets on at), from that stop's own movements:
///
/// 1. the latest reported ARRIVAL there, against the arrival baseline;
/// 2. else the latest reported DEPARTURE there (a station that reports
///    departures only), against the departure baseline;
/// 3. else a forecast ([`forecast_delay`]) from `current_working_delay`, on
///    the arrival side when the call has an arrival time, else the
///    departure side. `None` when the running delay is not known either.
///
/// `movements` are this call's own, oldest first. A pass is ignored.
pub fn delay_at_stop(
    movements: &[CallMovement],
    arrival: ScheduledSide,
    departure: ScheduledSide,
    current_working_delay: Option<i32>,
) -> Option<StopDelay> {
    let latest = |kind: MovementKind| movements.iter().rev().find(|m| m.kind == kind);
    if let Some(movement) = latest(MovementKind::Arrival) {
        return reported_delay(movement.reported, arrival).map(StopDelay::measured);
    }
    if let Some(movement) = latest(MovementKind::Departure) {
        return reported_delay(movement.reported, departure).map(StopDelay::measured);
    }
    let side = if arrival.working.is_some() || arrival.public.is_some() {
        arrival
    } else {
        departure
    };
    forecast_delay(side, current_working_delay?).map(StopDelay::forecast)
}

/// The train's current delay when no stop of the passenger's own is known
/// (the public train page, a line's train list): the latest movement at a
/// call (an ARRIVAL or DEPARTURE, never a pass), measured on its own side;
/// with none yet, the running delay on [`DelayBasis::Working`].
pub fn delay_at_latest_call(
    latest_call: Option<(CallMovement, ScheduledSide)>,
    current_working_delay: Option<i32>,
) -> Option<StopDelay> {
    if let Some((movement, side)) = latest_call
        && let Some(delay) = reported_delay(movement.reported, side)
    {
        return Some(StopDelay::measured(delay));
    }
    current_working_delay.map(|minutes| StopDelay {
        minutes,
        basis: DelayBasis::Working,
        provisional: false,
    })
}

#[cfg(feature = "postgres")]
pub mod db {
    //! The one read behind [`stop_delays`], shared by `api` and `notifier`.

    use std::collections::HashMap;

    use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
    use sqlx::PgPool;

    use super::{
        CallMovement, MovementKind, Reported, ScheduledSide, StopDelay, delay_at_latest_call,
        delay_at_stop,
    };

    /// One train, and the passenger's own stop on it.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct StopDelayTarget {
        pub trains_id: i64,
        pub train_uid: String,
        pub service_date: NaiveDate,
        /// The CRS the passenger gets off at; `None` for "no stop of their
        /// own" ([`super::delay_at_latest_call`]).
        pub stop_crs: Option<String>,
        /// `train_current_state.delay_minutes`: TRUST's running delay,
        /// measured on the working timetable.
        pub working_delay_minutes: Option<i32>,
    }

    /// One `train_movement_events` row, as far as a delay needs it.
    #[derive(sqlx::FromRow)]
    struct EventRow {
        trains_id: i64,
        loc_crs: String,
        event_type: Option<String>,
        planned_timestamp: Option<DateTime<Utc>>,
        actual_timestamp: Option<DateTime<Utc>>,
        gbtt_timestamp: Option<DateTime<Utc>>,
    }

    impl EventRow {
        fn movement(&self) -> Option<CallMovement> {
            Some(CallMovement {
                kind: MovementKind::from_event_type(self.event_type.as_deref()),
                reported: Reported {
                    actual: self.actual_timestamp?,
                    planned: self.planned_timestamp,
                    gbtt: self.gbtt_timestamp,
                },
            })
        }
    }

    /// One schedule call at a CRS (`schedule_calling_points_full`).
    #[derive(sqlx::FromRow)]
    struct CallRow {
        service_date: NaiveDate,
        uid: String,
        crs: String,
        public_arrival: Option<NaiveTime>,
        public_departure: Option<NaiveTime>,
        working_arrival: Option<NaiveTime>,
        working_departure: Option<NaiveTime>,
    }

    impl CallRow {
        fn arrival(&self) -> ScheduledSide {
            ScheduledSide {
                public: self.public_arrival,
                working: self.working_arrival,
            }
        }

        fn departure(&self) -> ScheduledSide {
            ScheduledSide {
                public: self.public_departure,
                working: self.working_departure,
            }
        }
    }

    /// The movements each target needs, from two narrow reads: every
    /// movement at a target's own stop (by `(trains_id, CRS)`), and, for a
    /// target with no stop, only its train's latest reported arrival or
    /// departure (a line's whole day of trains would otherwise read every
    /// movement of every train).
    struct Movements {
        /// `(trains_id, CRS)` -> that train's movements there, oldest first.
        at_stop: HashMap<(i64, String), Vec<EventRow>>,
        /// `trains_id` -> its latest reported call.
        latest: HashMap<i64, EventRow>,
    }

    async fn read_events(pool: &PgPool, targets: &[StopDelayTarget]) -> anyhow::Result<Movements> {
        let mut pairs: Vec<(i64, String)> = targets
            .iter()
            .filter_map(|t| Some((t.trains_id, t.stop_crs.as_deref()?.trim().to_uppercase())))
            .collect();
        pairs.sort();
        pairs.dedup();
        let mut latest_ids: Vec<i64> = targets
            .iter()
            .filter(|t| t.stop_crs.is_none())
            .map(|t| t.trains_id)
            .collect();
        latest_ids.sort_unstable();
        latest_ids.dedup();

        let mut at_stop: HashMap<(i64, String), Vec<EventRow>> = HashMap::new();
        if !pairs.is_empty() {
            let (ids, crs): (Vec<i64>, Vec<String>) = pairs.into_iter().unzip();
            let rows: Vec<EventRow> = sqlx::query_as(
                "SELECT e.trains_id, UPPER(e.loc_crs) AS loc_crs, e.event_type, \
                        e.planned_timestamp, e.actual_timestamp, e.gbtt_timestamp \
                 FROM train_movement_events e \
                 JOIN UNNEST($1::bigint[], $2::text[]) AS k(trains_id, crs) \
                   ON e.trains_id = k.trains_id AND UPPER(e.loc_crs) = k.crs \
                 ORDER BY e.trains_id, e.received_at ASC, e.id ASC",
            )
            .bind(&ids)
            .bind(&crs)
            .fetch_all(pool)
            .await?;
            for row in rows {
                at_stop
                    .entry((row.trains_id, row.loc_crs.clone()))
                    .or_default()
                    .push(row);
            }
        }
        let mut latest: HashMap<i64, EventRow> = HashMap::new();
        if !latest_ids.is_empty() {
            let rows: Vec<EventRow> = sqlx::query_as(
                "SELECT DISTINCT ON (trains_id) trains_id, UPPER(loc_crs) AS loc_crs, event_type, \
                        planned_timestamp, actual_timestamp, gbtt_timestamp \
                 FROM train_movement_events \
                 WHERE trains_id = ANY($1) AND loc_crs IS NOT NULL \
                   AND actual_timestamp IS NOT NULL \
                   AND event_type IN ('ARRIVAL', 'DEPARTURE') \
                 ORDER BY trains_id, received_at DESC, id DESC",
            )
            .bind(&latest_ids)
            .fetch_all(pool)
            .await?;
            latest = rows.into_iter().map(|row| (row.trains_id, row)).collect();
        }
        Ok(Movements { at_stop, latest })
    }

    /// The target trains' schedule calls at any of `crs`, keyed by
    /// `(date, uid, crs)`, each in stopping order.
    async fn read_calls(
        pool: &PgPool,
        targets: &[StopDelayTarget],
        crs: &[String],
        corpus_fallback: bool,
    ) -> anyhow::Result<HashMap<(NaiveDate, String, String), Vec<CallRow>>> {
        let mut calls_at: HashMap<(NaiveDate, String, String), Vec<CallRow>> = HashMap::new();
        if crs.is_empty() {
            return Ok(calls_at);
        }
        let mut keys: Vec<(NaiveDate, String)> = targets
            .iter()
            .map(|t| (t.service_date, t.train_uid.clone()))
            .collect();
        keys.sort();
        keys.dedup();
        let (dates, uids): (Vec<NaiveDate>, Vec<String>) = keys.into_iter().unzip();
        let rows: Vec<CallRow> = sqlx::query_as(
            "SELECT c.service_date, c.uid, x.crs, \
                    c.public_arrival, c.public_departure, \
                    COALESCE(c.working_arrival, c.booked_arrival) AS working_arrival, \
                    COALESCE(c.working_departure, c.booked_departure) AS working_departure \
             FROM schedule_calling_points_full c \
             JOIN UNNEST($1::date[], $2::text[]) AS k(service_date, uid) \
               ON c.service_date = k.service_date AND c.uid = k.uid \
             JOIN LATERAL ( \
                 SELECT UPPER(m.crs) AS crs FROM ( \
                     SELECT crs, 1 AS priority FROM tiploc_crs \
                      WHERE tiploc = UPPER(TRIM(c.tiploc)) \
                     UNION ALL \
                     SELECT crs, 2 AS priority FROM stanox_crs \
                      WHERE tiploc = UPPER(TRIM(c.tiploc)) \
                     UNION ALL \
                     SELECT crs, 3 AS priority FROM corpus_tiploc_crs \
                      WHERE $4 AND tiploc = UPPER(TRIM(c.tiploc)) \
                 ) m ORDER BY m.priority LIMIT 1 \
             ) x ON TRUE \
             WHERE x.crs = ANY($3) \
             ORDER BY c.service_date, c.uid, c.seq",
        )
        .bind(&dates)
        .bind(&uids)
        .bind(crs)
        .bind(corpus_fallback)
        .fetch_all(pool)
        .await?;
        for row in rows {
            calls_at
                .entry((row.service_date, row.uid.clone(), row.crs.clone()))
                .or_default()
                .push(row);
        }
        Ok(calls_at)
    }

    /// One target's delay from the already-read movements and calls.
    fn delay_for(
        target: &StopDelayTarget,
        movements: &Movements,
        calls_at: &HashMap<(NaiveDate, String, String), Vec<CallRow>>,
    ) -> Option<StopDelay> {
        let schedule = |crs: &str| {
            calls_at
                .get(&(
                    target.service_date,
                    target.train_uid.clone(),
                    crs.to_string(),
                ))
                .map(Vec::as_slice)
                .unwrap_or_default()
        };
        let Some(crs) = &target.stop_crs else {
            let latest = movements.latest.get(&target.trains_id).and_then(|event| {
                let movement = event.movement()?;
                let calls = schedule(&event.loc_crs);
                // A departure is from the first call there, an arrival at the
                // last (a loop's origin and terminus share a CRS).
                let side = match movement.kind {
                    MovementKind::Departure => calls
                        .iter()
                        .find(|c| c.working_departure.is_some())
                        .map(CallRow::departure),
                    _ => calls
                        .iter()
                        .rev()
                        .find(|c| c.working_arrival.is_some())
                        .map(CallRow::arrival),
                };
                Some((movement, side.unwrap_or_default()))
            });
            return delay_at_latest_call(latest, target.working_delay_minutes);
        };
        let crs = crs.trim().to_uppercase();
        let reported: Vec<CallMovement> = movements
            .at_stop
            .get(&(target.trains_id, crs.clone()))
            .into_iter()
            .flatten()
            .filter_map(EventRow::movement)
            .collect();
        let call = schedule(&crs).last();
        delay_at_stop(
            &reported,
            call.map(CallRow::arrival).unwrap_or_default(),
            call.map(CallRow::departure).unwrap_or_default(),
            target.working_delay_minutes,
        )
    }

    /// [`super::delay_at_stop`] (or [`super::delay_at_latest_call`] for a
    /// target with no stop) for each target, in the same order. Two reads
    /// for the whole batch: the trains' movements, and the schedule calls at
    /// the CRSs involved (`schedule_calling_points_full`, TIPLOC resolved to
    /// CRS through the same `tiploc_crs` > `stanox_crs` > CORPUS precedence
    /// as `api`'s `crs_for_tiplocs_batch`; the CORPUS step only with
    /// `corpus_fallback`). A train with no schedule row still gets a delay,
    /// on the fallbacks the module doc lists.
    ///
    /// A stop the train calls at twice (a loop) uses the LAST call there for
    /// the schedule, the one a passenger getting off would use.
    pub async fn stop_delays(
        pool: &PgPool,
        targets: &[StopDelayTarget],
        corpus_fallback: bool,
    ) -> anyhow::Result<Vec<Option<StopDelay>>> {
        if targets.is_empty() {
            return Ok(Vec::new());
        }
        let movements = read_events(pool, targets).await?;
        // The CRS each target needs a schedule call at.
        let mut wanted: Vec<String> = targets
            .iter()
            .filter_map(|t| match &t.stop_crs {
                Some(crs) => Some(crs.trim().to_uppercase()),
                None => movements
                    .latest
                    .get(&t.trains_id)
                    .map(|e| e.loc_crs.clone()),
            })
            .collect();
        wanted.sort();
        wanted.dedup();
        let calls_at = read_calls(pool, targets, &wanted, corpus_fallback).await?;
        Ok(targets
            .iter()
            .map(|target| delay_for(target, &movements, &calls_at))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(hms: &str) -> NaiveTime {
        NaiveTime::parse_from_str(hms, "%H:%M:%S").unwrap()
    }

    fn at(rfc3339: &str) -> DateTime<Utc> {
        rfc3339.parse().unwrap()
    }

    fn side(public: Option<&str>, working: Option<&str>) -> ScheduledSide {
        ScheduledSide {
            public: public.map(t),
            working: working.map(t),
        }
    }

    #[test]
    fn trusts_own_public_time_wins() {
        // Euston 2C01: working 06:19, public 06:22, arrived 06:21.
        let reported = Reported {
            actual: at("2026-10-01T05:21:00Z"),
            planned: Some(at("2026-10-01T05:19:00Z")),
            gbtt: Some(at("2026-10-01T05:22:00Z")),
        };
        let delay = reported_delay(reported, side(Some("06:22:00"), Some("06:19:00")));
        assert_eq!(
            delay,
            Some(MeasuredDelay {
                minutes: -1,
                basis: DelayBasis::Public
            })
        );
    }

    #[test]
    fn without_gbtt_the_schedules_public_gap_moves_trusts_planned_time() {
        // A backlog-matched movement: no gbtt. Planned 06:19 (WTT), public
        // three minutes later, actual 06:25 -> 3 late on public, not 6.
        let reported = Reported {
            actual: at("2026-10-01T05:25:00Z"),
            planned: Some(at("2026-10-01T05:19:00Z")),
            gbtt: None,
        };
        let delay = reported_delay(reported, side(Some("06:22:00"), Some("06:19:00")));
        assert_eq!(
            delay,
            Some(MeasuredDelay {
                minutes: 3,
                basis: DelayBasis::PublicSchedule
            })
        );
    }

    #[test]
    fn the_fallback_never_diffs_a_trust_instant_against_a_cif_one() {
        // TRUST's two fields skewed an hour from true UTC (the uncorrected
        // feed): only the CIF gap is added, so the skew cancels.
        let reported = Reported {
            actual: at("2026-10-01T06:25:00Z"),
            planned: Some(at("2026-10-01T06:19:00Z")),
            gbtt: None,
        };
        let delay = reported_delay(reported, side(Some("06:22:00"), Some("06:19:00")));
        assert_eq!(delay.map(|d| d.minutes), Some(3));
    }

    #[test]
    fn no_public_time_falls_back_to_the_working_timetable() {
        let reported = Reported {
            actual: at("2026-10-01T05:25:00Z"),
            planned: Some(at("2026-10-01T05:19:00Z")),
            gbtt: None,
        };
        for scheduled in [side(None, Some("06:19:00")), ScheduledSide::default()] {
            assert_eq!(
                reported_delay(reported, scheduled),
                Some(MeasuredDelay {
                    minutes: 6,
                    basis: DelayBasis::Working
                })
            );
        }
    }

    #[test]
    fn no_baseline_at_all_is_unknown() {
        let reported = Reported {
            actual: at("2026-10-01T05:25:00Z"),
            planned: None,
            gbtt: None,
        };
        assert_eq!(reported_delay(reported, side(Some("06:22:00"), None)), None);
    }

    #[test]
    fn an_implausible_gap_is_unknown() {
        let reported = Reported {
            actual: at("2026-10-05T05:25:00Z"),
            planned: Some(at("2026-10-01T05:19:00Z")),
            gbtt: Some(at("2026-10-01T05:19:00Z")),
        };
        assert_eq!(reported_delay(reported, ScheduledSide::default()), None);
    }

    #[test]
    fn a_half_minute_gap_across_midnight_is_thirty_seconds_not_a_day() {
        let gap = side(Some("00:00:00"), Some("23:59:30")).public_minus_working();
        assert_eq!(gap, Some(Duration::seconds(30)));
        let gap = side(Some("23:59:00"), Some("00:00:30")).public_minus_working();
        assert_eq!(gap, Some(Duration::seconds(-90)));
    }

    #[test]
    fn a_forecast_absorbs_the_terminus_recovery_margin() {
        // Running 3 late into a terminus whose public arrival is 3 minutes
        // after the working one: on time against the public timetable.
        assert_eq!(
            forecast_delay(side(Some("06:22:00"), Some("06:19:00")), 3),
            Some(MeasuredDelay {
                minutes: 0,
                basis: DelayBasis::PublicSchedule
            })
        );
        // A half-minute intermediate arrival (public rounded up): running 2
        // late on 20:50:30 -> 20:52:30 against 20:51 is 1.5, shown 1.
        assert_eq!(
            forecast_delay(side(Some("20:51:00"), Some("20:50:30")), 2).map(|d| d.minutes),
            Some(1)
        );
    }

    #[test]
    fn a_forecast_without_a_public_time_is_the_running_delay() {
        assert_eq!(
            forecast_delay(side(None, Some("06:19:00")), 7),
            Some(MeasuredDelay {
                minutes: 7,
                basis: DelayBasis::Working
            })
        );
        assert_eq!(forecast_delay(ScheduledSide::default(), 100_000), None);
    }

    fn movement(kind: MovementKind, actual: &str, planned: &str) -> CallMovement {
        CallMovement {
            kind,
            reported: Reported {
                actual: at(actual),
                planned: Some(at(planned)),
                gbtt: None,
            },
        }
    }

    #[test]
    fn delay_at_stop_prefers_the_arrival_then_the_departure_then_a_forecast() {
        let arrival = side(Some("10:02:00"), Some("10:01:30"));
        let departure = side(Some("10:03:00"), Some("10:03:00"));
        let arrived = movement(
            MovementKind::Arrival,
            "2026-10-01T09:20:00Z",
            "2026-10-01T09:01:30Z",
        );
        let departed = movement(
            MovementKind::Departure,
            "2026-10-01T09:25:00Z",
            "2026-10-01T09:03:00Z",
        );
        let passed = movement(
            MovementKind::Other,
            "2026-10-01T09:40:00Z",
            "2026-10-01T09:03:00Z",
        );
        assert_eq!(
            delay_at_stop(&[arrived, departed, passed], arrival, departure, Some(30)),
            Some(StopDelay {
                minutes: 18,
                basis: DelayBasis::PublicSchedule,
                provisional: false
            })
        );
        assert_eq!(
            delay_at_stop(&[departed], arrival, departure, Some(30)),
            Some(StopDelay {
                minutes: 22,
                basis: DelayBasis::PublicSchedule,
                provisional: false
            })
        );
        assert_eq!(
            delay_at_stop(&[passed], arrival, departure, Some(30)),
            Some(StopDelay {
                minutes: 29,
                basis: DelayBasis::PublicSchedule,
                provisional: true
            })
        );
        assert_eq!(delay_at_stop(&[], arrival, departure, None), None);
    }

    #[test]
    fn delay_at_stop_forecasts_on_the_departure_side_of_an_origin() {
        let departure = side(Some("09:59:00"), Some("10:00:00"));
        assert_eq!(
            delay_at_stop(&[], ScheduledSide::default(), departure, Some(5)),
            Some(StopDelay {
                minutes: 6,
                basis: DelayBasis::PublicSchedule,
                provisional: true
            })
        );
    }

    #[test]
    fn delay_at_latest_call_measures_the_latest_call_else_the_running_delay() {
        let departed = movement(
            MovementKind::Departure,
            "2026-10-01T09:25:00Z",
            "2026-10-01T09:03:00Z",
        );
        assert_eq!(
            delay_at_latest_call(
                Some((departed, side(Some("10:02:00"), Some("10:03:00")))),
                Some(40)
            ),
            Some(StopDelay {
                minutes: 23,
                basis: DelayBasis::PublicSchedule,
                provisional: false
            })
        );
        assert_eq!(
            delay_at_latest_call(None, Some(40)),
            Some(StopDelay {
                minutes: 40,
                basis: DelayBasis::Working,
                provisional: false
            })
        );
        assert_eq!(delay_at_latest_call(None, None), None);
    }
}
