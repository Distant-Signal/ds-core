//! A predicted late start from the train's previous working
//! (docs/superpowers/specs/2026-10-21-pre-departure-prediction-design.md,
//! sections 5 and 3a, and the 2026-10-22 decisions).
//!
//! Before TRUST reports a train T, DS knows nothing live about it. But the
//! unit that forms T usually arrives on an earlier train P, its previous
//! working (`schedule_turnarounds`). If P is late enough that the booked
//! turnaround cannot absorb it, T starts late:
//!
//! ```text
//! excess = A - S          A: P's delay at its terminus, S: booked turnaround
//! late   = max(0, excess + TURN_MIN)
//! ```
//!
//! `A` is P's current TRUST delay (uniform propagation, no recovery), or,
//! when P has not started either, P's own predicted late start from its
//! previous working (two links deep at most).
//!
//! **Confidence.**
//!
//! - `high`: one link, P's data is live and fresh (or P has arrived), the
//!   excess is between 0 and [`MEDIUM_EXCESS_LIMIT`] (P has eaten the whole
//!   slack), and the operator is not in [`MEDIUM_CAPPED_OPERATORS`]. Over a
//!   week of TRUST actuals, 92% of such trains started at least 3 minutes
//!   late, with a mean error of 2.1 minutes. Only `high` sends a push.
//! - `medium`: one link, fresh, excess at most [`MEDIUM_EXCESS_LIMIT`].
//! - `low`: the excess is above [`MEDIUM_EXCESS_LIMIT`], P's data is older
//!   than [`FRESHNESS`], or the prediction is two links deep. Shown only as
//!   "may start late", never as a number (`late_minutes` is `None`).
//! - Nothing above [`SHOW_EXCESS_LIMIT`] of excess (the unit was most
//!   likely swapped), nothing when P is cancelled, nothing when the
//!   prediction is 0, and nothing once T itself has a TRUST report.
//!
//! The rule is pure ([`rule`], [`assemble`]); [`predict_for_trains`] loads
//! the evidence for a batch of trains in one query.

use std::collections::HashMap;

use chrono::{DateTime, Duration, NaiveDate, Utc};
use serde::Serialize;
use sqlx::PgPool;

/// `TURN_MIN`: the shortest turnaround a unit is assumed to need. 4 minutes
/// fitted best over 2026-10-15..21 (spec section 3a).
pub const TURN_MIN_MINUTES: i32 = 4;
/// Above this much excess the rule over-predicts: confidence drops to low.
pub const MEDIUM_EXCESS_LIMIT: i32 = 15;
/// Above this much excess nothing is shown: the unit was likely swapped.
pub const SHOW_EXCESS_LIMIT: i32 = 30;
/// A prediction is offered from this long before T's booked departure.
pub const HORIZON: Duration = Duration::hours(3);
/// P's live data older than this lowers the confidence to low.
pub const FRESHNESS: Duration = Duration::minutes(10);
/// Operators whose high-confidence band was not precise enough over the
/// phase 0 week (spec section 3a: Thameslink 69%, Chiltern 50% on 8
/// pairs). Their predictions stop at medium, so they never push. Revisit
/// with phase 2's association links.
pub const MEDIUM_CAPPED_OPERATORS: &[&str] = &["TL", "CH"];
/// `predictedStart.basis`, and the stop estimates' `estimateSource`.
pub const BASIS: &str = "previous-working";

/// How sure the prediction is. Ordered: `Low < Medium < High`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Confidence {
    Low,
    Medium,
    High,
}

impl Confidence {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }
}

/// `predictedStart.previousWorking`: the incoming train the prediction
/// rests on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PreviousWorking {
    pub train_uid: String,
    pub service_date: NaiveDate,
    /// `inferred` (the platform sequence) or `association` (phase 2).
    pub link: String,
    /// P's `train_current_state.status` (`en_route`, `completed`,
    /// `awaiting_activation`), `None` without a state row.
    pub status: Option<String>,
    /// P's current TRUST delay, against the working timetable.
    pub delay_minutes: Option<i32>,
    /// When P's state was last reported.
    pub observed_at: Option<DateTime<Utc>>,
    pub booked_turnaround_minutes: i32,
}

/// A train's `predictedStart` (spec section 7). Additive and nullable on
/// the wire: absent knowledge is `null`, never a guess of "on time".
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictedStart {
    /// Minutes late T is expected to start; `None` at low confidence.
    pub late_minutes: Option<i32>,
    pub confidence: Confidence,
    /// Always [`BASIS`].
    pub basis: &'static str,
    pub computed_at: DateTime<Utc>,
    pub previous_working: PreviousWorking,
    /// The rule's number even at low confidence, for the server's own use
    /// (never served).
    #[serde(skip)]
    pub minutes: i32,
}

impl PredictedStart {
    /// The minutes stop estimates may use: medium or high only.
    pub fn estimate_minutes(&self) -> Option<i32> {
        (self.confidence >= Confidence::Medium).then_some(self.minutes)
    }
}

/// The rule's inputs for one link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Evidence<'a> {
    /// `A`: the incoming train's delay at its terminus, in minutes.
    pub incoming_delay_minutes: i32,
    /// `S`: the booked turnaround, in minutes.
    pub booked_turnaround_minutes: i32,
    /// T's operator (ATOC code).
    pub operator: Option<&'a str>,
    /// Whether the incoming train's data is fresh (or final).
    pub fresh: bool,
    /// 1 when the incoming train is live, 2 when its own prediction is used.
    pub depth: u8,
}

/// The section 5 rule: `(late minutes, confidence)`, or `None` when there
/// is nothing to show (no lateness, or too much excess to trust).
pub fn rule(evidence: &Evidence<'_>) -> Option<(i32, Confidence)> {
    let excess = evidence.incoming_delay_minutes - evidence.booked_turnaround_minutes;
    if excess > SHOW_EXCESS_LIMIT {
        return None;
    }
    let late = (excess + TURN_MIN_MINUTES).max(0);
    if late == 0 {
        return None;
    }
    let capped = evidence
        .operator
        .is_some_and(|op| MEDIUM_CAPPED_OPERATORS.contains(&op));
    let confidence = if evidence.depth > 1 || !evidence.fresh || excess > MEDIUM_EXCESS_LIMIT {
        Confidence::Low
    } else if excess >= 0 && !capped {
        Confidence::High
    } else {
        Confidence::Medium
    };
    Some((late, confidence))
}

/// A train's live state as stored (`train_current_state`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LiveState {
    pub status: Option<String>,
    pub delay_minutes: Option<i32>,
    pub observed_at: Option<DateTime<Utc>>,
}

impl LiveState {
    fn is_running_or_arrived(&self) -> bool {
        matches!(self.status.as_deref(), Some("en_route" | "completed"))
            && self.delay_minutes.is_some()
    }

    fn is_cancelled(&self) -> bool {
        self.status.as_deref() == Some("cancelled")
    }

    /// Whether TRUST has reported this train at all.
    fn has_started(&self) -> bool {
        matches!(
            self.status.as_deref(),
            Some("en_route" | "completed" | "cancelled")
        ) || self.delay_minutes.is_some()
    }

    fn is_fresh(&self, now: DateTime<Utc>) -> bool {
        self.status.as_deref() == Some("completed")
            || self.observed_at.is_some_and(|at| now - at <= FRESHNESS)
    }
}

/// One link's facts: the turnaround and the previous working's state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkFacts {
    pub prev_uid: String,
    pub prev_service_date: NaiveDate,
    pub booked_turnaround_minutes: i32,
    pub link: String,
    pub prev_state: LiveState,
}

/// Everything [`assemble`] needs for one train T.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrainFacts {
    /// T's booked working origin departure.
    pub departs_at: DateTime<Utc>,
    pub operator: Option<String>,
    pub own_state: LiveState,
    /// T's previous working P.
    pub previous: LinkFacts,
    /// P's own previous working, for when P has not started.
    pub previous_of_previous: Option<LinkFacts>,
}

/// T's prediction from its facts, or `None` (see the module docs for every
/// case).
pub fn assemble(facts: &TrainFacts, now: DateTime<Utc>) -> Option<PredictedStart> {
    if now < facts.departs_at - HORIZON || facts.own_state.has_started() {
        return None;
    }
    let previous = &facts.previous;
    let operator = facts.operator.as_deref();
    let (incoming_delay_minutes, fresh, depth) = if previous.prev_state.is_cancelled() {
        return None;
    } else if previous.prev_state.is_running_or_arrived() {
        (
            previous.prev_state.delay_minutes?,
            previous.prev_state.is_fresh(now),
            1,
        )
    } else {
        // P has not started: its own predicted late start is its delay.
        let upstream = facts.previous_of_previous.as_ref()?;
        if !upstream.prev_state.is_running_or_arrived() {
            return None;
        }
        let (late, _) = rule(&Evidence {
            incoming_delay_minutes: upstream.prev_state.delay_minutes?,
            booked_turnaround_minutes: upstream.booked_turnaround_minutes,
            operator,
            fresh: upstream.prev_state.is_fresh(now),
            depth: 1,
        })?;
        (late, upstream.prev_state.is_fresh(now), 2)
    };
    let (minutes, confidence) = rule(&Evidence {
        incoming_delay_minutes,
        booked_turnaround_minutes: previous.booked_turnaround_minutes,
        operator,
        fresh,
        depth,
    })?;
    Some(PredictedStart {
        late_minutes: (confidence >= Confidence::Medium).then_some(minutes),
        confidence,
        basis: BASIS,
        computed_at: now,
        previous_working: PreviousWorking {
            train_uid: previous.prev_uid.clone(),
            service_date: previous.prev_service_date,
            link: previous.link.clone(),
            status: previous.prev_state.status.clone(),
            delay_minutes: previous.prev_state.delay_minutes,
            observed_at: previous.prev_state.observed_at,
            booked_turnaround_minutes: previous.booked_turnaround_minutes,
        },
        minutes,
    })
}

#[derive(sqlx::FromRow)]
struct FactsRow {
    uid: String,
    service_date: NaiveDate,
    departs_at: DateTime<Utc>,
    operator_atoc: Option<String>,
    t_status: Option<String>,
    t_delay: Option<i32>,
    prev_uid: String,
    prev_service_date: NaiveDate,
    turnaround: i16,
    link: String,
    p_status: Option<String>,
    p_delay: Option<i32>,
    p_observed: Option<DateTime<Utc>>,
    p2_uid: Option<String>,
    p2_service_date: Option<NaiveDate>,
    p2_turnaround: Option<i16>,
    p2_link: Option<String>,
    p2_status: Option<String>,
    p2_delay: Option<i32>,
    p2_observed: Option<DateTime<Utc>>,
}

impl FactsRow {
    fn into_facts(self) -> ((String, NaiveDate), TrainFacts) {
        let previous_of_previous = match (self.p2_uid, self.p2_service_date, self.p2_turnaround) {
            (Some(prev_uid), Some(prev_service_date), Some(turnaround)) => Some(LinkFacts {
                prev_uid,
                prev_service_date,
                booked_turnaround_minutes: i32::from(turnaround),
                link: self.p2_link.unwrap_or_else(|| "inferred".to_string()),
                prev_state: LiveState {
                    status: self.p2_status,
                    delay_minutes: self.p2_delay,
                    observed_at: self.p2_observed,
                },
            }),
            _ => None,
        };
        (
            (self.uid, self.service_date),
            TrainFacts {
                departs_at: self.departs_at,
                operator: self.operator_atoc,
                own_state: LiveState {
                    status: self.t_status,
                    delay_minutes: self.t_delay,
                    observed_at: None,
                },
                previous: LinkFacts {
                    prev_uid: self.prev_uid,
                    prev_service_date: self.prev_service_date,
                    booked_turnaround_minutes: i32::from(self.turnaround),
                    link: self.link,
                    prev_state: LiveState {
                        status: self.p_status,
                        delay_minutes: self.p_delay,
                        observed_at: self.p_observed,
                    },
                },
                previous_of_previous,
            },
        )
    }
}

/// The facts for every `(train_uid, service_date)` in `trains` that has a
/// previous working, in one query: T's turnaround, operator and own state,
/// P's state, and P's own previous working with its state.
pub async fn facts_for_trains(
    pool: &PgPool,
    trains: &[(String, NaiveDate)],
) -> anyhow::Result<HashMap<(String, NaiveDate), TrainFacts>> {
    if trains.is_empty() {
        return Ok(HashMap::new());
    }
    let uids: Vec<&str> = trains.iter().map(|(uid, _)| uid.as_str()).collect();
    let dates: Vec<NaiveDate> = trains.iter().map(|(_, date)| *date).collect();
    let rows: Vec<FactsRow> = sqlx::query_as(
        "SELECT k.uid, k.service_date, t.departs_at, ss.operator_atoc, \
                cst.status AS t_status, cst.delay_minutes AS t_delay, \
                t.prev_uid, t.prev_service_date, t.booked_turnaround_minutes AS turnaround, \
                t.link, \
                csp.status AS p_status, csp.delay_minutes AS p_delay, \
                COALESCE(csp.event_time, csp.updated_at) AS p_observed, \
                tp.prev_uid AS p2_uid, tp.prev_service_date AS p2_service_date, \
                tp.booked_turnaround_minutes AS p2_turnaround, tp.link AS p2_link, \
                csp2.status AS p2_status, csp2.delay_minutes AS p2_delay, \
                COALESCE(csp2.event_time, csp2.updated_at) AS p2_observed \
         FROM (SELECT DISTINCT * FROM UNNEST($1::text[], $2::date[])) AS k(uid, service_date) \
         JOIN schedule_turnarounds t ON t.service_date = k.service_date AND t.uid = k.uid \
         LEFT JOIN schedule_services ss ON ss.service_date = k.service_date AND ss.uid = k.uid \
         LEFT JOIN trains trt ON trt.train_uid = k.uid AND trt.service_date = k.service_date \
         LEFT JOIN train_current_state cst ON cst.trains_id = trt.id \
         LEFT JOIN trains trp \
           ON trp.train_uid = t.prev_uid AND trp.service_date = t.prev_service_date \
         LEFT JOIN train_current_state csp ON csp.trains_id = trp.id \
         LEFT JOIN schedule_turnarounds tp \
           ON tp.service_date = t.prev_service_date AND tp.uid = t.prev_uid \
         LEFT JOIN trains trp2 \
           ON trp2.train_uid = tp.prev_uid AND trp2.service_date = tp.prev_service_date \
         LEFT JOIN train_current_state csp2 ON csp2.trains_id = trp2.id",
    )
    .bind(&uids)
    .bind(&dates)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(FactsRow::into_facts).collect())
}

/// [`assemble`] over [`facts_for_trains`]: every train in `trains` with a
/// prediction now. A train with none is absent.
pub async fn predict_for_trains(
    pool: &PgPool,
    trains: &[(String, NaiveDate)],
    now: DateTime<Utc>,
) -> anyhow::Result<HashMap<(String, NaiveDate), PredictedStart>> {
    Ok(facts_for_trains(pool, trains)
        .await?
        .into_iter()
        .filter_map(|(key, facts)| assemble(&facts, now).map(|prediction| (key, prediction)))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(hhmm: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(&format!("2099-07-10T{hhmm}:00Z"))
            .expect("valid instant")
            .with_timezone(&Utc)
    }

    fn evidence(delay: i32, turnaround: i32) -> Evidence<'static> {
        Evidence {
            incoming_delay_minutes: delay,
            booked_turnaround_minutes: turnaround,
            operator: Some("NT"),
            fresh: true,
            depth: 1,
        }
    }

    fn live(status: &str, delay: Option<i32>, observed: &str) -> LiveState {
        LiveState {
            status: Some(status.to_string()),
            delay_minutes: delay,
            observed_at: Some(at(observed)),
        }
    }

    fn facts(prev_state: LiveState) -> TrainFacts {
        TrainFacts {
            departs_at: at("10:12"),
            operator: Some("NT".to_string()),
            own_state: LiveState::default(),
            previous: LinkFacts {
                prev_uid: "P00001".to_string(),
                prev_service_date: NaiveDate::from_ymd_opt(2099, 7, 10).expect("date"),
                booked_turnaround_minutes: 12,
                link: "inferred".to_string(),
                prev_state,
            },
            previous_of_previous: None,
        }
    }

    #[test]
    fn a_delay_the_turnaround_absorbs_predicts_nothing() {
        // 5 late on a 12-minute turnaround: excess -7, late max(0, -3).
        assert_eq!(rule(&evidence(5, 12)), None);
        // Excess exactly -TURN_MIN: still 0.
        assert_eq!(rule(&evidence(8, 12)), None);
    }

    #[test]
    fn a_small_negative_excess_is_medium() {
        // Excess -2: late 2, but P has not eaten all the slack.
        assert_eq!(rule(&evidence(10, 12)), Some((2, Confidence::Medium)));
    }

    #[test]
    fn eating_the_whole_slack_is_high_up_to_fifteen_minutes() {
        assert_eq!(rule(&evidence(12, 12)), Some((4, Confidence::High)));
        assert_eq!(rule(&evidence(27, 12)), Some((19, Confidence::High)));
    }

    #[test]
    fn beyond_fifteen_minutes_of_excess_is_low_and_beyond_thirty_nothing() {
        assert_eq!(rule(&evidence(28, 12)), Some((20, Confidence::Low)));
        assert_eq!(rule(&evidence(42, 12)), Some((34, Confidence::Low)));
        assert_eq!(rule(&evidence(43, 12)), None);
    }

    #[test]
    fn stale_data_and_a_second_link_are_low() {
        let stale = Evidence {
            fresh: false,
            ..evidence(20, 12)
        };
        assert_eq!(rule(&stale), Some((12, Confidence::Low)));
        let deep = Evidence {
            depth: 2,
            ..evidence(20, 12)
        };
        assert_eq!(rule(&deep), Some((12, Confidence::Low)));
    }

    #[test]
    fn a_capped_operator_stops_at_medium() {
        for op in MEDIUM_CAPPED_OPERATORS {
            let capped = Evidence {
                operator: Some(op),
                ..evidence(20, 12)
            };
            assert_eq!(rule(&capped), Some((12, Confidence::Medium)));
        }
    }

    #[test]
    fn a_running_incoming_train_gives_a_prediction_within_the_horizon() {
        let predicted = assemble(&facts(live("en_route", Some(20), "09:55")), at("10:00"))
            .expect("a prediction");
        assert_eq!(predicted.late_minutes, Some(12));
        assert_eq!(predicted.confidence, Confidence::High);
        assert_eq!(predicted.basis, "previous-working");
        assert_eq!(predicted.previous_working.delay_minutes, Some(20));
        assert_eq!(predicted.estimate_minutes(), Some(12));
        let json = serde_json::to_value(&predicted).expect("serialize");
        assert_eq!(json["lateMinutes"], 12);
        assert_eq!(json["confidence"], "high");
        assert_eq!(json["previousWorking"]["trainUid"], "P00001");
        assert_eq!(json["previousWorking"]["bookedTurnaroundMinutes"], 12);
        assert!(json.get("minutes").is_none());
    }

    #[test]
    fn nothing_before_the_horizon() {
        assert_eq!(
            assemble(&facts(live("en_route", Some(20), "07:00")), at("07:11")),
            None
        );
        assert!(assemble(&facts(live("en_route", Some(20), "07:12")), at("07:12")).is_some());
    }

    #[test]
    fn stale_incoming_data_is_low_with_no_number() {
        let predicted = assemble(&facts(live("en_route", Some(20), "09:40")), at("10:00"))
            .expect("a prediction");
        assert_eq!(predicted.confidence, Confidence::Low);
        assert_eq!(predicted.late_minutes, None);
        assert_eq!(predicted.estimate_minutes(), None);
        assert_eq!(predicted.minutes, 12);
    }

    #[test]
    fn an_arrived_incoming_train_is_never_stale() {
        let predicted = assemble(&facts(live("completed", Some(20), "08:00")), at("10:00"))
            .expect("a prediction");
        assert_eq!(predicted.confidence, Confidence::High);
    }

    #[test]
    fn a_cancelled_incoming_train_predicts_nothing() {
        assert_eq!(
            assemble(&facts(live("cancelled", None, "09:55")), at("10:00")),
            None
        );
    }

    #[test]
    fn our_own_trust_report_supersedes_the_prediction() {
        let mut started = facts(live("en_route", Some(20), "09:55"));
        started.own_state = live("en_route", Some(3), "10:01");
        assert_eq!(assemble(&started, at("10:02")), None);
        let mut cancelled = facts(live("en_route", Some(20), "09:55"));
        cancelled.own_state = live("cancelled", None, "09:00");
        assert_eq!(assemble(&cancelled, at("10:00")), None);
    }

    #[test]
    fn a_not_started_incoming_train_uses_its_own_prediction_at_low_confidence() {
        let mut two_deep = facts(live("awaiting_activation", None, "09:00"));
        // P has a 10-minute turnaround from P2, which runs 30 late:
        // P starts 24 late; T's excess is 24 - 12 = 12, late 16, low.
        two_deep.previous_of_previous = Some(LinkFacts {
            prev_uid: "P00002".to_string(),
            prev_service_date: NaiveDate::from_ymd_opt(2099, 7, 10).expect("date"),
            booked_turnaround_minutes: 10,
            link: "inferred".to_string(),
            prev_state: live("en_route", Some(30), "09:55"),
        });
        let predicted = assemble(&two_deep, at("10:00")).expect("a prediction");
        assert_eq!(predicted.minutes, 16);
        assert_eq!(predicted.confidence, Confidence::Low);
        assert_eq!(predicted.late_minutes, None);

        // With no live data two links back, nothing.
        let mut unknown = two_deep;
        if let Some(upstream) = unknown.previous_of_previous.as_mut() {
            upstream.prev_state = LiveState::default();
        }
        assert_eq!(assemble(&unknown, at("10:00")), None);
    }

    #[test]
    fn an_incoming_train_with_no_state_and_no_upstream_predicts_nothing() {
        assert_eq!(assemble(&facts(LiveState::default()), at("10:00")), None);
    }
}

#[cfg(test)]
mod db_tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL=... cargo test -p ds-store \
                predicted_start -- --ignored --test-threads=1"]
    async fn predict_for_trains_reads_the_link_and_both_states() {
        let reader = crate::test_support::connect().await;
        // Fixtures through the owner when set: the api role only reads
        // these tables.
        let pool = crate::test_support::schedule_fixture_pool(&reader).await;
        let date = NaiveDate::from_ymd_opt(2099, 7, 11).expect("date");
        for table in ["schedule_turnarounds", "schedule_services"] {
            sqlx::query(&format!("DELETE FROM {table} WHERE service_date = $1"))
                .bind(date)
                .execute(&pool)
                .await
                .expect("cleanup");
        }
        sqlx::query("DELETE FROM trains WHERE service_date = $1")
            .bind(date)
            .execute(&pool)
            .await
            .expect("cleanup");

        let departs_at = Utc::now() + Duration::minutes(30);
        sqlx::query(
            "INSERT INTO schedule_turnarounds (service_date, uid, prev_service_date, prev_uid, \
             booked_turnaround_minutes, link, origin_tiploc, platform, departs_at) \
             VALUES ($1, 'PST0001', $1, 'PSP0001', 9, 'inferred', 'YORK', '3', $2)",
        )
        .bind(date)
        .bind(departs_at)
        .execute(&pool)
        .await
        .expect("insert turnaround");
        sqlx::query(
            "INSERT INTO schedule_services (service_date, uid, mode, operator_atoc, stp) \
             VALUES ($1, 'PST0001', 'train', 'NT', 'P')",
        )
        .bind(date)
        .execute(&pool)
        .await
        .expect("insert service");
        let prev_id: i64 = sqlx::query_scalar(
            "INSERT INTO trains (train_uid, service_date) VALUES ('PSP0001', $1) RETURNING id",
        )
        .bind(date)
        .fetch_one(&pool)
        .await
        .expect("insert trains");
        sqlx::query(
            "INSERT INTO train_current_state (trains_id, status, delay_minutes, event_time) \
             VALUES ($1, 'en_route', 15, now())",
        )
        .bind(prev_id)
        .execute(&pool)
        .await
        .expect("insert state");

        let key = ("PST0001".to_string(), date);
        let predictions = predict_for_trains(
            &reader,
            &[key.clone(), ("NOLINK1".to_string(), date)],
            Utc::now(),
        )
        .await
        .expect("predict");
        assert_eq!(predictions.len(), 1);
        let predicted = &predictions[&key];
        // 15 late on a 9-minute turnaround: excess 6, late 10, high.
        assert_eq!(predicted.late_minutes, Some(10));
        assert_eq!(predicted.confidence, Confidence::High);
        assert_eq!(
            predicted.previous_working.status.as_deref(),
            Some("en_route")
        );

        sqlx::query("DELETE FROM trains WHERE service_date = $1")
            .bind(date)
            .execute(&pool)
            .await
            .expect("cleanup");
        for table in ["schedule_turnarounds", "schedule_services"] {
            sqlx::query(&format!("DELETE FROM {table} WHERE service_date = $1"))
                .bind(date)
                .execute(&pool)
                .await
                .expect("cleanup");
        }
    }
}
