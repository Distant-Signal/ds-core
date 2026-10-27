//! A predicted late start from the train's previous working
//! (docs/superpowers/specs/2026-10-21-pre-departure-prediction-design.md,
//! sections 5 and 3a, and the 2026-10-22 decisions).
//!
//! Before TRUST reports a train T, DS knows nothing live about it. But the
//! unit that forms T usually arrives on an earlier train P, its previous
//! working. If P is late enough that the booked turnaround cannot absorb
//! it, T starts late:
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
//! **The link** (spec section 4). Darwin's live next-working association
//! (`darwin_previous_workings`, DS-152; `link = "association"`) is preferred:
//! it follows the operator's own on-the-day plan. Without one, the
//! timetable inference from the platform sequence (`schedule_turnarounds`,
//! `link = "inferred"`) is used.
//!
//! **Confidence.**
//!
//! - `high`: one link, P's data is live and fresh (or P has arrived), the
//!   excess is between 0 and [`MEDIUM_EXCESS_LIMIT`] (P has eaten the whole
//!   slack), and, for an inferred link only, the operator is not in
//!   [`MEDIUM_CAPPED_OPERATORS`] and neither train joins or divides that
//!   day (a CIF `JJ`/`VV` association in `schedule_associations`, spec
//!   section 3a: where the platform sequence is least safe). Over a week of
//!   TRUST actuals, 92% of such trains started at least 3 minutes late,
//!   with a mean error of 2.1 minutes. Only `high` sends a push.
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
/// pairs). Their inferred predictions stop at medium, so they never push;
/// a Darwin association link is exact and is not capped.
pub const MEDIUM_CAPPED_OPERATORS: &[&str] = &["TL", "CH"];
/// `predictedStart.basis`, and the stop estimates' `estimateSource`.
pub const BASIS: &str = "previous-working";
/// `previousWorking.link` of a Darwin live next-working association.
pub const LINK_ASSOCIATION: &str = "association";
/// `previousWorking.link` of the platform-sequence inference.
pub const LINK_INFERRED: &str = "inferred";

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
    /// [`LINK_INFERRED`] (the platform sequence) or [`LINK_ASSOCIATION`]
    /// (Darwin's live next-working link).
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
    /// T's booked working origin departure (never served): the notifier
    /// pushes only before it.
    #[serde(skip)]
    pub departs_at: DateTime<Utc>,
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
    /// The link is Darwin's live association, not the inference: neither
    /// cap below applies.
    pub association: bool,
    /// T or P joins or divides that day (CIF `JJ`/`VV`): an inferred link
    /// is capped at medium.
    pub join_divide: bool,
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
    let capped = !evidence.association
        && (evidence.join_divide
            || evidence
                .operator
                .is_some_and(|op| MEDIUM_CAPPED_OPERATORS.contains(&op)));
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
    /// [`LINK_INFERRED`] or [`LINK_ASSOCIATION`].
    pub link: String,
    /// Either train of the link joins or divides that day (CIF `JJ`/`VV`).
    pub join_divide: bool,
    pub prev_state: LiveState,
}

impl LinkFacts {
    fn is_association(&self) -> bool {
        self.link == LINK_ASSOCIATION
    }
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
            association: upstream.is_association(),
            join_divide: upstream.join_divide,
        })?;
        (late, upstream.prev_state.is_fresh(now), 2)
    };
    let (minutes, confidence) = rule(&Evidence {
        incoming_delay_minutes,
        booked_turnaround_minutes: previous.booked_turnaround_minutes,
        operator,
        fresh,
        depth,
        association: previous.is_association(),
        join_divide: previous.join_divide,
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
        departs_at: facts.departs_at,
    })
}

#[derive(sqlx::FromRow)]
struct FactsRow {
    uid: String,
    service_date: NaiveDate,
    departs_at: Option<DateTime<Utc>>,
    operator_atoc: Option<String>,
    t_status: Option<String>,
    t_delay: Option<i32>,
    prev_uid: String,
    prev_service_date: NaiveDate,
    turnaround: i16,
    link: String,
    join_divide: bool,
    p_status: Option<String>,
    p_delay: Option<i32>,
    p_observed: Option<DateTime<Utc>>,
    p2_uid: Option<String>,
    p2_service_date: Option<NaiveDate>,
    p2_turnaround: Option<i16>,
    p2_link: Option<String>,
    p2_join_divide: Option<bool>,
    p2_status: Option<String>,
    p2_delay: Option<i32>,
    p2_observed: Option<DateTime<Utc>>,
}

impl FactsRow {
    /// `None` when T's departure is unknown (a Darwin link for a train
    /// with no CIF origin call).
    fn into_facts(self) -> Option<((String, NaiveDate), TrainFacts)> {
        let previous_of_previous = match (self.p2_uid, self.p2_service_date, self.p2_turnaround) {
            (Some(prev_uid), Some(prev_service_date), Some(turnaround)) => Some(LinkFacts {
                prev_uid,
                prev_service_date,
                booked_turnaround_minutes: i32::from(turnaround),
                link: self.p2_link.unwrap_or_else(|| LINK_INFERRED.to_string()),
                join_divide: self.p2_join_divide.unwrap_or(false),
                prev_state: LiveState {
                    status: self.p2_status,
                    delay_minutes: self.p2_delay,
                    observed_at: self.p2_observed,
                },
            }),
            _ => None,
        };
        Some((
            (self.uid, self.service_date),
            TrainFacts {
                departs_at: self.departs_at?,
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
                    join_divide: self.join_divide,
                    prev_state: LiveState {
                        status: self.p_status,
                        delay_minutes: self.p_delay,
                        observed_at: self.p_observed,
                    },
                },
                previous_of_previous,
            },
        ))
    }
}

/// The previous-working link of every `(uid, service_date)` in the CTE
/// `keys`: Darwin's live association when there is one, else the
/// inference; with T's booked working origin departure (the inference's,
/// else T's CIF origin call's) and whether T or P joins or divides that
/// day.
fn link_sql(keys: &str) -> String {
    let join_divide = |uid: &str, date: &str| {
        format!(
            "EXISTS (SELECT 1 FROM schedule_associations sa \
                     WHERE sa.category IN ('JJ', 'VV') \
                       AND ((sa.service_date = {date} AND sa.main_uid = {uid}) \
                         OR (sa.assoc_service_date = {date} AND sa.assoc_uid = {uid})))"
        )
    };
    let t_join = join_divide("k.uid", "k.service_date");
    let p_join = join_divide(
        "COALESCE(d.prev_uid, t.prev_uid)",
        "COALESCE(d.prev_service_date, t.prev_service_date)",
    );
    format!(
        "SELECT k.uid, k.service_date, \
                COALESCE(d.prev_uid, t.prev_uid) AS prev_uid, \
                COALESCE(d.prev_service_date, t.prev_service_date) AS prev_service_date, \
                COALESCE(d.booked_turnaround_minutes, t.booked_turnaround_minutes) AS turnaround, \
                CASE WHEN d.uid IS NOT NULL THEN '{LINK_ASSOCIATION}' ELSE t.link END AS link, \
                COALESCE(t.departs_at, o.departs_at) AS departs_at, \
                ({t_join} OR {p_join}) AS join_divide \
         FROM {keys} k \
         LEFT JOIN schedule_turnarounds t ON t.service_date = k.service_date AND t.uid = k.uid \
         LEFT JOIN LATERAL ( \
             SELECT dp.uid, dp.prev_uid, dp.prev_service_date, dp.booked_turnaround_minutes \
             FROM darwin_previous_workings dp \
             WHERE dp.service_date = k.service_date AND dp.uid = k.uid \
             LIMIT 1 \
         ) d ON true \
         LEFT JOIN LATERAL ( \
             SELECT ((c.service_date + c.day_offset \
                      + COALESCE(c.working_departure, c.booked_departure)) \
                     AT TIME ZONE 'Europe/London') AS departs_at \
             FROM schedule_calling_points_full c \
             WHERE c.service_date = k.service_date AND c.uid = k.uid AND c.kind = 'origin' \
             ORDER BY c.seq LIMIT 1 \
         ) o ON t.uid IS NULL \
         WHERE t.uid IS NOT NULL OR d.uid IS NOT NULL"
    )
}

/// The facts for every `(train_uid, service_date)` in `trains` that has a
/// previous working, in one query: T's link, operator and own state, P's
/// state, and P's own previous working with its state.
pub async fn facts_for_trains(
    pool: &PgPool,
    trains: &[(String, NaiveDate)],
) -> anyhow::Result<HashMap<(String, NaiveDate), TrainFacts>> {
    if trains.is_empty() {
        return Ok(HashMap::new());
    }
    let uids: Vec<&str> = trains.iter().map(|(uid, _)| uid.as_str()).collect();
    let dates: Vec<NaiveDate> = trains.iter().map(|(_, date)| *date).collect();
    let sql = format!(
        "WITH k AS (SELECT DISTINCT * FROM UNNEST($1::text[], $2::date[]) AS k(uid, service_date)), \
              l1 AS ({l1}), \
              k2 AS (SELECT DISTINCT prev_uid AS uid, prev_service_date AS service_date FROM l1), \
              l2 AS ({l2}) \
         SELECT l1.uid, l1.service_date, l1.departs_at, ss.operator_atoc, \
                cst.status AS t_status, cst.delay_minutes AS t_delay, \
                l1.prev_uid, l1.prev_service_date, l1.turnaround, l1.link, l1.join_divide, \
                csp.status AS p_status, csp.delay_minutes AS p_delay, \
                COALESCE(csp.event_time, csp.updated_at) AS p_observed, \
                l2.prev_uid AS p2_uid, l2.prev_service_date AS p2_service_date, \
                l2.turnaround AS p2_turnaround, l2.link AS p2_link, \
                l2.join_divide AS p2_join_divide, \
                csp2.status AS p2_status, csp2.delay_minutes AS p2_delay, \
                COALESCE(csp2.event_time, csp2.updated_at) AS p2_observed \
         FROM l1 \
         LEFT JOIN schedule_services ss ON ss.service_date = l1.service_date AND ss.uid = l1.uid \
         LEFT JOIN trains trt ON trt.train_uid = l1.uid AND trt.service_date = l1.service_date \
         LEFT JOIN train_current_state cst ON cst.trains_id = trt.id \
         LEFT JOIN trains trp \
           ON trp.train_uid = l1.prev_uid AND trp.service_date = l1.prev_service_date \
         LEFT JOIN train_current_state csp ON csp.trains_id = trp.id \
         LEFT JOIN l2 ON l2.uid = l1.prev_uid AND l2.service_date = l1.prev_service_date \
         LEFT JOIN trains trp2 \
           ON trp2.train_uid = l2.prev_uid AND trp2.service_date = l2.prev_service_date \
         LEFT JOIN train_current_state csp2 ON csp2.trains_id = trp2.id",
        l1 = link_sql("k"),
        l2 = link_sql("k2"),
    );
    let rows: Vec<FactsRow> = sqlx::query_as(&sql)
        .bind(&uids)
        .bind(&dates)
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().filter_map(FactsRow::into_facts).collect())
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
            association: false,
            join_divide: false,
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
                join_divide: false,
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
    fn a_join_or_divide_caps_an_inferred_link_at_medium() {
        let joined = Evidence {
            join_divide: true,
            ..evidence(20, 12)
        };
        assert_eq!(rule(&joined), Some((12, Confidence::Medium)));
        // Low stays low.
        let stale = Evidence {
            fresh: false,
            ..joined
        };
        assert_eq!(rule(&stale), Some((12, Confidence::Low)));
    }

    #[test]
    fn an_association_link_is_not_capped() {
        for op in MEDIUM_CAPPED_OPERATORS {
            let linked = Evidence {
                operator: Some(op),
                association: true,
                join_divide: true,
                ..evidence(20, 12)
            };
            assert_eq!(rule(&linked), Some((12, Confidence::High)));
        }
    }

    #[test]
    fn assemble_applies_the_link_kind_and_the_join_cap() {
        let mut joined = facts(live("en_route", Some(20), "09:55"));
        joined.previous.join_divide = true;
        let predicted = assemble(&joined, at("10:00")).expect("a prediction");
        assert_eq!(predicted.confidence, Confidence::Medium);
        assert_eq!(predicted.departs_at, at("10:12"));

        let mut linked = joined;
        linked.previous.link = LINK_ASSOCIATION.to_string();
        let predicted = assemble(&linked, at("10:00")).expect("a prediction");
        assert_eq!(predicted.confidence, Confidence::High);
        let json = serde_json::to_value(&predicted).expect("serialize");
        assert_eq!(json["previousWorking"]["link"], "association");
        assert!(json.get("departsAt").is_none());
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
            join_divide: false,
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

    async fn exec(pool: &PgPool, sql: &str, date: NaiveDate) {
        sqlx::query(sql)
            .bind(date)
            .execute(pool)
            .await
            .unwrap_or_else(|err| panic!("{sql}: {err}"));
    }

    async fn clean_link_fixtures(pool: &PgPool, date: NaiveDate) {
        for sql in [
            "DELETE FROM trains WHERE service_date = $1",
            "DELETE FROM schedule_turnarounds WHERE service_date = $1",
            "DELETE FROM schedule_services WHERE service_date = $1",
            "DELETE FROM schedule_calling_points_full WHERE service_date = $1",
            "DELETE FROM schedule_associations WHERE service_date = $1",
            "DELETE FROM darwin_associations WHERE main_ssd = $1",
            "DELETE FROM darwin_services WHERE ssd = $1",
        ] {
            exec(pool, sql, date).await;
        }
    }

    /// A TRUST state for `uid`: `delay` minutes late, reported now.
    async fn running(pool: &PgPool, uid: &str, date: NaiveDate, delay: i32) {
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO trains (train_uid, service_date) VALUES ($1, $2) RETURNING id",
        )
        .bind(uid)
        .bind(date)
        .fetch_one(pool)
        .await
        .expect("insert trains");
        sqlx::query(
            "INSERT INTO train_current_state (trains_id, status, delay_minutes, event_time) \
             VALUES ($1, 'en_route', $2, now())",
        )
        .bind(id)
        .bind(delay)
        .execute(pool)
        .await
        .expect("insert state");
    }

    /// DS-152: Darwin's live next-working link wins over the inference, an
    /// association-only train takes its departure from its CIF origin call,
    /// and a capped operator is not capped on an association link.
    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL=... cargo test -p ds-store \
                predicted_start -- --ignored --test-threads=1"]
    async fn a_darwin_association_link_is_preferred_over_the_inference() {
        let reader = crate::test_support::connect().await;
        let pool = crate::test_support::schedule_fixture_pool(&reader).await;
        let date = NaiveDate::from_ymd_opt(2099, 7, 14).expect("date");
        crate::test_support::assert_synthetic_date(date);
        clean_link_fixtures(&pool, date).await;

        let departs_at = Utc::now() + Duration::minutes(30);
        // T1: inferred from PI00001 (on time), but Darwin links PD00001.
        sqlx::query(
            "INSERT INTO schedule_turnarounds (service_date, uid, prev_service_date, prev_uid, \
             booked_turnaround_minutes, link, origin_tiploc, platform, departs_at) \
             VALUES ($1, 'TD00001', $1, 'PI00001', 9, 'inferred', 'YORK', '3', $2)",
        )
        .bind(date)
        .bind(departs_at)
        .execute(&pool)
        .await
        .expect("insert turnaround");
        // TL: a capped operator for the inference, not for Darwin.
        exec(
            &pool,
            "INSERT INTO schedule_services (service_date, uid, mode, operator_atoc, stp) \
             VALUES ($1, 'TD00001', 'train', 'TL', 'P'), ($1, 'TD00002', 'train', 'NT', 'P')",
            date,
        )
        .await;
        // T2: no inference at all; its CIF origin departs 30 minutes from
        // now (London time).
        let local = departs_at.with_timezone(&chrono_tz::Europe::London);
        let origin_date = local.date_naive();
        let day_offset = i16::try_from((origin_date - date).num_days()).expect("small");
        sqlx::query(
            "INSERT INTO schedule_calling_points_full (service_date, uid, seq, tiploc, kind, \
             booked_departure, working_departure, day_offset) \
             VALUES ($1, 'TD00002', 1, 'LEEDS', 'origin', $2, $2, $3)",
        )
        .bind(date)
        .bind(local.time())
        .bind(day_offset)
        .execute(&pool)
        .await
        .expect("insert origin call");
        // Darwin: PD00001 forms TD00001 (8-minute turnaround) and PD00002
        // forms TD00002 (10 minutes).
        exec(
            &pool,
            "INSERT INTO darwin_services (rid, uid, ssd, message_ts) VALUES \
             ('209907148800001', 'TD00001', $1, now()), ('209907148800002', 'PD00001', $1, now()), \
             ('209907148800003', 'TD00002', $1, now()), ('209907148800004', 'PD00002', $1, now())",
            date,
        )
        .await;
        exec(
            &pool,
            "INSERT INTO darwin_associations (main_rid, assoc_rid, tiploc, category, main_wta, \
             assoc_wtd, main_ssd, message_ts) VALUES \
             ('209907148800002', '209907148800001', 'YORK', 'NP', '10:00', '10:08', $1, now()), \
             ('209907148800004', '209907148800003', 'LEEDS', 'NP', '10:00', '10:10', $1, now())",
            date,
        )
        .await;
        running(&pool, "PI00001", date, 0).await;
        running(&pool, "PD00001", date, 16).await;
        running(&pool, "PD00002", date, 20).await;

        let t1 = ("TD00001".to_string(), date);
        let t2 = ("TD00002".to_string(), date);
        let predictions = predict_for_trains(&reader, &[t1.clone(), t2.clone()], Utc::now())
            .await
            .expect("predict");
        let p1 = &predictions[&t1];
        assert_eq!(p1.previous_working.train_uid, "PD00001");
        assert_eq!(p1.previous_working.link, LINK_ASSOCIATION);
        assert_eq!(p1.previous_working.booked_turnaround_minutes, 8);
        // 16 late on 8: excess 8, late 12, high even for TL.
        assert_eq!(p1.late_minutes, Some(12));
        assert_eq!(p1.confidence, Confidence::High);
        assert!((p1.departs_at - departs_at).num_milliseconds().abs() < 1000);
        let p2 = &predictions[&t2];
        assert_eq!(p2.previous_working.train_uid, "PD00002");
        // 20 late on 10: excess 10, late 14.
        assert_eq!(p2.late_minutes, Some(14));
        assert_eq!(p2.confidence, Confidence::High);
        assert!((p2.departs_at - departs_at).num_seconds().abs() < 60);

        clean_link_fixtures(&pool, date).await;
    }

    /// DS-152, spec section 3a: an inferred link where either train joins or
    /// divides that day is capped at medium.
    #[tokio::test]
    #[ignore = "requires a live database; run with DATABASE_URL=... cargo test -p ds-store \
                predicted_start -- --ignored --test-threads=1"]
    async fn a_join_or_divide_caps_an_inferred_link_at_medium() {
        let reader = crate::test_support::connect().await;
        let pool = crate::test_support::schedule_fixture_pool(&reader).await;
        let date = NaiveDate::from_ymd_opt(2099, 7, 15).expect("date");
        crate::test_support::assert_synthetic_date(date);
        clean_link_fixtures(&pool, date).await;

        let departs_at = Utc::now() + Duration::minutes(30);
        for (uid, prev) in [("TJ00001", "PJ00001"), ("TJ00002", "PJ00002")] {
            sqlx::query(
                "INSERT INTO schedule_turnarounds (service_date, uid, prev_service_date, \
                 prev_uid, booked_turnaround_minutes, link, origin_tiploc, platform, departs_at) \
                 VALUES ($1, $2, $1, $3, 9, 'inferred', 'YORK', '3', $4)",
            )
            .bind(date)
            .bind(uid)
            .bind(prev)
            .bind(departs_at)
            .execute(&pool)
            .await
            .expect("insert turnaround");
            running(&pool, prev, date, 15).await;
        }
        // PJ00002, the second train's previous working, divides that day
        // (as the associated train).
        exec(
            &pool,
            "INSERT INTO schedule_associations (service_date, main_uid, assoc_uid, \
             assoc_service_date, category, tiploc) \
             VALUES ($1, 'MJ00009', 'PJ00002', $1, 'VV', 'DONC')",
            date,
        )
        .await;

        let t1 = ("TJ00001".to_string(), date);
        let t2 = ("TJ00002".to_string(), date);
        let predictions = predict_for_trains(&reader, &[t1.clone(), t2.clone()], Utc::now())
            .await
            .expect("predict");
        assert_eq!(predictions[&t1].confidence, Confidence::High);
        assert_eq!(predictions[&t2].confidence, Confidence::Medium);
        assert_eq!(predictions[&t2].late_minutes, Some(10));

        clean_link_fixtures(&pool, date).await;
    }
}
