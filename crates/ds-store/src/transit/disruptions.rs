//! The read side of a network's situations and line statuses (transit spec
//! §10.2, §11; plan 4.5): what the api adds to its network, line and stop
//! responses and serves at `/public/transit/{network}/situations`.
//!
//! **Freshness decides what is shown.** The situations freshness marker
//! (`transit-<network>-situations`) gives a [`StatusState`]:
//!
//! - no marker (the SIRI-SX loop never ran, `situationsEnabled` off):
//!   [`StatusState::None`], and nothing about status is shown;
//! - older than [`STATUS_STALE_AFTER`]: [`StatusState::Unavailable`], with
//!   the as-of time and no statuses or situations, since stale disruption
//!   news reads as current;
//! - otherwise [`StatusState::Current`].
//!
//! Current and upcoming are decided at read time from each situation's
//! progress, publication and validity (`common::transit::status`), so a
//! situation that ended since the last snapshot drops off at once.

use std::collections::{BTreeSet, HashMap};

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use common::transit::status::{NetworkLine, concerns_line, is_current, upcoming_period};
use common::transit::{
    Period, SituationScope, TransitConsequence, TransitSituation, situations_freshness_source,
};
use common::{LineStatus, UpcomingDisruption};
use serde::Serialize;
use sqlx::PgPool;

/// How old the situations may be before the status is "unavailable".
pub const STATUS_STALE_AFTER: Duration = Duration::minutes(30);

/// What the situations' freshness allows to be shown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StatusState {
    Current,
    Unavailable,
    None,
}

/// `as_of` (the situations marker) at `now`.
pub fn state_at(as_of: Option<DateTime<Utc>>, now: DateTime<Utc>) -> StatusState {
    match as_of {
        None => StatusState::None,
        Some(at) if now - at > STATUS_STALE_AFTER => StatusState::Unavailable,
        Some(_) => StatusState::Current,
    }
}

/// One status of a line.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatusEntry {
    /// A `common::Severity` number.
    pub severity: u8,
    /// The severity's name (`Minor Delays`).
    pub description: String,
    pub reason: String,
    /// The situation behind it; `None` for "No reported disruption".
    pub situation_id: Option<String>,
}

/// An announced situation that has not started.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpcomingEntry {
    pub from: DateTime<Utc>,
    pub to: Option<DateTime<Utc>>,
    pub summary: String,
    pub situation_id: String,
}

/// A line's status as the api shows it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LineStatusView {
    pub state: StatusState,
    /// The situations marker.
    pub as_of: Option<DateTime<Utc>>,
    /// Worst first; the first is the headline. Empty unless `current`.
    pub statuses: Vec<StatusEntry>,
    /// Empty unless `current`.
    pub upcoming: Vec<UpcomingEntry>,
}

/// A situation as the api shows it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SituationView {
    pub id: String,
    pub summary: String,
    /// Plain text, as published; a client must render it as text.
    pub description: Option<String>,
    pub info_url: Option<String>,
    pub planned: bool,
    pub reason: Option<String>,
    /// `line`, `stop` or `network`.
    pub scope: SituationScope,
    /// Public line ids (`metrolink:blue`).
    pub lines: Vec<String>,
    /// Station ids.
    pub stops: Vec<String>,
    /// Advice from its consequences, deduplicated.
    pub advice: Vec<String>,
    pub validity: Vec<Period>,
    /// The upcoming period's start, for an upcoming situation.
    pub starts: Option<DateTime<Utc>>,
}

/// Current and upcoming situations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SituationsView {
    pub state: StatusState,
    pub as_of: Option<DateTime<Utc>>,
    /// Ordered by validity start, then id.
    pub current: Vec<SituationView>,
    /// Ordered by start, then id.
    pub upcoming: Vec<SituationView>,
}

/// The network's situations marker.
pub async fn situations_as_of(pool: &PgPool, network: &str) -> Result<Option<DateTime<Utc>>> {
    let Ok(id) = common::transit::NetworkId::parse(network) else {
        return Ok(None);
    };
    sqlx::query_scalar("SELECT fetched_at FROM ingest_freshness WHERE source = $1")
        .bind(situations_freshness_source(&id))
        .fetch_optional(pool)
        .await
        .context("situations freshness")
}

/// Every line's stored statuses and upcoming situations, by local id.
pub async fn stored_line_statuses(
    pool: &PgPool,
    network: &str,
) -> Result<HashMap<String, (Vec<LineStatus>, Vec<UpcomingDisruption>)>> {
    let rows: Vec<(String, serde_json::Value, serde_json::Value)> = sqlx::query_as(
        "SELECT line_id, statuses, upcoming FROM line_status \
          WHERE source = 'transit' AND starts_with(line_id, $1 || ':')",
    )
    .bind(network)
    .fetch_all(pool)
    .await
    .context("transit line statuses")?;
    let prefix = format!("{network}:");
    rows.into_iter()
        .map(|(id, statuses, upcoming)| {
            let local = id.strip_prefix(&prefix).unwrap_or(&id).to_owned();
            Ok((
                local,
                (
                    serde_json::from_value(statuses).context("line_status.statuses")?,
                    serde_json::from_value(upcoming).unwrap_or_default(),
                ),
            ))
        })
        .collect()
}

/// A line's view from its stored row (`None`: not written yet, shown as
/// unavailable while the situations are current).
pub fn line_view(
    state: StatusState,
    as_of: Option<DateTime<Utc>>,
    stored: Option<&(Vec<LineStatus>, Vec<UpcomingDisruption>)>,
) -> LineStatusView {
    let (state, statuses, upcoming) = match (state, stored) {
        (StatusState::Current, Some((statuses, upcoming))) => (
            StatusState::Current,
            statuses
                .iter()
                .map(|s| StatusEntry {
                    severity: s.severity as u8,
                    description: s.severity.description().to_owned(),
                    reason: s.reason.clone(),
                    situation_id: s
                        .disruption
                        .as_ref()
                        .and_then(|d| d.source.as_deref())
                        .and_then(|src| src.strip_prefix("transit-situation-"))
                        .map(str::to_owned),
                })
                .collect(),
            upcoming
                .iter()
                .map(|u| UpcomingEntry {
                    from: u.from,
                    to: u.to,
                    summary: u.summary.clone(),
                    situation_id: u.incident_id.clone(),
                })
                .collect(),
        ),
        (StatusState::Current, None) => (StatusState::Unavailable, Vec::new(), Vec::new()),
        (state, _) => (state, Vec::new(), Vec::new()),
    };
    LineStatusView {
        state,
        as_of,
        statuses,
        upcoming,
    }
}

type SituationRow = (
    String,
    Option<i32>,
    String,
    bool,
    Option<String>,
    String,
    Option<String>,
    Option<String>,
    sqlx::types::Json<Vec<Period>>,
    sqlx::types::Json<Vec<Period>>,
    sqlx::types::Json<Vec<TransitConsequence>>,
    Vec<String>,
    Vec<String>,
    String,
);

/// Every stored situation of the network, by id.
pub async fn stored_situations(pool: &PgPool, network: &str) -> Result<Vec<TransitSituation>> {
    let rows: Vec<SituationRow> = sqlx::query_as(
        "SELECT situation_id, version, progress, planned, reason, summary, description, \
                info_url, validity, publication, consequences, line_ids, stop_ids, scope \
           FROM transit_situations WHERE network = $1 ORDER BY situation_id",
    )
    .bind(network)
    .fetch_all(pool)
    .await
    .context("transit situations")?;
    Ok(rows
        .into_iter()
        .map(|r| TransitSituation {
            situation_id: r.0,
            version: r.1,
            progress: r.2,
            planned: r.3,
            reason: r.4,
            summary: r.5,
            description: r.6,
            info_url: r.7,
            validity: r.8.0,
            publication: r.9.0,
            consequences: r.10.0,
            line_ids: r.11,
            stop_ids: r.12,
            scope: match r.13.as_str() {
                "line" => SituationScope::Line,
                "stop" => SituationScope::Stop,
                _ => SituationScope::Network,
            },
        })
        .collect())
}

fn view(
    network: &str,
    situation: &TransitSituation,
    starts: Option<DateTime<Utc>>,
) -> SituationView {
    let advice: BTreeSet<String> = situation
        .consequences
        .iter()
        .filter_map(|c| c.advice.clone())
        .collect();
    SituationView {
        id: situation.situation_id.clone(),
        summary: situation.summary.clone(),
        description: situation.description.clone(),
        info_url: situation.info_url.clone(),
        planned: situation.planned,
        reason: situation.reason.clone(),
        scope: situation.scope,
        lines: situation
            .line_ids
            .iter()
            .map(|l| format!("{network}:{l}"))
            .collect(),
        stops: situation.stop_ids.clone(),
        advice: advice.into_iter().collect(),
        validity: situation.validity.clone(),
        starts,
    }
}

/// The current and upcoming situations among `situations` that `keep`
/// accepts, at `now` (nothing unless `state` is current).
pub fn situations_view(
    network: &str,
    situations: &[TransitSituation],
    state: StatusState,
    as_of: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
    keep: impl Fn(&TransitSituation) -> bool,
) -> SituationsView {
    let mut current = Vec::new();
    let mut upcoming = Vec::new();
    if state == StatusState::Current {
        for situation in situations.iter().filter(|s| keep(s)) {
            if is_current(situation, now) {
                current.push(view(network, situation, None));
            } else if let Some(period) = upcoming_period(situation, now) {
                upcoming.push(view(network, situation, Some(period.start)));
            }
        }
    }
    let first_start = |v: &SituationView| v.validity.iter().map(|p| p.start).min();
    current.sort_by(|a, b| {
        first_start(a)
            .cmp(&first_start(b))
            .then_with(|| a.id.cmp(&b.id))
    });
    upcoming.sort_by(|a, b| a.starts.cmp(&b.starts).then_with(|| a.id.cmp(&b.id)));
    SituationsView {
        state,
        as_of,
        current,
        upcoming,
    }
}

/// Whether `situation` concerns the line `line_id` serving `stations`.
pub fn concerns(situation: &TransitSituation, line_id: &str, stations: &BTreeSet<String>) -> bool {
    concerns_line(
        situation,
        &NetworkLine {
            line_id: line_id.to_owned(),
            name: String::new(),
            stations: stations.clone(),
        },
    )
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use common::Severity;

    use super::*;

    fn at(h: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 14, h, 0, 0).unwrap()
    }

    #[test]
    fn freshness_gives_the_state() {
        assert_eq!(state_at(None, at(12)), StatusState::None);
        assert_eq!(state_at(Some(at(12)), at(12)), StatusState::Current);
        assert_eq!(
            state_at(Some(at(12) - Duration::minutes(30)), at(12)),
            StatusState::Current
        );
        assert_eq!(
            state_at(Some(at(12) - Duration::minutes(31)), at(12)),
            StatusState::Unavailable
        );
    }

    fn status(severity: Severity, source: Option<&str>) -> LineStatus {
        LineStatus {
            severity,
            reason: format!("{severity:?}"),
            validity: common::ValidityPeriod {
                from_date: at(6),
                to_date: None,
                is_now: true,
            },
            disruption: source.map(|s| common::Disruption {
                category: "RealTime".to_owned(),
                description: String::new(),
                affected_stops: Vec::new(),
                affected_routes: Vec::new(),
                source: Some(s.to_owned()),
                impact_type: None,
            }),
            data_quality: common::DataQuality::Transit,
            sample_stats: None,
            sample_availability: common::SampleAvailability::no_coverage_default(),
            full_coverage_stats: None,
            full_coverage_availability: common::FullCoverageAvailability::not_enabled_default(),
        }
    }

    #[test]
    fn only_a_current_state_shows_the_statuses() {
        let stored = (
            vec![status(Severity::MinorDelays, Some("transit-situation-S1"))],
            vec![UpcomingDisruption {
                from: at(20),
                to: None,
                summary: "Works".to_owned(),
                incident_id: "S2".to_owned(),
            }],
        );
        let fresh = line_view(StatusState::Current, Some(at(12)), Some(&stored));
        assert_eq!(fresh.state, StatusState::Current);
        assert_eq!(fresh.statuses[0].severity, 9);
        assert_eq!(fresh.statuses[0].description, "Minor Delays");
        assert_eq!(fresh.statuses[0].situation_id.as_deref(), Some("S1"));
        assert_eq!(fresh.upcoming[0].situation_id, "S2");
        let stale = line_view(StatusState::Unavailable, Some(at(11)), Some(&stored));
        assert_eq!(stale.state, StatusState::Unavailable);
        assert_eq!(stale.as_of, Some(at(11)));
        assert!(stale.statuses.is_empty() && stale.upcoming.is_empty());
        assert_eq!(
            line_view(StatusState::None, None, Some(&stored))
                .statuses
                .len(),
            0
        );
        assert_eq!(
            line_view(StatusState::Current, Some(at(12)), None).state,
            StatusState::Unavailable
        );
    }
}
