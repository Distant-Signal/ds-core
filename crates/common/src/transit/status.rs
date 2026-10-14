//! A transit network's line statuses from its published situations
//! (transit spec §10.2; plan 4.2). Pure: the writer calls [`derive`] with
//! the network's whole situation set and its lines, and writes the result
//! to `line_status` (§10.3).
//!
//! **When a situation counts.** It is *current* when its `progress` is
//! `open`, now is inside a publication window and inside a validity period
//! (an empty list of either means "always"). It is *upcoming* when it is
//! open and published and its next validity period starts within
//! [`UPCOMING_WITHIN`]; upcoming situations go to `upcoming`, never into
//! the status.
//!
//! **What a consequence does** (the spec's first table):
//!
//! - an equipment reason (`equipment:liftFailure`, …) with stop points is a
//!   stop notice and leaves every line alone;
//! - a situation of network scope (no mappable line or stop) leaves every
//!   line alone;
//! - otherwise the consequence affects its mapped lines, or every line for
//!   `AllLines`; with stop points, only those of them serving a stop (or,
//!   with no line named, every line serving one);
//! - its condition and severity give the status (the spec's second table,
//!   [`severity_for`]); a stop subset turns Suspended into Part suspended.
//!
//! **Per line** the statuses are listed worst first, one per contributing
//! situation (its worst consequence on that line). A line with no current
//! disruptive situation reads [`Severity::NoReportedDisruption`] first, and
//! its Information situations follow it: Information never degrades a
//! line, so a reader shows the first status as the line's headline. **The
//! rules never use the timetable**: a scheduled tram with no situation is
//! not "running" (§2).

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Duration, Utc};

use super::{
    NetworkId, Period, QualifiedLineId, SituationScope, TransitConsequence, TransitSituation,
};
use crate::{
    DataQuality, Disruption, FullCoverageAvailability, LineStatus, SampleAvailability, Severity,
    UpcomingDisruption, ValidityPeriod, severity_rank,
};

/// How far ahead an announced situation is listed as upcoming.
pub const UPCOMING_WITHIN: Duration = Duration::days(7);

/// The reason text of a line with nothing published.
pub const NO_REPORTED_DISRUPTION: &str = "No disruption has been published for this line.";

/// One of the network's lines, with every station it serves (both
/// directions).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkLine {
    /// The local id (`blue`).
    pub line_id: String,
    pub name: String,
    pub stations: BTreeSet<String>,
}

/// One line's derived row for `line_status` (spec §10.3).
#[derive(Debug, Clone)]
pub struct DerivedLineStatus {
    pub line_id: QualifiedLineId,
    pub name: String,
    /// Worst first; never empty.
    pub statuses: Vec<LineStatus>,
    /// Ordered by start, then situation id.
    pub upcoming: Vec<UpcomingDisruption>,
}

const SUSPENDED: &[&str] = &["noService", "suspended", "cancelled"];
const REDUCED: &[&str] = &["diverted", "altered", "stopMoved", "partService"];
const DELAYED: &[&str] = &["delayed", "disrupted", "interrupted"];
const SEVERE: &[&str] = &["severe", "verySevere"];

/// The status a consequence's SIRI condition and severity give (spec
/// §10.2's second table). `stop_subset`: the consequence names stop
/// points, so it affects part of the line.
pub fn severity_for(
    condition: Option<&str>,
    severity: Option<&str>,
    stop_subset: bool,
) -> Severity {
    let condition = condition.unwrap_or("unknown");
    if SUSPENDED.contains(&condition) {
        if stop_subset {
            Severity::PartSuspended
        } else {
            Severity::Suspended
        }
    } else if REDUCED.contains(&condition) {
        Severity::ReducedService
    } else if DELAYED.contains(&condition) {
        if severity.is_some_and(|s| SEVERE.contains(&s)) {
            Severity::SevereDelays
        } else {
            Severity::MinorDelays
        }
    } else {
        Severity::Information
    }
}

/// Where now falls in a list of periods.
#[derive(Debug, Clone, Copy)]
enum Within<'a> {
    /// The list is empty: no bound stated.
    Always,
    In(&'a Period),
}

impl<'a> Within<'a> {
    fn period(self) -> Option<&'a Period> {
        match self {
            Self::Always => None,
            Self::In(period) => Some(period),
        }
    }
}

fn within(periods: &[Period], now: DateTime<Utc>) -> Option<Within<'_>> {
    if periods.is_empty() {
        return Some(Within::Always);
    }
    periods
        .iter()
        .find(|p| p.start <= now && p.end.is_none_or(|end| now < end))
        .map(Within::In)
}

fn published(situation: &TransitSituation, now: DateTime<Utc>) -> bool {
    situation.progress == "open" && within(&situation.publication, now).is_some()
}

/// Where now falls in `situation`'s validity, or `None` when it is not
/// current.
fn current_period(situation: &TransitSituation, now: DateTime<Utc>) -> Option<Within<'_>> {
    if !published(situation, now) {
        return None;
    }
    within(&situation.validity, now)
}

/// The next validity period starting after now and within
/// [`UPCOMING_WITHIN`], for a published situation that is not current.
fn upcoming_period(situation: &TransitSituation, now: DateTime<Utc>) -> Option<&Period> {
    if !published(situation, now) {
        return None;
    }
    situation
        .validity
        .iter()
        .filter(|p| p.start > now && p.start <= now + UPCOMING_WITHIN)
        .min_by_key(|p| p.start)
}

fn is_equipment(situation: &TransitSituation) -> bool {
    situation
        .reason
        .as_deref()
        .is_some_and(|r| r.starts_with("equipment"))
}

/// The lines `consequence` affects, and whether it covers only part of
/// them (a stop subset).
fn affected<'a>(
    consequence: &TransitConsequence,
    lines: &'a [NetworkLine],
) -> (Vec<&'a NetworkLine>, bool) {
    let named: Vec<&NetworkLine> = if consequence.all_lines {
        lines.iter().collect()
    } else {
        lines
            .iter()
            .filter(|l| consequence.line_ids.contains(&l.line_id))
            .collect()
    };
    if consequence.stop_ids.is_empty() {
        return (named, false);
    }
    let serves = |line: &NetworkLine| {
        consequence
            .stop_ids
            .iter()
            .any(|s| line.stations.contains(s))
    };
    let pool: Vec<&NetworkLine> = if named.is_empty() && consequence.line_ids.is_empty() {
        lines.iter().collect()
    } else {
        named
    };
    (pool.into_iter().filter(|l| serves(l)).collect(), true)
}

/// Each line's worst status from `situation`, keyed by local line id.
fn line_effects(situation: &TransitSituation, lines: &[NetworkLine]) -> BTreeMap<String, Severity> {
    let mut out: BTreeMap<String, Severity> = BTreeMap::new();
    if situation.scope == SituationScope::Network
        || (is_equipment(situation) && !situation.stop_ids.is_empty())
    {
        return out;
    }
    for consequence in &situation.consequences {
        let (affected, subset) = affected(consequence, lines);
        let severity = severity_for(
            consequence.condition.as_deref(),
            consequence.severity.as_deref(),
            subset,
        );
        for line in affected {
            out.entry(line.line_id.clone())
                .and_modify(|worst| {
                    if severity_rank(severity) > severity_rank(*worst) {
                        *worst = severity;
                    }
                })
                .or_insert(severity);
        }
    }
    out
}

fn status(
    severity: Severity,
    reason: String,
    validity: ValidityPeriod,
    disruption: Option<Disruption>,
) -> LineStatus {
    LineStatus {
        severity,
        reason,
        validity,
        disruption,
        data_quality: DataQuality::Transit,
        sample_stats: None,
        sample_availability: SampleAvailability::no_coverage_default(),
        full_coverage_stats: None,
        full_coverage_availability: FullCoverageAvailability::not_enabled_default(),
    }
}

fn situation_status(
    situation: &TransitSituation,
    severity: Severity,
    period: Option<&Period>,
    now: DateTime<Utc>,
) -> LineStatus {
    let category = if severity == Severity::Information {
        "Information"
    } else if situation.planned {
        "PlannedWork"
    } else {
        "RealTime"
    };
    status(
        severity,
        situation.summary.clone(),
        ValidityPeriod {
            from_date: period.map_or(now, |p| p.start),
            to_date: period.and_then(|p| p.end),
            is_now: true,
        },
        Some(Disruption {
            category: category.to_owned(),
            description: situation
                .description
                .clone()
                .unwrap_or_else(|| situation.summary.clone()),
            affected_stops: situation.stop_ids.clone(),
            affected_routes: Vec::new(),
            source: Some(format!("transit-situation-{}", situation.situation_id)),
            impact_type: None,
        }),
    )
}

/// Every line's status from `situations` at `now` (spec §10.2), ordered by
/// line id. `lines` is the network's whole line list: a line missing from
/// it gets no row, and a situation naming an unknown line affects nothing.
pub fn derive(
    network: &NetworkId,
    situations: &[TransitSituation],
    lines: &[NetworkLine],
    now: DateTime<Utc>,
) -> Vec<DerivedLineStatus> {
    // (rank, situation id, status) per line; sorted below.
    let mut current: BTreeMap<&str, Vec<(u8, &str, LineStatus)>> = BTreeMap::new();
    let mut upcoming: BTreeMap<&str, Vec<UpcomingDisruption>> = BTreeMap::new();
    for situation in situations {
        let effects = line_effects(situation, lines);
        if effects.is_empty() {
            continue;
        }
        if let Some(period) = current_period(situation, now) {
            for (line_id, severity) in &effects {
                let Some(line) = lines.iter().find(|l| &l.line_id == line_id) else {
                    continue;
                };
                current.entry(line.line_id.as_str()).or_default().push((
                    severity_rank(*severity),
                    situation.situation_id.as_str(),
                    situation_status(situation, *severity, period.period(), now),
                ));
            }
        } else if let Some(period) = upcoming_period(situation, now) {
            for line in lines.iter().filter(|l| effects.contains_key(&l.line_id)) {
                upcoming
                    .entry(line.line_id.as_str())
                    .or_default()
                    .push(UpcomingDisruption {
                        from: period.start,
                        to: period.end,
                        summary: situation.summary.clone(),
                        incident_id: situation.situation_id.clone(),
                    });
            }
        }
    }

    let mut out: Vec<DerivedLineStatus> = lines
        .iter()
        .filter_map(|line| {
            let line_id = QualifiedLineId::new(network, &line.line_id).ok()?;
            let mut entries = current.remove(line.line_id.as_str()).unwrap_or_default();
            entries.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(b.1)));
            let disrupted = entries
                .first()
                .is_some_and(|(_, _, s)| s.severity != Severity::Information);
            let mut statuses: Vec<LineStatus> = Vec::with_capacity(entries.len() + 1);
            if !disrupted {
                statuses.push(status(
                    Severity::NoReportedDisruption,
                    NO_REPORTED_DISRUPTION.to_owned(),
                    ValidityPeriod {
                        from_date: now,
                        to_date: None,
                        is_now: true,
                    },
                    None,
                ));
            }
            statuses.extend(entries.into_iter().map(|(_, _, s)| s));
            let mut soon = upcoming.remove(line.line_id.as_str()).unwrap_or_default();
            soon.sort_by(|a, b| {
                a.from
                    .cmp(&b.from)
                    .then_with(|| a.incident_id.cmp(&b.incident_id))
            });
            Some(DerivedLineStatus {
                line_id,
                name: line.name.clone(),
                statuses,
                upcoming: soon,
            })
        })
        .collect();
    out.sort_by_key(|row| row.line_id.to_string());
    out
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;
    use crate::transit::TransitSituations;

    fn at(h: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 14, h, 0, 0).unwrap()
    }

    fn network() -> NetworkId {
        NetworkId::parse("metrolink").unwrap()
    }

    fn line(id: &str, stations: &[&str]) -> NetworkLine {
        NetworkLine {
            line_id: id.to_owned(),
            name: format!("{id} line"),
            stations: stations.iter().map(|s| (*s).to_owned()).collect(),
        }
    }

    fn lines() -> Vec<NetworkLine> {
        vec![
            line("blue", &["A", "B", "C"]),
            line("green", &["A", "D"]),
            line("pink", &["E", "F"]),
        ]
    }

    fn consequence(
        condition: &str,
        severity: &str,
        lines: &[&str],
        stops: &[&str],
        all: bool,
    ) -> TransitConsequence {
        TransitConsequence {
            condition: Some(condition.to_owned()),
            severity: Some(severity.to_owned()),
            all_lines: all,
            line_ids: lines.iter().map(|s| (*s).to_owned()).collect(),
            unmapped_line_refs: Vec::new(),
            stop_ids: stops.iter().map(|s| (*s).to_owned()).collect(),
            advice: None,
        }
    }

    fn situation(id: &str, consequences: Vec<TransitConsequence>) -> TransitSituation {
        let line_ids: BTreeSet<String> = consequences
            .iter()
            .flat_map(|c| c.line_ids.clone())
            .collect();
        let stop_ids: BTreeSet<String> = consequences
            .iter()
            .flat_map(|c| c.stop_ids.clone())
            .collect();
        let scope = if !line_ids.is_empty() || consequences.iter().any(|c| c.all_lines) {
            SituationScope::Line
        } else if !stop_ids.is_empty() {
            SituationScope::Stop
        } else {
            SituationScope::Network
        };
        TransitSituation {
            situation_id: id.to_owned(),
            version: None,
            progress: "open".to_owned(),
            planned: false,
            reason: Some("miscellaneous:unknown".to_owned()),
            summary: format!("summary {id}"),
            description: None,
            info_url: None,
            validity: vec![Period {
                start: at(6),
                end: Some(at(20)),
            }],
            publication: Vec::new(),
            consequences,
            line_ids: line_ids.into_iter().collect(),
            stop_ids: stop_ids.into_iter().collect(),
            scope,
        }
    }

    fn headline(rows: &[DerivedLineStatus], line: &str) -> Severity {
        rows.iter()
            .find(|r| r.line_id.line() == line)
            .unwrap()
            .statuses[0]
            .severity
    }

    #[test]
    fn the_condition_and_severity_table() {
        use Severity::*;
        for (condition, severity, subset, expected) in [
            ("noService", "slight", false, Suspended),
            ("suspended", "unknown", false, Suspended),
            ("cancelled", "severe", false, Suspended),
            ("suspended", "slight", true, PartSuspended),
            ("noService", "slight", true, PartSuspended),
            ("diverted", "slight", false, ReducedService),
            ("altered", "severe", false, ReducedService),
            ("stopMoved", "slight", true, ReducedService),
            ("partService", "slight", false, ReducedService),
            ("delayed", "severe", false, SevereDelays),
            ("disrupted", "verySevere", false, SevereDelays),
            ("interrupted", "severe", true, SevereDelays),
            ("delayed", "slight", false, MinorDelays),
            ("disrupted", "unknown", false, MinorDelays),
            ("interrupted", "normal", true, MinorDelays),
            ("unknown", "severe", false, Information),
            ("normal", "slight", false, Information),
            ("somethingNew", "verySevere", false, Information),
        ] {
            assert_eq!(
                severity_for(Some(condition), Some(severity), subset),
                expected,
                "{condition}/{severity}/{subset}"
            );
        }
        assert_eq!(severity_for(None, None, false), Information);
        assert_eq!(severity_for(Some("delayed"), None, false), MinorDelays);
    }

    #[test]
    fn the_scope_rules() {
        let n = network();
        let mut lift = situation(
            "lift",
            vec![consequence("unknown", "slight", &[], &["A"], true)],
        );
        lift.reason = Some("equipment:liftFailure".to_owned());
        lift.scope = SituationScope::Stop;
        let mapped = situation(
            "mapped",
            vec![consequence("delayed", "slight", &["blue"], &[], false)],
        );
        let all = situation(
            "all",
            vec![consequence("suspended", "slight", &[], &[], true)],
        );
        let all_with_stops = situation(
            "stops",
            vec![consequence("suspended", "slight", &[], &["D"], true)],
        );
        let info = situation(
            "info",
            vec![consequence("unknown", "slight", &["pink"], &[], false)],
        );
        let mut nowhere = situation(
            "nowhere",
            vec![consequence("suspended", "severe", &[], &[], false)],
        );
        nowhere.scope = SituationScope::Network;

        // Equipment with stop points: a notice only.
        let rows = derive(&n, std::slice::from_ref(&lift), &lines(), at(12));
        for row in &rows {
            assert_eq!(row.statuses.len(), 1);
            assert_eq!(row.statuses[0].severity, Severity::NoReportedDisruption);
        }
        // Mapped line refs: that line only.
        let rows = derive(&n, std::slice::from_ref(&mapped), &lines(), at(12));
        assert_eq!(headline(&rows, "blue"), Severity::MinorDelays);
        assert_eq!(headline(&rows, "green"), Severity::NoReportedDisruption);
        // AllLines with no stops: every line.
        let rows = derive(&n, std::slice::from_ref(&all), &lines(), at(12));
        assert!(
            rows.iter()
                .all(|r| r.statuses[0].severity == Severity::Suspended)
        );
        // AllLines with stops: the lines serving them, part suspended.
        let rows = derive(&n, std::slice::from_ref(&all_with_stops), &lines(), at(12));
        assert_eq!(headline(&rows, "green"), Severity::PartSuspended);
        assert_eq!(headline(&rows, "blue"), Severity::NoReportedDisruption);
        assert_eq!(headline(&rows, "pink"), Severity::NoReportedDisruption);
        // A non-disruptive condition: Information, after "No reported
        // disruption", which stays the headline.
        let rows = derive(&n, std::slice::from_ref(&info), &lines(), at(12));
        let pink = rows.iter().find(|r| r.line_id.line() == "pink").unwrap();
        assert_eq!(
            pink.statuses.iter().map(|s| s.severity).collect::<Vec<_>>(),
            [Severity::NoReportedDisruption, Severity::Information]
        );
        // Network scope: nothing.
        let rows = derive(&n, std::slice::from_ref(&nowhere), &lines(), at(12));
        assert!(
            rows.iter()
                .all(|r| r.statuses[0].severity == Severity::NoReportedDisruption)
        );
    }

    #[test]
    fn the_worst_wins_and_every_contributor_is_listed() {
        let rows = derive(
            &network(),
            &[
                situation(
                    "s2",
                    vec![consequence("delayed", "slight", &["blue"], &[], false)],
                ),
                situation(
                    "s1",
                    vec![consequence("delayed", "severe", &["blue"], &[], false)],
                ),
                situation(
                    "s3",
                    vec![consequence("unknown", "slight", &["blue"], &[], false)],
                ),
            ],
            &lines(),
            at(12),
        );
        let blue = rows.iter().find(|r| r.line_id.line() == "blue").unwrap();
        assert_eq!(
            blue.statuses.iter().map(|s| s.severity).collect::<Vec<_>>(),
            [
                Severity::SevereDelays,
                Severity::MinorDelays,
                Severity::Information
            ]
        );
        assert_eq!(blue.statuses[0].reason, "summary s1");
        assert_eq!(blue.statuses[0].data_quality, DataQuality::Transit);
        let d = blue.statuses[0].disruption.as_ref().unwrap();
        assert_eq!(d.category, "RealTime");
        assert_eq!(d.source.as_deref(), Some("transit-situation-s1"));
        assert_eq!(blue.statuses[0].validity.from_date, at(6));
        assert_eq!(blue.statuses[0].validity.to_date, Some(at(20)));
    }

    #[test]
    fn only_open_published_valid_situations_are_current() {
        let base = situation(
            "s",
            vec![consequence("suspended", "severe", &["blue"], &[], false)],
        );
        let mut closed = base.clone();
        closed.progress = "closed".to_owned();
        let mut unpublished = base.clone();
        unpublished.publication = vec![Period {
            start: at(13),
            end: None,
        }];
        let mut expired = base.clone();
        expired.validity = vec![Period {
            start: at(6),
            end: Some(at(12)),
        }];
        for (what, s) in [
            ("closed", closed),
            ("unpublished", unpublished),
            ("expired", expired),
        ] {
            let rows = derive(&network(), &[s], &lines(), at(12));
            assert_eq!(
                headline(&rows, "blue"),
                Severity::NoReportedDisruption,
                "{what}"
            );
            assert!(rows.iter().all(|r| r.upcoming.is_empty()), "{what}");
        }
        let rows = derive(&network(), &[base], &lines(), at(12));
        assert_eq!(headline(&rows, "blue"), Severity::Suspended);
    }

    #[test]
    fn a_future_validity_is_upcoming() {
        let mut soon = situation(
            "soon",
            vec![consequence("suspended", "severe", &["blue"], &[], false)],
        );
        soon.validity = vec![
            Period {
                start: at(12) + Duration::days(3),
                end: Some(at(12) + Duration::days(4)),
            },
            Period {
                start: at(12) + Duration::days(1),
                end: None,
            },
        ];
        let mut later = soon.clone();
        later.situation_id = "later".to_owned();
        later.validity = vec![Period {
            start: at(12) + Duration::days(8),
            end: None,
        }];
        let rows = derive(&network(), &[soon, later], &lines(), at(12));
        let blue = rows.iter().find(|r| r.line_id.line() == "blue").unwrap();
        assert_eq!(blue.statuses[0].severity, Severity::NoReportedDisruption);
        assert_eq!(blue.upcoming.len(), 1);
        assert_eq!(blue.upcoming[0].incident_id, "soon");
        assert_eq!(blue.upcoming[0].from, at(12) + Duration::days(1));
        assert!(
            rows.iter()
                .filter(|r| r.line_id.line() != "blue")
                .all(|r| r.upcoming.is_empty())
        );
    }

    #[test]
    fn the_output_is_ordered_by_line_and_deterministic() {
        let mut reversed = lines();
        reversed.reverse();
        let s = [
            situation(
                "b",
                vec![consequence("delayed", "slight", &["blue"], &[], false)],
            ),
            situation(
                "a",
                vec![consequence("delayed", "slight", &["blue"], &[], false)],
            ),
        ];
        let rows = derive(&network(), &s, &reversed, at(12));
        assert_eq!(
            rows.iter()
                .map(|r| r.line_id.to_string())
                .collect::<Vec<_>>(),
            ["metrolink:blue", "metrolink:green", "metrolink:pink"]
        );
        let reasons: Vec<&str> = rows[0].statuses.iter().map(|s| s.reason.as_str()).collect();
        assert_eq!(reasons, ["summary a", "summary b"]);
        let again = derive(&network(), &s, &lines(), at(12));
        assert_eq!(
            serde_json::to_value(&again[0].statuses).unwrap(),
            serde_json::to_value(&rows[0].statuses).unwrap()
        );
    }

    #[test]
    fn the_lift_failure_fixture_leaves_every_line_undisrupted() {
        let fixture: TransitSituations =
            serde_json::from_str(include_str!("../../tests/fixtures/transit/situations.json"))
                .unwrap();
        let network_lines = vec![
            line("blue", &["9400ZZMAHEA", "9400ZZMAECC"]),
            line("green", &["9400ZZMAHEA"]),
        ];
        // 2026-10-14: the lift failure is current; the engineering works
        // (18 to 19 October) are upcoming on the blue line.
        let rows = derive(
            &fixture.network,
            &fixture.situations,
            &network_lines,
            at(12),
        );
        for row in &rows {
            assert_eq!(row.statuses.len(), 1, "{}", row.line_id);
            assert_eq!(row.statuses[0].severity, Severity::NoReportedDisruption);
        }
        let blue = rows.iter().find(|r| r.line_id.line() == "blue").unwrap();
        assert_eq!(blue.upcoming.len(), 1);
        assert_eq!(blue.upcoming[0].incident_id, "TFGM-1002");
        // During the works, blue is suspended and green is not.
        let rows = derive(
            &fixture.network,
            &fixture.situations,
            &network_lines,
            Utc.with_ymd_and_hms(2026, 10, 18, 12, 0, 0).unwrap(),
        );
        assert_eq!(headline(&rows, "blue"), Severity::Suspended);
        assert_eq!(
            rows[0].statuses[0].disruption.as_ref().unwrap().category,
            "PlannedWork"
        );
        assert_eq!(headline(&rows, "green"), Severity::NoReportedDisruption);
    }
}
