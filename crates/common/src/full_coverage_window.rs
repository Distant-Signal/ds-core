//! Windowed full-coverage stats: the wire types the full-coverage consumer
//! posts, and the one severity mapping the aggregator (`shadow`/`enforce`)
//! and `compare_full_coverage --windows` both use, so the two can never
//! disagree about what a window means.
//!
//! See docs/superpowers/specs/2026-09-27-full-coverage-windowed-stats-design.md
//! (sections 5-7, and its "Decisions (2026-09-27)" section).

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use crate::{Defaults, SampleStats, Severity, severity_rank};

/// `stats_version` of every windowed and v2 closed-day row: only trains
/// already due are counted, and "no event seen" is presumed cancelled only
/// for a train that was never even activated. Version 1 (every existing
/// `full_coverage_line_stats` row) counted the whole day's population,
/// unseen = cancelled.
pub const FULL_COVERAGE_STATS_VERSION: u16 = 2;

/// Minutes late at a train's first calling point on the line from which it
/// counts as delayed in full-coverage stats (user decision, 2026-09-27).
/// The default of `Defaults::full_coverage_delay_threshold_minutes`,
/// overridable per line through `severity_overrides`. Deliberately lower
/// than LDBWS's `delay_threshold_minutes` (5): the design's measurements
/// were taken at 5, so expect more delayed trains than it reports.
pub const FULL_COVERAGE_DELAY_THRESHOLD_MINUTES: i64 = 3;

/// The default of `Defaults::full_coverage_severe_min_affected` (user
/// decision, 2026-10-02): the Severe tiers (Part Suspended, Severe Delays)
/// need at least 5 affected trains. The 4.3-weekday shadow run fired Severe
/// on 3.53% of judgeable windows (limit 3%), with 8 lines Severe in more
/// than 20% of their daytime windows, mostly "3 of 6 trains 3+ minutes
/// late".
pub const FULL_COVERAGE_SEVERE_MIN_AFFECTED: i64 = 5;

/// A window row older than this is `Ineligible(StaleRow)`: the consumer
/// writes every 60 s, so three missed writes mean it has stopped.
pub const FULL_COVERAGE_WINDOW_MAX_AGE_SECS: i64 = 180;

/// The default of `Defaults::full_coverage_sparse_min_cancelled` (user
/// decision, 2026-10-02): a window too small for the rate tiers (fewer than
/// `full_coverage_min_sample_size` evaluable trains) still reads Part
/// Suspended when every one of at least this many trains was explicitly
/// cancelled. 0 turns the rule off. 3 would drop exactly the branch lines
/// the rule exists for (a two-trains-an-hour shuttle), so the default is 2.
pub const FULL_COVERAGE_SPARSE_MIN_CANCELLED: i64 = 2;

/// An explicit cancellation that arrived at least this many minutes before
/// the train was due counts as "cancelled in advance"
/// (`FullCoverageWindowCounts::cancelled_in_advance`). Only an annotation
/// on the sparse rule's reason; it changes no verdict.
pub const FULL_COVERAGE_CANCELLED_IN_ADVANCE_MINUTES: u32 = 180;

/// The default phase gate (user decision, 2026-09-27): only verdicts at the
/// Severe tier (`severity_rank` 4: Severe Delays, Part Suspended) are
/// enforced. Lower tiers are still computed and stored as
/// `would_escalate_to` with `below_min_rank = true`.
pub const FULL_COVERAGE_WINDOW_DEFAULT_MIN_ESCALATION_RANK: u8 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FullCoverageWindowKind {
    /// Trains due in the last `W` minutes, ending `grace` ago.
    Recent,
    /// Trains due since the rail day started, up to `grace` ago.
    DayToDate,
}

impl FullCoverageWindowKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Recent => "recent",
            Self::DayToDate => "day_to_date",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "recent" => Some(Self::Recent),
            "day_to_date" => Some(Self::DayToDate),
            _ => None,
        }
    }
}

/// The outcome counts of one window. `total` is the evaluable trains:
/// every class except `pending` and `unobserved`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FullCoverageWindowCounts {
    pub total: u32,
    pub on_time: u32,
    pub delayed: u32,
    pub cancelled_explicit: u32,
    pub cancelled_presumed: u32,
    pub skipped: u32,
    /// Activated or reporting upstream, not yet seen on the line. Not in
    /// `total`.
    pub pending: u32,
    /// Due before this process could see every event. Not in `total`.
    pub unobserved: u32,
    pub avg_delay_minutes: f64,
    /// Of `cancelled_explicit`, the trains whose cancellation arrived at
    /// least [`FULL_COVERAGE_CANCELLED_IN_ADVANCE_MINUTES`] before they were
    /// due. 0 from a consumer older than this field.
    #[serde(default)]
    pub cancelled_in_advance: u32,
}

impl FullCoverageWindowCounts {
    pub fn cancelled(&self) -> u32 {
        self.cancelled_explicit + self.cancelled_presumed
    }

    /// The `SampleStats` shape every existing reader understands
    /// (`cancelled = explicit + presumed`).
    pub fn to_sample_stats(&self) -> SampleStats {
        SampleStats {
            total: self.total as usize,
            delayed: self.delayed as usize,
            cancelled: self.cancelled() as usize,
            skipped: self.skipped as usize,
            avg_delay_minutes: self.avg_delay_minutes,
        }
    }
}

/// One window of one line, as `POST /private/full-coverage-window-stats`
/// takes it. `bucket_start` is derived by `api` from `computed_at`, never
/// taken from the wire.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FullCoverageWindowStatsRow {
    pub line_id: String,
    pub window_kind: FullCoverageWindowKind,
    pub service_date: NaiveDate,
    /// The due-time range covered.
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub computed_at: DateTime<Utc>,
    pub counts: FullCoverageWindowCounts,
    /// `"full"` (the population carried operator and train status) or
    /// `"stops_only"` (an older `schedule-reference`: presumed cancellation
    /// is off).
    pub relevance: String,
    pub presumed_enabled: bool,
    pub partial: bool,
    pub feed_stale: bool,
    pub stats_version: u16,
}

/// Why a window cannot influence severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IneligibleReason {
    /// Fewer evaluable trains than `full_coverage_min_sample_size`.
    BelowThreshold,
    /// The window starts before this process could see every event.
    Partial,
    /// The movement feed looked unhealthy when the window was computed.
    FeedStale,
    /// The row is older than [`FULL_COVERAGE_WINDOW_MAX_AGE_SECS`].
    StaleRow,
}

impl IneligibleReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BelowThreshold => "below_threshold",
            Self::Partial => "partial",
            Self::FeedStale => "feed_stale",
            Self::StaleRow => "stale_row",
        }
    }
}

/// Which rule produced an `Escalate` verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EscalationBasis {
    /// The rate tiers, over a window of at least
    /// `full_coverage_min_sample_size` evaluable trains.
    Rate,
    /// Rule A ([`sparse_all_cancelled`]): a window below the sample size in
    /// which every train was explicitly cancelled. The aggregator enforces
    /// it under its own allowlist (`FULL_COVERAGE_WINDOW_SPARSE_ENFORCE_LINES`),
    /// so it can stay shadow-only while the rate tiers are enforced.
    SparseAllCancelled,
}

impl EscalationBasis {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rate => "rate",
            Self::SparseAllCancelled => "sparse_all_cancelled",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "rate" => Some(Self::Rate),
            "sparse_all_cancelled" => Some(Self::SparseAllCancelled),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum WindowVerdict {
    Ineligible(IneligibleReason),
    Good,
    Escalate {
        severity: Severity,
        reason: String,
        basis: EscalationBasis,
    },
}

impl WindowVerdict {
    /// `"ineligible"`, `"good"` or `"escalate"` -- a metric label.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Ineligible(_) => "ineligible",
            Self::Good => "good",
            Self::Escalate { .. } => "escalate",
        }
    }

    pub fn severity(&self) -> Option<Severity> {
        match self {
            Self::Escalate { severity, .. } => Some(*severity),
            _ => None,
        }
    }

    /// The rule behind an `Escalate`; `None` for any other verdict.
    pub fn basis(&self) -> Option<EscalationBasis> {
        match self {
            Self::Escalate { basis, .. } => Some(*basis),
            _ => None,
        }
    }
}

/// The severity a `recent` window supports on its own (design section 6):
///
/// 1. Ineligible when `feed_stale`, `partial`, older than
///    [`FULL_COVERAGE_WINDOW_MAX_AGE_SECS`] at `now`, or
///    `total < full_coverage_min_sample_size` -- except that a window below
///    the sample size in which every train was explicitly cancelled is Part
///    Suspended ([`sparse_all_cancelled`], basis
///    [`EscalationBasis::SparseAllCancelled`]).
/// 2. Otherwise the first tier met, in this order (rates over `total`, each
///    also needing at least `full_coverage_min_affected` trains, and the
///    three Severe-rank tiers at least `full_coverage_severe_min_affected`):
///    Part Suspended (cancelled), Severe Delays (late), Severe Delays
///    (skipped), Reduced Service (cancelled), Minor Delays (late), Minor
///    Delays (skipped), else Good. Severe is checked before Reduced because
///    it ranks higher.
/// 3. Presumed cancellations never decide a tier: the cancellation tiers
///    count explicit cancellations only, which is the same as dropping to
///    the next tier met without the presumed ones.
pub fn classify_full_coverage_window(
    row: &FullCoverageWindowStatsRow,
    thresholds: &Defaults,
    now: DateTime<Utc>,
) -> WindowVerdict {
    if row.feed_stale {
        return WindowVerdict::Ineligible(IneligibleReason::FeedStale);
    }
    if row.partial {
        return WindowVerdict::Ineligible(IneligibleReason::Partial);
    }
    if now - row.computed_at > chrono::Duration::seconds(FULL_COVERAGE_WINDOW_MAX_AGE_SECS) {
        return WindowVerdict::Ineligible(IneligibleReason::StaleRow);
    }
    let c = &row.counts;
    if i64::from(c.total) < thresholds.full_coverage_min_sample_size || c.total == 0 {
        return sparse_all_cancelled(row, thresholds)
            .unwrap_or(WindowVerdict::Ineligible(IneligibleReason::BelowThreshold));
    }

    let total = f64::from(c.total);
    let min_affected = thresholds.full_coverage_min_affected.max(1);
    // A Severe tier never needs fewer affected trains than a lower one.
    let severe_min_affected = thresholds
        .full_coverage_severe_min_affected
        .max(min_affected);
    let met_with = |count: u32, pct: f64, floor: i64| {
        i64::from(count) >= floor && f64::from(count) / total >= pct
    };
    let met = |count: u32, pct: f64| met_with(count, pct, min_affected);
    let met_severe = |count: u32, pct: f64| met_with(count, pct, severe_min_affected);
    let span = window_phrase(row);
    let n = c.total;

    let cancelled = c.cancelled_explicit;
    let cancel_reason = || {
        let presumed = if c.cancelled_presumed > 0 {
            format!(" ({} more not seen at all)", c.cancelled_presumed)
        } else {
            String::new()
        };
        format!("{cancelled} of {n} trains due {span} were cancelled{presumed}.")
    };
    let late_reason = || format!("{} of {n} trains due {span} were late.", c.delayed);
    let skip_reason = || {
        format!(
            "{} of {n} trains due {span} skipped part of the line.",
            c.skipped
        )
    };

    let tiers: [(bool, Severity, &dyn Fn() -> String); 6] = [
        (
            met_severe(cancelled, thresholds.part_suspended_pct),
            Severity::PartSuspended,
            &cancel_reason,
        ),
        (
            met_severe(c.delayed, thresholds.severe_delays_pct),
            Severity::SevereDelays,
            &late_reason,
        ),
        (
            met_severe(c.skipped, thresholds.severe_delays_skip_pct),
            Severity::SevereDelays,
            &skip_reason,
        ),
        (
            met(cancelled, thresholds.reduced_service_pct),
            Severity::ReducedService,
            &cancel_reason,
        ),
        (
            met(c.delayed, thresholds.minor_delays_pct),
            Severity::MinorDelays,
            &late_reason,
        ),
        (
            met(c.skipped, thresholds.minor_delays_skip_pct),
            Severity::MinorDelays,
            &skip_reason,
        ),
    ];
    for (hit, severity, reason) in tiers {
        if hit {
            return WindowVerdict::Escalate {
                severity,
                reason: reason(),
                basis: EscalationBasis::Rate,
            };
        }
    }
    WindowVerdict::Good
}

/// Rule A, sparse all-cancelled detection ("Decisions (2026-10-02, sparse
/// windows)"): a window too small for the rate tiers is Part Suspended when
/// ALL of these hold, and `None` (still `BelowThreshold`) otherwise:
///
/// - `2 <= total < full_coverage_min_sample_size`;
/// - every evaluable train was explicitly cancelled, and there are at least
///   `full_coverage_sparse_min_cancelled` of them (0 turns the rule off);
/// - nothing is `pending`, presumed cancelled or `unobserved`;
/// - the population carried train status (`relevance == "full"`).
///
/// Only reached after the `feed_stale`, `partial` and row-age checks, which
/// still win. A feed gap cannot fake it: missing TRUST data yields pending
/// or presumed trains, never explicit cancellations, and planned closures
/// (STP-cancelled trains, buses) are not in the population at all.
pub fn sparse_all_cancelled(
    row: &FullCoverageWindowStatsRow,
    thresholds: &Defaults,
) -> Option<WindowVerdict> {
    let c = &row.counts;
    let min_cancelled = thresholds.full_coverage_sparse_min_cancelled;
    let applies = min_cancelled > 0
        && c.total >= 2
        && i64::from(c.total) < thresholds.full_coverage_min_sample_size
        && c.cancelled_explicit == c.total
        && i64::from(c.cancelled_explicit) >= min_cancelled
        && c.pending == 0
        && c.cancelled_presumed == 0
        && c.unobserved == 0
        && row.relevance == "full";
    if !applies {
        return None;
    }
    let n = c.total;
    let span = window_phrase(row);
    let in_advance = if c.cancelled_in_advance >= n {
        " (cancelled in advance)"
    } else {
        ""
    };
    Some(WindowVerdict::Escalate {
        severity: Severity::PartSuspended,
        reason: format!("All {n} trains due {span} were cancelled{in_advance}."),
        basis: EscalationBasis::SparseAllCancelled,
    })
}

fn window_phrase(row: &FullCoverageWindowStatsRow) -> String {
    let minutes = (row.window_end - row.window_start).num_minutes();
    match row.window_kind {
        FullCoverageWindowKind::DayToDate => "so far today".to_string(),
        FullCoverageWindowKind::Recent if minutes == 60 => "in the last hour".to_string(),
        FullCoverageWindowKind::Recent => format!("in the last {minutes} minutes"),
    }
}

/// What a verdict would do to a line currently at `current`
/// (escalate-only, design section 6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EscalationDecision {
    /// The verdict's severity when it ranks strictly above `current`;
    /// `None` when it would change nothing (Good, Ineligible, or not
    /// worse than what the line already shows).
    pub would_escalate_to: Option<Severity>,
    /// `would_escalate_to` is below the phase gate
    /// (`FULL_COVERAGE_WINDOW_MIN_ESCALATION_RANK`), so it is recorded but
    /// never enforced -- the evidence for lowering the gate later.
    pub below_min_rank: bool,
}

pub fn escalation_decision(
    verdict: &WindowVerdict,
    current: Severity,
    min_rank: u8,
) -> EscalationDecision {
    let would_escalate_to = verdict
        .severity()
        .filter(|severity| severity_rank(*severity) > severity_rank(current));
    EscalationDecision {
        would_escalate_to,
        below_min_rank: would_escalate_to.is_some_and(|s| severity_rank(s) < min_rank),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::thresholds_for;

    fn now() -> DateTime<Utc> {
        "2026-09-27T12:00:00Z".parse().unwrap()
    }

    fn row(counts: FullCoverageWindowCounts) -> FullCoverageWindowStatsRow {
        FullCoverageWindowStatsRow {
            line_id: "line-a".to_string(),
            window_kind: FullCoverageWindowKind::Recent,
            service_date: "2026-09-27".parse().unwrap(),
            window_start: "2026-09-27T10:50:00Z".parse().unwrap(),
            window_end: "2026-09-27T11:50:00Z".parse().unwrap(),
            computed_at: now(),
            counts,
            relevance: "full".to_string(),
            presumed_enabled: true,
            partial: false,
            feed_stale: false,
            stats_version: FULL_COVERAGE_STATS_VERSION,
        }
    }

    fn counts(
        total: u32,
        delayed: u32,
        explicit: u32,
        presumed: u32,
        skipped: u32,
    ) -> FullCoverageWindowCounts {
        FullCoverageWindowCounts {
            total,
            on_time: total - delayed - explicit - presumed,
            delayed,
            cancelled_explicit: explicit,
            cancelled_presumed: presumed,
            skipped,
            ..Default::default()
        }
    }

    fn verdict(c: FullCoverageWindowCounts) -> WindowVerdict {
        classify_full_coverage_window(&row(c), &Defaults::default(), now())
    }

    fn severity(c: FullCoverageWindowCounts) -> Option<Severity> {
        verdict(c).severity()
    }

    #[test]
    fn defaults_for_the_new_keys() {
        let d = Defaults::default();
        assert_eq!(d.full_coverage_min_sample_size, 6);
        assert_eq!(d.full_coverage_min_affected, 3);
        assert_eq!(d.full_coverage_severe_min_affected, 5);
        assert_eq!(FULL_COVERAGE_SEVERE_MIN_AFFECTED, 5);
        assert_eq!(
            d.full_coverage_delay_threshold_minutes,
            FULL_COVERAGE_DELAY_THRESHOLD_MINUTES
        );
        assert_eq!(FULL_COVERAGE_DELAY_THRESHOLD_MINUTES, 3);
        assert_eq!(
            FULL_COVERAGE_WINDOW_DEFAULT_MIN_ESCALATION_RANK,
            severity_rank(Severity::SevereDelays)
        );
    }

    #[test]
    fn each_tier_at_its_threshold() {
        // 12 trains: 60% = 7.2 -> 8 cancelled; 50% = 6 late; 25% = 3.
        assert_eq!(
            severity(counts(12, 0, 8, 0, 0)),
            Some(Severity::PartSuspended)
        );
        assert_eq!(
            severity(counts(12, 0, 7, 0, 0)),
            Some(Severity::ReducedService)
        );
        assert_eq!(
            severity(counts(12, 6, 0, 0, 0)),
            Some(Severity::SevereDelays)
        );
        assert_eq!(
            severity(counts(12, 5, 0, 0, 0)),
            Some(Severity::MinorDelays)
        );
        assert_eq!(
            severity(counts(12, 3, 0, 0, 0)),
            Some(Severity::MinorDelays)
        );
        assert_eq!(severity(counts(12, 2, 0, 0, 0)), None);
        assert_eq!(verdict(counts(12, 2, 2, 0, 0)), WindowVerdict::Good);
        assert_eq!(
            severity(counts(12, 0, 3, 0, 0)),
            Some(Severity::ReducedService)
        );
        assert_eq!(
            severity(counts(12, 0, 0, 0, 6)),
            Some(Severity::SevereDelays)
        );
        assert_eq!(
            severity(counts(12, 0, 0, 0, 3)),
            Some(Severity::MinorDelays)
        );
    }

    /// With 6 trains, 2 late is 33% (over the 25% tier) but below the
    /// three-train minimum.
    #[test]
    fn the_min_affected_edge() {
        assert_eq!(verdict(counts(6, 2, 0, 0, 0)), WindowVerdict::Good);
        assert_eq!(verdict(counts(6, 0, 2, 0, 0)), WindowVerdict::Good);
    }

    /// The 2026-10-02 calibration: 3 of 6 trains late is 50%, but below the
    /// Severe tiers' five-train minimum, so it reads Minor Delays.
    #[test]
    fn three_of_six_late_is_minor_not_severe() {
        assert_eq!(severity(counts(6, 3, 0, 0, 0)), Some(Severity::MinorDelays));
    }

    #[test]
    fn five_of_eight_late_is_severe() {
        assert_eq!(
            severity(counts(8, 5, 0, 0, 0)),
            Some(Severity::SevereDelays)
        );
    }

    /// Exactly 5 affected trains meet the Severe tiers; 4 do not, even at
    /// a rate well over the Severe threshold.
    #[test]
    fn the_severe_min_affected_boundary() {
        // Late: 5 of 10 is exactly 50% and exactly 5 trains.
        assert_eq!(
            severity(counts(10, 5, 0, 0, 0)),
            Some(Severity::SevereDelays)
        );
        assert_eq!(severity(counts(6, 4, 0, 0, 0)), Some(Severity::MinorDelays));
        // Cancelled: 5 of 8 (62.5%) is Part Suspended; 4 of 6 (67%) only
        // Reduced Service.
        assert_eq!(
            severity(counts(8, 0, 5, 0, 0)),
            Some(Severity::PartSuspended)
        );
        assert_eq!(
            severity(counts(6, 0, 4, 0, 0)),
            Some(Severity::ReducedService)
        );
        // Skipped: 5 of 10 is Severe Delays; 4 of 6 only Minor Delays.
        assert_eq!(
            severity(counts(10, 0, 0, 0, 5)),
            Some(Severity::SevereDelays)
        );
        assert_eq!(severity(counts(6, 0, 0, 0, 4)), Some(Severity::MinorDelays));
    }

    /// A Severe floor set below `full_coverage_min_affected` is raised to it.
    #[test]
    fn the_severe_floor_is_never_below_the_lower_tiers_floor() {
        let mut overrides = HashMap::new();
        overrides.insert("full_coverage_severe_min_affected".to_string(), 1.0);
        let t = thresholds_for(&Defaults::default(), &overrides);
        assert_eq!(t.full_coverage_severe_min_affected, 1);
        let v = classify_full_coverage_window(&row(counts(6, 2, 0, 0, 0)), &t, now());
        assert_eq!(v, WindowVerdict::Good);
        let v = classify_full_coverage_window(&row(counts(6, 3, 0, 0, 0)), &t, now());
        assert_eq!(v.severity(), Some(Severity::SevereDelays));
    }

    #[test]
    fn presumed_cancellations_alone_never_decide_a_tier() {
        // 8 of 12 cancelled, but only 2 explicitly: not Part Suspended, not
        // Reduced Service (2 < 3 affected) -> falls through to the delays.
        assert_eq!(
            severity(counts(12, 3, 2, 6, 0)),
            Some(Severity::MinorDelays)
        );
        assert_eq!(verdict(counts(12, 0, 2, 6, 0)), WindowVerdict::Good);
        // Explicit alone meets Reduced; presumed would have made it Part
        // Suspended.
        assert_eq!(
            severity(counts(12, 0, 4, 4, 0)),
            Some(Severity::ReducedService)
        );
    }

    /// "30% cancelled and 60% late" is Severe Delays, not Reduced Service.
    #[test]
    fn severe_is_checked_before_reduced() {
        assert_eq!(
            severity(counts(20, 12, 6, 0, 0)),
            Some(Severity::SevereDelays)
        );
    }

    #[test]
    fn ineligible_for_each_reason() {
        let mut r = row(counts(12, 12, 0, 0, 0));
        r.feed_stale = true;
        assert_eq!(
            classify_full_coverage_window(&r, &Defaults::default(), now()),
            WindowVerdict::Ineligible(IneligibleReason::FeedStale)
        );
        let mut r = row(counts(12, 12, 0, 0, 0));
        r.partial = true;
        assert_eq!(
            classify_full_coverage_window(&r, &Defaults::default(), now()),
            WindowVerdict::Ineligible(IneligibleReason::Partial)
        );
        let r = row(counts(12, 12, 0, 0, 0));
        assert_eq!(
            classify_full_coverage_window(
                &r,
                &Defaults::default(),
                now() + chrono::Duration::minutes(4)
            ),
            WindowVerdict::Ineligible(IneligibleReason::StaleRow)
        );
        assert_eq!(
            verdict(counts(5, 5, 0, 0, 0)),
            WindowVerdict::Ineligible(IneligibleReason::BelowThreshold)
        );
        assert_eq!(
            verdict(counts(0, 0, 0, 0, 0)),
            WindowVerdict::Ineligible(IneligibleReason::BelowThreshold)
        );
    }

    #[test]
    fn line_overrides_for_the_new_keys() {
        let mut overrides = HashMap::new();
        overrides.insert("full_coverage_min_sample_size".to_string(), 4.0);
        overrides.insert("full_coverage_min_affected".to_string(), 2.0);
        overrides.insert("full_coverage_delay_threshold_minutes".to_string(), 5.0);
        let t = thresholds_for(&Defaults::default(), &overrides);
        assert_eq!(t.full_coverage_min_sample_size, 4);
        assert_eq!(t.full_coverage_min_affected, 2);
        assert_eq!(t.full_coverage_delay_threshold_minutes, 5);
        assert_eq!(t.full_coverage_severe_min_affected, 5);
        let v = classify_full_coverage_window(&row(counts(4, 2, 0, 0, 0)), &t, now());
        assert_eq!(v.severity(), Some(Severity::MinorDelays));
        overrides.insert("full_coverage_severe_min_affected".to_string(), 2.0);
        let t = thresholds_for(&Defaults::default(), &overrides);
        assert_eq!(t.full_coverage_severe_min_affected, 2);
        let v = classify_full_coverage_window(&row(counts(4, 2, 0, 0, 0)), &t, now());
        assert_eq!(v.severity(), Some(Severity::SevereDelays));
    }

    #[test]
    fn the_reason_text_names_trains_due_not_samples() {
        let WindowVerdict::Escalate { reason, .. } = verdict(counts(12, 0, 5, 1, 0)) else {
            panic!()
        };
        assert_eq!(
            reason,
            "5 of 12 trains due in the last hour were cancelled (1 more not seen at all)."
        );
        let WindowVerdict::Escalate { reason, .. } = verdict(counts(12, 7, 0, 0, 0)) else {
            panic!()
        };
        assert_eq!(reason, "7 of 12 trains due in the last hour were late.");
    }

    #[test]
    fn escalation_decisions_are_escalate_only_and_gated_by_rank() {
        let severe = verdict(counts(12, 6, 0, 0, 0));
        let minor = verdict(counts(12, 3, 0, 0, 0));
        let gate = FULL_COVERAGE_WINDOW_DEFAULT_MIN_ESCALATION_RANK;

        let d = escalation_decision(&severe, Severity::GoodService, gate);
        assert_eq!(d.would_escalate_to, Some(Severity::SevereDelays));
        assert!(!d.below_min_rank);

        let d = escalation_decision(&minor, Severity::GoodService, gate);
        assert_eq!(d.would_escalate_to, Some(Severity::MinorDelays));
        assert!(d.below_min_rank, "recorded, not enforced");
        assert!(!escalation_decision(&minor, Severity::GoodService, 3).below_min_rank);

        // Not worse than what the line already shows.
        let d = escalation_decision(&minor, Severity::ReducedService, gate);
        assert_eq!(d.would_escalate_to, None);
        assert!(!d.below_min_rank);
        let d = escalation_decision(&severe, Severity::PartSuspended, gate);
        assert_eq!(d.would_escalate_to, None);
        let d = escalation_decision(&WindowVerdict::Good, Severity::GoodService, gate);
        assert_eq!(d.would_escalate_to, None);
    }

    // --- Rule A: sparse all-cancelled (2026-10-02) ---

    fn sparse(total: u32, explicit: u32) -> FullCoverageWindowCounts {
        counts(total, 0, explicit, 0, 0)
    }

    #[test]
    fn two_of_two_cancelled_is_part_suspended_on_the_sparse_basis() {
        let v = verdict(sparse(2, 2));
        assert_eq!(
            v,
            WindowVerdict::Escalate {
                severity: Severity::PartSuspended,
                reason: "All 2 trains due in the last hour were cancelled.".to_string(),
                basis: EscalationBasis::SparseAllCancelled,
            }
        );
        assert_eq!(v.basis(), Some(EscalationBasis::SparseAllCancelled));
        // Up to one below the sample size.
        assert_eq!(
            verdict(sparse(5, 5)).basis(),
            Some(EscalationBasis::SparseAllCancelled)
        );
    }

    #[test]
    fn sparse_needs_every_train_and_at_least_two_cancelled() {
        let below = WindowVerdict::Ineligible(IneligibleReason::BelowThreshold);
        assert_eq!(verdict(sparse(1, 1)), below, "a single cancellation");
        assert_eq!(verdict(sparse(3, 2)), below, "one train ran");
        let mut c = sparse(3, 2);
        c.skipped = 1;
        c.on_time = 0;
        assert_eq!(verdict(c), below, "one train ran part of the line");
    }

    #[test]
    fn sparse_needs_nothing_pending_presumed_or_unobserved() {
        let below = WindowVerdict::Ineligible(IneligibleReason::BelowThreshold);
        let mut c = sparse(2, 2);
        c.pending = 1;
        assert_eq!(verdict(c), below, "2 cancelled + 1 pending");
        // 2 explicit + 1 presumed: total 3, explicit != total.
        assert_eq!(verdict(counts(3, 0, 2, 1, 0)), below);
        let mut c = sparse(2, 2);
        c.cancelled_presumed = 1;
        assert_eq!(verdict(c), below, "presumed > 0 even if miscounted");
        let mut c = sparse(2, 2);
        c.unobserved = 1;
        assert_eq!(verdict(c), below);
    }

    #[test]
    fn sparse_needs_full_relevance() {
        let mut r = row(sparse(3, 3));
        r.relevance = "stops_only".to_string();
        assert_eq!(
            classify_full_coverage_window(&r, &Defaults::default(), now()),
            WindowVerdict::Ineligible(IneligibleReason::BelowThreshold)
        );
    }

    /// At or above the sample size the rate tiers decide, on the rate
    /// basis, exactly as before.
    #[test]
    fn six_or_more_trains_use_the_rate_tiers() {
        let v = verdict(sparse(6, 6));
        assert_eq!(v.severity(), Some(Severity::PartSuspended));
        assert_eq!(v.basis(), Some(EscalationBasis::Rate));
        assert_eq!(
            verdict(sparse(6, 6)),
            WindowVerdict::Escalate {
                severity: Severity::PartSuspended,
                reason: "6 of 6 trains due in the last hour were cancelled.".to_string(),
                basis: EscalationBasis::Rate,
            }
        );
        assert_eq!(
            verdict(counts(6, 0, 4, 0, 0)).basis(),
            Some(EscalationBasis::Rate)
        );
    }

    #[test]
    fn sparse_min_cancelled_override_and_zero_is_off() {
        let mut overrides = HashMap::new();
        overrides.insert("full_coverage_sparse_min_cancelled".to_string(), 3.0);
        let t = thresholds_for(&Defaults::default(), &overrides);
        assert_eq!(t.full_coverage_sparse_min_cancelled, 3);
        let judge = |c, t: &Defaults| classify_full_coverage_window(&row(c), t, now());
        assert_eq!(
            judge(sparse(2, 2), &t),
            WindowVerdict::Ineligible(IneligibleReason::BelowThreshold)
        );
        assert_eq!(
            judge(sparse(3, 3), &t).severity(),
            Some(Severity::PartSuspended)
        );
        overrides.insert("full_coverage_sparse_min_cancelled".to_string(), 0.0);
        let off = thresholds_for(&Defaults::default(), &overrides);
        assert_eq!(
            judge(sparse(5, 5), &off),
            WindowVerdict::Ineligible(IneligibleReason::BelowThreshold)
        );
        // A min-cancelled of 1 still needs two trains.
        overrides.insert("full_coverage_sparse_min_cancelled".to_string(), 1.0);
        let one = thresholds_for(&Defaults::default(), &overrides);
        assert_eq!(
            judge(sparse(1, 1), &one),
            WindowVerdict::Ineligible(IneligibleReason::BelowThreshold)
        );
    }

    #[test]
    fn cancelled_in_advance_is_an_annotation_only() {
        let mut c = sparse(3, 3);
        c.cancelled_in_advance = 3;
        let WindowVerdict::Escalate {
            severity, reason, ..
        } = verdict(c)
        else {
            panic!()
        };
        assert_eq!(severity, Severity::PartSuspended);
        assert_eq!(
            reason,
            "All 3 trains due in the last hour were cancelled (cancelled in advance)."
        );
        // Only when every one of them was.
        let mut c = sparse(3, 3);
        c.cancelled_in_advance = 2;
        let WindowVerdict::Escalate { reason, .. } = verdict(c) else {
            panic!()
        };
        assert_eq!(reason, "All 3 trains due in the last hour were cancelled.");
    }

    /// `feed_stale`, `partial` and a stale row still win over the sparse
    /// rule.
    #[test]
    fn existing_guards_win_over_the_sparse_rule() {
        let mut r = row(sparse(3, 3));
        r.feed_stale = true;
        assert_eq!(
            classify_full_coverage_window(&r, &Defaults::default(), now()),
            WindowVerdict::Ineligible(IneligibleReason::FeedStale)
        );
        let mut r = row(sparse(3, 3));
        r.partial = true;
        assert_eq!(
            classify_full_coverage_window(&r, &Defaults::default(), now()),
            WindowVerdict::Ineligible(IneligibleReason::Partial)
        );
        let r = row(sparse(3, 3));
        assert_eq!(
            classify_full_coverage_window(
                &r,
                &Defaults::default(),
                now() + chrono::Duration::minutes(4)
            ),
            WindowVerdict::Ineligible(IneligibleReason::StaleRow)
        );
    }

    #[test]
    fn basis_round_trips() {
        for b in [EscalationBasis::Rate, EscalationBasis::SparseAllCancelled] {
            assert_eq!(EscalationBasis::parse(b.as_str()), Some(b));
        }
        assert_eq!(EscalationBasis::parse("other"), None);
        assert_eq!(WindowVerdict::Good.basis(), None);
        assert_eq!(Defaults::default().full_coverage_sparse_min_cancelled, 2);
    }

    #[test]
    fn wire_shape() {
        let r = row(counts(12, 1, 1, 0, 0));
        let json = serde_json::to_value(&r).unwrap();
        assert_eq!(json["window_kind"], "recent");
        assert_eq!(json["counts"]["cancelled_explicit"], 1);
        let back: FullCoverageWindowStatsRow = serde_json::from_value(json).unwrap();
        assert_eq!(back, r);
        assert_eq!(r.counts.to_sample_stats().cancelled, 1);
        // A row from a consumer without `cancelled_in_advance` still parses.
        let mut json = serde_json::to_value(&r).unwrap();
        json["counts"]
            .as_object_mut()
            .unwrap()
            .remove("cancelled_in_advance");
        let back: FullCoverageWindowStatsRow = serde_json::from_value(json).unwrap();
        assert_eq!(back.counts.cancelled_in_advance, 0);
    }
}
