//! Decide which lines a Knowledgebase incident affects, and classify the
//! scope of each match. Ported from `src/matcher.py`.
//!
//! This module lived in `crates/aggregator` until 2026-09-17. It moved here
//! so `api` can run the *same* matcher over an incident at ingest time and
//! persist the answer in `incidents.affected_lines` — the incident archive's
//! Line filter had been asking "which lines?" a second, different way
//! (station overlap against `incidents.affected_stations`, a column no
//! production writer ever populates) and therefore returned zero rows for
//! every line. See
//! `docs/superpowers/specs/2026-09-16-tfl-incident-archive-design.md` §1c.
//! Nothing about the matching logic itself changed in the move; the one
//! addition is [`LineMatcher`], a thin owner of the catalogue + segment
//! index for callers that do not already hold both.

use std::collections::{HashMap, HashSet};

use crate::segments::SegmentRegistry;
use crate::{IncidentMessage, LineDefinition};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchScope {
    ExclusiveSegment,
    SharedSegment,
    StationHit,
    KeywordOnly,
    OperatorOnly,
}

// `segments`/`operators`/`keywords` are faithful ports of the Python
// prototype's `evidence` dict (`src/matcher.py`) and are intentionally kept
// as API surface for future consumers (e.g. richer disruption messages,
// debugging) even though only `.stations` is read today.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Evidence {
    pub stations: Vec<String>,
    pub segments: Vec<String>,
    pub operators: Vec<String>,
    pub keywords: Vec<String>,
}

pub struct Match<'a> {
    pub line: &'a LineDefinition,
    pub scope: MatchScope,
    pub evidence: Evidence,
}

/// Return all lines the incident could plausibly affect, classified.
///
/// # Reads exactly four fields of `IncidentMessage`
///
/// `summary`, `description`, `operators`, `affected_stations` -- and
/// nothing else, through `match_one` and `is_excluded` alike.
/// `api::data::incident_line_backfill` relies on that: it loads only those
/// four columns and fabricates the rest of the struct, precisely so that
/// one unparseable archived `validity_periods` cannot abort a backfill over
/// a field this function never consults. **If you make this function (or
/// anything it calls) read a fifth field -- `is_planned` is the plausible
/// one -- update `load_batch` there in the same change, or backfilled rows
/// will silently get answers computed from fabricated values, with no
/// compile error to warn you.**
pub fn lines_affected_by<'a>(
    incident: &IncidentMessage,
    lines: &'a HashMap<String, LineDefinition>,
    registry: &SegmentRegistry,
) -> Vec<Match<'a>> {
    let haystack = format!("{} {}", incident.summary, incident.description).to_lowercase();
    let mut out: Vec<Match<'a>> = Vec::new();

    for line in lines.values() {
        if is_excluded(line, &haystack) {
            continue;
        }
        if let Some(m) = match_one(line, incident, registry, &haystack) {
            out.push(m);
        }
    }

    // Drop an operator-only match only when another line sharing at least
    // one of the same operator codes got a more precise match elsewhere in
    // this incident -- they're almost certainly false positives where that
    // other line on the same operator is the actual target. This is scoped
    // per-operator on purpose: it must NOT strip operator-only matches for
    // an unrelated operator just because some other operator's line got a
    // precise (e.g. keyword) hit somewhere in the same incident text. That
    // cross-operator interaction was the actual bug (a spurious keyword hit
    // for one operator deleting a different operator's correct
    // operator-only matches outright) -- do not collapse this back into a
    // single incident-wide `has_precise` check.
    //
    // Operators for which some line got a more-precise-than-OperatorOnly
    // match anywhere in this incident.
    let mut precise_operators: HashSet<&str> = HashSet::new();
    for m in &out {
        if m.scope != MatchScope::OperatorOnly {
            precise_operators.extend(m.line.operators.iter().map(String::as_str));
        }
    }
    out.retain(|m| {
        m.scope != MatchScope::OperatorOnly
            || !m
                .line
                .operators
                .iter()
                .any(|op| precise_operators.contains(op.as_str()))
    });

    out
}

/// Owns a line catalogue and its derived [`SegmentRegistry`] so a caller
/// that does not already hold both (i.e. anything other than the
/// aggregator's poll loop) can build the pair once and reuse it.
///
/// The point of this type is that there is exactly ONE answer in this
/// codebase to "which catalogue lines does this incident affect" —
/// [`lines_affected_by`] — and both the live status pipeline and the
/// incident archive's stored `affected_lines` go through it. Anything that
/// re-derives that answer by a different route (the archive's original
/// `affected_stations &&` filter, for instance) will disagree with what the
/// user sees on the live status pages.
pub struct LineMatcher {
    lines: HashMap<String, LineDefinition>,
    registry: SegmentRegistry,
}

impl LineMatcher {
    pub fn new(lines: &[LineDefinition]) -> Self {
        let lines: HashMap<String, LineDefinition> = lines
            .iter()
            .map(|line| (line.id.clone(), line.clone()))
            .collect();
        let registry = SegmentRegistry::new(&lines);
        Self { lines, registry }
    }

    /// Every catalogue line id this incident matches, sorted and deduped.
    ///
    /// Sorted because `lines_affected_by` iterates a `HashMap`'s values and
    /// so returns matches in an arbitrary, run-to-run-varying order; the
    /// result of this function is written to a database column and compared
    /// in tests, both of which want a stable array. (The matcher's own
    /// cross-line `OperatorOnly` post-filter is order-independent — it
    /// collects every match first, then retains — so sorting afterwards
    /// cannot change *which* lines come back, only their order.)
    pub fn affected_line_ids(&self, incident: &IncidentMessage) -> Vec<String> {
        let mut ids: Vec<String> = lines_affected_by(incident, &self.lines, &self.registry)
            .into_iter()
            .map(|m| m.line.id.clone())
            .collect();
        ids.sort();
        ids.dedup();
        ids
    }

    /// True if `line_id` is in this catalogue. Lets a caller reject an
    /// unknown line id without reaching for the catalogue separately.
    pub fn knows_line(&self, line_id: &str) -> bool {
        self.lines.contains_key(line_id)
    }

    /// How many lines this matcher was built from. Exists so a caller can
    /// refuse to act on a matcher built from an empty catalogue: such a
    /// matcher returns no lines for every incident, which is
    /// indistinguishable from a real answer and would silently erase stored
    /// attribution (see `api::data::incident_line_backfill`).
    pub fn line_count(&self) -> usize {
        self.lines.len()
    }
}

fn match_one<'a>(
    line: &'a LineDefinition,
    incident: &IncidentMessage,
    registry: &SegmentRegistry,
    haystack: &str,
) -> Option<Match<'a>> {
    let operator_overlap: Vec<String> = line
        .operators
        .iter()
        .filter(|op| incident.operators.contains(op))
        .cloned()
        .collect();
    let station_hits: Vec<String> = incident
        .affected_stations
        .iter()
        .filter(|crs| line.has_station(crs))
        .cloned()
        .collect();
    let keyword_hits: Vec<String> = line
        .match_keywords
        .iter()
        .filter(|kw| haystack.contains(&kw.to_lowercase()))
        .cloned()
        .collect();

    // Tier 1: station hits — try to classify by segment.
    if !station_hits.is_empty() {
        let segments: HashSet<String> = registry.segments_touched_by(line, &station_hits);
        let evidence = Evidence {
            stations: station_hits,
            segments: segments.iter().cloned().collect(),
            operators: operator_overlap,
            keywords: keyword_hits,
        };

        if !segments.is_empty()
            && segments
                .iter()
                .all(|s| registry.is_exclusive_to(s, &line.id))
        {
            return Some(Match {
                line,
                scope: MatchScope::ExclusiveSegment,
                evidence,
            });
        }
        if !segments.is_empty() && segments.iter().any(|s| registry.is_shared(s)) {
            return Some(Match {
                line,
                scope: MatchScope::SharedSegment,
                evidence,
            });
        }
        return Some(Match {
            line,
            scope: MatchScope::StationHit,
            evidence,
        });
    }

    // Tier 2: keyword match, unless the incident's own structured operator
    // list positively excludes this line's operator.
    if !keyword_hits.is_empty() {
        let contradicted = !incident.operators.is_empty() && operator_overlap.is_empty();
        if !contradicted {
            return Some(Match {
                line,
                scope: MatchScope::KeywordOnly,
                evidence: Evidence {
                    stations: vec![],
                    segments: vec![],
                    operators: operator_overlap,
                    keywords: keyword_hits,
                },
            });
        }
        // else: fall through. Tier 3 also requires non-empty
        // operator_overlap, which is false here by construction of
        // `contradicted`, so match_one naturally returns None below --
        // no special-cased early return needed.
    }

    // Tier 3: operator only.
    if !operator_overlap.is_empty() {
        return Some(Match {
            line,
            scope: MatchScope::OperatorOnly,
            evidence: Evidence {
                stations: vec![],
                segments: vec![],
                operators: operator_overlap,
                keywords: vec![],
            },
        });
    }

    None
}

fn is_excluded(line: &LineDefinition, haystack: &str) -> bool {
    line.excluded_keywords
        .iter()
        .any(|kw| haystack.contains(&kw.to_lowercase()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn load_line(id: &str) -> HashMap<String, LineDefinition> {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../lines");
        let all = LineDefinition::from_dir(&dir).expect("lines/ directory should parse");
        all.into_iter()
            .filter(|l| l.id == id)
            .map(|l| (l.id.clone(), l))
            .collect()
    }

    fn load_all_lines() -> HashMap<String, LineDefinition> {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../lines");
        LineDefinition::from_dir(&dir)
            .expect("lines/ directory should parse")
            .into_iter()
            .map(|l| (l.id.clone(), l))
            .collect()
    }

    fn incident(
        id: &str,
        summary: &str,
        description: &str,
        operators: &[&str],
        affected_stations: &[&str],
    ) -> IncidentMessage {
        IncidentMessage {
            incident_id: id.to_string(),
            summary: summary.to_string(),
            description: description.to_string(),
            operators: operators.iter().map(|s| s.to_string()).collect(),
            affected_stations: affected_stations.iter().map(|s| s.to_string()).collect(),
            priority: 0,
            validity: vec![],
            is_planned: false,
            is_cleared: false,
        }
    }

    #[test]
    fn wcml_operators_use_avantis_real_code_not_transport_for_wales() {
        // Regression guard: this file's operators list once had "AW"
        // (Transport for Wales' code) where "VT" (Avanti West Coast's
        // real code, inherited from Virgin Trains) belonged. See
        // lines/west-coast-main-line.toml's `operators` comment for the
        // sourcing behind this fix.
        let lines = load_line("wcml");
        let wcml = lines.get("wcml").expect("wcml line should exist");
        assert!(
            wcml.operators.iter().any(|op| op == "VT"),
            "wcml operators should contain VT (Avanti West Coast)"
        );
        assert!(
            !wcml.operators.iter().any(|op| op == "AW"),
            "wcml operators should not contain AW (Transport for Wales)"
        );
    }

    #[test]
    fn excluded_keyword_vetoes_match() {
        let lines = load_line("wcml");
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "T1",
            "Cross Country delays",
            "Cross Country services are delayed at Rugby.",
            &[],
            &["RUG"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        assert!(matches.is_empty(), "excluded keyword should veto match");
    }

    #[test]
    fn keyword_only_match() {
        let lines = load_line("wcml");
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "T2",
            "WCML engineering",
            "Overnight engineering work on the West Coast Main Line.",
            &[],
            &[],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].scope, MatchScope::KeywordOnly);
    }

    #[test]
    fn swr_shared_trunk_incident_propagates() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SWR-1",
            "Signal failure at Woking",
            "Signal failure causing delays to SWR services.",
            &["SW"],
            &["WOK"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(matched_ids.contains("swr-south-west-main"));
        assert!(matched_ids.contains("swr-portsmouth-direct"));
        assert!(matched_ids.contains("swr-alton"));
        for m in &matches {
            if m.line.id.starts_with("swr-") {
                assert_eq!(
                    m.scope,
                    MatchScope::SharedSegment,
                    "{} should be SharedSegment",
                    m.line.id
                );
            }
        }
    }

    #[test]
    fn xc_hub_incident_propagates_to_every_cross_country_arm() {
        // `wcml-birmingham.toml` (added after this test was first written) also
        // terminates at Birmingham New Street, on its own exclusive
        // `wcml-birmingham-branch` segment -- station-level overlap with the
        // CrossCountry hub, same precedent xc-south-coast.toml/xc-manchester.toml
        // already documented for Coventry/Stafford (xc-manchester.toml no
        // longer lists Crewe at all after its 2026-09-21 route correction --
        // see that file's own comment). It's a real sixth
        // line affected by this incident, just with a different scope.
        //
        // `wmr-cross-city.toml` (added after this test was first written) also
        // passes through Birmingham New Street, on its own exclusive
        // `wmr-cross-city-trunk` segment -- same station-level-overlap-only
        // pattern as `wcml-birmingham.toml` above (this task's own brief calls
        // out this exact precedent). It's a real seventh line affected by this
        // incident, still ExclusiveSegment.
        //
        // `lnwr-birmingham-crewe.toml` (added after this test was first
        // written, Task 1.8) also terminates at Birmingham New Street, on its
        // own exclusive `lnwr-birmingham` segment -- same station-overlap-only
        // pattern as the two lines above. It's a real eighth line affected by
        // this incident, still ExclusiveSegment.
        //
        // Updated by the Midlands batch 2: `wmr-chase-line.toml` and
        // `wmr-darlaston-line.toml` both terminate at Birmingham New Street
        // too, on the literal `wmr-chase-line-newstreet` segment name they
        // deliberately share (both files were authored together in the same
        // batch and keep this segment's extent byte-identical: BHM/DUD/AST/
        // WTT/PRY/HSD/TAB in both -- see either file's own comment for the
        // full derivation, including why this is a NEW name rather than a
        // reuse of `wmr-cross-city.toml`'s own trunk segment). Real ninth
        // and tenth lines, SharedSegment with each other but not with
        // anything else here. `wmr-camp-hill-line.toml` (same batch) also
        // terminates at Birmingham New Street, on its own exclusive
        // `wmr-camp-hill` segment -- an eleventh line, station-overlap-only
        // like `wcml-birmingham`/`wmr-cross-city`/`lnwr-birmingham-crewe`
        // above.
        //
        // Updated by the Midlands EMR/WMR/LNWR sanity review: `wmr-malvern-
        // line.toml` (a new file from that review) also terminates at
        // Birmingham New Street, on its own exclusive `wmr-malvern-line`
        // segment -- a twelfth line, same station-overlap-only pattern.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "XC-1",
            "Signal failure at Birmingham New Street",
            "Services are delayed.",
            &["XC"],
            &["BHM"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "cross-country".to_string(),
                "xc-manchester".to_string(),
                "xc-cardiff".to_string(),
                "xc-south-coast".to_string(),
                "xc-stansted".to_string(),
                "wcml-birmingham".to_string(),
                "wmr-cross-city".to_string(),
                "lnwr-birmingham-crewe".to_string(),
                "wmr-chase-line".to_string(),
                "wmr-darlaston-line".to_string(),
                "wmr-camp-hill-line".to_string(),
                "wmr-malvern-line".to_string(),
            ])
        );
        for m in &matches {
            let expected = match m.line.id.as_str() {
                "wcml-birmingham"
                | "wmr-cross-city"
                | "lnwr-birmingham-crewe"
                | "wmr-malvern-line"
                | "wmr-camp-hill-line" => MatchScope::ExclusiveSegment,
                _ => MatchScope::SharedSegment,
            };
            assert_eq!(m.scope, expected, "{} should be {:?}", m.line.id, expected);
        }
    }

    #[test]
    fn elizabeth_branch_incident_stays_on_its_branch() {
        // Shenfield is a real Greater Anglia main-line station too (see
        // greater-anglia-main-line.toml's Shenfield-corridor decision), so
        // now that that line is catalogued, an incident here legitimately
        // matches both lines by station overlap. What this test still
        // guards: neither match escalates to MatchScope::SharedSegment (the
        // two files deliberately use distinct segment names at Shenfield —
        // station overlap, not a shared trunk), and the incident does not
        // leak to elizabeth-line or elizabeth-heathrow, the other two XR
        // branches.
        //
        // Updated by the Wales/East Anglia batch: `greater-anglia-southend-
        // victoria.toml` also has its own western terminus/junction at
        // Shenfield (its own `greater-anglia-southend-victoria` segment) -
        // a third independent ExclusiveSegment match.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "XR-1",
            "Trespass at Shenfield",
            "Trespass incident causing delays.",
            &["XR"],
            &["SNF"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "elizabeth-shenfield".to_string(),
                "greater-anglia-main-line".to_string(),
                "greater-anglia-southend-victoria".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment, not shared",
                m.line.id
            );
        }
    }

    #[test]
    fn greater_anglia_shenfield_corridor_is_station_overlap_only() {
        // Romford is on both greater-anglia-main-line.toml (a genuine GA
        // main-line stop) and elizabeth-shenfield.toml (a metro stop on
        // dedicated Elizabeth line tracks). The two files deliberately do
        // NOT share a segment name for this corridor (see
        // greater-anglia-main-line.toml's decision comment), so an incident
        // here should match both lines independently, each still classified
        // as ExclusiveSegment rather than escalating to SharedSegment.
        //
        // Romford is also overground-liberty's own terminus (its file's own
        // comments already document this exact overlap, pre-dating this
        // batch's merge) -- its `overground-liberty` segment name is
        // exclusive catalogue-wide, so it joins the other two as a third
        // independent ExclusiveSegment match, not a shared trunk.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-1",
            "Points failure at Romford",
            "Points failure causing delays.",
            &["LE"],
            &["RMF"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "greater-anglia-main-line".to_string(),
                "elizabeth-shenfield".to_string(),
                "overground-liberty".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment, not shared",
                m.line.id
            );
        }
    }

    #[test]
    fn greater_anglia_exclusive_far_end_does_not_propagate() {
        // Diss is well beyond Shenfield, on Greater Anglia's exclusive
        // territory — the Elizabeth line goes no further than Shenfield —
        // and isn't a junction for any branch in this batch either (unlike
        // Colchester, which greater-anglia-sunshine-coast.toml (formerly
        // part of the bundled greater-anglia-essex-branches.toml, split by
        // brand) also lists as its own real junction — but as
        // station-level overlap only, each independently ExclusiveSegment,
        // not a shared `geml-mainline` segment; see
        // sunshine_coast_colchester_is_station_overlap_only_with_main_line
        // below). An incident here should stay scoped to
        // greater-anglia-main-line only.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-2",
            "Signal failure at Diss",
            "Signal failure causing delays.",
            &["LE"],
            &["DIS"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["greater-anglia-main-line".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn swr_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SWR-2",
            "Power supply issue at Alton",
            "Power supply problem causing delays at Alton.",
            &["SW"],
            &["AON"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["swr-alton".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // --- SWR suburban corridor (lines/swr-kingston-loop.toml,
    // lines/swr-chessington.toml) -----------------------------------------
    //
    // The README requires a new complex-operator line to be exercised in
    // BOTH shapes: an incident on its shared trunk, and one on its
    // exclusive segment. These four tests do that for the two suburban SWR
    // files, mirroring `swr_shared_trunk_incident_propagates` and
    // `swr_exclusive_segment_incident_does_not_propagate` above.

    /// Raynes Park is the junction where the Epsom/Mole Valley line (and
    /// with it the Chessington branch) leaves the South West Main Line, so
    /// per the README's junction rule both suburban files carry it on
    /// `swr-trunk-waterloo`. An incident there must therefore be a
    /// SharedSegment event across both of them.
    ///
    /// It must NOT reach the three fast-line SWR files: their services run
    /// through Raynes Park without calling, so none of them lists RAY, and
    /// their operator-only matches are dropped once a precise match exists.
    /// That asymmetry is the whole point of the segment machinery -- the
    /// shared *name* annotates this file's own matches correctly without
    /// inventing matches for lines that don't serve the station.
    #[test]
    fn swr_suburban_shared_trunk_incident_propagates_between_both_suburban_lines() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SWR-KL-1",
            "Points failure at Raynes Park",
            "Points failure is causing delays to South Western Railway services.",
            &["SW"],
            &["RAY"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        // Updated by the SWR suburban-gap batch: swr-shepperton-branch.toml
        // and swr-epsom-mole-valley.toml (two new files) also reuse
        // `swr-trunk-waterloo` verbatim through Raynes Park (Shepperton's
        // own trains run over the same New Malden-ward formation before
        // diverging at Shacklegate Junction, several stations further on;
        // the Epsom/Mole Valley line's own route continues straight past
        // Raynes Park towards Motspur Park) - both genuinely call here, so
        // both now match too.
        assert_eq!(
            matched_ids,
            HashSet::from([
                "swr-kingston-loop".to_string(),
                "swr-chessington".to_string(),
                "swr-shepperton-branch".to_string(),
                "swr-epsom-mole-valley".to_string(),
            ]),
            "every line that actually calls at Raynes Park should match"
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment at the Raynes Park junction",
                m.line.id
            );
        }
    }

    // The full-fan-out shared-trunk case for these two files -- an incident
    // at Wimbledon reaching all FIVE SWR lines as SharedSegment -- is
    // asserted by
    // `wim_station_overlap_matches_swr_trunk_and_thameslink_southern_as_independent_segments`
    // further down this file, which already owned that station's expected
    // match set and was extended rather than duplicated here.

    /// Kingston itself is on `swr-kingston-loop`, which the SWR
    /// suburban-gap batch's own swr-shepperton-branch.toml now also reuses
    /// verbatim (Shepperton's own trains run over this exact New Malden-
    /// Teddington stretch before diverging at Shacklegate Junction, several
    /// stations further on) -- so an incident here must now reach BOTH
    /// files as SharedSegment. See
    /// `swr_kingston_loop_own_exclusive_segment_incident_does_not_propagate`
    /// immediately below for the genuinely-still-exclusive case
    /// (Strawberry Hill, past the point where Shepperton's own trains
    /// diverge).
    #[test]
    fn swr_kingston_loop_shared_with_shepperton_segment_incident_propagates() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SWR-KL-3",
            "Trespass incident at Kingston",
            "Trespassers on the railway are causing delays at Kingston.",
            &["SW"],
            &["KNG"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "swr-kingston-loop".to_string(),
                "swr-shepperton-branch".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(m.scope, MatchScope::SharedSegment);
        }
    }

    /// Strawberry Hill sits past Shacklegate Junction, where Shepperton's
    /// own off-peak trains diverge away from the loop (per
    /// swr-kingston-loop.toml's own STW note) -- so unlike Kingston above,
    /// this station is on the loop's own renamed, genuinely exclusive
    /// `swr-strawberry-hill` segment. Nothing else in the catalogue serves
    /// it, so the incident must stay local -- including not reaching the
    /// Chessington branch, which leaves the main line a station earlier and
    /// never passes through here, or the Shepperton branch itself.
    #[test]
    fn swr_kingston_loop_own_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SWR-KL-3B",
            "Trespass incident at Strawberry Hill",
            "Trespassers on the railway are causing delays at Strawberry Hill.",
            &["SW"],
            &["STW"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["swr-kingston-loop".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    /// Same shape for the Chessington branch's own terminus.
    #[test]
    fn swr_chessington_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SWR-KL-4",
            "Points failure at Chessington South",
            "A points failure is causing delays at Chessington South.",
            &["SW"],
            &["CSS"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["swr-chessington".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    /// Richmond is both `swr-kingston-loop.toml`'s station (on its
    /// `swr-windsor-lines` segment) and `overground-mildmay.toml`'s western
    /// terminus (on `overground-mildmay-west`). The two approach Richmond
    /// over separate infrastructure, so this is deliberately station
    /// overlap only, NOT a shared segment -- the same treatment
    /// `gwr_heart_of_wessex_station_overlap_with_swr_south_west_main_stays_
    /// exclusive_each_line` and the xc-south-coast.toml/Reading precedent
    /// already apply elsewhere.
    ///
    /// The Wessex/Thames-Valley/Isle-of-Wight batch's own
    /// swr-windsor-lines.toml is the real, intended reuse
    /// swr-kingston-loop.toml's own `swr-windsor-lines` segment name was
    /// coined for (see that file's own SEGMENTS comment: "named for the
    /// track rather than for this service, so a future Reading / Windsor &
    /// Eton Riverside... file can reuse the name"). The SWR suburban-gap
    /// batch's own swr-waterloo-reading.toml is a further such reuse (its
    /// own Vauxhall/Richmond-side approach is copied verbatim from
    /// swr-windsor-lines.toml). So Richmond is now a genuine four-way case:
    /// swr-kingston-loop, swr-windsor-lines and swr-waterloo-reading share
    /// real track and all resolve as SharedSegment, while overground-
    /// mildmay stays ExclusiveSegment (still separate infrastructure,
    /// unaffected by either new file).
    #[test]
    fn richmond_station_overlap_between_kingston_loop_and_mildmay_stays_exclusive_each_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SWR-KL-5",
            "Station closure at Richmond",
            "Richmond station is closed due to a fire alarm activation.",
            &["SW", "LO"],
            &["RMD"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "swr-kingston-loop".to_string(),
                "overground-mildmay".to_string(),
                "swr-windsor-lines".to_string(),
                "swr-waterloo-reading".to_string(),
            ])
        );
        for m in &matches {
            let expected = if m.line.id == "overground-mildmay" {
                MatchScope::ExclusiveSegment
            } else {
                MatchScope::SharedSegment
            };
            assert_eq!(m.scope, expected, "{} scope mismatch", m.line.id);
        }
    }

    // London Overground's Liberty line (Romford - Emerson Park - Upminster)
    // is a standalone line with no interchange with any sibling Overground
    // line (confirmed by its own sourcing in the line-catalogue research
    // pass). Per the Global Constraints, standalone lines get an
    // exclusive-segment test only — no shared-segment propagation test is
    // possible or required, the same exception class as c2c/Merseyrail.
    #[test]
    fn overground_liberty_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LO-1",
            "Signal failure at Upminster",
            "Signal failure causing delays at Upminster.",
            &["LO"],
            &["UPM"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        // Upminster is also c2c's own Ockendon-loop junction (lines/c2c.toml,
        // segment `c2c-main-line` -- confirmed exclusive catalogue-wide, same
        // as `overground-liberty`'s own segment name). Station-level overlap,
        // distinct segment names -- same pattern as the Halifax/Berwick
        // precedents: both lines match by station, both stay ExclusiveSegment.
        assert_eq!(
            matched_ids,
            HashSet::from(["overground-liberty".to_string(), "c2c".to_string()])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    // London Overground's Lioness line (former Watford DC line, Euston -
    // Watford Junction) shares only a single station (Willesden Junction)
    // with the Mildmay line, on physically separate track either side of
    // it — a station-level overlap, not a shared segment. Standalone for
    // the shared-segment testing convention: exclusive-segment test only,
    // same exception class as c2c/Merseyrail.
    //
    // Uses Bushey (BSH) rather than Watford Junction (WFJ): WFJ also
    // appears on `west-coast-main-line.toml` (a real station-level overlap
    // the brief didn't call out), which would make an incident there match
    // both lines and defeat the point of this exclusive-segment test.
    //
    // Update (Midlands new-lines batch, 2026-09): `lnwr-euston-tring.toml`
    // was later added and also curates Bushey (its own comment: reused
    // verbatim from this file, "the physically distinct Watford DC/Lioness
    // line sharing the same station buildings" — a real station-level
    // overlap, deliberately kept as station-overlap-only, not a shared
    // segment, since the two lines run on physically separate tracks). So
    // this incident now genuinely matches both lines, each staying its own
    // ExclusiveSegment — mirroring `xc_manchester_station_overlap_with_
    // wmr_snow_hill_stays_exclusive_each_line`'s shape.
    #[test]
    fn overground_lioness_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LO-2",
            "Points failure at Bushey",
            "Points failure causing delays at Bushey.",
            &["LO"],
            &["BSH"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "overground-lioness".to_string(),
                "lnwr-euston-tring".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should stay ExclusiveSegment (station overlap, not a shared segment)",
                m.line.id
            );
        }
    }

    // London Overground's Mildmay line (former North London line core +
    // West London line north end) — exclusive segment test, mirroring
    // `elizabeth_branch_incident_stays_on_its_branch`. Mildmay's
    // shared-segment propagation test (`overground-canonbury-curve` with
    // the Windrush line) lives alongside `overground-windrush`'s own tests
    // below, since it needs both lines' files to exist.
    //
    // Uses Kew Gardens (KWG) rather than Stratford (SRA): SRA also appears
    // on `elizabeth-shenfield.toml` (a real station-level overlap the brief
    // didn't call out), which would make an incident there match both
    // lines and defeat the point of this exclusive-segment test.
    //
    // It used Richmond (RMD) until `lines/swr-kingston-loop.toml` was added
    // -- Richmond is that line's own station too (on its `swr-windsor-lines`
    // segment), so RMD stopped being a single-line station for exactly the
    // reason SRA never was. Kew Gardens is the neighbouring station on this
    // same `overground-mildmay-west` segment and is unique to this file, so
    // it tests the identical thing without the overlap. The Richmond overlap
    // itself is now asserted deliberately, by
    // `richmond_station_overlap_between_kingston_loop_and_mildmay_stays_exclusive_each_line`.
    #[test]
    fn overground_mildmay_exclusive_segment_incident_stays_on_its_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LO-3",
            "Trespass at Kew Gardens",
            "Trespass incident causing delays at Kew Gardens.",
            &["LO"],
            &["KWG"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["overground-mildmay".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // London Overground's Suffragette line (former Gospel Oak to Barking
    // line) has no genuine shared segment with any sibling Overground
    // line — its only touchpoint (Gospel Oak, with Mildmay) is a
    // station-level overlap, and South Tottenham (this line) is a
    // genuinely different station from Seven Sisters (Weaver line)
    // despite being nearby. Standalone for the shared-segment testing
    // convention: exclusive-segment test only, same exception class as
    // c2c/Merseyrail.
    #[test]
    fn overground_suffragette_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LO-4",
            "Points failure at Barking Riverside",
            "Points failure causing delays at Barking Riverside.",
            &["LO"],
            &["BGV"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["overground-suffragette".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // London Overground's Weaver line (former Lea Valley lines) has no
    // genuine shared segment with any sibling Overground line — its
    // Enfield Town/Cheshunt sub-trunk sharing is internal to this one
    // line, and Seven Sisters (this line) is a genuinely different
    // station from Suffragette's South Tottenham despite proximity.
    // Standalone for the shared-segment testing convention:
    // exclusive-segment test only, same exception class as c2c/Merseyrail.
    #[test]
    fn overground_weaver_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LO-5",
            "Signal failure at Chingford",
            "Signal failure causing delays at Chingford.",
            &["LO"],
            &["CHI"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["overground-weaver".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // London Overground's Windrush line (former East London line,
    // extended) — exclusive segment test for its West Croydon branch,
    // well clear of the shared Canonbury curve.
    //
    // Updated by the Southern real-world-sanity review: two new files,
    // southern-metro-crystal-palace.toml and southern-metro-sutton.toml,
    // also terminate/junction at West Croydon, each on its own distinct
    // exclusive segment (`southern-metro-west-croydon-branch`/
    // `southern-metro-sutton-branch`) - station overlap only against this
    // line and each other, so both join as independent ExclusiveSegment
    // matches.
    #[test]
    fn overground_windrush_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LO-6",
            "Signal failure at West Croydon",
            "Signal failure causing delays at West Croydon.",
            &["LO"],
            &["WCY"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "overground-windrush".to_string(),
                "southern-metro-crystal-palace".to_string(),
                "southern-metro-sutton".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    // The `overground-canonbury-curve` shared segment (Highbury &
    // Islington, Canonbury) is the only genuine cross-line shared trunk
    // among the six London Overground lines — a real curve of track
    // connecting the North London (Mildmay) and East London (Windrush)
    // route alignments. Mirrors `swr_shared_trunk_incident_propagates`;
    // needs both `overground-mildmay` and `overground-windrush` loaded,
    // hence `load_all_lines()` and placement here (after both files exist).
    #[test]
    fn overground_canonbury_curve_incident_propagates_to_mildmay_and_windrush() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LO-7",
            "Signal failure at Canonbury",
            "Signal failure causing delays at Canonbury.",
            &["LO"],
            &["CNN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "overground-mildmay".to_string(),
                "overground-windrush".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment",
                m.line.id
            );
        }
    }

    // `lines/thameslink-bedford.toml` (Batch 5, Task 5.12) does not exist in
    // this worktree's `lines/` directory as of authoring, so
    // `emr-mml-south` cannot be tested as a shared trunk here - only the
    // Nottingham spur's exclusive-segment behaviour is guaranteed testable
    // right now (see the ruling comment in
    // `lines/emr-midland-main-line.toml`).
    //
    // Updated by the Midlands EMR/WMR/LNWR sanity review: `emr-derwent-
    // valley.toml`'s own re-scope (Lincoln/Cleethorpes-Matlock, formerly
    // Derby-Matlock only) and the new `emr-crewe-derby.toml` both now also
    // call at Beeston, each on its own exclusive segment - two different
    // citations of the same general Derby-Nottingham corridor,
    // deliberately NOT sharing a segment name with `emr-midland-main-
    // line.toml`'s own `emr-mml-nottingham-spur` or with each other (see
    // either new file's own ruling comment on its Beeston entry for the
    // full reasoning). So this incident now also matches both,
    // independently ExclusiveSegment.
    //
    // Updated again by the national/WCML/XC sanity review: cross-
    // country.toml's own new Nottingham extension (`xc-cardiff.toml`'s own
    // `xc-cardiff-nottingham` segment) also calls at Beeston, reusing
    // NOT/BEE/ATB/LGE's CRS codes from emr-midland-main-line.toml verbatim
    // but deliberately NOT sharing a segment name with it (station overlap
    // only) - so this incident now also matches xc-cardiff, independently
    // ExclusiveSegment.
    #[test]
    fn emr_nottingham_spur_incident_stays_on_its_branch() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "EMR-1",
            "Points failure at Beeston",
            "Points failure causing delays to services at Beeston.",
            &["EM"],
            &["BEE"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "emr-midland-main-line".to_string(),
                "emr-derwent-valley".to_string(),
                "emr-crewe-derby".to_string(),
                "xc-cardiff".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    // `lines/emr-regional.toml` (Batch 7, Task 7.2): exclusive-segment check
    // on the Erewash Valley stretch (Alfreton), which - per that file's
    // Ruling 3 comment - is station-overlap-only with
    // `emr-midland-main-line.toml`'s Nottingham spur, not a shared segment.
    //
    // Updated by the Midlands new-lines batch 2: `northern-erewash-
    // valley.toml` also calls at Alfreton (its own `northern-erewash-
    // valley` segment) - genuine physical track-sharing, documented there
    // as deliberately NOT a shared segment name either, for the same
    // coarse-granularity reason. So this incident now also matches that
    // line, independently ExclusiveSegment.
    #[test]
    fn emr_regional_erewash_incident_stays_on_its_own_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "EMRR-1",
            "Signal failure at Alfreton",
            "Signal failure causing delays to services at Alfreton.",
            &["EM"],
            &["ALF"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "emr-regional".to_string(),
                "northern-erewash-valley".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    // Same file, Ruling 2: Chesterfield-Sheffield genuinely shares Midland
    // Main Line trackage with `emr-midland-main-line.toml`'s `emr-mml-north`
    // segment, so an incident there should propagate to both lines as
    // SharedSegment. `cross-country.toml` also lists CHD (its own
    // `xc-yorkshire` segment, per that Sheffield/Chesterfield stretch's
    // established station-overlap-only precedent) so it legitimately
    // appears too, as ExclusiveSegment - not asserted away, just not the
    // focus of this test.
    #[test]
    fn emr_regional_chesterfield_incident_shared_with_midland_main_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "EMRR-2",
            "Points failure at Chesterfield",
            "Points failure causing delays to services at Chesterfield.",
            &["EM"],
            &["CHD"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let by_id: HashMap<String, MatchScope> = matches
            .iter()
            .map(|m| (m.line.id.clone(), m.scope))
            .collect();
        assert_eq!(by_id.get("emr-regional"), Some(&MatchScope::SharedSegment));
        assert_eq!(
            by_id.get("emr-midland-main-line"),
            Some(&MatchScope::SharedSegment)
        );
    }

    // Same file, Ruling 1 (revised after final review): Manchester
    // Piccadilly-Stockport-Sheffield genuinely shares Hope Valley Line
    // *track* with `northern-hope-valley.toml`, but NOT a shared *segment
    // name* (see that file's Ruling 1 comment for why - a coarse-
    // granularity mismatch, the same shape as the Grantham test below).
    // This test confirms the intended, narrower outcome: an incident at
    // Stockport matches both lines independently, each still classified
    // within its own file (`emr-regional` as ExclusiveSegment on its own
    // `emr-regional-hope-valley` segment, `northern-hope-valley` as
    // ExclusiveSegment on its own `northern-hope-valley` segment - neither
    // reports SharedSegment for the other). `xc-manchester.toml` also
    // lists SPT (its own `xc-manchester` segment, an unrelated WCML route
    // that merely passes through the same station) so it legitimately
    // appears too, as ExclusiveSegment - not asserted away, just not the
    // focus of this test.
    #[test]
    fn emr_regional_stockport_and_hope_valley_both_match_without_over_propagating() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "EMRR-3",
            "Overhead line damage at Stockport",
            "Overhead line damage causing delays to services at Stockport.",
            &["EM"],
            &["SPT"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let by_id: HashMap<String, MatchScope> = matches
            .iter()
            .map(|m| (m.line.id.clone(), m.scope))
            .collect();
        assert_eq!(
            by_id.get("emr-regional"),
            Some(&MatchScope::ExclusiveSegment)
        );
        assert_eq!(
            by_id.get("northern-hope-valley"),
            Some(&MatchScope::ExclusiveSegment)
        );
    }

    // `lines/emr-connect.toml` (Batch 7, Task 7.3): the real EMR Connect
    // route runs St Pancras - Corby, not just to Luton Airport Parkway, so
    // it shares `emr-midland-main-line.toml`'s `emr-mml-south` (St Pancras
    // - Bedford) and `emr-mml-trunk` (Wellingborough - Kettering) segments
    // for its entire route bar the final station. An incident anywhere on
    // that shared stretch should propagate to both lines as SharedSegment -
    // mirrors `swr_shared_trunk_incident_propagates`.
    #[test]
    fn emr_connect_shared_trunk_incident_propagates() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "EMC-1",
            "Signal failure at Wellingborough",
            "Signal failure causing delays to services at Wellingborough.",
            &["EM"],
            &["WEL"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let by_id: HashMap<String, MatchScope> = matches
            .iter()
            .map(|m| (m.line.id.clone(), m.scope))
            .collect();
        assert_eq!(by_id.get("emr-connect"), Some(&MatchScope::SharedSegment));
        assert_eq!(
            by_id.get("emr-midland-main-line"),
            Some(&MatchScope::SharedSegment)
        );
    }

    // Same file: Corby is this line's only exclusive station (not on the
    // shared St Pancras-Kettering trunk, and not present in
    // `emr-midland-main-line.toml` at all - see that file's route-scope
    // ruling comment), so an incident there should stay local to
    // `emr-connect` as ExclusiveSegment.
    #[test]
    fn emr_connect_corby_incident_stays_on_its_own_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "EMC-2",
            "Points failure at Corby",
            "Points failure causing delays to services at Corby.",
            &["EM"],
            &["COR"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["emr-connect".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `lines/emr-robin-hood.toml` (originally Batch 7, Task 7.4, when this
    // line was still bundled into `emr-rural-branches.toml`; that file has
    // since been split one-line-per-file): Worksop was this line's own
    // exclusive territory at the time - no other file in this catalogue
    // listed WRK, and this file's own ruling documents confirming (rather
    // than assuming) no genuine shared trunk exists for this line beyond
    // the Nottingham station itself.
    //
    // Updated by the Yorkshire/North East batch: `northern-sheffield-
    // lincoln.toml` also calls at Worksop (its own `northern-sheffield-
    // lincoln` segment, a different name) - a genuine station overlap, not
    // a shared trunk, so both now match independently as ExclusiveSegment.
    #[test]
    fn emr_robin_hood_worksop_incident_stays_on_its_own_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "EMRB-1",
            "Signal failure at Worksop",
            "Signal failure causing delays to services at Worksop.",
            &["EM"],
            &["WRK"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "emr-robin-hood".to_string(),
                "northern-sheffield-lincoln".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    // `lines/emr-poacher.toml`: the brief anticipated no genuine shared-trunk
    // stretch for any of the three rural branches beyond station-level
    // overlap. Research found genuine shared *track* between the Poacher
    // Line (Nottingham-Skegness) and `emr-regional.toml`'s Liverpool-Norwich
    // service, both of which run over the same Nottingham-Grantham line
    // metals (the dedicated "Nottingham-Grantham line" Wikipedia article
    // confirms this) - but this file's own ruling comment explains why the
    // segment *name* is deliberately NOT shared regardless: `emr-regional.toml`'s
    // `emr-regional-east` segment is coarser than the genuine overlap (it
    // also spans that file's deliberately-exclusive Alfreton station and its
    // Peterborough-Ely-Norwich continuation), so reusing it here would
    // incorrectly promote those unrelated stations to "shared" too - reusing
    // it in an earlier draft of this file broke the pre-existing
    // `emr_regional_erewash_incident_stays_on_its_own_line` test below by
    // doing exactly that. This test instead confirms the intended, narrower
    // outcome: an incident at Grantham matches both lines independently,
    // each still classified within its own file (`emr-poacher` as
    // ExclusiveSegment on its own `emr-poacher-skegness` segment,
    // `emr-regional` as ExclusiveSegment on its own `emr-regional-east`
    // segment - neither reports SharedSegment for the other).
    #[test]
    fn emr_poacher_line_and_emr_regional_both_match_grantham_without_over_propagating() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "EMRB-2",
            "Points failure at Grantham",
            "Points failure causing delays to services at Grantham.",
            &["EM"],
            &["GRA"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let by_id: HashMap<String, MatchScope> = matches
            .iter()
            .map(|m| (m.line.id.clone(), m.scope))
            .collect();
        assert_eq!(
            by_id.get("emr-poacher"),
            Some(&MatchScope::ExclusiveSegment)
        );
        assert_eq!(
            by_id.get("emr-regional"),
            Some(&MatchScope::ExclusiveSegment)
        );
    }

    // `lines/emr-derwent-valley.toml`: the second confirmed shared-trunk
    // exception, and this one DOES reuse a sibling file's segment name (a
    // clean subset, unlike the Poacher Line case above - see this file's own
    // ruling comment for why the two cases are treated differently). The
    // Derwent Valley Line (Derby-Matlock) diverges from the Midland Main
    // Line at Ambergate Junction, just south of Ambergate station
    // (Wikipedia's "Ambergate railway station" article), so Derby-Ambergate
    // is genuine shared trunk with `emr-midland-main-line.toml`'s
    // `emr-mml-derby` segment, reused verbatim in this file. Derby (DBY) is
    // the only station common to both files' own station lists (the
    // intercity MML service skips Duffield/Belper/Ambergate entirely), so it
    // is the only station where an incident can demonstrate both lines
    // matching together as SharedSegment.
    //
    // Updated by the Midlands EMR/WMR/LNWR sanity review: `emr-crewe-
    // derby.toml` (a new file from that review) also reuses `emr-mml-derby`
    // verbatim for its own Derby entry, so a Derby incident now also
    // matches it as a third SharedSegment line -- not asserted by name
    // here (this test only checks specific `by_id` entries, not the full
    // match set), but consistent with the ruling above.
    #[test]
    fn emr_derwent_valley_shared_with_midland_main_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "EMRB-3",
            "Overhead line damage at Derby",
            "Overhead line damage causing delays to services at Derby.",
            &["EM"],
            &["DBY"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let by_id: HashMap<String, MatchScope> = matches
            .iter()
            .map(|m| (m.line.id.clone(), m.scope))
            .collect();
        assert_eq!(
            by_id.get("emr-derwent-valley"),
            Some(&MatchScope::SharedSegment)
        );
        assert_eq!(
            by_id.get("emr-midland-main-line"),
            Some(&MatchScope::SharedSegment)
        );
    }

    // Same file: Matlock itself is this line's terminus, on the exclusive
    // `emr-matlock-branch` segment (starts at Whatstandwell, the station
    // after Ambergate Junction) - confirms the exclusive tail behaves
    // correctly alongside the shared-trunk stretch tested above.
    #[test]
    fn emr_derwent_valley_matlock_incident_stays_on_its_own_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "EMRB-4",
            "Trespass at Matlock",
            "Trespass incident causing delays at Matlock.",
            &["EM"],
            &["MAT"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["emr-derwent-valley".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `lines/emr-poacher.toml`, originally Task 7.4 (Batch 7): fills the five
    // previously-omitted Nottingham-Grantham intermediate stations named
    // directly in that task's spec (Netherfield & Colwick / NET,
    // Radcliffe-on-Trent / RDF, Aslockton & Whatton / ALK, Elton & Orston /
    // ELO, Bottesford / BTF), two-source confirmed and inserted at their
    // true geographic position around the pre-existing Bingham (BIN) entry -
    // see the comment above `[[stations]] crs = "BIN"` in
    // `lines/emr-poacher.toml` for the full sourcing. All five sit on
    // `emr-poacher-skegness`, the same segment as their BIN/GRA neighbours,
    // and (per that file's own ruling) that segment name is deliberately NOT
    // shared with any sibling line's segment name even though genuine
    // Nottingham-Grantham track-sharing exists with `emr-regional` - so
    // unlike `emr_derwent_valley_shared_with_midland_main_line` above, there
    // is no cross-file SharedSegment assertion to add here; see
    // `emr_poacher_line_and_emr_regional_both_match_grantham_without_over_propagating`
    // for why that's already covered at Grantham itself.
    #[test]
    fn emr_poacher_infill_stations_present() {
        let lines = load_line("emr-poacher");
        let line = lines
            .get("emr-poacher")
            .expect("emr-poacher line should exist");
        for crs in ["NET", "RDF", "ALK", "ELO", "BTF"] {
            assert!(line.has_station(crs), "emr-poacher should list {crs}");
        }
    }

    // Same file, same task: an incident at one of the newly-added stations
    // (Bottesford) should behave exactly like the Worksop exclusive-segment
    // case above - matches only this line, as ExclusiveSegment on
    // `emr-poacher-skegness`.
    #[test]
    fn emr_poacher_bottesford_incident_stays_on_its_own_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "EMRB-5",
            "Signal failure at Bottesford",
            "Signal failure causing delays to services at Bottesford.",
            &["EM"],
            &["BTF"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["emr-poacher".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn cumbrian_coast_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "NT-1",
            "Signal failure at Whitehaven",
            "Signal failure causing delays on the Cumbrian Coast.",
            &["NT"],
            &["WTH"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["northern-cumbrian-coast".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn cumbrian_coast_shared_trunk_incident_propagates_to_furness() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "NT-2",
            "Points failure at Barrow-in-Furness",
            "Points failure causing delays at Barrow.",
            &["NT"],
            &["BIF"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(matched_ids.contains("northern-cumbrian-coast"));
        assert!(matched_ids.contains("northern-furness"));
        for m in &matches {
            if m.line.id == "northern-cumbrian-coast" || m.line.id == "northern-furness" {
                assert_eq!(
                    m.scope,
                    MatchScope::SharedSegment,
                    "{} should be SharedSegment",
                    m.line.id
                );
            }
        }
    }

    // Regression test for the segment-split fix applied when
    // `lines/northern-cumbrian-coast.toml` was merged: `northern-furness.toml`
    // originally tagged ALL FOUR of its stations (not just the shared
    // junction, BIF) with `segment = "northern-furness"`, which would have
    // silently reclassified a Lancaster/Carnforth/Ulverston incident as
    // SharedSegment purely because a second file also used that literal
    // segment name for BIF. LAN/CNF/ULV now sit on their own exclusive
    // `northern-furness-branch` segment -- this asserts that split holds.
    #[test]
    fn furness_branch_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "NT-FUR",
            "Signal failure at Ulverston",
            "Signal failure causing delays at Ulverston.",
            &["NT"],
            &["ULV"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["northern-furness".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn calder_valley_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "NT-3",
            "Signal failure at Todmorden",
            "Signal failure causing delays on the Calder Valley Line at Todmorden.",
            &["NT"],
            &["TOD"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["northern-calder-valley".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn airedale_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Keighley also gained a station on `lner-leeds.toml` (a later
        // national-mainlines audit, which reused Keighley's already
        // in-repo-verified CRS from this file's own KEI entry to model
        // LNER's own daily Skipton return working). Real station-level
        // overlap between two different operators' files -- same pattern
        // `lner_hull_exclusive_segment_incident_does_not_propagate` already
        // documents for Selby/hull-trains -- so both lines match, each
        // staying `ExclusiveSegment` on its own, differently-named segment
        // (`northern-airedale-skipton-approach` vs `lner-leeds-skipton`).
        //
        // Updated by the Yorkshire/North East batch: `northern-settle-
        // carlisle.toml` also calls at Keighley, on the literal
        // `northern-airedale-skipton-approach` segment name (reconciled
        // during integration merge -- see `northern-airedale.toml`'s own
        // Bingley/Keighley/Skipton note), so it joins `northern-airedale`
        // as a genuine SharedSegment pair while `lner-leeds` stays
        // independently ExclusiveSegment.
        let inc = incident(
            "NT-4",
            "Signal failure at Keighley",
            "Signal failure causing delays on the Airedale Line at Keighley.",
            &["NT"],
            &["KEI"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "northern-airedale".to_string(),
                "lner-leeds".to_string(),
                "northern-settle-carlisle".to_string(),
            ])
        );
        for m in &matches {
            let expected = if m.line.id == "lner-leeds" {
                MatchScope::ExclusiveSegment
            } else {
                MatchScope::SharedSegment
            };
            assert_eq!(m.scope, expected, "{} should be {:?}", m.line.id, expected);
        }
    }

    #[test]
    fn wharfedale_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "NT-5",
            "Signal failure at Ilkley",
            "Signal failure causing delays on the Wharfedale Line at Ilkley.",
            &["NT"],
            &["ILK"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["northern-wharfedale".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // The `northern-shipley-trunk` shared-trunk regression test, owned by
    // Task 8.4 per the plan (added once `lines/northern-wharfedale.toml`
    // also exists and shares that segment name - see the shared-trunk
    // naming comment at the top of `lines/northern-airedale.toml`).
    #[test]
    fn shipley_trunk_shared_incident_propagates_to_airedale_and_wharfedale() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "NT-6",
            "Signal failure at Shipley",
            "Signal failure causing delays to Northern services at Shipley.",
            &["NT"],
            &["SHY"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(matched_ids.contains("northern-airedale"));
        assert!(matched_ids.contains("northern-wharfedale"));
        for m in &matches {
            if m.line.id == "northern-airedale" || m.line.id == "northern-wharfedale" {
                assert_eq!(
                    m.scope,
                    MatchScope::SharedSegment,
                    "{} should be SharedSegment",
                    m.line.id
                );
            }
        }
    }

    // Task 2.1 (updated by Task 2.10): `lines/northern-airedale.toml` and
    // `lines/northern-wharfedale.toml` both gained Frizinghall (FZH), which
    // sits between BDQ and SHY on the shared `northern-shipley-trunk`
    // segment (see `northern-airedale.toml`'s "RESOLUTION for Task 2.10"
    // comment, and `northern-wharfedale.toml`'s own "[FILL-IN] resolved
    // (Task 2.10)" comment). Confirms `has_station` picks it up on both
    // files and that an incident there resolves as SharedSegment for both,
    // mirroring the existing SHY-based shared-trunk test above (now that
    // both files list FZH directly, this is a direct station-hit match on
    // each line, not just segment-name propagation).
    #[test]
    fn airedale_and_wharfedale_frizinghall_has_station_and_is_shared_trunk() {
        let lines = load_line("northern-airedale");
        let airedale = lines
            .get("northern-airedale")
            .expect("northern-airedale should load");
        assert!(
            airedale.has_station("FZH"),
            "northern-airedale should now list Frizinghall (FZH)"
        );
        assert_eq!(airedale.segment_for("FZH"), Some("northern-shipley-trunk"));

        let all_lines = load_all_lines();
        let wharfedale = all_lines
            .get("northern-wharfedale")
            .expect("northern-wharfedale should load");
        assert!(
            wharfedale.has_station("FZH"),
            "northern-wharfedale should now list Frizinghall (FZH)"
        );
        assert_eq!(
            wharfedale.segment_for("FZH"),
            Some("northern-shipley-trunk")
        );

        let registry = SegmentRegistry::new(&all_lines);
        assert!(
            registry.is_shared("northern-shipley-trunk"),
            "northern-shipley-trunk should still be a shared segment"
        );
        let inc = incident(
            "NT-8",
            "Signal failure at Frizinghall",
            "Signal failure causing delays to Northern services at Frizinghall.",
            &["NT"],
            &["FZH"],
        );
        let matches = lines_affected_by(&inc, &all_lines, &registry);
        let by_id: HashMap<String, MatchScope> = matches
            .iter()
            .map(|m| (m.line.id.clone(), m.scope))
            .collect();
        assert_eq!(
            by_id.get("northern-airedale"),
            Some(&MatchScope::SharedSegment)
        );
        assert_eq!(
            by_id.get("northern-wharfedale"),
            Some(&MatchScope::SharedSegment)
        );
    }

    // Task 2.1: SAE (Saltaire), by contrast, is the first station on this
    // file's own exclusive `northern-airedale` segment after the Shipley
    // junction - confirms it doesn't propagate to Wharfedale, mirroring
    // `airedale_exclusive_segment_incident_does_not_propagate` above.
    #[test]
    fn airedale_saltaire_has_station_and_stays_exclusive() {
        let lines = load_line("northern-airedale");
        let airedale = lines
            .get("northern-airedale")
            .expect("northern-airedale should load");
        assert!(
            airedale.has_station("SAE"),
            "northern-airedale should now list Saltaire (SAE)"
        );
        assert_eq!(airedale.segment_for("SAE"), Some("northern-airedale"));

        let all_lines = load_all_lines();
        let registry = SegmentRegistry::new(&all_lines);
        let inc = incident(
            "NT-9",
            "Signal failure at Saltaire",
            "Signal failure causing delays on the Airedale Line at Saltaire.",
            &["NT"],
            &["SAE"],
        );
        let matches = lines_affected_by(&inc, &all_lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["northern-airedale".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `lines/northern-esk-valley.toml` (Task 8.5) is a genuinely standalone
    // line per the gap analysis ("entirely separate from anything currently
    // modelled") - no other line in this catalogue shares any track with
    // it, so there is no shared-trunk regression test to write, only the
    // exclusive-segment one below.
    #[test]
    fn esk_valley_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "NT-7",
            "Signal failure at Glaisdale",
            "Signal failure causing delays on the Esk Valley Line at Glaisdale.",
            &["NT"],
            &["GLS"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["northern-esk-valley".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Blackburn is a genuine junction shared by `northern-clitheroe` and
    // `northern-east-lancashire.toml` (North West England line-coverage
    // audit, 2026-09-21), but the two files deliberately do not share a
    // segment name there -- no sourced fact establishes actual
    // through-running between the two lines' own physical routes beyond
    // both calling at the same station. Mirrors
    // `emr_regional_stockport_and_hope_valley_both_match_without_over_propagating`.
    #[test]
    fn clitheroe_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "NT-8",
            "Signal failure at Blackburn",
            "Signal failure causing delays on the Ribble Valley Line at Blackburn.",
            &["NT"],
            &["BBN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let by_id: HashMap<String, MatchScope> = matches
            .iter()
            .map(|m| (m.line.id.clone(), m.scope))
            .collect();
        assert_eq!(
            by_id.get("northern-clitheroe"),
            Some(&MatchScope::ExclusiveSegment)
        );
        assert_eq!(
            by_id.get("northern-east-lancashire"),
            Some(&MatchScope::ExclusiveSegment)
        );
    }

    // `northern-clitheroe.toml`'s MCV entry joins the existing
    // `northern-manchester` shared segment already used by `northern.toml`
    // and `northern-blackpool.toml` (confirmed genuine track-sharing through
    // Bolton, not just both routes touching Manchester - see the
    // top-of-file comment in `lines/northern-clitheroe.toml`), making it a
    // three-way shared trunk.
    #[test]
    fn manchester_victoria_hub_incident_propagates_to_northern_blackpool_and_clitheroe() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "NT-9",
            "Signal failure at Manchester Victoria",
            "Signal failure causing delays to Northern services at Manchester Victoria.",
            &["NT"],
            &["MCV"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(matched_ids.contains("northern"));
        assert!(matched_ids.contains("northern-blackpool"));
        assert!(matched_ids.contains("northern-clitheroe"));
        for m in &matches {
            if m.line.id == "northern"
                || m.line.id == "northern-blackpool"
                || m.line.id == "northern-clitheroe"
            {
                assert_eq!(
                    m.scope,
                    MatchScope::SharedSegment,
                    "{} should be SharedSegment",
                    m.line.id
                );
            }
        }
        // `northern-calder-valley.toml` also has an MCV entry, but tagged
        // with its own exclusive `northern-calder-valley` segment rather
        // than `northern-manchester` (see that file's own comment on why
        // the two termini are unrelated track). Guard against a future
        // regression where Calder Valley's MCV entry gets accidentally
        // merged into the shared `northern-manchester` segment.
        if matched_ids.contains("northern-calder-valley") {
            let calder_valley_match = matches
                .iter()
                .find(|m| m.line.id == "northern-calder-valley")
                .unwrap();
            assert_eq!(
                calder_valley_match.scope,
                MatchScope::ExclusiveSegment,
                "northern-calder-valley should be ExclusiveSegment"
            );
        }
    }

    // Task 2.2: `lines/northern-blackpool.toml` gained the full Bolton to
    // Preston (via Chorley) local calling pattern, previously omitted. CRL
    // (Chorley) is strictly between the file's existing BON and PRE entries
    // and inherits the file's own exclusive `northern-blackpool` segment
    // (Salford Crescent to Bolton is run non-stop by this service, so no
    // new station landed on the shared `northern-manchester` segment -
    // see the file's own top-of-file comment). Mirrors
    // `airedale_exclusive_segment_incident_does_not_propagate` above.
    #[test]
    fn northern_blackpool_chorley_has_station_and_stays_exclusive() {
        let lines = load_line("northern-blackpool");
        let blackpool = lines
            .get("northern-blackpool")
            .expect("northern-blackpool should load");
        assert!(
            blackpool.has_station("CRL"),
            "northern-blackpool should now list Chorley (CRL)"
        );
        assert_eq!(blackpool.segment_for("CRL"), Some("northern-blackpool"));

        let all_lines = load_all_lines();
        let registry = SegmentRegistry::new(&all_lines);
        let inc = incident(
            "NT-10",
            "Signal failure at Chorley",
            "Signal failure causing delays on the Blackpool Line at Chorley.",
            &["NT"],
            &["CRL"],
        );
        let matches = lines_affected_by(&inc, &all_lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["northern-blackpool".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn lner_ecml_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Aberdeen sits on `ecml-aberdeen`, north of the Doncaster/Newark
        // junctions that Tasks 6.2-6.4's not-yet-written Leeds/Hull/Lincoln
        // branches will share `ecml-doncaster`/`ecml-fenland` with — no
        // other line touches `ecml-aberdeen` today, so this should stay
        // exclusive to `lner-ecml` and not propagate anywhere else.
        //
        // Aberdeen is also scotrail-aberdeen-inverness.toml's own terminus
        // (merged separately, Batch 10), on its exclusive
        // `scotrail-aberdeen-inverness` segment -- station-level overlap,
        // distinct segment names, both stay ExclusiveSegment.
        //
        // Updated (Scotland real-world-sanity review): two new inter-city
        // spine files, scotrail-edinburgh-aberdeen.toml and scotrail-
        // glasgow-aberdeen.toml, also terminate at Aberdeen, both reusing
        // `scotrail-dundee-aberdeen` verbatim for their shared Dundee-
        // Aberdeen approach - a genuine SharedSegment pair with each other,
        // station overlap only against lner-ecml/scotrail-aberdeen-
        // inverness.
        let inc = incident(
            "LNER-1",
            "Points failure at Aberdeen",
            "Points failure causing delays at Aberdeen.",
            &["GR"],
            &["ABD"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "lner-ecml".to_string(),
                "scotrail-aberdeen-inverness".to_string(),
                "scotrail-edinburgh-aberdeen".to_string(),
                "scotrail-glasgow-aberdeen".to_string(),
            ])
        );
        for m in &matches {
            let expected = match m.line.id.as_str() {
                "scotrail-edinburgh-aberdeen" | "scotrail-glasgow-aberdeen" => {
                    MatchScope::SharedSegment
                }
                _ => MatchScope::ExclusiveSegment,
            };
            assert_eq!(m.scope, expected, "{} should be {:?}", m.line.id, expected);
        }
    }

    #[test]
    fn lner_leeds_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Harrogate sits on `lner-leeds-harrogate`, exclusive to this file
        // (LNER's Skipton working diverges at Leeds onto a different physical
        // line and isn't modeled as stations — see the file's comments).
        //
        // Updated by the Yorkshire/North East batch: `northern-harrogate-
        // line.toml` also calls at Harrogate (its own `northern-harrogate-
        // line` segment, a different name — see that file's own "Segment/
        // overlap decision" comment) - a genuine station overlap, not a
        // shared trunk, so both now match independently as ExclusiveSegment.
        let inc = incident(
            "LNER-2",
            "Signal failure at Harrogate",
            "Signal failure causing delays to services at Harrogate.",
            &["GR"],
            &["HGT"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "lner-leeds".to_string(),
                "northern-harrogate-line".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    #[test]
    fn lner_leeds_doncaster_shared_trunk_propagates_to_ecml() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Doncaster is `ecml-doncaster`, shared between `lner-ecml` and
        // `lner-leeds` (both run over the same ECML trunk to Doncaster
        // before the Leeds branch peels off onto the Wakefield Line).
        // `cross-country.toml` also has a station at Doncaster, but on its
        // own exclusive `xc-yorkshire` segment, so it's not asserted here.
        let inc = incident(
            "LNER-3",
            "Points failure at Doncaster",
            "Points failure causing disruption to services through Doncaster.",
            &["GR"],
            &["DON"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(matched_ids.contains("lner-ecml"));
        assert!(matched_ids.contains("lner-leeds"));
        for m in &matches {
            if m.line.id.starts_with("lner-") {
                assert_eq!(
                    m.scope,
                    MatchScope::SharedSegment,
                    "{} should be SharedSegment",
                    m.line.id
                );
            }
        }
    }

    #[test]
    fn lner_hull_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Selby sits on `lner-hull`, exclusive to this file — the real
        // physical divergence from the ECML is Temple Hirst Junction
        // (north of Doncaster, no CRS code), but the shared trunk still
        // ends at Doncaster per `lner-ecml.toml`'s own instruction (see
        // that file's DON entry and this file's comments), so Selby is
        // this branch's first exclusive station.
        //
        // Since Task 6.6 added `hull-trains.toml` (a different operator,
        // `HT`, that also genuinely calls at Selby en route to Hull
        // Paragon — see that file's own research comments), Selby now also
        // matches `hull-trains` by station. This mirrors
        // `lner_leeds_station_overlap_at_leeds_does_not_share_northern_segment`'s
        // precedent: both lines match, but each stays `ExclusiveSegment`
        // because neither's own segment name (`lner-hull` vs.
        // `ht-kings-cross-hull`) is literally shared with the other — this
        // is real station-level overlap between two different operators'
        // files, not a shared-trunk relationship the matcher recognizes by
        // segment name.
        //
        // Updated by the Yorkshire/North East batch: `northern-leeds-
        // selby.toml` also calls at Selby (its own `northern-leeds-selby`
        // segment, a third different name) - the same station-overlap-only
        // pattern, so it joins this set as a third independent
        // ExclusiveSegment match.
        //
        // Updated by the Northern NW/TPE sanity review: `tpe-north-
        // hull.toml` (new, split out of tpe-north.toml) also calls at Selby
        // on its own exclusive `tpe-north-hull` segment - a fourth
        // independent ExclusiveSegment match.
        let inc = incident(
            "LNER-4",
            "Signal failure at Selby",
            "Signal failure causing delays to services at Selby.",
            &["GR"],
            &["SBY"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "lner-hull".to_string(),
                "hull-trains".to_string(),
                "northern-leeds-selby".to_string(),
                "tpe-north-hull".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    #[test]
    fn lner_hull_doncaster_shared_trunk_propagates_to_ecml() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Doncaster is `ecml-doncaster`, shared between `lner-ecml` and
        // `lner-hull` (both run over the same ECML trunk to Doncaster
        // before the Hull branch peels off toward Selby/Brough). Mirrors
        // `lner_leeds_doncaster_shared_trunk_propagates_to_ecml` above;
        // `lner-leeds` also shares this segment, so it's expected to show
        // up here too, but only `lner-ecml`/`lner-hull` are asserted since
        // that's what this test is about.
        let inc = incident(
            "LNER-5",
            "Points failure at Doncaster",
            "Points failure causing disruption to services through Doncaster.",
            &["GR"],
            &["DON"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(matched_ids.contains("lner-ecml"));
        assert!(matched_ids.contains("lner-hull"));
        for m in &matches {
            if m.line.id.starts_with("lner-") {
                assert_eq!(
                    m.scope,
                    MatchScope::SharedSegment,
                    "{} should be SharedSegment",
                    m.line.id
                );
            }
        }
    }

    #[test]
    fn lner_lincoln_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Lincoln sits on `lner-lincoln`, exclusive to this file — the real
        // physical divergence from the ECML is the Newark flat crossing,
        // just north of Newark Northgate (no CRS code), but the shared
        // trunk still ends at Newark Northgate per `lner-ecml.toml`'s own
        // instruction (see that file's NNG entry and this file's
        // comments), so Lincoln was this branch's first exclusive station.
        //
        // Updated by the Midlands batch 2 / Yorkshire-North East batches:
        // `emr-nottingham-lincoln.toml` (its own `emr-nottingham-lincoln`
        // segment) and `northern-sheffield-lincoln.toml` (its own
        // `northern-sheffield-lincoln` segment) both also terminate at
        // Lincoln, each via a physically different approach - genuine
        // station overlap, not a shared trunk, so all three now match
        // independently as ExclusiveSegment.
        //
        // Updated again by the Midlands EMR/WMR/LNWR sanity review:
        // `emr-crewe-derby.toml` and the re-scoped `emr-derwent-
        // valley.toml` (formerly Derby-Matlock only, now Lincoln/
        // Cleethorpes-Matlock) both deliberately REUSE
        // `emr-nottingham-lincoln.toml`'s own `emr-nottingham-lincoln`
        // segment name verbatim for their own Lincoln entries (same
        // physical Nottingham-Newark Castle-Lincoln stretch, byte-
        // identical station list in all three files - see either new
        // file's own comment for the derivation). So those three now
        // report SharedSegment with EACH OTHER at Lincoln, while
        // `emr-lincoln-peterborough.toml` (its own exclusive
        // `emr-lincoln-peterborough` segment - a different, Peterborough/
        // Doncaster-facing approach) joins `lner-lincoln`/
        // `northern-sheffield-lincoln` as a fourth independent
        // ExclusiveSegment match.
        let inc = incident(
            "LNER-6",
            "Signal failure at Lincoln",
            "Signal failure causing delays to services at Lincoln.",
            &["GR"],
            &["LCN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "lner-lincoln".to_string(),
                "emr-nottingham-lincoln".to_string(),
                "northern-sheffield-lincoln".to_string(),
                "emr-crewe-derby".to_string(),
                "emr-derwent-valley".to_string(),
                "emr-lincoln-peterborough".to_string(),
            ])
        );
        for m in &matches {
            let expected = match m.line.id.as_str() {
                "emr-nottingham-lincoln" | "emr-crewe-derby" | "emr-derwent-valley" => {
                    MatchScope::SharedSegment
                }
                _ => MatchScope::ExclusiveSegment,
            };
            assert_eq!(m.scope, expected, "{} should be {:?}", m.line.id, expected);
        }
    }

    #[test]
    fn lner_lincoln_newark_northgate_shared_trunk_propagates_to_ecml() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Newark Northgate is `ecml-fenland`, shared between ALL FOUR of
        // `lner-ecml`, `lner-hull`, `lner-leeds` and `lner-lincoln` today
        // (all run over the same ECML trunk to/through Newark Northgate
        // before their own branches peel off further north or, for
        // Lincoln, at the Newark flat crossing right here) -- confirmed by
        // grepping each `lner-*.toml` file's own NNG entry. Final review,
        // Fix #5: tightened from the previous `contains`-based partial
        // assertion (which only checked `lner-ecml`/`lner-lincoln`) to
        // exact `HashSet` equality, matching the Global Constraint's own
        // wording ("matches every line sharing it") more literally, mirrors
        // `xc_hub_incident_propagates_to_every_cross_country_arm`'s style
        // above.
        let inc = incident(
            "LNER-7",
            "Points failure at Newark Northgate",
            "Points failure causing disruption to services through Newark Northgate.",
            &["GR"],
            &["NNG"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "lner-ecml".to_string(),
                "lner-hull".to_string(),
                "lner-leeds".to_string(),
                "lner-lincoln".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment",
                m.line.id
            );
        }
    }

    #[test]
    fn lner_leeds_station_overlap_at_leeds_does_not_share_northern_segment() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Leeds (LDS) is a station on all three of `lner-leeds`, `northern`
        // and `northern-yorkshire-coast` — but research found no LNER
        // service running the physical Leeds<->York trunk that Northern's
        // `northern-yorkshire` segment represents (see the file comment on
        // `lner-leeds.toml`'s LDS entry), so `lner-leeds` deliberately does
        // NOT reuse that segment name. This mirrors `xc-south-coast.toml`'s
        // "station overlap is fine, segment-sharing is a deliberate choice"
        // precedent: all three lines match this incident by station, but
        // `lner-leeds` stays ExclusiveSegment (its own `lner-leeds` segment
        // isn't shared with anyone) while Northern's two lines are
        // SharedSegment between themselves via `northern-yorkshire`.
        let inc = incident(
            "LNER-4",
            "Overhead line damage at Leeds",
            "Overhead line damage causing disruption to services at Leeds.",
            &["GR"],
            &["LDS"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        // Leeds is now this catalogue's most-contested station, per Batch 8's
        // own final review: five more Northern-family files (all landed on
        // `main` after this test was originally written) also stop at LDS,
        // and Batch 9 (TransPennine Express) added a sixth line. Each line's
        // scope here is a genuine, individually-derived fact about that
        // line's OWN segment name at LDS -- not a blanket
        // "Northern shares, LNER doesn't" rule:
        //   - lner-leeds: its own `lner-leeds` segment, used nowhere else.
        //   - northern / northern-yorkshire-coast: both on `northern-yorkshire`,
        //     shared between exactly those two.
        //   - northern-airedale: on `northern-shipley-trunk` -- shared
        //     catalogue-wide with northern-wharfedale.toml's own BDQ/SHY
        //     entries (even though Wharfedale's own LDS entry uses a
        //     different, exclusive segment -- sharing is evaluated per
        //     segment NAME across the whole catalogue, not per station).
        //   - northern-wharfedale: its own `northern-wharfedale` segment at
        //     LDS specifically, used nowhere else -- exclusive despite
        //     sharing `northern-shipley-trunk` with Airedale at BDQ/SHY.
        //   - northern-calder-valley: its own `northern-calder-valley`
        //     segment, used nowhere else.
        //   - tpe-north: its own `tpe-north` segment, used nowhere else in
        //     the catalogue (confirmed via exact-match grep, not the
        //     substring search that once wrongly suggested 4 files used it).
        //
        // Updated by the Yorkshire/North East batch (three more files, all
        // landed after this test was last updated):
        //   - northern-harrogate-line: its own `northern-harrogate-line`
        //     segment at LDS, used nowhere else -- exclusive (see that
        //     file's own "Segment/overlap decision" comment, which
        //     deliberately declines to reuse `lner-leeds`/`lner-leeds-
        //     harrogate` despite genuine physical track-sharing, mirroring
        //     `hull-trains.toml`'s identical precedent).
        //   - northern-leeds-selby: at the time this comment was written,
        //     its own `northern-leeds-selby` segment at LDS, used nowhere
        //     else -- exclusive. Superseded below (Yorkshire real-world-
        //     sanity review): LDS now carries the narrower
        //     `northern-leeds-micklefield` segment instead, shared with the
        //     new `northern-leeds-york.toml` -- see that file's own
        //     "Segment split" comment.
        //   - northern-settle-carlisle: reuses `northern-shipley-trunk`
        //     verbatim at LDS (the same Leeds-Shipley approach already
        //     shared between Airedale and Wharfedale) -- joins
        //     northern-airedale as SharedSegment.
        //
        // Updated again by the Yorkshire real-world-sanity review
        // (2026-09-21), which split `northern.toml`'s own incoherent
        // catch-all fragments into two new, genuine sibling lines (both
        // also touch LDS) and added a Leeds extension to the renamed
        // `northern-wakefield-line.toml` (formerly `northern-dearne-
        // valley.toml`, which never touched LDS before):
        //   - northern-huddersfield: its own `northern-huddersfield`
        //     segment at LDS, used nowhere else -- exclusive.
        //   - northern-leeds-york: shares `northern-leeds-micklefield`
        //     with `northern-leeds-selby` at LDS (see that file's own
        //     "Segment split" comment) -- SharedSegment, and flips
        //     `northern-leeds-selby` from ExclusiveSegment to SharedSegment
        //     too, since that segment name is no longer used by only one
        //     file.
        //   - northern-wakefield-line: its own `northern-wakefield-sheffield`
        //     segment at LDS, used nowhere else -- exclusive. Superseded
        //     below (review2 fix, item 10): LDS now carries the narrower
        //     `northern-outwood-wakefield-trunk` segment instead, genuinely
        //     shared with `northern-pontefract-line.toml`'s own Wakefield
        //     branch (at WKF/OUT, not at LDS itself -- see both files' own
        //     top-of-file comments) -- flips to SharedSegment.
        //
        // Updated again by the Northern NW/TPE sanity review (tpe-north.toml
        // split into four new siblings): tpe-north-teesside, tpe-north-
        // scarborough and tpe-north-hull each call at LDS on their own
        // exclusive segment (`tpe-north-teesside`/`tpe-north-scarborough`/
        // `tpe-north-hull` respectively, each used nowhere else) -- three
        // more independent ExclusiveSegment matches.
        //
        // Updated by the national/WCML/XC sanity review: cross-country.toml
        // also calls at LDS (a real gap-fill, its own new Leeds branch), on
        // its own exclusive `xc-yorkshire-leeds` segment -- another
        // independent ExclusiveSegment match.
        //
        // Updated by the Yorkshire real-world-sanity review's own second
        // pass (northern-hallam-line.toml, northern-pontefract-line.toml,
        // both new): both reuse `northern-leeds-castleford` verbatim for
        // their shared Leeds-Castleford approach -- a genuine SharedSegment
        // pair with each other, independent of every other family here.
        //
        // Updated by review2 item 10 (northern-pontefract-line.toml's
        // Wakefield branch extended to its own real Leeds terminus via
        // Outwood): `northern-wakefield-line` moves from ExclusiveSegment to
        // SharedSegment -- see the bullet above.
        assert_eq!(
            matched_ids,
            HashSet::from([
                "lner-leeds".to_string(),
                "northern".to_string(),
                "northern-yorkshire-coast".to_string(),
                "northern-airedale".to_string(),
                "northern-wharfedale".to_string(),
                "northern-calder-valley".to_string(),
                "tpe-north".to_string(),
                "northern-harrogate-line".to_string(),
                "northern-leeds-selby".to_string(),
                "northern-settle-carlisle".to_string(),
                "northern-huddersfield".to_string(),
                "northern-leeds-york".to_string(),
                "northern-wakefield-line".to_string(),
                "tpe-north-teesside".to_string(),
                "tpe-north-scarborough".to_string(),
                "tpe-north-hull".to_string(),
                "cross-country".to_string(),
                "northern-hallam-line".to_string(),
                "northern-pontefract-line".to_string(),
            ])
        );
        for m in &matches {
            let expected = match m.line.id.as_str() {
                "lner-leeds"
                | "northern-wharfedale"
                | "northern-calder-valley"
                | "tpe-north"
                | "northern-harrogate-line"
                | "northern-huddersfield"
                | "tpe-north-teesside"
                | "tpe-north-scarborough"
                | "tpe-north-hull"
                | "cross-country" => MatchScope::ExclusiveSegment,
                "northern"
                | "northern-yorkshire-coast"
                | "northern-airedale"
                | "northern-settle-carlisle"
                | "northern-leeds-selby"
                | "northern-leeds-york"
                | "northern-hallam-line"
                | "northern-pontefract-line"
                | "northern-wakefield-line" => MatchScope::SharedSegment,
                other => panic!("unexpected line in Leeds overlap test: {other}"),
            };
            assert_eq!(m.scope, expected, "{} scope mismatch", m.line.id);
        }
    }

    #[test]
    fn grand_central_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Sunderland sits on `gc-sunderland`, exclusive to `grand-central` —
        // at the time this test was written no other line in this catalogue
        // reached Sunderland. Per the task brief, no shared-trunk test
        // against `lner-ecml.toml` (or any other LNER file) is required for
        // Grand Central: the plan is explicit that Grand Central's
        // relationship to LNER is station-overlap-only (shared at King's
        // Cross/Peterborough/Doncaster/York, none of which this test
        // touches), not a forced shared segment.
        //
        // Updated by the Yorkshire/North East batch: `northern-durham-
        // coast.toml` also calls at Sunderland (its own `northern-durham-
        // coast` segment, a different name) - a genuine station overlap,
        // not a shared trunk, so both now match independently as
        // ExclusiveSegment.
        let inc = incident(
            "GC-1",
            "Signal failure at Sunderland",
            "Signal failure causing delays to services at Sunderland.",
            &["GC"],
            &["SUN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "grand-central".to_string(),
                "northern-durham-coast".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    #[test]
    fn grand_central_kings_cross_trunk_shared_trunk_propagates_to_bradford() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Final review, Fix #1: `grand-central.toml` used to model BOTH the
        // King's Cross-Sunderland and King's Cross-Bradford Interchange
        // routes as one non-linear file, with a `gc-trunk-kings-cross`
        // segment that could never be a real shared trunk (only one *line*
        // used the name). Split into `grand-central.toml` (Sunderland) and
        // `grand-central-bradford.toml` (Bradford Interchange), which now
        // genuinely share `gc-trunk-kings-cross` across King's Cross,
        // Peterborough and Doncaster — mirroring
        // `swr_shared_trunk_incident_propagates` above. An incident at
        // Doncaster should now match both files, each SharedSegment.
        let inc = incident(
            "GC-2",
            "Points failure at Doncaster",
            "Points failure causing disruption to services through Doncaster.",
            &["GC"],
            &["DON"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(matched_ids.contains("grand-central"));
        assert!(matched_ids.contains("grand-central-bradford"));
        for m in &matches {
            if m.line.id.starts_with("grand-central") {
                assert_eq!(
                    m.scope,
                    MatchScope::SharedSegment,
                    "{} should be SharedSegment",
                    m.line.id
                );
            }
        }
    }

    #[test]
    fn grand_central_bradford_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Halifax sits on `gc-bradford`, exclusive to `grand-central-bradford`
        // — this was written when Batch 8's `lines/northern-calder-valley.toml`
        // (also a real Halifax stop, on its own distinctly-named
        // `northern-calder-valley` segment) didn't exist yet in this worktree.
        // Real station-level overlap, no shared segment name between the two
        // files, so both independently classify as ExclusiveSegment — this is
        // the correct, unchanged matcher behaviour; only the expected match
        // set needed updating once both batches landed on `main` together.
        let inc = incident(
            "GC-3",
            "Signal failure at Halifax",
            "Signal failure causing delays to services at Halifax.",
            &["GC"],
            &["HFX"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "grand-central-bradford".to_string(),
                "northern-calder-valley".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    #[test]
    fn grand_central_birmingham_shopping_centre_mention_vetoes_match() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Final review, Fix #3: `excluded_keywords` was narrowed from a bare
        // "Birmingham" to "Grand Central, Birmingham" (the shopping
        // centre's own name+city, per Wikipedia's "Grand Central,
        // Birmingham" article), so it should still veto an incident that
        // genuinely mentions the shopping centre. Mirrors
        // `excluded_keyword_vetoes_match`'s style above.
        let inc = incident(
            "GC-4",
            "Fire alarm at Grand Central, Birmingham",
            "A fire alarm was activated at the Grand Central, Birmingham shopping centre, next to New Street station.",
            &[],
            &[],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(
            !matched_ids.contains("grand-central"),
            "shopping-centre mention should still veto grand-central"
        );
        assert!(
            !matched_ids.contains("grand-central-bradford"),
            "shopping-centre mention should still veto grand-central-bradford"
        );
    }

    #[test]
    fn grand_central_unrelated_birmingham_mention_does_not_veto_match() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Final review, Fix #3: the old bare "Birmingham" exclusion would
        // have wrongly vetoed a genuine Grand Central incident that happens
        // to mention Birmingham for an unrelated reason (e.g. a diversion
        // routed via Birmingham). The narrowed "Grand Central, Birmingham"
        // phrase should NOT fire here, so the incident matches via the
        // `match_keywords` "Grand Central" phrase instead.
        let inc = incident(
            "GC-5",
            "Grand Central service diverted",
            "A Grand Central service was diverted via Birmingham due to engineering works.",
            &[],
            &[],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(
            matched_ids.contains("grand-central"),
            "unrelated Birmingham mention should not veto grand-central"
        );
        assert!(
            matched_ids.contains("grand-central-bradford"),
            "unrelated Birmingham mention should not veto grand-central-bradford"
        );
        for m in &matches {
            if m.line.id.starts_with("grand-central") {
                assert_eq!(
                    m.scope,
                    MatchScope::KeywordOnly,
                    "{} should match via keyword only",
                    m.line.id
                );
            }
        }
    }

    #[test]
    fn hull_trains_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Howden sits on `ht-kings-cross-hull`, exclusive to `hull-trains`.
        // A later national-mainlines audit added Howden to `lner-hull.toml`
        // too (LNER's own daily King's Cross-Hull working genuinely calls
        // there, at the same position already modelled here) — real
        // station-level overlap between two different operators' files,
        // same pattern this test's sibling
        // `lner_hull_exclusive_segment_incident_does_not_propagate` already
        // documents for Selby: both lines match, but each stays
        // `ExclusiveSegment` since neither's own segment name
        // (`ht-kings-cross-hull` vs `lner-hull`) is literally shared with
        // the other.
        //
        // Updated by the Yorkshire/North East batch: `northern-leeds-
        // selby.toml` also calls at Howden (its own `northern-leeds-selby`
        // segment, a third different name) - the same station-overlap-only
        // pattern, so it joins this set as a third independent
        // ExclusiveSegment match.
        let inc = incident(
            "HT-1",
            "Signal failure at Howden",
            "Signal failure causing delays to services at Howden.",
            &["HT"],
            &["HOW"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "hull-trains".to_string(),
                "lner-hull".to_string(),
                "northern-leeds-selby".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    #[test]
    fn lumo_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Falkirk High sits on `lumo-glasgow`, exclusive to `lumo` — no
        // other line in this catalogue has a station at Falkirk High, so
        // this should stay exclusive and not propagate anywhere else. Per
        // the task brief (the same standalone-operator exception
        // `grand-central.toml` and `hull-trains.toml` already established
        // for their own relationship to LNER), no shared-trunk test against
        // any `lner-*.toml` file is required for Lumo: `lumo.toml`'s
        // station-level overlap with `lner-ecml.toml` (King's Cross,
        // Stevenage, Newcastle, Morpeth, Edinburgh Waverley, Haymarket) is
        // deliberate and documented, not a forced shared segment. None of
        // those shared stations are used here, so no pre-existing test
        // needed updating for this task (unlike Task 6.6's Selby situation
        // with `lner-hull.toml`) — checked: no other test in this file
        // references King's Cross, Stevenage, Newcastle, Morpeth, Edinburgh
        // Waverley or Haymarket.
        //
        // Falkirk High is also scotrail-central-belt.toml's own station
        // (merged separately, Batch 10), on its exclusive
        // `scotrail-central-belt` segment -- station-level overlap,
        // distinct segment names, both stay ExclusiveSegment.
        let inc = incident(
            "LD-1",
            "Signal failure at Falkirk High",
            "Signal failure causing delays to services at Falkirk High.",
            &["LD"],
            &["FKK"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["lumo".to_string(), "scotrail-central-belt".to_string()])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    // No shared-segment-propagation test for tpe-anglo-scottish: per the
    // batch's pre-flight scan it only overlaps sibling TPE lines at
    // station level (Liverpool Lime Street / Manchester Piccadilly, and
    // Edinburgh Waverley with tpe-borders), and has no shared segment with
    // wcml/xc-manchester/northern by design (station-overlap-only, same
    // precedent as xc-manchester.toml). It's a genuinely standalone line
    // for this assertion.
    #[test]
    fn tpe_anglo_scottish_exclusive_segment_incident_does_not_propagate() {
        // Motherwell is also scotrail-argyle.toml's own junction (its own
        // `scotrail-argyle-east` segment, merged separately, Batch 10;
        // scotrail-argyle.toml is the Argyle Line split successor of the
        // former scotrail-glasgow-suburban.toml) -- station-level overlap,
        // distinct segment names, both stay ExclusiveSegment.
        //
        // Updated by the national/WCML/XC sanity review: the new
        // wcml-scotland.toml also calls at Motherwell, on its own exclusive
        // `wcml-scotland-glasgow` segment -- a third independent
        // ExclusiveSegment match.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TPE-1",
            "Points failure at Motherwell",
            "Points failure causing delays to TransPennine Express services at Motherwell.",
            &["TP"],
            &["MTH"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "tpe-anglo-scottish".to_string(),
                "scotrail-argyle".to_string(),
                "wcml-scotland".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    // Task 8.1 (Batch 8): fills genuinely missing intermediate stations
    // that TPE's own May-2026 Anglo-Scottish timetable PDF confirms this
    // route calls at but which the file's prior "principal stations only"
    // listing omitted - St Helens Central (SNH) and Wigan North Western
    // (WGN) on the Liverpool leg, Manchester Oxford Road (MCO) and Bolton
    // (BON) on the Manchester leg, and Carstairs (CRS, literally that
    // three-letter code) - the route's actual Glasgow/Edinburgh split
    // point, previously misattributed to Lockerbie. See the updated
    // comments in lines/tpe-anglo-scottish.toml for full sourcing.
    #[test]
    fn tpe_anglo_scottish_batch8_infill_stations_present() {
        let lines = load_line("tpe-anglo-scottish");
        let line = lines
            .get("tpe-anglo-scottish")
            .expect("tpe-anglo-scottish line should exist");
        for crs in ["SNH", "WGN", "MCO", "BON", "CRS"] {
            assert!(
                line.has_station(crs),
                "tpe-anglo-scottish should now list {crs}"
            );
        }
    }

    // Same task: St Helens Central sits on the newly-filled
    // `tpe-anglo-scottish-nw` segment, which (per this batch's own
    // pre-flight scan, confirmed unchanged by this task's research) is not
    // shared with any sibling line - an incident here should match only
    // tpe-anglo-scottish, as ExclusiveSegment, same shape as
    // emr_poacher_bottesford_incident_stays_on_its_own_line
    // above. St Helens Central was chosen over Wigan North Western /
    // Manchester Oxford Road / Bolton because those three also appear
    // (station-level only, via wcml / emr-regional / northern-clitheroe /
    // northern-blackpool) in other line files, which would otherwise
    // widen this assertion's matched-line set without adding anything
    // tpe_anglo_scottish_exclusive_segment_incident_does_not_propagate
    // above doesn't already cover.
    //
    // Updated by the Northern NW/TPE sanity review: the new northern-
    // liverpool-st-helens-wigan.toml also calls at St Helens Central, on its
    // own exclusive segment of the same name -- station overlap only, an
    // independent ExclusiveSegment match.
    #[test]
    fn tpe_anglo_scottish_st_helens_central_incident_stays_exclusive() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TPE-3",
            "Signal failure at St Helens Central",
            "Signal failure causing delays to TransPennine Express services at St Helens Central.",
            &["TP"],
            &["SNH"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "tpe-anglo-scottish".to_string(),
                "northern-liverpool-st-helens-wigan".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    // No shared-segment-propagation test for tpe-south either: per this
    // task's own pre-flight scan it only overlaps sibling TPE lines at
    // station level (Liverpool Lime Street / Manchester Piccadilly with
    // tpe-anglo-scottish and tpe-north; no overlap at all with
    // tpe-borders), and has no shared segment with xc-manchester/northern
    // by design (station-overlap-only, same precedent as
    // xc-manchester.toml and tpe-anglo-scottish.toml). Genuinely standalone
    // for this assertion.
    //
    // Updated by the Midlands EMR/WMR/LNWR sanity review: `emr-barton-
    // line.toml` and the re-scoped `emr-derwent-valley.toml` (formerly
    // Derby-Matlock only, now Lincoln/Cleethorpes-Matlock) both also call
    // at Grimsby Town, each via its own exclusive segment name (genuine
    // physical overlap, materially different calling pattern - see either
    // file's own ruling comment). So this incident now also matches both,
    // independently ExclusiveSegment.
    #[test]
    fn tpe_south_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TPE-2",
            "Signal failure at Grimsby Town",
            "Signal failure causing delays to TransPennine Express services at Grimsby Town.",
            &["TP"],
            &["GMB"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "tpe-south".to_string(),
                "emr-barton-line".to_string(),
                "emr-derwent-valley".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    // Task 8.4 (Batch 8): fills the 9 genuinely missing, currently-open,
    // currently-served intermediate calls on this route's Liverpool leg
    // (Liverpool South Parkway, Warrington West, Warrington Central,
    // Birchwood, Irlam, Urmston) and Manchester Piccadilly-Cleethorpes leg
    // (Manchester Oxford Road, Meadowhall, Barnetby, Habrough), confirmed
    // via Wikipedia's TransPennine Express route table. See the updated
    // comments in lines/tpe-south.toml for full sourcing. All nine sit on
    // the file's own single `tpe-south` segment, which (per
    // tpe_south_exclusive_segment_incident_does_not_propagate above) has no
    // shared-segment overlap with any sibling line, so no second,
    // MatchScope-asserting test is added for this task.
    #[test]
    fn tpe_south_batch8_infill_stations_present() {
        let lines = load_line("tpe-south");
        let line = lines.get("tpe-south").expect("tpe-south line should exist");
        for crs in [
            "LPY", "WAW", "WAC", "BWD", "IRL", "URM", "MCO", "MHS", "BTB", "HAB",
        ] {
            assert!(line.has_station(crs), "tpe-south should now list {crs}");
        }
    }

    // No shared-SEGMENT-propagation test for tpe-borders: per this task's
    // own pre-flight scan its own segment name `tpe-borders` has no real
    // overlap with anything else in the catalogue, including this batch's
    // own tpe-north — the Newcastle boundary between them is ruled a
    // terminus-to-terminus handoff, not a shared trunk (mirrors how
    // emr-regional.toml and northern-hope-valley.toml treat their own
    // Stockport overlap -- station-level only, distinct segment names on
    // each side; see
    // `emr_regional_stockport_and_hope_valley_both_match_without_over_propagating`
    // above. This replaces a prior analogy to xc-manchester.toml's own
    // Crewe entry, which no longer exists after that file's 2026-09-21
    // route correction). What this task's own pre-flight scan didn't (and
    // couldn't) anticipate: `lner-ecml.toml` (merged separately, in an
    // earlier batch, and absent from this batch's own isolated worktree)
    // also stops at Berwick-upon-Tweed, via its own distinct `ecml-borders`
    // segment (confirmed exclusive to `lner-ecml.toml` via exact-match
    // grep). Station-level overlap, different segment names — same pattern
    // as the Halifax/Grand-Central-Bradford precedent: both lines match by
    // station, both stay ExclusiveSegment on their own segment names.
    //
    // Updated by the national/WCML/XC sanity review: cross-country.toml's
    // own new Edinburgh extension also calls at Berwick-upon-Tweed, on its
    // own exclusive `xc-borders` segment -- a third independent
    // ExclusiveSegment match.
    #[test]
    fn tpe_borders_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TPE-3",
            "Signal failure at Berwick-upon-Tweed",
            "Signal failure causing delays to TransPennine Express services at Berwick-upon-Tweed.",
            &["TP"],
            &["BWK"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "tpe-borders".to_string(),
                "lner-ecml".to_string(),
                "cross-country".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    // Task 8.2 (Batch 8): fills two genuinely missing, currently-served,
    // very-low-frequency intermediate stations Cramlington (CRM, one train
    // per day, between Newcastle and Morpeth) and East Linton (ELT, reopened
    // 13 December 2023, between Dunbar and Edinburgh Waverley) - both
    // confirmed via Wikipedia and nationalrail.co.uk. See the updated
    // comments in lines/tpe-borders.toml for full sourcing. Both stations
    // sit on the file's own single `tpe-borders` segment, which (per
    // tpe_borders_exclusive_segment_incident_does_not_propagate above) has
    // no shared-segment overlap with any sibling line, so no second,
    // MatchScope-asserting test is added for this task.
    #[test]
    fn tpe_borders_batch8_infill_stations_present() {
        let lines = load_line("tpe-borders");
        let line = lines
            .get("tpe-borders")
            .expect("tpe-borders line should exist");
        for crs in ["CRM", "ELT"] {
            assert!(line.has_station(crs), "tpe-borders should now list {crs}");
        }
    }

    // No shared-segment-propagation test for tpe-north: no segment name it
    // uses is shared with any other line, including this batch's own
    // tpe-borders (the Newcastle boundary between them is a
    // terminus-to-terminus handoff, not a shared trunk — see tpe-north's
    // own file comments, consistent with tpe-borders's). Genuinely
    // standalone for that assertion, despite unusually heavy *station*-level
    // overlap with northern/cross-country/northern-tyne-valley (station
    // hits alone still produce a `Match` per overlapping line — see below).
    //
    // Station choice for the exclusive-segment test below: almost every
    // principal station on tpe-north's route also appears in another line
    // file (LIV/NLW/MCV/HUD/LDS/YRK in `northern.toml`, DAR/DHM/YRK also in
    // `cross-country.toml`, NCL in `cross-country.toml`/
    // `northern-tyne-valley.toml`/`tpe-borders.toml`), and `match_one`
    // matches per-line on raw station hits before segment classification —
    // so an incident at any of those stations would also match those other
    // lines (each independently as their own ExclusiveSegment, since no
    // segment *name* collides), failing a "matches only tpe-north"
    // assertion. Verified by grepping `lines/*.toml`: Chester-le-Street
    // (CLS) is the one tpe-north station that appears in no other line
    // file, so it's used here instead of the Darlington/Newcastle choice
    // this task's brief originally suggested.
    #[test]
    fn tpe_north_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TPE-4",
            "Signal failure at Chester-le-Street",
            "Signal failure causing delays to TransPennine Express services at Chester-le-Street.",
            &["TP"],
            &["CLS"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["tpe-north".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn chiltern_stratford_branch_incident_does_not_propagate() {
        // Strict-equality on the whole match set would break once a future
        // WMR Snow Hill lines entry (station-overlap-only, per
        // chiltern-main-line.toml's own comments) plausibly also lists
        // Wilmcote on the North Warwickshire line to Stratford - so, like
        // `chiltern_banbury_incident_matches_by_station_not_shared_segment`,
        // this only asserts chiltern-main-line is among the matches and is
        // classified ExclusiveSegment, not that nothing else could match.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "CH-1",
            "Trespass incident at Wilmcote",
            "Trespass incident causing delays to Chiltern Railways services.",
            &["CH"],
            &["WMC"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(matched_ids.contains("chiltern-main-line"));
        for m in &matches {
            if m.line.id == "chiltern-main-line" {
                assert_eq!(m.scope, MatchScope::ExclusiveSegment);
            }
        }
    }

    #[test]
    fn chiltern_birmingham_approach_incident_does_not_propagate() {
        // Strict-equality on the whole match set would break once a future
        // WMR Snow Hill lines entry (station-overlap-only, per
        // chiltern-main-line.toml's own comments) plausibly also lists
        // Solihull on the Dorridge approach - so, like
        // `chiltern_banbury_incident_matches_by_station_not_shared_segment`,
        // this only asserts chiltern-main-line is among the matches and is
        // classified ExclusiveSegment, not that nothing else could match.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "CH-2",
            "Signal failure at Solihull",
            "Signal failure causing delays to Chiltern Railways services.",
            &["CH"],
            &["SOL"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(matched_ids.contains("chiltern-main-line"));
        for m in &matches {
            if m.line.id == "chiltern-main-line" {
                assert_eq!(m.scope, MatchScope::ExclusiveSegment);
            }
        }

        // Station-level overlap with Birmingham only, not a shared segment:
        // an incident at Birmingham New Street (XC's hub) must not match
        // this line, and this line's Birmingham approach (Snow Hill/Moor
        // Street) is a different station entirely.
        let bhm_inc = incident(
            "XC-BHM",
            "Points failure at Birmingham New Street",
            "Points failure causing delays.",
            &["XC"],
            &["BHM"],
        );
        let bhm_matches = lines_affected_by(&bhm_inc, &lines, &registry);
        let bhm_matched_ids: HashSet<String> =
            bhm_matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(!bhm_matched_ids.contains("chiltern-main-line"));
    }

    #[test]
    fn chiltern_banbury_incident_matches_by_station_not_shared_segment() {
        // Banbury sits on both chiltern-main-line and xc-south-coast's
        // physical trunk, but the two files deliberately don't share a
        // segment name there (see chiltern-main-line.toml's own comment).
        // An incident should therefore match both lines individually by
        // station, each classified against its own (exclusive) segment.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "CH-3",
            "Overhead line damage at Banbury",
            "Overhead line damage causing delays.",
            &[],
            &["BAN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(matched_ids.contains("chiltern-main-line"));
        assert!(matched_ids.contains("xc-south-coast"));
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment (station overlap, not segment-sharing)",
                m.line.id
            );
        }
    }

    #[test]
    fn chiltern_marylebone_shared_trunk_incident_propagates_to_both_files() {
        // chiltern-aylesbury.toml reuses chiltern-main-line.toml's
        // "chiltern-marylebone" segment tag for Marylebone itself (Task
        // 12.1's comment invited this): both files' services genuinely
        // originate there before diverging at Neasden Junction.
        //
        // Updated (real-world sanity review): chiltern-oxford.toml (split
        // out of the formerly bundled chiltern-aylesbury.toml) also reuses
        // this same "chiltern-marylebone" tag for its own Marylebone entry
        // - a third file now genuinely sharing this trunk.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "CH-4",
            "Points failure at Marylebone",
            "Points failure causing delays to Chiltern Railways services.",
            &["CH"],
            &["MYB"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(matched_ids.contains("chiltern-main-line"));
        assert!(matched_ids.contains("chiltern-aylesbury"));
        assert!(matched_ids.contains("chiltern-oxford"));
        for m in &matches {
            if m.line.id.starts_with("chiltern-") {
                assert_eq!(
                    m.scope,
                    MatchScope::SharedSegment,
                    "{} should be SharedSegment",
                    m.line.id
                );
            }
        }
    }

    #[test]
    fn chiltern_aylesbury_branch_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "CH-5",
            "Signal failure at Amersham",
            "Signal failure causing delays to Chiltern Railways services.",
            &["CH"],
            &["AMR"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["chiltern-aylesbury".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Updated (real-world sanity review): the Oxford branch now lives in
    // its own file, chiltern-oxford.toml, split out of the formerly
    // bundled chiltern-aylesbury.toml.
    #[test]
    fn chiltern_oxford_branch_incident_does_not_propagate() {
        // The Oxford branch (chiltern-oxford.toml) is a physically distinct
        // corridor from both chiltern-aylesbury.toml's own Amersham/Princes
        // Risborough branches and chiltern-main-line.toml's Birmingham
        // route (it only shares Marylebone itself, per the file's own
        // comments) - an incident here should stay exclusive to
        // chiltern-oxford and not leak onto chiltern-main-line,
        // chiltern-aylesbury or xc-south-coast (which also calls at
        // Oxford, station-overlap only).
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "CH-6",
            "Trespass incident at Bicester Village",
            "Trespass incident causing delays to Chiltern Railways services.",
            &["CH"],
            &["BIT"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["chiltern-oxford".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn c2c_exclusive_segment_incident_does_not_propagate() {
        // c2c is a standalone line with no shared segment anywhere in the
        // catalogue (per the 2026-08-29 line-coverage gap analysis and
        // lines/c2c.toml's own comment) - only the exclusive-segment
        // assertion is required here, no shared-trunk test.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "CC-1",
            "Signal failure at Basildon",
            "Signal failure causing delays to c2c services.",
            &["CC"],
            &["BSO"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["c2c".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn c2c_rainham_branch_stations_are_in_the_catalogue() {
        // Regression guard for the Rainham-branch research pass: lines/c2c.toml
        // used to scope the Barking - Dagenham Dock - Rainham - Purfleet -
        // Grays route out entirely ("omitted rather than guessed at"), so
        // incidents on c2c's third route matched nothing by station. All three
        // new stations are c2c-managed and currently passenger-served (see that
        // file's `c2c-rainham-branch` comment for the two-source citations).
        let lines = load_line("c2c");
        let c2c = lines.get("c2c").expect("c2c line should exist");
        for crs in ["DDK", "RNM", "PFL"] {
            assert!(
                c2c.has_station(crs),
                "c2c should include {crs} on the Rainham branch"
            );
        }
        // Rainham (Essex) is RNM; RAI is Rainham (Kent) on Southeastern's
        // Chatham main line and must never appear here.
        assert!(
            !c2c.has_station("RAI"),
            "RAI is Rainham (Kent), not c2c's Rainham (Essex)"
        );
    }

    #[test]
    fn c2c_rainham_branch_incident_does_not_propagate() {
        // Same standalone-line exception class as
        // `c2c_exclusive_segment_incident_does_not_propagate` above: `DDK`,
        // `RNM` and `PFL` each appear only in lines/c2c.toml (re-checked
        // catalogue-wide when they were added), so an incident on the new
        // `c2c-rainham-branch` segment stays exclusive to c2c.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "CC-2",
            "Points failure at Rainham",
            "Points failure causing delays to c2c services between Barking and Grays.",
            &["CC"],
            &["RNM"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["c2c".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn merseyrail_northern_kirkby_branch_incident_does_not_propagate() {
        // The Kirkby/Headbolt Lane branch (merseyrail-northern-kirkby) is
        // exclusive to merseyrail-northern.toml - it doesn't touch the
        // Southport or Ormskirk branches, nor (per that file's own
        // central-Liverpool research comment, honored by
        // merseyrail-wirral.toml) the Wirral Line. See
        // `merseyrail_central_liverpool_incident_matches_by_station_not_shared_segment`
        // below for the complementary "station overlap, not shared segment"
        // test at Liverpool Central.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "ME-1",
            "Signal failure at Fazakerley",
            "Signal failure causing delays to Merseyrail Northern Line services.",
            &["ME"],
            &["FAZ"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["merseyrail-northern".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn merseyrail_wirral_new_brighton_branch_incident_does_not_propagate() {
        // The New Brighton branch (merseyrail-wirral-new-brighton) is
        // exclusive to merseyrail-wirral.toml - it doesn't touch the West
        // Kirby, Chester or Ellesmere Port branches, nor does it touch
        // merseyrail-northern.toml at all (that file has no stations on the
        // Wirral peninsula).
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "ME-2",
            "Signal failure at Wallasey Grove Road",
            "Signal failure causing delays to Merseyrail Wirral Line services.",
            &["ME"],
            &["WLG"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["merseyrail-wirral".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn merseyrail_central_liverpool_incident_matches_by_station_not_shared_segment() {
        // Liverpool Central sits on both merseyrail-northern.toml and
        // merseyrail-wirral.toml, but the two files deliberately don't
        // share a segment name there (see merseyrail-northern.toml's own
        // central-Liverpool research comment, honored unchanged by
        // merseyrail-wirral.toml): the Northern Line's Link tunnel and the
        // Wirral Line's Loop tunnel are physically distinct, meeting only
        // at the station buildings. An incident there should therefore
        // match both lines individually by station, each classified
        // against its own (exclusive) segment - mirrors
        // `chiltern_banbury_incident_matches_by_station_not_shared_segment`.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "ME-3",
            "Points failure at Liverpool Central",
            "Points failure causing delays to Merseyrail services.",
            &[],
            &["LVC"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(matched_ids.contains("merseyrail-northern"));
        assert!(matched_ids.contains("merseyrail-wirral"));
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment (station overlap, not segment-sharing)",
                m.line.id
            );
        }
    }
    #[test]
    fn west_anglia_exclusive_segment_incident_does_not_propagate() {
        // Newport is on `waml-mainline` (Elsenham-Cambridge), well beyond
        // Stansted Mountfitchet where greater-anglia-stansted-express.toml
        // (Task 2.3) diverges onto its own airport branch, and beyond
        // Cambridge is the only other overlap this line has with any other
        // committed line (see the next test). No other `lines/*.toml` file
        // touches this station, so this should stay scoped to
        // greater-anglia-west-anglia only.
        //
        // NOTE: this test used to use Bishop's Stortford (BIS), but that
        // station is on `waml-trunk-london`, which greater-anglia-
        // stansted-express.toml now genuinely shares (see that file's
        // segment-decision comment and the
        // stansted_express_shared_trunk_incident_propagates test below) — an
        // incident there now correctly escalates to SharedSegment and
        // matches both lines, so it's no longer a valid "stays exclusive"
        // example.
        //
        // UPDATED (Task 9.7, 2026-09-01): `xc-stansted.toml`'s own fresh
        // route-diagram pass reused NWE verbatim too (its own
        // Cambridge-Stansted Mountfitchet stretch runs over this same
        // physical West Anglia Main Line trunk, per that file's own
        // comment) on its own exclusive `xc-stansted` segment — a second
        // genuine station overlap, so "no other file touches this station"
        // no longer holds. Updated rather than left stale, mirroring Task
        // 9.3's own precedent.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-3",
            "Signal failure at Newport",
            "Signal failure causing delays to Greater Anglia services.",
            &["LE"],
            &["NWE"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "greater-anglia-west-anglia".to_string(),
                "xc-stansted".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should stay ExclusiveSegment",
                m.line.id
            );
        }
    }

    #[test]
    fn stansted_express_shared_trunk_incident_propagates() {
        // Tottenham Hale is on `waml-trunk-london`, genuinely shared between
        // greater-anglia-west-anglia.toml (Task 2.2) and
        // greater-anglia-stansted-express.toml (Task 2.3) per the latter's
        // segment-decision comment (both routes run over the same physical
        // West Anglia Main Line tracks from Liverpool Street through
        // Stansted Mountfitchet). An incident here should propagate to both
        // lines as SharedSegment.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-5",
            "Signal failure at Tottenham Hale",
            "Signal failure causing delays to Greater Anglia services.",
            &["LE"],
            &["TOM"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "greater-anglia-west-anglia".to_string(),
                "greater-anglia-stansted-express".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment",
                m.line.id
            );
        }
    }

    #[test]
    fn stansted_express_airport_is_station_overlap_only_with_xc_stansted() {
        // Stansted Airport (SSD) is the terminus of
        // greater-anglia-stansted-express.toml's own exclusive
        // `stansted-express-branch` segment, but is also the terminus of
        // xc-stansted.toml's whole-route `xc-stansted` segment (CrossCountry's
        // Birmingham-Stansted service, approaching via a different leg of the
        // triangular junction north of Stansted Mountfitchet — see the
        // segment-decision comment in greater-anglia-stansted-express.toml).
        // The two files deliberately do NOT share a segment name here, so an
        // incident should match both lines independently, each still
        // classified as ExclusiveSegment rather than escalating to
        // SharedSegment.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-6",
            "Points failure at Stansted Airport",
            "Points failure causing delays.",
            &["LE"],
            &["SSD"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "greater-anglia-stansted-express".to_string(),
                "xc-stansted".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment, not shared",
                m.line.id
            );
        }
    }

    #[test]
    fn west_anglia_cambridge_is_station_overlap_only_with_xc_stansted() {
        // Cambridge (CBG) is on greater-anglia-west-anglia.toml's
        // `waml-mainline` segment, xc-stansted.toml's `xc-stansted` segment
        // (CrossCountry's Birmingham-Stansted service also calls there) and,
        // since Task 2.6, greater-anglia-breckland-line.toml's own
        // `breckland-line` segment (originally part of the bundled
        // greater-anglia-norfolk-branches.toml, since split by brand --
        // real-world sanity review; the Breckland Line's Cambridge
        // terminus, reached via an entirely different physical corridor —
        // Ely and Cambridge North, not Elsenham/Audley End — that only
        // converges with the other two at this station). None of the three
        // files share a segment name for this station (see
        // greater-anglia-west-anglia.toml's and
        // greater-anglia-breckland-line.toml's decision comments —
        // reusing another file's segment name here would incorrectly mark
        // its whole trunk as shared with this line), so an incident here
        // should match all three lines independently, each still classified
        // as ExclusiveSegment rather than escalating to SharedSegment.
        //
        // Cambridge is also great-northern-kings-lynn.toml's (its own
        // `gn-cambridge-kings-lynn-branch` segment) and
        // thameslink-cambridge.toml's (its own `thameslink-cambridge-branch`
        // segment) terminus, both merged separately (Batch 5) -- two more
        // independent ExclusiveSegment matches by the same pattern.
        //
        // Updated by the Wales/East Anglia batch: `greater-anglia-ipswich-
        // cambridge.toml` also terminates at Cambridge (its own
        // `greater-anglia-ipswich-cambridge` segment) -- a sixth
        // independent ExclusiveSegment match.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-4",
            "Points failure at Cambridge",
            "Points failure causing delays.",
            &["LE"],
            &["CBG"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "greater-anglia-west-anglia".to_string(),
                "xc-stansted".to_string(),
                "greater-anglia-breckland-line".to_string(),
                "great-northern-kings-lynn".to_string(),
                "thameslink-cambridge".to_string(),
                "greater-anglia-ipswich-cambridge".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment, not shared",
                m.line.id
            );
        }
    }

    // Task 9.7 (2026-09-01) fresh route-diagram pass on `xc-stansted.toml`
    // added 20 real, currently-open, currently-served intermediate stations
    // across the whole Birmingham-Stansted Airport corridor's five named
    // CRS-pending stations plus several more this task's own research
    // found. All inherit the existing exclusive `xc-stansted` segment.
    // Regression guard that `has_station` now recognises a representative
    // spread across every leg researched (see
    // `west_anglia_exclusive_segment_incident_does_not_propagate` above for
    // the updated Newport overlap case, and the new Audley End overlap test
    // below for another West Anglia Main Line station-overlap case).
    #[test]
    fn xc_stansted_recognises_newly_added_stations() {
        let lines = load_line("xc-stansted");
        let line = lines.get("xc-stansted").expect("xc-stansted should load");
        for crs in [
            "WTO", "CEH", "HNK", "NBR", "SWS", "MMO", "OKM", "SMD", "WLE", "MCH", "MNE", "CMS",
            "SED", "WLF", "GRC", "AUD", "NWE", "ESM", "SST",
        ] {
            assert!(
                line.has_station(crs),
                "{crs} should now be recognised on xc-stansted"
            );
        }
    }

    // Audley End (AUD) is a second genuine West Anglia Main Line station
    // overlap this task's own research found with
    // `greater-anglia-west-anglia.toml` (beyond the Newport case already
    // updated above), reusing its CRS verbatim. Station-level overlap only,
    // no segment shared, so both lines should stay ExclusiveSegment.
    #[test]
    fn xc_stansted_station_overlap_with_west_anglia_at_audley_end_stays_exclusive_each_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "XC-AUD-1",
            "Signal failure at Audley End",
            "Signal failure causing delays at Audley End.",
            &["LE"],
            &["AUD"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "greater-anglia-west-anglia".to_string(),
                "xc-stansted".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should stay ExclusiveSegment",
                m.line.id
            );
        }
    }

    #[test]
    fn braintree_branch_witham_is_station_overlap_only_with_main_line() {
        // Witham is on both greater-anglia-main-line.toml's `geml-mainline`
        // segment and greater-anglia-braintree-branch.toml's own
        // `braintree-branch` segment (the Braintree branch's real junction;
        // this file was originally bundled as part of
        // greater-anglia-essex-branches.toml, since split by brand --
        // real-world sanity review). Per that file's segment-decision note, the two files deliberately
        // do NOT share a segment name here — reusing `geml-mainline` verbatim
        // would (confirmed empirically while drafting that file) incorrectly
        // reclassify unrelated far-flung `geml-mainline` stations (e.g. Diss,
        // Norwich) as shared trunk too, since SegmentRegistry::is_shared marks
        // a segment name shared globally, not per overlapping station. So an
        // incident here should match both lines independently, each still
        // classified as ExclusiveSegment rather than escalating to
        // SharedSegment — mirroring the Romford/Shenfield precedent in
        // `elizabeth_branch_incident_stays_on_its_branch` above.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-7",
            "Points failure at Witham",
            "Points failure causing delays to Greater Anglia services.",
            &["LE"],
            &["WTM"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "greater-anglia-main-line".to_string(),
                "greater-anglia-braintree-branch".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment, not shared",
                m.line.id
            );
        }
    }

    #[test]
    fn sunshine_coast_colchester_is_station_overlap_only_with_main_line() {
        // Colchester is on both greater-anglia-main-line.toml's
        // `geml-mainline` segment and greater-anglia-sunshine-coast.toml's
        // own `sunshine-coast-main` segment (the Sunshine Coast line's real
        // junction; this file was originally bundled as part of
        // greater-anglia-essex-branches.toml, since split by brand --
        // real-world sanity review). Same reasoning and same non-sharing
        // decision as Witham above: station-level overlap only, each line
        // classified independently as ExclusiveSegment.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-9",
            "Points failure at Colchester",
            "Points failure causing delays to Greater Anglia services.",
            &["LE"],
            &["COL"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "greater-anglia-main-line".to_string(),
                "greater-anglia-sunshine-coast".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment, not shared",
                m.line.id
            );
        }
    }

    #[test]
    fn crouch_valley_exclusive_segment_incident_does_not_propagate() {
        // Southminster is on `crouch-valley-line`, exclusive to
        // greater-anglia-crouch-valley.toml (originally bundled as part of
        // greater-anglia-essex-branches.toml, since split by brand --
        // real-world sanity review). Per that file's Southminster-branch
        // deviation note: the branch's real, verified junction is
        // Wickford on the Shenfield-Southend line, two hops from the GEML
        // via a line not covered by any file in this catalogue — so unlike
        // the Braintree/Sunshine Coast branches above, this segment is not
        // tagged as shared with greater-anglia-main-line or any other line.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-8",
            "Signal failure at Southminster",
            "Signal failure causing delays to Greater Anglia services.",
            &["LE"],
            &["SMN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["greater-anglia-crouch-valley".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn gainsborough_line_exclusive_segment_incident_does_not_propagate() {
        // Sudbury is the terminus of `gainsborough-line`, exclusive to
        // greater-anglia-gainsborough-line.toml (originally bundled as part
        // of greater-anglia-suffolk-branches.toml, since split by brand --
        // real-world sanity review). It isn't a junction or overlap point
        // for any other committed line, so an incident here should stay
        // scoped to greater-anglia-gainsborough-line only.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-10",
            "Signal failure at Sudbury",
            "Signal failure causing delays to Greater Anglia services.",
            &["LE"],
            &["SUY"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["greater-anglia-gainsborough-line".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn gainsborough_line_marks_tey_is_station_overlap_only_with_main_line() {
        // Marks Tey is on both greater-anglia-main-line.toml's
        // `geml-mainline` segment and greater-anglia-gainsborough-line.toml's
        // own `gainsborough-line` segment (the Sudbury branch's real
        // junction; this file was originally bundled as part of
        // greater-anglia-suffolk-branches.toml, since split by brand --
        // real-world sanity review). Per that file's segment-decision note
        // (mirroring Task 2.4's Witham/Colchester precedent), the two files
        // deliberately do NOT share a segment name here — reusing
        // `geml-mainline` verbatim would incorrectly reclassify unrelated
        // far-flung `geml-mainline` stations (e.g. Diss, Norwich) as shared
        // trunk too, since SegmentRegistry::is_shared marks a segment name
        // shared globally, not per overlapping station. So an incident here
        // should match both lines independently, each still classified as
        // ExclusiveSegment rather than escalating to SharedSegment.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-11",
            "Points failure at Marks Tey",
            "Points failure causing delays to Greater Anglia services.",
            &["LE"],
            &["MKT"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "greater-anglia-main-line".to_string(),
                "greater-anglia-gainsborough-line".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment, not shared",
                m.line.id
            );
        }
    }

    #[test]
    fn felixstowe_branch_ipswich_is_station_overlap_only_with_main_line() {
        // Ipswich is on both greater-anglia-main-line.toml's `geml-mainline`
        // segment and greater-anglia-felixstowe-branch.toml's own
        // `felixstowe-branch` segment (where Felixstowe branch passenger
        // services originate; the branch's true physical fork is one stop
        // further out at Westerfield; this file was originally bundled as
        // part of greater-anglia-suffolk-branches.toml, since split by
        // brand -- real-world sanity review). Same non-sharing decision as
        // Marks Tey above: station-level overlap only, each line classified
        // independently as ExclusiveSegment.
        //
        // Updated by the Wales/East Anglia batch: `greater-anglia-east-
        // suffolk.toml` and `greater-anglia-ipswich-cambridge.toml` both
        // also have Ipswich as their own junction (their own
        // `greater-anglia-east-suffolk`/`greater-anglia-ipswich-cambridge`
        // segments) - two more independent ExclusiveSegment matches.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-12",
            "Points failure at Ipswich",
            "Points failure causing delays to Greater Anglia services.",
            &["LE"],
            &["IPS"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "greater-anglia-main-line".to_string(),
                "greater-anglia-felixstowe-branch".to_string(),
                "greater-anglia-east-suffolk".to_string(),
                "greater-anglia-ipswich-cambridge".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment, not shared",
                m.line.id
            );
        }
    }

    #[test]
    fn mayflower_line_manningtree_is_station_overlap_only_with_main_line() {
        // Manningtree is on both greater-anglia-main-line.toml's
        // `geml-mainline` segment and greater-anglia-mayflower-line.toml's
        // own `mayflower-line` segment (the Mayflower line's real junction;
        // this file was originally bundled as part of
        // greater-anglia-suffolk-branches.toml, since split by brand --
        // real-world sanity review). Same non-sharing decision as Marks Tey
        // and Ipswich above: station-level overlap only, each line
        // classified independently as ExclusiveSegment.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-13",
            "Points failure at Manningtree",
            "Points failure causing delays to Greater Anglia services.",
            &["LE"],
            &["MNG"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "greater-anglia-main-line".to_string(),
                "greater-anglia-mayflower-line".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment, not shared",
                m.line.id
            );
        }
    }

    #[test]
    fn bittern_line_exclusive_segment_incident_does_not_propagate() {
        // Sheringham is the terminus of `bittern-line`, exclusive to
        // greater-anglia-bittern-line.toml (originally bundled as part of
        // greater-anglia-norfolk-branches.toml, since split by brand --
        // real-world sanity review). It isn't a junction or overlap point
        // for any other committed line, so an incident here should stay
        // scoped to greater-anglia-bittern-line only.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-14",
            "Signal failure at Sheringham",
            "Signal failure causing delays to Greater Anglia services.",
            &["LE"],
            &["SHM"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["greater-anglia-bittern-line".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn wherry_lines_great_yarmouth_exclusive_segment_incident_does_not_propagate() {
        // Great Yarmouth (the Acle route's terminus, and also the physical
        // terminus of the separate, much lower-frequency Berney Arms route —
        // see greater-anglia-wherry-lines.toml's (originally bundled as
        // part of greater-anglia-norfolk-branches.toml, since split by
        // brand -- real-world sanity review) Wherry Lines segment note for
        // why GYM is listed once, under `wherry-acle-branch`) isn't a
        // junction or overlap point for any other committed line, so an
        // incident here should stay scoped to greater-anglia-wherry-lines
        // only.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-15",
            "Signal failure at Great Yarmouth",
            "Signal failure causing delays to Greater Anglia services.",
            &["LE"],
            &["GYM"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["greater-anglia-wherry-lines".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn norfolk_branches_norwich_is_station_overlap_only_with_main_line() {
        // Norwich is on both greater-anglia-main-line.toml's `geml-mainline`
        // segment (as GEML's terminus) and the shared origin of the
        // Bittern, Wherry and Breckland lines -- originally the single
        // bundled greater-anglia-norfolk-branches.toml's own
        // `norfolk-branches-norwich` segment, since split by brand
        // (real-world sanity review) into three separate files
        // (greater-anglia-bittern-line.toml, greater-anglia-wherry-lines.toml,
        // greater-anglia-breckland-line.toml), each of which now lists NRW
        // under its own exclusive segment name. Per the original file's
        // segment-decision note (mirroring Task 2.4's Witham/Colchester and
        // Task 2.5's Marks Tey/Ipswich/Manningtree precedent — and a
        // deliberate departure from this task's own brief, which suggested a
        // SharedSegment-asserting test here), none of these files share a
        // segment name at Norwich with each other or with
        // greater-anglia-main-line.toml — reusing `geml-mainline` verbatim
        // would incorrectly reclassify unrelated far-flung `geml-mainline`
        // stations (e.g. Diss, Ingatestone) as shared trunk too, since
        // SegmentRegistry::is_shared marks a segment name shared globally,
        // not per overlapping station, and there is no track beyond Norwich
        // that GEML and these three branches jointly occupy. So an incident
        // here should match all lines independently, each still classified
        // as ExclusiveSegment rather than escalating to SharedSegment.
        //
        // Norwich is also emr-regional.toml's own terminus (its own
        // `emr-regional-east` segment, exclusive catalogue-wide, merged
        // separately from this batch) -- another independent ExclusiveSegment
        // match by the same station-overlap pattern.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-16",
            "Points failure at Norwich",
            "Points failure causing delays to Greater Anglia services.",
            &["LE"],
            &["NRW"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "greater-anglia-main-line".to_string(),
                "greater-anglia-bittern-line".to_string(),
                "greater-anglia-wherry-lines".to_string(),
                "greater-anglia-breckland-line".to_string(),
                "emr-regional".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment, not shared",
                m.line.id
            );
        }
    }

    #[test]
    fn breckland_line_ely_is_station_overlap_only_with_xc_stansted() {
        // Ely is on both greater-anglia-breckland-line.toml's own
        // `breckland-line` segment (the Breckland Line's route to Cambridge;
        // this file was originally bundled as part of
        // greater-anglia-norfolk-branches.toml, since split by brand --
        // real-world sanity review) and xc-stansted.toml's whole-route
        // `xc-stansted` segment (CrossCountry's Birmingham-Stansted Airport
        // route also approaches Cambridge via Peterborough and Ely). This
        // overlap wasn't previously exercised by any regression test, since
        // no other committed line touched Ely before this file existed. The
        // two files deliberately do NOT share a segment name here (see
        // greater-anglia-breckland-line.toml's Ely/Cambridge decision
        // note — reusing `xc-stansted` verbatim would incorrectly mark
        // xc-stansted.toml's entire Midlands trunk as shared with this
        // line), so an incident here should match both lines independently,
        // each still classified as ExclusiveSegment rather than escalating
        // to SharedSegment.
        //
        // Ely is also emr-regional.toml's own `emr-regional-east` segment
        // (exclusive catalogue-wide, merged separately from this batch) --
        // a third independent ExclusiveSegment match by the same
        // station-overlap pattern.
        //
        // Ely is also great-northern-kings-lynn.toml's own junction (its
        // own `gn-cambridge-kings-lynn-branch` segment, merged separately,
        // Batch 5) -- a fourth independent ExclusiveSegment match.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LE-17",
            "Points failure at Ely",
            "Points failure causing delays to Greater Anglia services.",
            &["LE"],
            &["ELY"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "greater-anglia-breckland-line".to_string(),
                "xc-stansted".to_string(),
                "emr-regional".to_string(),
                "great-northern-kings-lynn".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment, not shared",
                m.line.id
            );
        }
    }

    #[test]
    fn wcml_birmingham_exclusive_segment_incident_does_not_propagate() {
        // `lnwr-birmingham-crewe.toml` (added after this test was first
        // written, Task 1.8) also calls at Birmingham International, on its
        // own exclusive `lnwr-birmingham` segment -- station-level overlap
        // with the Avanti branch, same "overlap is fine, segment-sharing is
        // a deliberate choice" precedent already exercised elsewhere in this
        // file (e.g. `xc_hub_incident_propagates_to_every_cross_country_arm`).
        // It's a real second line affected by this incident, still
        // ExclusiveSegment (different segment name, no sharing).
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "VT-1",
            "Points failure at Birmingham International",
            "Points failure causing delays to services at Birmingham International.",
            &["VT"],
            &["BHI"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "wcml-birmingham".to_string(),
                "lnwr-birmingham-crewe".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    #[test]
    fn wcml_birmingham_has_station_marston_green() {
        // Task 9.3: eight West Midlands Trains local stops (Canley, Tile
        // Hill, Berkswell, Hampton-in-Arden, Marston Green, Lea Hall,
        // Stechford, Adderley Park) were added to `lines/wcml-birmingham.toml`
        // as real, currently-open, currently-served stations between
        // Coventry and Birmingham New Street -- previously omitted on the
        // (rejected, per this plan's full-coverage mandate) reasoning that
        // Avanti's own service doesn't call there. Marston Green (MGN) here
        // stands in for all eight as the `has_station` regression this
        // task's testing convention requires; each was independently
        // two-source verified (Wikipedia's own station article
        // cross-checked against nationalrail.co.uk's live
        // /stations/<crs>/details.html page) per the file's own header
        // comment.
        //
        // All eight sit on `wcml-birmingham-branch`, which -- per a grep of
        // every `lines/*.toml` file -- remains exclusive to this file: no
        // sibling line reuses that segment name. (`lnwr-birmingham-crewe.toml`
        // covers the same physical Coventry-Birmingham track under its own,
        // differently-named `lnwr-birmingham` segment, and doesn't itself
        // model Marston Green/Lea Hall/Stechford/Adderley Park at all -- see
        // that file's Task 9.2 note.) So per this task's testing convention,
        // there's no sibling `MatchScope` assertion to add here; skipped.
        let lines = load_line("wcml-birmingham");
        let line = lines
            .get("wcml-birmingham")
            .expect("wcml-birmingham line should exist");
        assert!(
            line.has_station("MGN"),
            "wcml-birmingham should now include Marston Green (MGN)"
        );
    }

    #[test]
    fn wcml_birmingham_shared_trunk_incident_propagates_to_wcml_spine() {
        // Rugby is the diverging junction: `wcml-birmingham.toml` reuses
        // `west-coast-main-line.toml`'s own `wcml-midlands` segment tag there
        // (see that file's comment), so an incident at Rugby should be a
        // SharedSegment match for both lines, not exclusive to either.
        //
        // `wcml-manchester.toml` (added after this test was first written)
        // also reuses the same `wcml-midlands` tag at Rugby -- both of its
        // branches (via Stoke-on-Trent and via Crewe/Wilmslow) travel over
        // this same shared spine before diverging further north, so it's a
        // real third line affected by this incident, all three SharedSegment.
        //
        // `wcml-liverpool.toml` (added after this test was first written)
        // also reuses `wcml-midlands` at Rugby -- it doesn't diverge from
        // the spine until Crewe, further north -- so it's a real fourth
        // line affected by this incident, still SharedSegment.
        //
        // `wcml-north-wales.toml` (added after this test was first written)
        // also reuses `wcml-midlands` at Rugby -- like `wcml-liverpool.toml`
        // it doesn't diverge from the spine until Crewe -- so it's a real
        // fifth line affected by this incident, still SharedSegment.
        //
        // `lnwr-birmingham-crewe.toml` (added after this test was first
        // written, Task 1.8) also calls at Rugby -- it's the genuine physical
        // reconvergence point between that file's two internal branches
        // (Northampton Loop and Trent Valley Line), tagged there with its
        // own exclusive `lnwr-rugby` segment, deliberately NOT sharing
        // `wcml-midlands` (same station-overlap-not-segment-sharing
        // precedent `xc-manchester.toml` already set, per that task's own
        // brief). It's a real sixth line affected by this incident, but
        // ExclusiveSegment rather than SharedSegment.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "VT-2",
            "Signal failure at Rugby",
            "Signal failure causing delays to services at Rugby.",
            &["VT"],
            &["RUG"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "wcml".to_string(),
                "wcml-birmingham".to_string(),
                "wcml-manchester".to_string(),
                "wcml-liverpool".to_string(),
                "wcml-north-wales".to_string(),
                "lnwr-birmingham-crewe".to_string(),
            ])
        );
        for m in &matches {
            if m.line.id == "lnwr-birmingham-crewe" {
                assert_eq!(
                    m.scope,
                    MatchScope::ExclusiveSegment,
                    "{} should be ExclusiveSegment",
                    m.line.id
                );
            } else {
                assert_eq!(
                    m.scope,
                    MatchScope::SharedSegment,
                    "{} should be SharedSegment",
                    m.line.id
                );
            }
        }
    }

    #[test]
    fn wcml_manchester_exclusive_segment_incident_does_not_propagate() {
        // Stoke-on-Trent is on the exclusive `wcml-manchester-stoke` branch
        // segment, not shared with any other line's segment tag.
        //
        // Uses SOT, Stoke-on-Trent's real CRS code (confirmed via Wikipedia)
        // -- this file originally had it wrong as "STO", which is actually
        // South Tottenham's own real code and collided with
        // overground-suffragette.toml once that file merged. Fixed at the
        // data level (lines/wcml-manchester.toml), not just here.
        //
        // `xc-manchester.toml` also lists SOT as of its 2026-09-21 route
        // correction (it had previously modelled the wrong Wilmslow/Crewe
        // corridor; it now correctly runs via Stockport/Macclesfield/
        // Stoke-on-Trent, the real CrossCountry corridor) -- station-level
        // overlap only, under its own unrelated `xc-manchester` segment
        // name, so it legitimately appears here too as a second
        // ExclusiveSegment match, same pattern as
        // `emr_regional_stockport_and_hope_valley_both_match_without_over_propagating`
        // above.
        //
        // Updated by the Midlands EMR/WMR/LNWR sanity review: `emr-crewe-
        // derby.toml` and `lnwr-stafford-crewe.toml` (both real coverage
        // gaps this review added) also call at Stoke-on-Trent, each on its
        // own exclusive segment (different operators, different physical
        // approaches from each other and from the two lines above) -- two
        // more independent ExclusiveSegment matches.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "VT-5",
            "Points failure at Stoke-on-Trent",
            "Points failure causing delays to services at Stoke-on-Trent.",
            &["VT"],
            &["SOT"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "wcml-manchester".to_string(),
                "xc-manchester".to_string(),
                "emr-crewe-derby".to_string(),
                "lnwr-stafford-crewe".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    #[test]
    fn wcml_liverpool_exclusive_segment_incident_does_not_propagate() {
        // Runcorn is on the exclusive `wcml-liverpool-branch` segment,
        // starting immediately after the Crewe junction (per the
        // shared-trunk rule of thumb) -- not shared with any other line's
        // segment tag, even though `wcml-manchester.toml` also diverges at
        // Crewe (onto a different physical branch, via Wilmslow).
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "VT-6",
            "Overhead line damage at Runcorn",
            "Overhead line damage causing delays to services at Runcorn.",
            &["VT"],
            &["RUN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["wcml-liverpool".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn wcml_north_wales_shared_corridor_incident_propagates_to_tfw() {
        // Rhyl sits on the shared `tfw-north-wales-coast` segment name, now
        // reused by both this file and `tfw-north-wales-coast.toml` (2026-
        // 09-21 real-world-sanity review, reversing the original
        // station-overlap-only ruling -- see either file's own "TfW/WCML
        // shared corridor"/"Cross-batch note" comment for the full
        // reasoning: Avanti and TfW genuinely run over the same physical
        // double-track main line here, not two corridors meeting at a
        // point). Starting immediately after the Crewe junction (per the
        // shared-trunk rule of thumb), this is a real SharedSegment match
        // for both lines -- not shared with `wcml-manchester.toml`'s or
        // `wcml-liverpool.toml`'s own different branches, which diverge at
        // Crewe onto physically distinct corridors (via Wilmslow and
        // Runcorn respectively).
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "VT-7",
            "Overhead line damage at Rhyl",
            "Overhead line damage causing delays to services at Rhyl.",
            &["VT"],
            &["RHL"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "wcml-north-wales".to_string(),
                "tfw-north-wales-coast".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment",
                m.line.id
            );
        }
    }

    #[test]
    fn wmr_snow_hill_dorridge_branch_exclusive_segment_incident_does_not_propagate() {
        // Dorridge is on the exclusive `wmr-snow-hill-dorridge` segment,
        // starting after the Tyseley junction (per the shared-trunk rule of
        // thumb) -- this line has no meaningful overlap with any existing
        // WCML/XC file (Snow Hill/Moor Street, not Birmingham New Street).
        //
        // Dorridge is also chiltern-main-line.toml's own station (merged
        // separately, after this test was first written), on its exclusive
        // `chiltern-birmingham-approach` segment -- station-level overlap,
        // distinct segment names, both stay ExclusiveSegment.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LM-1",
            "Signal failure at Dorridge",
            "Signal failure causing delays to services at Dorridge.",
            &["LM"],
            &["DDG"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "wmr-snow-hill".to_string(),
                "chiltern-main-line".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    #[test]
    fn wmr_snow_hill_stratford_branch_exclusive_segment_incident_does_not_propagate() {
        // Stratford-upon-Avon is on the exclusive `wmr-snow-hill-stratford`
        // segment (the North Warwickshire Line), starting after the same
        // Tyseley junction as the Dorridge branch above, but tagged with a
        // distinct segment name since it's a different physical branch.
        //
        // Stratford-upon-Avon is also chiltern-main-line.toml's own terminus
        // (merged separately, after this test was first written), on its
        // exclusive `chiltern-stratford-branch` segment -- station-level
        // overlap, distinct segment names, both stay ExclusiveSegment.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LM-2",
            "Points failure at Stratford-upon-Avon",
            "Points failure causing delays to services at Stratford-upon-Avon.",
            &["LM"],
            &["SAV"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "wmr-snow-hill".to_string(),
                "chiltern-main-line".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    #[test]
    fn wmr_cross_city_redditch_branch_exclusive_segment_incident_does_not_propagate() {
        // Redditch is on the exclusive `wmr-cross-city-redditch` segment,
        // starting after the Barnt Green junction (per the shared-trunk rule
        // of thumb) -- this line's only real station-level overlaps with
        // other catalogue files are at Lichfield Trent Valley, University and
        // Birmingham New Street, all on the trunk, and none of those are
        // shared-segment (see `wmr-cross-city.toml`'s own comment) -- so
        // there's no shared-segment counterpart to assert for this line.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LM-3",
            "Signal failure at Redditch",
            "Signal failure causing delays to services at Redditch.",
            &["LM"],
            &["RDC"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["wmr-cross-city".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn wmr_cross_city_bromsgrove_branch_exclusive_segment_incident_does_not_propagate() {
        // Bromsgrove is on the exclusive `wmr-cross-city-bromsgrove` segment,
        // starting after the same Barnt Green junction as the Redditch branch
        // above, but tagged with a distinct segment name since it's a
        // different physical branch (added as a second southern terminus once
        // electrification reached it in 2018).
        //
        // Updated by the Midlands EMR/WMR/LNWR sanity review: `lines/wmr-
        // malvern-line.toml` (a new file from that review) also calls at
        // Bromsgrove, on its own exclusive `wmr-malvern-line` segment --
        // genuine physical track-sharing but a materially different
        // calling pattern beyond this point, so station-overlap only (see
        // that file's own ruling comment). This incident now also matches
        // it, independently ExclusiveSegment.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LM-4",
            "Points failure at Bromsgrove",
            "Points failure causing delays to services at Bromsgrove.",
            &["LM"],
            &["BMV"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["wmr-cross-city".to_string(), "wmr-malvern-line".to_string()])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    #[test]
    fn lnwr_northampton_incident_matches_single_remaining_lnwr_line() {
        // Was `lnwr_northampton_shared_segment_incident_propagates_to_both_lnwr_lines`,
        // asserting a two-line SharedSegment match between this file and
        // Task 1.7's `lnwr-euston-commuter.toml`. That file was deleted
        // (2026-08-31 line-catalogue-coverage follow-up): fresh research
        // reconfirmed it modelled a service that doesn't exist as a distinct
        // real-world working, and its entire station list was already a
        // strict subset of this file's own -- see the FOLD-IN NOTE in
        // `lines/lnwr-birmingham-crewe.toml`. With only one catalogued LNWR
        // line left, `lnwr-northampton` is no longer a name shared across
        // multiple files, so an incident here is now ExclusiveSegment, not
        // SharedSegment -- same shape as
        // `lnwr_birmingham_crewe_exclusive_segment_incident_does_not_propagate`
        // below.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LM-5",
            "Signal failure at Northampton",
            "Signal failure causing delays to services at Northampton.",
            &["LM"],
            &["NMP"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["lnwr-birmingham-crewe".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn lnwr_euston_trunk_incident_matches_single_remaining_lnwr_line() {
        // Was `lnwr_euston_trunk_shared_segment_incident_propagates_to_both_lnwr_lines`,
        // asserting a two-line SharedSegment match between this file and
        // Task 1.7's `lnwr-euston-commuter.toml`. That file was deleted
        // (2026-08-31 line-catalogue-coverage follow-up) -- see the FOLD-IN
        // NOTE in `lines/lnwr-birmingham-crewe.toml` for the full research
        // and sourcing. Leighton Buzzard appears in no other catalogued
        // line, and with only one catalogued LNWR line left,
        // `lnwr-euston-trunk` is no longer shared across multiple files, so
        // this is now a plain single-line ExclusiveSegment match.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LM-6",
            "Overhead line damage at Leighton Buzzard",
            "Overhead line damage causing delays to services at Leighton Buzzard.",
            &["LM"],
            &["LBZ"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["lnwr-birmingham-crewe".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn lnwr_birmingham_crewe_exclusive_segment_incident_does_not_propagate() {
        // Canley is on the exclusive `lnwr-birmingham` segment (the
        // Birmingham branch, beyond the Rugby reconvergence point) -- not
        // shared with any other catalogued line's segment tag.
        //
        // `wcml-birmingham.toml` (Task 9.3, added after this test was first
        // written) now also calls at Canley -- previously omitted there as
        // "not called at by Avanti", but per this plan's full-coverage
        // mandate a real, currently-served station belongs in `stations`
        // regardless of which of this file's own operators calls there. It's
        // a real second line affected by this incident, on its own exclusive
        // `wcml-birmingham-branch` segment -- station-level overlap only,
        // same "overlap is fine, segment-sharing is a deliberate choice"
        // precedent this file already exercises elsewhere in this test
        // module (e.g. `wcml_birmingham_exclusive_segment_incident_does_not_propagate`
        // at Birmingham International), so still ExclusiveSegment for both.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LM-7",
            "Signal failure at Canley",
            "Signal failure causing delays to services at Canley.",
            &["LM"],
            &["CNL"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "lnwr-birmingham-crewe".to_string(),
                "wcml-birmingham".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    // `gwr-main-line`'s Bristol-bound exclusive segment starts at Chippenham
    // (CPM), the first station beyond Swindon reached only by GWML-via-Bath
    // services (South Wales Main Line diverges at Wootton Bassett Junction,
    // just west of Swindon), mirroring
    // `swr_exclusive_segment_incident_does_not_propagate`. See
    // `gwr_trunk_paddington_incident_propagates_to_cotswold` below for the
    // shared-trunk case, now that `gwr-cotswold` (Task 4.2) also shares
    // `gwr-trunk-paddington`.
    //
    // The Wessex/Thames-Valley/Isle-of-Wight batch's own gwr-transwilts.toml
    // adds a real station overlap here too: its own TransWilts service also
    // calls at Chippenham before diverging onto the separate Melksham branch
    // (see that file's own CPM comment), but it deliberately does not reuse
    // `gwr-main-line`'s segment name (which also covers Bath Spa/Bristol,
    // neither reached by the TransWilts line) — so this stays two
    // independent ExclusiveSegment matches, not a SharedSegment one.
    #[test]
    fn gwr_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GW-1",
            "Points failure at Chippenham",
            "Points failure causing delays at Chippenham.",
            &["GW"],
            &["CPM"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["gwr-main-line".to_string(), "gwr-transwilts".to_string()])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    // `gwr-cotswold`'s (Task 4.2) own exclusive segment starts at Oxford
    // (OXF), the first station beyond Didcot reached only by Cotswold Line
    // services (South Wales/Bristol-bound gwr-main-line services continue
    // west towards Swindon at Didcot instead). Mirrors
    // `swr_exclusive_segment_incident_does_not_propagate` /
    // `gwr_exclusive_segment_incident_does_not_propagate`.
    #[test]
    fn gwr_cotswold_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GW-2",
            "Signal failure at Moreton-in-Marsh",
            "Signal failure causing delays at Moreton-in-Marsh.",
            &["GW"],
            &["MIM"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["gwr-cotswold".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Station Catalogue Completeness Task 1.2: fills in five previously
    // missing minor Cotswold Line halts on gwr-cotswold.toml (Hanborough,
    // Combe, Finstock, Ascott-under-Wychwood, Shipton), two-source confirmed
    // and inserted at their true geographic position between OXF and KGM.
    // Hanborough was not in the task's original starting list (Combe,
    // Finstock, Ascott-under-Wychwood, Shipton) but was surfaced by a fresh
    // route-diagram read required by that task. See that file's own
    // "Ordered London to Worcester" comment for the full sourcing/rationale.
    #[test]
    fn gwr_cotswold_minor_halts_infill_stations_present() {
        let lines = load_line("gwr-cotswold");
        let line = lines
            .get("gwr-cotswold")
            .expect("gwr-cotswold line should exist");
        for crs in ["HND", "CME", "FIN", "AUW", "SIP"] {
            assert!(line.has_station(crs), "gwr-cotswold should now list {crs}");
        }
    }

    // Same task: an incident at one of the newly-added stations (Hanborough)
    // should behave exactly like the pre-existing Moreton-in-Marsh
    // exclusive-segment case above -- `gwr-cotswold` is not shared with any
    // sibling line's segment (confirmed by grepping the catalogue: only
    // comments in gwr-thames-valley.toml reference the name, no other file's
    // `[[stations]]` entries actually set `segment = "gwr-cotswold"`), so
    // this stays a clean ExclusiveSegment match with no shared-segment
    // propagation to assert, mirroring
    // `gwr_cornish_main_line_saltash_incident_stays_on_its_own_line`'s
    // identical judgment call for that file's own infill task.
    #[test]
    fn gwr_cotswold_hanborough_incident_stays_on_its_own_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GW-18",
            "Signal failure at Hanborough",
            "Signal failure causing delays at Hanborough.",
            &["GW"],
            &["HND"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["gwr-cotswold".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `gwr-main-line` and `gwr-cotswold` both share the `gwr-trunk-paddington`
    // segment (PAD/RDG/DID) established by Task 4.1 and reused verbatim by
    // Task 4.2. An incident at Didcot (a station on that shared segment)
    // should propagate to both lines as a shared-trunk event. Mirrors
    // `swr_shared_trunk_incident_propagates`'s shape.
    //
    // Task 4.5 (`gwr-thames-valley`) also has a DID station, but on its own
    // `gwr-thames-valley` segment (deliberately not sharing
    // `gwr-trunk-paddington` — see that file's own segment-naming comment),
    // so it's excluded from the SharedSegment check below even though its ID
    // also starts with "gwr-".
    #[test]
    fn gwr_trunk_paddington_incident_propagates_to_cotswold() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GW-3",
            "Signal failure at Didcot Parkway",
            "Signal failure causing delays to GWR services.",
            &["GW"],
            &["DID"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(matched_ids.contains("gwr-main-line"));
        assert!(matched_ids.contains("gwr-cotswold"));
        for m in &matches {
            if m.line.id.starts_with("gwr-") && m.line.id != "gwr-thames-valley" {
                assert_eq!(
                    m.scope,
                    MatchScope::SharedSegment,
                    "{} should be SharedSegment",
                    m.line.id
                );
            }
        }
    }

    // `gwr-south-wales`'s (Task 4.3) own exclusive segment covers Bristol
    // Parkway through Swansea. Bridgend (BGN) is not shared with any other
    // line's own station list in this catalogue, so this is a clean
    // ExclusiveSegment case, mirroring
    // `swr_exclusive_segment_incident_does_not_propagate` /
    // `gwr_cotswold_exclusive_segment_incident_does_not_propagate`. See
    // `gwr_south_wales_station_overlap_with_xc_cardiff_stays_exclusive_each_line`
    // below for the deliberately-not-shared overlap case at Newport/Cardiff
    // (task-4.3-brief.md's plan-mandated "don't force a shared segment"
    // decision), and `gwr_trunk_paddington_incident_propagates_to_south_wales`
    // for the shared-trunk case.
    #[test]
    fn gwr_south_wales_exclusive_segment_incident_does_not_propagate() {
        // Updated by the Wales/East Anglia batch: `tfw-vale-of-glamorgan.
        // toml` and `tfw-maesteg.toml` both also terminate at Bridgend
        // (their own `tfw-vale-of-glamorgan`/`tfw-maesteg` segments) - a
        // genuine station overlap, not a shared trunk, so both now match
        // independently as ExclusiveSegment too.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GW-4",
            "Overhead line damage at Bridgend",
            "Overhead line damage causing delays at Bridgend.",
            &["GW"],
            &["BGN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "gwr-south-wales".to_string(),
                "tfw-vale-of-glamorgan".to_string(),
                "tfw-maesteg".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    // Station Catalogue Completeness Task 1.3: fills in three previously
    // missing intermediate stations on gwr-south-wales.toml (Patchway,
    // Pilning, Severn Tunnel Junction), two-source confirmed and inserted at
    // their true geographic position between BPW and NWP. See that file's
    // own segment-naming comment for the full sourcing/rationale.
    #[test]
    fn gwr_south_wales_infill_stations_present() {
        let lines = load_line("gwr-south-wales");
        let line = lines
            .get("gwr-south-wales")
            .expect("gwr-south-wales line should exist");
        for crs in ["PWY", "PIL", "STJ"] {
            assert!(
                line.has_station(crs),
                "gwr-south-wales should now list {crs}"
            );
        }
    }

    // Same task: an incident at one of the newly-added stations (Severn
    // Tunnel Junction) should behave exactly like the pre-existing Bridgend
    // exclusive-segment case above -- `gwr-south-wales` is not shared with
    // any sibling line's own segment at this station (grepping the
    // catalogue confirms no other line file lists PWY/PIL/STJ as a station
    // at all, let alone shares the `gwr-south-wales` segment name -- see
    // that file's own research comment on the deliberate decision not to
    // reuse `xc-cardiff`'s segment name despite the genuine physical track
    // convergence there), so this stays a clean ExclusiveSegment match with
    // no shared-segment propagation to assert, mirroring
    // `gwr_cornish_main_line_saltash_incident_stays_on_its_own_line`'s /
    // `gwr_cotswold_hanborough_incident_stays_on_its_own_line`'s identical
    // judgment call for those files' own infill tasks.
    #[test]
    fn gwr_south_wales_severn_tunnel_junction_incident_stays_on_its_own_line() {
        // `xc-cardiff.toml` (Task 9.4, added after this test was first
        // written) independently added Severn Tunnel Junction too -- a real
        // second line affected by this incident, on its own exclusive
        // `xc-cardiff` segment, not `gwr-south-wales`'s. Station-level
        // overlap only, same precedent as
        // `lnwr_birmingham_crewe_exclusive_segment_incident_does_not_propagate`
        // above, so still ExclusiveSegment for both.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GW-19",
            "Flooding at Severn Tunnel Junction",
            "Flooding causing delays at Severn Tunnel Junction.",
            &["GW"],
            &["STJ"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["gwr-south-wales".to_string(), "xc-cardiff".to_string()])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    // `gwr-main-line`, `gwr-cotswold` and `gwr-south-wales` all share the
    // `gwr-trunk-paddington` segment, but only at the stations each file
    // actually lists on it: gwr-cotswold.toml stops at DID (Cotswold
    // services diverge onto the Cherwell Valley line there, before Swindon),
    // while gwr-main-line.toml and gwr-south-wales.toml both continue
    // through SWI. Didcot Parkway (DID) is therefore the one station all
    // three files genuinely share, so an incident there should propagate to
    // all three as a shared-trunk event. Mirrors
    // `swr_shared_trunk_incident_propagates`'s full-set-assertion shape, now
    // extended to a third sibling per task-4.3-brief.md's test requirement
    // #2 (that requirement names Swindon as the example station, but Swindon
    // is not actually on gwr-cotswold.toml's own station list — see that
    // file's own segment-naming comment — so Didcot is used here instead to
    // get a real three-way match rather than a two-way one).
    //
    // Task 4.5 (`gwr-thames-valley`) also stops at DID, but on its own
    // exclusive `gwr-thames-valley` segment (deliberately not sharing
    // `gwr-trunk-paddington` — see that file's own segment-naming comment),
    // so it's included in the matched set (a real station overlap) but stays
    // ExclusiveSegment rather than SharedSegment.
    //
    // UPDATED (Task 9.6, 2026-09-01): `xc-south-coast.toml`'s own fresh
    // route-diagram pass added DID too (its own OXF-RDG stretch runs via
    // Didcot Parkway, the only physical route), on its own exclusive
    // `xc-south-coast` segment — a fifth genuine station overlap, same
    // ExclusiveSegment treatment as gwr-thames-valley above. This assertion
    // is updated (not left to silently go stale) because the previous
    // four-line exact-set assertion is now factually false with DID added
    // to a fifth file, mirroring Task 9.3's own precedent for updating a
    // pre-existing test a new station addition invalidates.
    //
    // UPDATED (Wessex/Thames-Valley/Isle-of-Wight batch): gwr-golden-valley.
    // toml and gwr-transwilts.toml both genuinely run over this same
    // Paddington-Reading-Didcot-Swindon approach before diverging beyond
    // Swindon (see each file's own segment-naming comment), and both reuse
    // `gwr-trunk-paddington` verbatim rather than an exclusive segment name
    // — so both are real SharedSegment additions here, not station-overlap
    // exceptions like gwr-thames-valley/xc-south-coast above.
    #[test]
    fn gwr_trunk_paddington_incident_propagates_to_south_wales() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GW-5",
            "Signal failure at Didcot Parkway",
            "Signal failure causing delays to GWR services.",
            &["GW"],
            &["DID"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "gwr-main-line".to_string(),
                "gwr-cotswold".to_string(),
                "gwr-south-wales".to_string(),
                "gwr-thames-valley".to_string(),
                "xc-south-coast".to_string(),
                "gwr-golden-valley".to_string(),
                "gwr-transwilts".to_string(),
            ])
        );
        for m in &matches {
            if m.line.id == "gwr-thames-valley" || m.line.id == "xc-south-coast" {
                assert_eq!(
                    m.scope,
                    MatchScope::ExclusiveSegment,
                    "{} should stay ExclusiveSegment",
                    m.line.id
                );
            } else {
                assert_eq!(
                    m.scope,
                    MatchScope::SharedSegment,
                    "{} should be SharedSegment",
                    m.line.id
                );
            }
        }
    }

    // task-4.3-brief.md's plan-mandated regression guard: Cardiff Central is
    // a real station overlap between `gwr-south-wales` (this task) and
    // `xc-cardiff.toml` (already committed) — both lines call there, but via
    // physically different corridors for most of their length (South Wales
    // Main Line via Bristol Parkway/Severn Tunnel vs. xc-cardiff's Gloucester
    // to Newport Line via Chepstow), and per this task's file-scope
    // restriction (lines/gwr-south-wales.toml + matcher.rs only, not
    // xc-cardiff.toml) neither file reuses the other's segment name for
    // NWP/CDF. So an incident at Cardiff Central should match BOTH lines
    // (real station overlap — each notified about an incident at "their"
    // station) but EACH must stay `MatchScope::ExclusiveSegment` for its own
    // segment, never `SharedSegment` — confirming the two lines' overlap
    // here stays a station-level thing, not a segment-level one. See
    // gwr-south-wales.toml's own segment-naming comment for the research
    // this decision is based on (a genuine, not just assumed, finding that
    // physical track sharing exists further up the corridor at Severn Tunnel
    // Junction, deliberately not modelled as a shared segment given this
    // task's file-scope limits).
    #[test]
    fn gwr_south_wales_station_overlap_with_xc_cardiff_stays_exclusive_each_line() {
        // Cardiff Central is also the terminus of tfw-city-line.toml
        // (originally part of tfw-valley-lines-south.toml, Task 11.7; split
        // out into its own file by the 2026-09-21 real-world-sanity review --
        // that split doesn't change which stations reach CDF, since Cardiff
        // Central was always the City Line's own share of that file, not the
        // Coryton or Cardiff Bay Lines') and of all three of the former
        // tfw-valley-lines-north.toml's successor files (Batch 11's later
        // data-driven split: tfw-valley-rhymney.toml, tfw-valley-merthyr.toml,
        // tfw-valley-rhondda.toml), tagged on every side with their
        // genuinely shared `tfw-valley-cardiff-hub` segment -- those four
        // resolve SharedSegment *with each other*, while gwr-south-wales/
        // xc-cardiff stay ExclusiveSegment on their own distinct segment
        // names, same station-overlap-only pattern as this test already
        // established.
        //
        // Updated by the Wales/East Anglia batch: `tfw-ebbw-vale.toml` and
        // `tfw-vale-of-glamorgan.toml` both also terminate at Cardiff
        // Central (their own `tfw-ebbw-vale`/`tfw-vale-of-glamorgan`
        // segments) - two more independent ExclusiveSegment matches by the
        // same station-overlap pattern.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GW-6",
            "Points failure at Cardiff Central",
            "Points failure causing delays at Cardiff Central.",
            &["GW"],
            &["CDF"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "gwr-south-wales".to_string(),
                "xc-cardiff".to_string(),
                "tfw-valley-rhymney".to_string(),
                "tfw-valley-merthyr".to_string(),
                "tfw-valley-rhondda".to_string(),
                "tfw-city-line".to_string(),
                "tfw-ebbw-vale".to_string(),
                "tfw-vale-of-glamorgan".to_string(),
            ])
        );
        for m in &matches {
            let expected = match m.line.id.as_str() {
                "tfw-valley-rhymney" | "tfw-valley-merthyr" | "tfw-valley-rhondda"
                | "tfw-city-line" => MatchScope::SharedSegment,
                _ => MatchScope::ExclusiveSegment,
            };
            assert_eq!(m.scope, expected, "{} scope mismatch", m.line.id);
        }
    }

    // Task 9.4's fresh route-diagram pass on xc-cardiff.toml added two
    // previously-missing, real, currently-open, currently-served stations to
    // its exclusive `xc-cardiff` segment between Chepstow and Newport:
    // Caldicot (CDT) and Severn Tunnel Junction (STJ). Regression guard that
    // `has_station` now recognises one of them. `xc-cardiff` is not shared
    // with any other line file (grepped `lines/*.toml` for
    // `segment = "xc-cardiff"`: only this file uses it, and neither CDT nor
    // STJ appears on any other file's station list -- gwr-south-wales.toml's
    // own comment documents the genuine physical track sharing near Severn
    // Tunnel Junction but deliberately doesn't model it as a shared segment),
    // so per the testing convention only the has_station assertion applies
    // here -- no sibling-line MatchScope assertion to add.
    #[test]
    fn xc_cardiff_has_station_severn_tunnel_junction() {
        let lines = load_line("xc-cardiff");
        let xc_cardiff = lines
            .get("xc-cardiff")
            .expect("xc-cardiff line should exist");
        assert!(
            xc_cardiff.has_station("STJ"),
            "xc-cardiff should now recognise Severn Tunnel Junction (STJ)"
        );
        assert!(
            xc_cardiff.has_station("CDT"),
            "xc-cardiff should now recognise Caldicot (CDT)"
        );
    }

    // Task 9.5 (2026-09-01) fresh route-diagram pass on `xc-manchester.toml`
    // added intermediate stations this file's own "minor intermediate calls
    // are omitted" boilerplate had left out, but got the Manchester-Stafford
    // end wrong: it modelled Levenshulme/Heaton Chapel/Cheadle
    // Hulme/Handforth/Wilmslow/Alderley Edge/Chelford/Goostrey/Holmes
    // Chapel/Sandbach/Crewe, the Crewe-Manchester line via Wilmslow -- a
    // real route, but run by Avanti West Coast and Northern, not
    // CrossCountry (Wilmslow's own Wikipedia article lists neither XC).
    // CORRECTED (2026-09-21, data-driven audit + independent
    // re-verification, see `lines/xc-manchester.toml`'s own comment for the
    // full sourcing): CrossCountry's real Manchester Piccadilly corridor
    // runs via Stockport, Macclesfield and Stoke-on-Trent instead, rejoining
    // this same Stafford-Wolverhampton-Birmingham stretch, whose own
    // Penkridge/Coseley/Tipton/Dudley Port/Sandwell & Dudley/Smethwick
    // Galton Bridge/Smethwick Rolfe Street sourcing was never in question
    // and is unchanged. All stations inherit this file's own exclusive
    // `xc-manchester` segment (no sibling line shares that segment name --
    // grepped `lines/*.toml`), so per the testing convention only the
    // has_station assertion applies for most of them; see the separate
    // overlap test below for Smethwick Galton Bridge specifically, which is
    // also a station (not segment) overlap with `wmr-snow-hill.toml`.
    #[test]
    fn xc_manchester_recognises_newly_added_stations() {
        let lines = load_line("xc-manchester");
        let line = lines
            .get("xc-manchester")
            .expect("xc-manchester should load");
        for crs in [
            "MAC", "SOT", "PKG", "CSY", "TIP", "DDP", "SAD", "SGB", "SMR",
        ] {
            assert!(
                line.has_station(crs),
                "{crs} should now be recognised on xc-manchester"
            );
        }
        for crs in [
            "LVM", "HTC", "CHU", "HTH", "WML", "ALD", "CEL", "GTR", "HCH", "SDB", "CRE",
        ] {
            assert!(
                !line.has_station(crs),
                "{crs} was on the old, physically-wrong Wilmslow/Crewe route and \
                 should no longer be recognised on xc-manchester"
            );
        }
    }

    // Smethwick Galton Bridge (SGB) is a genuine split-level interchange:
    // its high-level platforms carry `wmr-snow-hill.toml`'s Snow Hill route
    // (segment `wmr-snow-hill-trunk`), its low-level platforms carry this
    // file's Stour Valley stretch (segment `xc-manchester`) -- station-level
    // overlap only, the two segment names are genuinely distinct strings, so
    // an incident there should stay ExclusiveSegment on both lines rather
    // than propagate as a shared trunk. Mirrors
    // `gwr_south_wales_station_overlap_with_xc_cardiff_stays_exclusive_each_line`.
    //
    // Updated by the national/WCML/XC sanity review: wcml-birmingham.toml's
    // own extension to Wolverhampton also calls at Smethwick Galton Bridge,
    // on its own distinct exclusive `wcml-birmingham-branch` segment -- a
    // third independent ExclusiveSegment match.
    #[test]
    fn xc_manchester_station_overlap_with_wmr_snow_hill_stays_exclusive_each_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "XC-SGB-1",
            "Signalling fault at Smethwick Galton Bridge",
            "Signalling fault causing delays at Smethwick Galton Bridge.",
            &["XC"],
            &["SGB"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "xc-manchester".to_string(),
                "wmr-snow-hill".to_string(),
                "wcml-birmingham".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} scope mismatch",
                m.line.id
            );
        }
    }

    // Task 4.4 split `gwr-west-of-england` (Reading-Taunton line) into its own
    // file. Originally its exclusive segment (`gwr-west-of-england`) covered
    // Newbury through Castle Cary with no *cross-file segment-name* sharing
    // at all. Task 4.6 (originally gwr-bristol-suburban.toml, later split
    // into gwr-severn-beach.toml/gwr-heart-of-wessex.toml) found genuine
    // physical track sharing at Westbury/Castle Cary, but an early draft of
    // that fix reused the whole `gwr-west-of-england` segment name (including
    // Newbury, which gwr-heart-of-wessex's own service never reaches, and
    // Frome/Bruton, which are gwr-heart-of-wessex's own exclusive territory)
    // — wrong, since segment sharing is tracked per segment *name*, not per
    // individual station, so that draft mislabelled all three as "shared"
    // catalogue-wide. The final-review fix wave introduced a new, narrower
    // segment name, `gwr-westbury-castle-cary`, covering ONLY Westbury (WSB)
    // and Castle Cary (CLC) — the two stations both files' own cited sources
    // actually name as shared. Newbury (NBY) reverts to being a genuinely
    // exclusive station on this line's own `gwr-west-of-england` segment
    // (gwr-heart-of-wessex.toml never reaches it), and Frome/Bruton move onto
    // gwr-heart-of-wessex.toml's own `gwr-bristol-weymouth` segment.
    // See `gwr_westbury_castle_cary_trunk_incident_propagates_to_heart_of_
    // wessex` below for the corrected shared-segment case, and
    // `gwr_thames_valley_station_overlap_with_gwr_west_of_england_stays_
    // exclusive_each_line` below for the Newbury case, now back to
    // ExclusiveSegment on both sides (a real station overlap, not a segment
    // share).

    // Task 4.4's second file, `gwr-cornish-main-line`, picks up its own
    // exclusive segment (also named `gwr-cornish-main-line`) at Liskeard,
    // the first station west of Plymouth not already claimed by
    // cross-country.toml's `xc-south-west` segment. Truro is deep inside
    // that exclusive stretch, so this should stay a clean ExclusiveSegment
    // case, mirroring `swr_exclusive_segment_incident_does_not_propagate` /
    // `gwr_cotswold_exclusive_segment_incident_does_not_propagate` above.
    //
    // Devon/Cornwall branch-line batch (2026-09): `gwr-maritime-line.toml`
    // now also lists Truro as its own real junction station (station
    // overlap only, per this catalogue's established convention -- its own
    // exclusive `gwr-maritime-line` segment is not shared with
    // `gwr-cornish-main-line`'s own exclusive segment). An incident at Truro
    // now genuinely matches both lines, each with its own ExclusiveSegment
    // scope; the assertion below is updated to expect both.
    #[test]
    fn gwr_cornish_main_line_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GW-8",
            "Signal failure at Truro",
            "Signal failure causing delays at Truro.",
            &["GW"],
            &["TRU"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "gwr-cornish-main-line".to_string(),
                "gwr-maritime-line".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    // Station Catalogue Completeness Task 1.1: fills in seven previously
    // missing Plymouth-area suburban stations on gwr-cornish-main-line.toml
    // (Devonport, Dockyard, Keyham, St Budeaux Ferry Road, Saltash, St
    // Germans, Menheniot), two-source confirmed and inserted at their true
    // geographic position between PLY and LSK. See that file's own
    // segment-naming comment for the full sourcing/rationale.
    #[test]
    fn gwr_cornish_main_line_plymouth_area_infill_stations_present() {
        let lines = load_line("gwr-cornish-main-line");
        let line = lines
            .get("gwr-cornish-main-line")
            .expect("gwr-cornish-main-line line should exist");
        for crs in ["DPT", "DOC", "KEY", "SBF", "STS", "SGM", "MEN"] {
            assert!(
                line.has_station(crs),
                "gwr-cornish-main-line should now list {crs}"
            );
        }
    }

    // Same task: an incident at one of the newly-added stations (Saltash)
    // should behave exactly like the pre-existing Truro exclusive-segment
    // case above -- `gwr-cornish-main-line` is not shared with any sibling
    // line's segment (confirmed by grepping the catalogue: the segment name
    // is exclusive to this one file), so this stays a clean ExclusiveSegment
    // match with no shared-segment propagation to assert, mirroring
    // `emr_poacher_bottesford_incident_stays_on_its_own_line`'s
    // identical judgment call for that file's own infill task.
    #[test]
    fn gwr_cornish_main_line_saltash_incident_stays_on_its_own_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GW-10",
            "Points failure at Saltash",
            "Points failure causing delays at Saltash.",
            &["GW"],
            &["STS"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["gwr-cornish-main-line".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 4.4's research found a genuine multi-station shared trunk with
    // cross-country.toml's own `xc-south-west` segment (Taunton-Exeter St
    // Davids-Newton Abbot-Plymouth, on the Bristol to Exeter line / South
    // Devon Main Line) — unlike gwr-south-wales.toml's single-waypoint
    // overlaps with cross-country.toml/xc-cardiff.toml, which deliberately
    // stayed station-only (see
    // `gwr_south_wales_station_overlap_with_xc_cardiff_stays_exclusive_each_line`
    // above). Exeter St Davids (EXD) is the station both of Task 4.4's own
    // files share with each other (the west-of-england/Cornish Main Line
    // split boundary) *and* with cross-country.toml, so an incident there
    // should propagate to all three as a shared-trunk event, mirroring
    // `swr_shared_trunk_incident_propagates`'s / `xc_hub_incident_propagates_
    // to_every_cross_country_arm`'s full-set-assertion shape.
    //
    // Devon/Cornwall branch-line batch (2026-09): `gwr-tarka-line.toml`,
    // `gwr-avocet-line.toml` and `gwr-dartmoor-line.toml` also list Exeter
    // St Davids, on their own genuine cross-file shared trunk
    // `gwr-exeter-central-approach` (sourced from Wikipedia's "Exeter
    // Central railway station": "The SWR and GWR services combine to give
    // up to five trains per hour each way between Exeter Central and Exeter
    // St Davids", confirming this stretch carries the Avocet/Tarka/Dartmoor
    // Line services too) -- a different shared-segment name from
    // `xc-south-west`, but still genuinely shared (across those three new
    // files), so all three now also match this same EXD incident with
    // SharedSegment scope.
    //
    // The Wessex/Thames-Valley/Isle-of-Wight batch's own
    // swr-west-of-england.toml (SWR's OWN, differently-named Waterloo-
    // Salisbury-Exeter route, not to be confused with gwr-west-of-england.
    // toml's Reading-Taunton line above) also calls at Exeter St Davids as
    // its own real terminus, so it's a fourth genuine station overlap here
    // too. Its own research found no sourced evidence of shared TRACK with
    // the Bristol-Taunton-Exeter corridor `xc-south-west` represents (its
    // own approach is via Exeter Central, a different direction, converging
    // only at the St Davids station throat) — so, unlike the other three, it
    // deliberately does NOT reuse `xc-south-west` and stays its own
    // `swr-west-of-england` segment, i.e. ExclusiveSegment scope, not
    // SharedSegment.
    //
    // GWR/southwest sanity review (2026-09-21): the new gwr-riviera-line.toml
    // also lists Exeter St Davids, reusing `xc-south-west` verbatim for its
    // own Exeter-Newton Abbot approach (the same South Devon Main Line
    // sea-wall route) - an eighth line, SharedSegment.
    //
    // The assertion below is updated to expect all eight lines.
    #[test]
    fn gwr_trunk_xc_south_west_incident_propagates_across_west_of_england_and_cornish_main_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GW-9",
            "Flooding at Exeter St Davids",
            "Flooding causing delays to GWR and CrossCountry services.",
            &["GW"],
            &["EXD"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "gwr-west-of-england".to_string(),
                "gwr-cornish-main-line".to_string(),
                "cross-country".to_string(),
                "gwr-tarka-line".to_string(),
                "gwr-avocet-line".to_string(),
                "gwr-dartmoor-line".to_string(),
                "swr-west-of-england".to_string(),
                "gwr-riviera-line".to_string(),
            ])
        );
        for m in &matches {
            if m.line.id == "swr-west-of-england" {
                assert_eq!(
                    m.scope,
                    MatchScope::ExclusiveSegment,
                    "{} should stay ExclusiveSegment (station overlap, not a shared segment)",
                    m.line.id
                );
            } else {
                assert_eq!(
                    m.scope,
                    MatchScope::SharedSegment,
                    "{} should be SharedSegment",
                    m.line.id
                );
            }
        }
    }

    // Task 4.5's research found this line's real local/stopping service runs
    // on the physically separate Relief lines (Reading railway station's own
    // Wikipedia article, independently corroborated at Southall), not the
    // express Main lines gwr-trunk-paddington represents — so, unlike its
    // GWR siblings, `gwr-thames-valley` does NOT reuse `gwr-trunk-paddington`
    // anywhere, including at PAD/RDG. The whole line (both the Didcot-Oxford
    // and Reading-Newbury branches) uses one exclusive segment,
    // `gwr-thames-valley`, not shared with any other catalogued line. Culham
    // (CUM), on the Oxford branch, is a clean ExclusiveSegment case, mirroring
    // `swr_exclusive_segment_incident_does_not_propagate` /
    // `gwr_cotswold_exclusive_segment_incident_does_not_propagate`.
    //
    // UPDATED (Task 9.6, 2026-09-01): `xc-south-coast.toml`'s own fresh
    // route-diagram pass reused CUM verbatim (its own Oxford-Reading
    // stretch runs via Didcot Parkway, the same physical Oxford branch this
    // file curates) on its own exclusive `xc-south-coast` segment — a
    // genuine second station overlap, so this incident now also matches
    // xc-south-coast, staying ExclusiveSegment there too. Updated rather
    // than left stale, mirroring Task 9.3's own precedent.
    #[test]
    fn gwr_thames_valley_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GW-10",
            "Signal failure at Culham",
            "Signal failure causing delays at Culham.",
            &["GW"],
            &["CUM"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "gwr-thames-valley".to_string(),
                "xc-south-coast".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should stay ExclusiveSegment",
                m.line.id
            );
        }
    }

    // Task 9.6 (2026-09-01) fresh route-diagram pass on `xc-south-coast.toml`
    // added 30 real, currently-open, currently-served intermediate stations
    // across this whole Birmingham-Bournemouth corridor's "minor calls
    // omitted" boilerplate. All inherit the existing exclusive
    // `xc-south-coast` segment. Regression guard that `has_station` now
    // recognises a representative spread across every leg researched (see
    // the overlap-specific tests below for the DID/CUM/RDW station-overlap
    // cases with `gwr-thames-valley.toml`, already updated above).
    #[test]
    fn xc_south_coast_recognises_newly_added_stations() {
        let lines = load_line("xc-south-coast");
        let line = lines
            .get("xc-south-coast")
            .expect("xc-south-coast should load");
        for crs in [
            "KNW", "KGS", "HYD", "TAC", "RAD", "APF", "CHO", "GOR", "PAN", "TLH", "RDW", "RGP",
            "MOR", "BMY", "MIC", "SHW", "ESL", "SOA", "SWG", "SDN", "MBK", "RDB", "TTN", "ANF",
            "BEU", "BCU", "SWY", "NWM", "HNA", "CHR", "POK",
        ] {
            assert!(
                line.has_station(crs),
                "{crs} should now be recognised on xc-south-coast"
            );
        }
    }

    // Reading West (RDW) is a genuine second overlap this task's own
    // research found with `gwr-thames-valley.toml`: both files reach it via
    // shared trackage as far as Southcote Junction (that file's own
    // Reading-Newbury branch, this file's Reading-Basingstoke stretch),
    // reusing RDW's CRS/TIPLOC verbatim. Station-level overlap only, no
    // segment shared, so both lines should stay ExclusiveSegment.
    #[test]
    fn xc_south_coast_station_overlap_with_gwr_thames_valley_at_reading_west_stays_exclusive_each_line()
     {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "XC-RDW-1",
            "Points failure at Reading West",
            "Points failure causing delays at Reading West.",
            &["GW"],
            &["RDW"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "gwr-thames-valley".to_string(),
                "xc-south-coast".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should stay ExclusiveSegment",
                m.line.id
            );
        }
    }

    // task-4.5-brief.md's plan-mandated regression guard: Maidenhead (MAI) is
    // a real station overlap between `gwr-thames-valley` (this task) and
    // elizabeth-line.toml's own `elizabeth-west` segment — both lines call
    // there (different service classes on the same physical station: a 2tph
    // GWR semi-fast continuing past Reading to Didcot/Oxford/Newbury, versus
    // the Elizabeth line's high-frequency metro stopper terminating at
    // Reading) but this task's research deliberately did not force
    // segment-sharing (see gwr-thames-valley.toml's own segment-naming
    // comment). So an incident at Maidenhead should match BOTH lines (real
    // station overlap) but EACH must stay `MatchScope::ExclusiveSegment` for
    // its own segment, never `SharedSegment` — mirrors
    // `gwr_south_wales_station_overlap_with_xc_cardiff_stays_exclusive_each_line`.
    //
    // The Wessex/Thames-Valley/Isle-of-Wight batch's own
    // gwr-marlow-branch.toml adds a third real overlap here: Maidenhead is
    // also where that branch diverges, on its own exclusive
    // `gwr-marlow-branch` segment (see that file's own MAI comment) — a
    // third independent ExclusiveSegment match, same shape as the other two.
    #[test]
    fn gwr_thames_valley_station_overlap_with_elizabeth_west_stays_exclusive_each_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GW-11",
            "Points failure at Maidenhead",
            "Points failure causing delays at Maidenhead.",
            &["GW"],
            &["MAI"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "gwr-thames-valley".to_string(),
                "elizabeth-line".to_string(),
                "gwr-marlow-branch".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should stay ExclusiveSegment (station overlap, not a shared segment)",
                m.line.id
            );
        }
    }

    // A second, genuine (not just assumed) overlap this task's own research
    // found beyond what task-4.5-brief.md names: Oxford (OXF) is also
    // gwr-cotswold.toml's own exclusive segment's starting station, and the
    // Didcot-Oxford stretch both lines use is real single-track shared
    // infrastructure (see gwr-thames-valley.toml's own segment-naming
    // comment). Kept as station overlap only for this task's file-scope
    // reasons, so both lines should stay ExclusiveSegment, never
    // SharedSegment, mirroring the Maidenhead/elizabeth-line case above.
    // xc-south-coast.toml also calls at OXF (already documented, by
    // gwr-cotswold.toml's own comment, as a pre-existing station overlap
    // with that line) — included here too, also staying ExclusiveSegment.
    #[test]
    fn gwr_thames_valley_station_overlap_with_gwr_cotswold_stays_exclusive_each_line() {
        // Oxford is also chiltern-oxford.toml's own terminus (originally
        // merged as part of chiltern-aylesbury.toml, Batch 12, since split
        // out by the real-world sanity review), on its exclusive
        // `chiltern-oxford-branch` segment -- a fourth independent
        // ExclusiveSegment match by the same station-overlap pattern the
        // other three already establish.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GW-12",
            "Overhead line damage at Oxford",
            "Overhead line damage causing delays at Oxford.",
            &["GW"],
            &["OXF"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "gwr-thames-valley".to_string(),
                "gwr-cotswold".to_string(),
                "xc-south-coast".to_string(),
                "chiltern-oxford".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should stay ExclusiveSegment (station overlap, not a shared segment)",
                m.line.id
            );
        }
    }

    // A third genuine overlap this task's own research found: Newbury (NBY)
    // is also gwr-west-of-england.toml's own exclusive segment's starting
    // station, and the Reading-Newbury stretch both lines use (via Southcote
    // Junction) is real shared Berks and Hants line track (see
    // gwr-thames-valley.toml's own segment-naming comment). Kept as station
    // overlap only for this task's file-scope reasons, mirroring the Oxford/
    // gwr-cotswold.toml case above. An earlier draft of the file now split
    // into gwr-severn-beach.toml/gwr-heart-of-wessex.toml's own Westbury/
    // Castle Cary fix mistakenly reused the whole `gwr-west-of-england`
    // segment name (not just WSB/CLC), which pulled NBY into SharedSegment
    // status too even though gwr-heart-of-wessex's own service never reaches
    // it. The final-review fix wave narrowed that shared segment to a new
    // name, `gwr-westbury-castle-cary` (WSB/CLC only — see
    // `gwr_westbury_castle_cary_trunk_incident_propagates_to_heart_of_
    // wessex` below), so NBY is once again a genuinely exclusive station on
    // gwr-west-of-england's own `gwr-west-of-england` segment: both lines
    // should now stay `MatchScope::ExclusiveSegment` for their own segment,
    // confirming this is a real station overlap, not a segment-level share.
    #[test]
    fn gwr_thames_valley_station_overlap_with_gwr_west_of_england_stays_exclusive_each_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GW-13",
            "Points failure at Newbury",
            "Points failure causing delays at Newbury.",
            &["GW"],
            &["NBY"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "gwr-thames-valley".to_string(),
                "gwr-west-of-england".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should stay ExclusiveSegment (station overlap, not a shared segment)",
                m.line.id
            );
        }
    }

    // Station Catalogue Completeness Task 1.4: fills in the previously
    // missing Paddington-area inner-suburban stations on
    // gwr-thames-valley.toml (Southall, Hayes & Harlington, West Drayton,
    // Iver, Langley, Burnham, Taplow), two-source confirmed and inserted at
    // their true geographic position between PAD and MAI. Ealing Broadway,
    // the eighth station in that task's starting candidate list, is
    // deliberately NOT added — its own Wikipedia "Services" section and
    // nationalrail.co.uk's own details page both show no current GWR
    // calling service there (Elizabeth line/Underground only) — see that
    // file's own segment-naming comment for the full sourcing/rationale.
    #[test]
    fn gwr_thames_valley_paddington_area_infill_stations_present() {
        let lines = load_line("gwr-thames-valley");
        let line = lines
            .get("gwr-thames-valley")
            .expect("gwr-thames-valley line should exist");
        for crs in ["STL", "HAY", "WDT", "IVR", "LNY", "BNM", "TAP"] {
            assert!(
                line.has_station(crs),
                "gwr-thames-valley should now list {crs}"
            );
        }
        assert!(
            !line.has_station("EAL"),
            "gwr-thames-valley should NOT list EAL (Ealing Broadway) — GWR does not currently call there, see the file's own Task 1.4 comment"
        );
    }

    // Same task: Southall (STL) is a real, additional station overlap with
    // elizabeth-line.toml's own `elizabeth-trunk-west` segment. Unlike the
    // `elizabeth-west` overlap at MAI/SLO/TWY/WDT, `elizabeth-trunk-west` is
    // itself a genuine shared trunk between elizabeth-line.toml and
    // elizabeth-heathrow.toml (both list HAY/STL/EAL on that exact segment
    // name — checked directly), so an incident at Southall propagates as
    // SharedSegment between those two Elizabeth-line arms, while
    // gwr-thames-valley — on its own unrelated `gwr-thames-valley` segment —
    // still only sees a station-level hit and stays ExclusiveSegment,
    // mirroring the MAI/SLO/TWY station-overlap precedent for this line's
    // own segment even though the *other* two lines here happen to share a
    // trunk with each other.
    #[test]
    fn gwr_thames_valley_station_overlap_with_elizabeth_trunk_west_stays_exclusive_each_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GW-20",
            "Signal failure at Southall",
            "Signal failure causing delays at Southall.",
            &["GW"],
            &["STL"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let by_id: HashMap<String, MatchScope> = matches
            .iter()
            .map(|m| (m.line.id.clone(), m.scope))
            .collect();
        assert_eq!(
            by_id.keys().cloned().collect::<HashSet<_>>(),
            HashSet::from([
                "gwr-thames-valley".to_string(),
                "elizabeth-line".to_string(),
                "elizabeth-heathrow".to_string()
            ])
        );
        assert_eq!(
            by_id.get("gwr-thames-valley"),
            Some(&MatchScope::ExclusiveSegment),
            "gwr-thames-valley should stay ExclusiveSegment (station overlap only, not a shared segment, for its own segment)"
        );
        assert_eq!(
            by_id.get("elizabeth-line"),
            Some(&MatchScope::SharedSegment),
            "elizabeth-line should be SharedSegment (elizabeth-trunk-west)"
        );
        assert_eq!(
            by_id.get("elizabeth-heathrow"),
            Some(&MatchScope::SharedSegment),
            "elizabeth-heathrow should be SharedSegment (elizabeth-trunk-west)"
        );
    }

    // Same task, later revisited by the London line-definition audit that
    // fixed lines/elizabeth-line.toml's ZCW/WWA CRS bugs and infilled its
    // `elizabeth-west` segment: Iver (IVR) is now also on elizabeth-line.toml
    // (added by that audit, real CIF schedule confirmation + Wikipedia/TfL
    // timetable), on its own exclusive `elizabeth-west` segment — a genuine
    // station overlap, not a shared segment, mirroring the MAI/SLO/TWY/WDT
    // precedent already established above. Both lines therefore stay
    // ExclusiveSegment.
    #[test]
    fn gwr_thames_valley_iver_incident_stays_on_its_own_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GW-21",
            "Points failure at Iver",
            "Points failure causing delays at Iver.",
            &["GW"],
            &["IVR"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "gwr-thames-valley".to_string(),
                "elizabeth-line".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should stay ExclusiveSegment (station overlap, not a shared segment)",
                m.line.id
            );
        }
    }

    // Station Catalogue Completeness Task 1.5: fills in four previously
    // missing stations on gwr-west-of-england.toml's own exclusive stretch
    // west of Newbury — Kintbury (KIT), Hungerford (HGD), Bedwyn (BDW) and
    // Pewsey (PEW), two-source confirmed (Wikipedia + nationalrail.co.uk,
    // TIPLOCs additionally cross-checked against railwaycodes.org.uk) and
    // inserted at their true geographic position between NBY and WSB, all
    // tagged this line's own exclusive `gwr-west-of-england` segment (not
    // `gwr-westbury-castle-cary`, which starts at the next station, WSB, per
    // the shared-trunk rule of thumb). Six other named candidates from this
    // task's starting list are deliberately NOT added here: Reading West,
    // Theale, Aldermaston, Midgham, Thatcham and Newbury Racecourse already
    // live on gwr-thames-valley.toml's own "Branch 2: Reading-Newbury"
    // section (a different, Reading-based local service, not this file's
    // Reading-Taunton express) and Frome already lives on
    // gwr-heart-of-wessex.toml (reached only via a branch off this line's
    // direct route) — both untouched by this task. A further five named
    // candidates — Savernake (Low Level), Woodborough, Patney and Chirton,
    // Lavington, and Edington and Bratton — were checked to the same
    // two-source bar and found closed (all closed to passengers between
    // 1952 and 1966; railwaycodes.org.uk shows no CRS code for any of them)
    // so they stay out too. See that file's own segment-naming comment for
    // the full sourcing/rationale.
    #[test]
    fn gwr_west_of_england_berks_and_hants_infill_stations_present() {
        let lines = load_line("gwr-west-of-england");
        let line = lines
            .get("gwr-west-of-england")
            .expect("gwr-west-of-england line should exist");
        for crs in ["KIT", "HGD", "BDW", "PEW"] {
            assert!(
                line.has_station(crs),
                "gwr-west-of-england should now list {crs}"
            );
        }
        for crs in ["RDW", "THE", "AMT", "MDG", "THA", "NRC", "FRO"] {
            assert!(
                !line.has_station(crs),
                "gwr-west-of-england should NOT list {crs} — it belongs to a sibling file, see the file's own Task 1.5 comment"
            );
        }
    }

    // Same task: none of the four newly-added stations sit on a segment name
    // shared with any other catalogued line (`gwr-west-of-england` is this
    // line's own exclusive segment throughout, confirmed by grepping the
    // catalogue), so an incident at one of them — Kintbury, picked as a
    // representative example — should stay a clean ExclusiveSegment case,
    // mirroring `gwr_thames_valley_iver_incident_stays_on_its_own_line`'s /
    // `gwr_cornish_main_line_saltash_incident_stays_on_its_own_line`'s
    // identical judgment call for their own infill tasks.
    #[test]
    fn gwr_west_of_england_kintbury_incident_stays_on_its_own_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GW-22",
            "Signal failure at Kintbury",
            "Signal failure causing delays at Kintbury.",
            &["GW"],
            &["KIT"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["gwr-west-of-england".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 4.6's own exclusive segment (`gwr-severn-beach`) covers the whole
    // Severn Beach branch — Severn Beach itself (SVB) is not shared with any
    // other catalogued line, so this should stay a clean ExclusiveSegment
    // case, mirroring `swr_exclusive_segment_incident_does_not_propagate` /
    // `gwr_cotswold_exclusive_segment_incident_does_not_propagate`. Originally
    // part of the combined gwr-bristol-suburban.toml; that file was later
    // split into gwr-severn-beach.toml/gwr-heart-of-wessex.toml, so this test
    // now asserts against the `gwr-severn-beach` line id.
    #[test]
    fn gwr_severn_beach_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GW-14",
            "Trespass incident at Severn Beach",
            "Trespass incident causing delays at Severn Beach.",
            &["GW"],
            &["SVB"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["gwr-severn-beach".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 4.6's research found a genuine, multi-station shared trunk with
    // gwr-west-of-england.toml between Westbury and Castle Cary — both
    // stations' own Wikipedia pages independently confirm this ("the Reading
    // to Taunton Line and the Heart of Wessex Line... share tracks between
    // Westbury and Castle Cary stations"), the same shape as `xc-south-west`/
    // `gwr-trunk-paddington` earlier in this batch. An earlier draft reused
    // gwr-west-of-england.toml's own `gwr-west-of-england` segment name
    // verbatim for this — wrong, because that also pulled Newbury (not
    // reached by this line's service) and Frome/Bruton (this line's own
    // exclusive territory, not actually shared) into "shared" status. The
    // final-review fix wave introduced a new, narrower segment name,
    // `gwr-westbury-castle-cary`, covering ONLY Westbury (WSB) and Castle
    // Cary (CLC) — the two stations both files' own cited sources actually
    // name as shared. An incident at either should still propagate to both
    // lines as a shared-trunk event, mirroring
    // `swr_shared_trunk_incident_propagates`'s / `gwr_trunk_xc_south_west_
    // incident_propagates_across_west_of_england_and_cornish_main_line`'s
    // shape. WSB/CLC live on gwr-heart-of-wessex.toml since the later split
    // of the combined gwr-bristol-suburban.toml — see that file's own split
    // note.
    //
    // The Wessex/Thames-Valley/Isle-of-Wight batch's own gwr-wessex-main.toml
    // adds a genuine third participant: Westbury is also where its own
    // southward continuation towards Warminster/Salisbury meets this same
    // junction (en.wikipedia.org/wiki/Castle_Cary_railway_station's own
    // quote covers this line too), and it reuses `gwr-westbury-castle-cary`
    // verbatim for its own WSB row — a real three-way SharedSegment now.
    #[test]
    fn gwr_westbury_castle_cary_trunk_incident_propagates_to_heart_of_wessex() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GW-15",
            "Points failure at Westbury",
            "Points failure causing delays at Westbury.",
            &["GW"],
            &["WSB"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "gwr-west-of-england".to_string(),
                "gwr-heart-of-wessex".to_string(),
                "gwr-wessex-main".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment",
                m.line.id
            );
        }
    }

    // A second, genuine (not just assumed) overlap this task's own research
    // found beyond the Westbury/Castle Cary one the brief itself names: Bath
    // Spa (BTH) and Bristol Temple Meads (BRI) are also gwr-main-line.toml's
    // own exclusive-segment stations (its `gwr-main-line` segment). This
    // task's research confirms genuine physical track sharing (there is only
    // one railway between Bristol Temple Meads and Bath Spa), but
    // `gwr-main-line` also covers Chippenham (CPM), which this line's own
    // Bristol-Weymouth service does not reach — the same file-scope reason
    // gwr-thames-valley.toml's Oxford/Newbury cases stayed station-overlap
    // only (see `gwr_thames_valley_station_overlap_with_gwr_cotswold_stays_
    // exclusive_each_line` above). So an incident at Bath Spa should match
    // both lines (real station overlap) but each must stay
    // `MatchScope::ExclusiveSegment`, never `SharedSegment`. Originally part
    // of the combined gwr-bristol-suburban.toml; BTH lives on
    // gwr-heart-of-wessex.toml since that file's later split — see its own
    // split note.
    //
    // The Wessex/Thames-Valley/Isle-of-Wight batch's own gwr-wessex-main.toml
    // adds a THIRD real station overlap here, for the identical reason: its
    // own research also confirms genuine physical track sharing
    // Bristol-Bath-Westbury with both gwr-heart-of-wessex.toml (formerly
    // gwr-bristol-suburban.toml, before that file's later split) and
    // gwr-main-line.toml, but each sibling's own segment covers stations
    // this line doesn't reach (Chippenham for gwr-main-line; Frome/Castle
    // Cary/Yeovil/Weymouth for gwr-heart-of-wessex), so it too stays
    // station-overlap-only on its own `gwr-wessex-main` segment — see that
    // file's own BTH/BRI comment for the full sourcing.
    #[test]
    fn gwr_heart_of_wessex_station_overlap_with_gwr_main_line_stays_exclusive_each_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GW-16",
            "Overhead line damage at Bath Spa",
            "Overhead line damage causing delays at Bath Spa.",
            &["GW"],
            &["BTH"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "gwr-heart-of-wessex".to_string(),
                "gwr-main-line".to_string(),
                "gwr-wessex-main".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should stay ExclusiveSegment (station overlap, not a shared segment)",
                m.line.id
            );
        }
    }

    // A third genuine overlap this task's own research found, caught during
    // review after an earlier draft of the combined gwr-bristol-suburban.
    // toml's own WEY comment wrongly claimed SWR's route "is not otherwise
    // catalogued yet": Weymouth (WEY) is also swr-south-west-main.toml's own
    // terminus (its own exclusive `swr-swml-south` segment). This line's own
    // Bristol-Weymouth service never runs over any of swr-south-west-
    // main.toml's own claimed stations except WEY itself, so this stays
    // station overlap only, not a shared segment — different segment names
    // (`gwr-bristol-weymouth` vs `swr-swml-south`) mean no incorrect
    // `SharedSegment` cross-propagation. Mirrors
    // `gwr_heart_of_wessex_station_overlap_with_gwr_main_line_stays_exclusive_each_line`
    // above. Originally part of the combined gwr-bristol-suburban.toml; WEY
    // lives on gwr-heart-of-wessex.toml since that file's later split — see
    // its own split note.
    #[test]
    fn gwr_heart_of_wessex_station_overlap_with_swr_south_west_main_stays_exclusive_each_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GW-17",
            "Flooding at Weymouth",
            "Flooding causing delays at Weymouth.",
            &["GW"],
            &["WEY"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "gwr-heart-of-wessex".to_string(),
                "swr-south-west-main".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should stay ExclusiveSegment (station overlap, not a shared segment)",
                m.line.id
            );
        }
    }

    // `tfw-cambrian`'s own trunk segment (`tfw-cambrian-trunk`) is still
    // unique to Cambrian -- no other file shares that exact segment name,
    // so an incident away from Shrewsbury only ever matches Cambrian as
    // `MatchScope::ExclusiveSegment`, which is what this test covers.
    // Shrewsbury itself, however, is now a genuine three-way overlap point:
    // `tfw-marches.toml` (Task 11.5) and `tfw-heart-of-wales.toml` both tag
    // SHR with the shared `tfw-heart-of-wales-shrewsbury` segment, while
    // Cambrian keeps SHR on its own exclusive `tfw-cambrian-trunk` segment
    // (the Cambrian Line diverges west immediately, still station-overlap
    // only there -- see `lines/tfw-cambrian.toml`'s own comment). So an
    // incident specifically at Shrewsbury resolves three ways at once: see
    // `shrewsbury_three_way_overlap_resolves_per_line` below for that case.
    #[test]
    fn cambrian_coast_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-1",
            "Signal failure at Barmouth",
            "Signal failure causing delays to Transport for Wales services.",
            &["AW"],
            &["BRM"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["tfw-cambrian".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn cambrian_aberystwyth_branch_incident_does_not_propagate_to_coast_branch() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-2",
            "Trespass incident at Aberystwyth",
            "Trespass incident causing delays to Transport for Wales services.",
            &["AW"],
            &["AYW"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["tfw-cambrian".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `tfw-heart-of-wales` is standalone on its own southern stretch
    // (Craven Arms southwards) -- `tfw-marches.toml` (Task 11.5) now exists
    // but only shares the Shrewsbury-Craven Arms stretch, not the branch
    // south of Craven Arms towards Llandrindod/Swansea, so an incident well
    // south of Craven Arms should still match only Heart of Wales, as
    // `MatchScope::ExclusiveSegment`. See
    // `heart_of_wales_shrewsbury_shared_trunk_propagates` below for the
    // shared-trunk case Task 11.5 decided on.
    #[test]
    fn heart_of_wales_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-3",
            "Signal failure at Llandrindod",
            "Signal failure causing delays to Transport for Wales services.",
            &["AW"],
            &["LLO"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["tfw-heart-of-wales".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `tfw-conwy-valley` is, at this point in the line catalogue, a
    // genuinely standalone line: it uses a single whole-line segment name
    // and no other file in the catalogue shares it yet.
    // `tfw-north-wales-coast.toml` (Task 11.4) now exists and also reaches
    // Llandudno Junction, but Task 11.4 deliberately ruled that overlap
    // "station-overlap-only" rather than a genuine shared trunk (the Conwy
    // Valley Line diverges south immediately at the junction, with no track
    // actually shared by both lines' services beyond that one calling
    // point) -- so the two files use distinct segment names there and this
    // remains an exclusive-segment case for Conwy Valley. See
    // `lines/tfw-north-wales-coast.toml`'s comments for the full reasoning,
    // and `llj_station_overlap_matches_both_lines_as_exclusive` below for
    // the assertion that exercises the overlap itself.
    #[test]
    fn conwy_valley_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-4",
            "Signal failure at Betws-y-Coed",
            "Signal failure causing delays to Transport for Wales services.",
            &["AW"],
            &["BYC"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["tfw-conwy-valley".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `tfw-north-wales-coast` (Task 11.4). An incident on a station on this
    // line's own additional stretch -- one of the 11 stations
    // `wcml-north-wales.toml` doesn't separately list (see the 2026-09-21
    // real-world-sanity review's shared-corridor note above Chester in
    // this file) -- still only matches this one line
    // (`wcml-north-wales.toml` has no entry at this CRS code at all, so it
    // cannot match regardless of the segment name), but the match itself
    // is classified `MatchScope::SharedSegment`, not `ExclusiveSegment`:
    // `is_shared`/`is_exclusive_to` key off the segment *name* across the
    // whole catalogue, not per-station, and `tfw-north-wales-coast` is
    // that name here too (genuinely, not spuriously -- Avanti's own
    // through service physically runs over this exact stretch of track
    // even where it doesn't stop). e.g. Penmaenmawr.
    #[test]
    fn north_wales_coast_own_stretch_incident_is_shared_segment_but_single_match() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "VT-1",
            "Signal failure at Penmaenmawr",
            "Signal failure causing delays to services on the North Wales Coast Line.",
            &["AW"],
            &["PMW"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["tfw-north-wales-coast".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::SharedSegment);
    }

    // Llandudno Junction is on both `tfw-conwy-valley` and
    // `tfw-north-wales-coast`, but that overlap is station-overlap-only
    // rather than a shared trunk (see the comment above
    // `conwy_valley_exclusive_segment_incident_does_not_propagate`
    // and the comments in `lines/tfw-north-wales-coast.toml`). So an
    // incident there should match both lines independently, each still
    // classified as `MatchScope::ExclusiveSegment` (not `SharedSegment` --
    // that scope only applies when a segment name is genuinely shared
    // across line files, which is deliberately not the case here).
    //
    // Updated twice by the 2026-09-21 real-world-sanity review:
    //
    // 1. The former `tfw-llandudno-branch.toml` (a later Wales/East Anglia
    //    batch addition, which used to also call at Llandudno Junction on
    //    a dedicated, narrow `tfw-conwy-valley-llandudno-junction` segment
    //    name genuinely shared with `tfw-conwy-valley.toml`) turned out to
    //    be entirely redundant with `tfw-conwy-valley.toml`'s own real
    //    extent (that line's real terminus is Llandudno itself, not
    //    Llandudno Junction -- see `tfw-conwy-valley.toml`'s own
    //    top-of-file correction note) and has been deleted, its station
    //    data folded directly into `tfw-conwy-valley.toml`.
    //    `tfw-conwy-valley.toml`'s own LLJ entry keeps the same
    //    `tfw-conwy-valley-llandudno-junction` segment name unchanged, but
    //    since no other file uses that name any more, it now resolves as
    //    `MatchScope::ExclusiveSegment` rather than `SharedSegment`.
    // 2. `tfw-north-wales-coast.toml` and `wcml-north-wales.toml` now
    //    genuinely share a segment name for the whole Chester-Holyhead
    //    corridor, including Llandudno Junction (see either file's own
    //    "TfW/WCML shared corridor"/"Cross-batch note" comment) -- both
    //    resolve `MatchScope::SharedSegment` here, reversing their
    //    previous ExclusiveSegment classification.
    #[test]
    fn llj_station_overlap_matches_both_lines_as_exclusive() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-5",
            "Points failure at Llandudno Junction",
            "Points failure causing delays to Transport for Wales services.",
            &["AW"],
            &["LLJ"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "tfw-conwy-valley".to_string(),
                "tfw-north-wales-coast".to_string(),
                "wcml-north-wales".to_string(),
            ])
        );
        for m in &matches {
            let expected = if m.line.id == "tfw-conwy-valley" {
                MatchScope::ExclusiveSegment
            } else {
                MatchScope::SharedSegment
            };
            assert_eq!(m.scope, expected, "{} should be {:?}", m.line.id, expected);
        }
    }

    // `tfw-marches` (Task 11.5; rescoped to its real Shrewsbury-Crewe
    // northern extent by the 2026-09-21 real-world-sanity review -- see
    // `lines/tfw-marches.toml`'s own top-of-file correction note). An
    // incident on a station well away from this line's one coordination
    // point (the Shrewsbury-Craven Arms shared trunk with Heart of Wales)
    // should match only this line, as `MatchScope::ExclusiveSegment` --
    // e.g. Hereford, which sits on `tfw-marches-south`, a segment no other
    // line in the catalogue uses.
    //
    // Updated by the Midlands EMR/WMR/LNWR sanity review: `lines/wmr-
    // malvern-line.toml` (a new file from that review) also terminates at
    // Hereford, on its own exclusive `wmr-malvern-line` segment (a
    // physically distinct, Bromsgrove/Worcester-facing approach) --
    // station-overlap only, so this incident now also matches that line,
    // independently ExclusiveSegment.
    #[test]
    fn marches_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        // Updated by the GWR/southwest sanity review: gwr-cotswold.toml's
        // own extension now also terminates at Hereford, on its own
        // exclusive `gwr-cotswold` segment - a third independent
        // ExclusiveSegment match, station overlap only.
        let inc = incident(
            "AW-6",
            "Signal failure at Hereford",
            "Signal failure causing delays to Transport for Wales services.",
            &["AW"],
            &["HFD"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "tfw-marches".to_string(),
                "wmr-malvern-line".to_string(),
                "gwr-cotswold".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    // Craven Arms is on both `tfw-marches` and `tfw-heart-of-wales`, and
    // Task 11.5 ruled -- after independently verifying the physical
    // track-sharing claim against three separate Wikipedia articles (see
    // the comments above Craven Arms in `lines/tfw-marches.toml`) -- that
    // this is a genuine shared trunk, not mere station overlap: Heart of
    // Wales services physically run over Marches Line metals between
    // Craven Arms and Shrewsbury, calling at the intermediate station
    // Church Stretton along the way. Both files tag Craven Arms (and
    // Church Stretton, and Shrewsbury) with the same segment name,
    // `tfw-heart-of-wales-shrewsbury` (reusing the forward-bet name
    // `tfw-heart-of-wales.toml` left for this file to pick up), so an
    // incident there should propagate to both lines, each classified
    // `MatchScope::SharedSegment`.
    #[test]
    fn heart_of_wales_shrewsbury_shared_trunk_propagates() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-7",
            "Points failure at Craven Arms",
            "Points failure causing delays to Transport for Wales services.",
            &["AW"],
            &["CRV"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["tfw-marches".to_string(), "tfw-heart-of-wales".to_string()])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment",
                m.line.id
            );
        }
    }

    // Church Stretton (CTT) is an ordinary intermediate stop on the same
    // Shrewsbury-Craven Arms shared trunk exercised above, added to
    // `tfw-heart-of-wales.toml` during the final whole-branch review's fix
    // wave (Important #2) -- previously that file's station list jumped
    // straight from Shrewsbury to Craven Arms, omitting Church Stretton
    // entirely, which silently broke this exact propagation guarantee for
    // an incident reported there specifically (it would have matched
    // `tfw-marches` correctly but had no way to also match
    // `tfw-heart-of-wales`, since CTT wasn't in that file's station list at
    // all). Both files now tag Church Stretton with the same segment name,
    // `tfw-heart-of-wales-shrewsbury` (see the sourcing/decision note above
    // Craven Arms in `lines/tfw-marches.toml`, and the comment above CTT in
    // `lines/tfw-heart-of-wales.toml`), so an incident there should
    // propagate to both lines, each classified `MatchScope::SharedSegment`
    // -- mirroring `heart_of_wales_shrewsbury_shared_trunk_propagates`
    // above exactly, just at a different station on the same shared trunk.
    #[test]
    fn church_stretton_shared_trunk_propagates() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-15",
            "Signal failure at Church Stretton",
            "Signal failure causing delays to Transport for Wales services.",
            &["AW"],
            &["CTT"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["tfw-marches".to_string(), "tfw-heart-of-wales".to_string()])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment",
                m.line.id
            );
        }
    }

    // Chester is on both `tfw-shrewsbury-chester` and
    // `tfw-north-wales-coast`, but that overlap is station-overlap-only
    // rather than a shared trunk, mirroring `tfw-north-wales-coast.toml`'s
    // own Llandudno Junction decision against `tfw-conwy-valley.toml`:
    // despite genuine physical track sharing existing at Saltney Junction
    // on the final approach into Chester (see the comment above Chester in
    // `lines/tfw-shrewsbury-chester.toml`), `tfw-north-wales-coast.toml`
    // uses one single whole-line segment name for its entire route, so
    // reusing it here would incorrectly mark that line's whole route
    // (Rhyl, Bangor, Holyhead, etc.) as shared with this line. So the two
    // files use distinct segment names at Chester, and an incident there
    // should match both lines independently, each still classified as
    // `MatchScope::ExclusiveSegment` (not `SharedSegment`).
    //
    // Updated by the 2026-09-21 real-world-sanity review: this station
    // data used to live in `tfw-marches.toml` (Task 11.5), which
    // mistakenly modelled the "Marches Line" as running Shrewsbury-Chester.
    // It has been moved to a new, separately-branded
    // `lines/tfw-shrewsbury-chester.toml` file -- the real "Marches Line"
    // (`tfw-marches.toml`) now runs Shrewsbury-Crewe instead and no longer
    // touches Chester at all. See `lines/tfw-marches.toml`'s own
    // top-of-file correction note for the full story.
    #[test]
    fn chester_station_overlap_matches_both_lines_as_exclusive() {
        // Chester is also wcml-north-wales.toml's (Batch 1) and
        // merseyrail-wirral.toml's (Batch 12) own station. `wcml-north-
        // wales.toml` reuses `tfw-north-wales-coast.toml`'s own segment
        // name here (2026-09-21 real-world-sanity review, reversing the
        // original station-overlap-only ruling -- see either file's own
        // "TfW/WCML shared corridor"/"Cross-batch note" comment: Avanti and
        // TfW genuinely run over the same physical Chester-Holyhead main
        // line, not two corridors meeting at a point), so those two are
        // now a genuine SharedSegment pair here. `merseyrail-wirral.toml`
        // stays on its own exclusive segment (`merseyrail-wirral-chester`)
        // -- an independent ExclusiveSegment match by the ordinary
        // station-overlap pattern, same as `tfw-shrewsbury-chester.toml`
        // (a genuinely different physical corridor into Chester, per that
        // file's own comment). `northern-mid-cheshire.toml` (North West
        // England line-coverage audit, 2026-09-21) adds a fifth line here:
        // its own approach to Chester via Northwich, again station overlap
        // only, no shared track.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-8",
            "Points failure at Chester",
            "Points failure causing delays to Transport for Wales services.",
            &["AW"],
            &["CTR"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "tfw-shrewsbury-chester".to_string(),
                "tfw-north-wales-coast".to_string(),
                "wcml-north-wales".to_string(),
                "merseyrail-wirral".to_string(),
                "northern-mid-cheshire".to_string(),
            ])
        );
        for m in &matches {
            let expected =
                if m.line.id == "tfw-north-wales-coast" || m.line.id == "wcml-north-wales" {
                    MatchScope::SharedSegment
                } else {
                    MatchScope::ExclusiveSegment
                };
            assert_eq!(m.scope, expected, "{} should be {:?}", m.line.id, expected);
        }
    }

    // `tfw-marches`'s corrected Shrewsbury-Crewe stretch (2026-09-21
    // real-world-sanity review). An incident on a station on that stretch,
    // well away from the Shrewsbury/Craven Arms coordination point, should
    // match only this line, as `MatchScope::ExclusiveSegment` -- e.g.
    // Nantwich, which sits on `tfw-marches-crewe`, a segment no other line
    // in the catalogue uses.
    #[test]
    fn marches_crewe_stretch_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-16",
            "Signal failure at Nantwich",
            "Signal failure causing delays to Transport for Wales services.",
            &["AW"],
            &["NAN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["tfw-marches".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `tfw-shrewsbury-chester` (new file, 2026-09-21 real-world-sanity
    // review -- split out of the old, mis-scoped `tfw-marches.toml`). An
    // incident well away from its own Chester/Shrewsbury station-overlap
    // points should match only this line, as `MatchScope::ExclusiveSegment`
    // -- e.g. Ruabon, which sits on `tfw-shrewsbury-chester`, a segment no
    // other line in the catalogue uses.
    #[test]
    fn shrewsbury_chester_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-17",
            "Signal failure at Ruabon",
            "Signal failure causing delays to Transport for Wales services.",
            &["AW"],
            &["RUA"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["tfw-shrewsbury-chester".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Shrewsbury is a genuine three-way overlap point, introduced by Task
    // 11.5: `tfw-cambrian.toml` tags SHR with its own exclusive
    // `tfw-cambrian-trunk` segment (the Cambrian Line diverges west
    // immediately -- station-overlap only there, per that file's own
    // comment), while `tfw-marches.toml` and `tfw-heart-of-wales.toml` both
    // tag SHR with the shared `tfw-heart-of-wales-shrewsbury` segment (see
    // the comment above Craven Arms in `lines/tfw-marches.toml` for the
    // shared-trunk sourcing). So a single incident at SHR should resolve
    // differently per line, all at once: Cambrian stays
    // `MatchScope::ExclusiveSegment`, while Marches and Heart of Wales are
    // both `MatchScope::SharedSegment`.
    //
    // Updated by the Midlands batch 2: `wmr-shrewsbury-local.toml` and
    // `wmr-darlaston-line.toml` (both West Midlands Railway) also terminate
    // at Shrewsbury, sharing the literal `wmr-wolverhampton-shrewsbury`
    // segment name between themselves (a genuine shared approach from
    // Wolverhampton) -- a second, independent SharedSegment pair, on a
    // different segment name from the TfW trio, so both now also match as
    // SharedSegment with each other but ExclusiveSegment relative to every
    // TfW file here.
    //
    // Updated again by the 2026-09-21 real-world-sanity review:
    // `lines/tfw-shrewsbury-chester.toml` (split out of the old, mis-scoped
    // `tfw-marches.toml`) also has its own SHR entry, on its own exclusive
    // `tfw-shrewsbury-chester` segment (station-overlap only, same
    // reasoning as Cambrian at this station -- see that file's own SHR
    // comment) -- a sixth line now resolves at this one incident, as a
    // seventh independent ExclusiveSegment match.
    #[test]
    fn shrewsbury_three_way_overlap_resolves_per_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-9",
            "Signal failure at Shrewsbury",
            "Signal failure causing delays to Transport for Wales services.",
            &["AW"],
            &["SHR"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let scopes: HashMap<String, MatchScope> = matches
            .iter()
            .map(|m| (m.line.id.clone(), m.scope))
            .collect();
        assert_eq!(
            scopes.keys().cloned().collect::<HashSet<String>>(),
            HashSet::from([
                "tfw-cambrian".to_string(),
                "tfw-marches".to_string(),
                "tfw-heart-of-wales".to_string(),
                "wmr-shrewsbury-local".to_string(),
                "wmr-darlaston-line".to_string(),
                "tfw-shrewsbury-chester".to_string(),
            ])
        );
        assert_eq!(scopes["tfw-cambrian"], MatchScope::ExclusiveSegment);
        assert_eq!(scopes["tfw-marches"], MatchScope::SharedSegment);
        assert_eq!(scopes["tfw-heart-of-wales"], MatchScope::SharedSegment);
        assert_eq!(scopes["wmr-shrewsbury-local"], MatchScope::SharedSegment);
        assert_eq!(scopes["wmr-darlaston-line"], MatchScope::SharedSegment);
        assert_eq!(
            scopes["tfw-shrewsbury-chester"],
            MatchScope::ExclusiveSegment
        );
    }

    // `tfw-valley-rhymney` (originally `tfw-valley-lines-north`, Task 11.6;
    // split into `tfw-valley-rhymney.toml`, `tfw-valley-merthyr.toml` and
    // `tfw-valley-rhondda.toml` by a later data-driven catalogue audit -- see
    // those files' own "Split history" comments). An incident on a station
    // well into the Rhymney Line's own exclusive corridor (its own segment,
    // `tfw-valley-rhymney`, used by no other line in the catalogue) should
    // match only this line, as `MatchScope::ExclusiveSegment` -- e.g.
    // Caerphilly.
    #[test]
    fn valley_rhymney_exclusive_segment_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-10",
            "Signal failure at Caerphilly",
            "Signal failure causing delays to Transport for Wales services.",
            &["AW"],
            &["CPH"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["tfw-valley-rhymney".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `tfw-valley-rhymney`/`tfw-valley-merthyr`/`tfw-valley-rhondda`
    // (originally `tfw-valley-lines-north`, Task 11.6, later split three
    // ways) x `tfw-coryton-line`/`tfw-cardiff-bay-line` (originally
    // `tfw-valley-lines-south`, Task 11.7, itself later split three ways by
    // the 2026-09-21 real-world-sanity review -- see `tfw-city-line.toml`'s
    // own split-history comment): the Cardiff hub segment-sharing decision.
    // Task 11.7 independently verified genuine same-platform sharing (all
    // routes call at Cardiff Central and/or Cardiff Queen Street) and
    // deliberately reused `tfw-valley-lines-north.toml`'s
    // `tfw-valley-cardiff-hub` segment name in the former
    // `tfw-valley-lines-south.toml`. Cardiff Queen Street specifically is
    // only ever the origin for the Coryton Line and Cardiff Bay Line (not
    // the City Line, which reaches Cardiff Central instead) -- both true
    // before and after the south-side split -- so an incident there
    // propagates to Rhymney/Merthyr/Rhondda (all three touch both CDF and
    // CDQ) plus `tfw-coryton-line` and `tfw-cardiff-bay-line`, five lines in
    // total, all `MatchScope::SharedSegment`, mirroring
    // `xc_hub_incident_propagates_to_every_cross_country_arm`.
    #[test]
    fn valley_lines_cardiff_hub_shared_segment_propagates() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-11",
            "Points failure at Cardiff Queen Street",
            "Points failure causing delays to Transport for Wales services.",
            &["AW"],
            &["CDQ"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "tfw-valley-rhymney".to_string(),
                "tfw-valley-merthyr".to_string(),
                "tfw-valley-rhondda".to_string(),
                "tfw-coryton-line".to_string(),
                "tfw-cardiff-bay-line".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment",
                m.line.id
            );
        }
    }

    // `tfw-coryton-line` (originally part of `tfw-valley-lines-south`, Task
    // 11.7; split out into its own file by the 2026-09-21 real-world-sanity
    // review, for consistency with the same real-world-branding split
    // already applied on the north side of the network -- see
    // `tfw-city-line.toml`'s own split-history comment). An incident on a
    // station well into the Coryton Line's own exclusive corridor (its own
    // segment, `tfw-coryton-line`, used by no other line in the catalogue)
    // should match only this line, as `MatchScope::ExclusiveSegment` --
    // e.g. Birchgrove.
    #[test]
    fn coryton_line_exclusive_segment_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-12",
            "Signal failure at Birchgrove",
            "Signal failure causing delays to Transport for Wales services.",
            &["AW"],
            &["BCG"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["tfw-coryton-line".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `tfw-valley-merthyr`/`tfw-valley-rhondda` (originally
    // `tfw-valley-lines-north`, Task 11.6, later split three ways) x
    // `tfw-city-line` (originally part of `tfw-valley-lines-south`, Task
    // 11.7, split out into its own file by the 2026-09-21 real-world-sanity
    // review): the Radyr junction-sharing decision (see
    // `tfw-city-line.toml`'s own comment). Radyr carries its own dedicated,
    // Radyr-only segment name, `tfw-valley-radyr-junction`, minted in all
    // three files that touch it today (fix round 1: this used to reuse
    // `tfw-valley-lines-north.toml`'s `tfw-valley-taff-trunk` segment name
    // for Radyr alone, which incorrectly made every other station on that
    // segment register as shared too -- see
    // `valley_taff_trunk_shared_segment_propagates_after_split` below for the
    // regression test guarding against the equivalent mistake post-split) --
    // so an incident at Radyr itself should propagate to all three files
    // that carry it, all `MatchScope::SharedSegment`. (Rhymney does not
    // carry Radyr at all -- it takes its own separate corridor via the
    // Caerphilly Tunnel -- and neither `tfw-coryton-line` nor
    // `tfw-cardiff-bay-line` ever touch Radyr at all, so none of them
    // appear here.)
    #[test]
    fn valley_lines_radyr_junction_shared_segment_propagates() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-13",
            "Points failure at Radyr",
            "Points failure causing delays to Transport for Wales services.",
            &["AW"],
            &["RDR"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "tfw-valley-merthyr".to_string(),
                "tfw-valley-rhondda".to_string(),
                "tfw-city-line".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment",
                m.line.id
            );
        }
    }

    // `tfw-valley-merthyr` x `tfw-valley-rhondda`: a genuine behavioural
    // change introduced by splitting the former `tfw-valley-lines-north`
    // (Task 11.6) into three separate line files. Pontypridd (PPD) sits on
    // `tfw-valley-taff-trunk`, the segment the Merthyr/Aberdare and Rhondda
    // lines share on their common approach from Cardiff before diverging at
    // Pontypridd. Before the split, all four branches lived under one line
    // `id` (`tfw-valley-lines-north`), so `SegmentRegistry` (which indexes
    // sharing by segment name across distinct line IDs, not by station count
    // within one file) resolved this trunk as `MatchScope::ExclusiveSegment`
    // -- see this test's predecessor,
    // `valley_lines_north_exclusive_pontypridd_segment_does_not_propagate`,
    // which guarded the original Task 11.7 bug where south's Radyr entry
    // reused `tfw-valley-taff-trunk` verbatim (incorrectly making Pontypridd
    // and five other trunk stations register as shared with the City Line
    // too). Splitting Merthyr/Aberdare and Rhondda into separate line IDs
    // while deliberately keeping the same `tfw-valley-taff-trunk` segment
    // name (see `tfw-valley-merthyr.toml`'s own "Taff Vale trunk segment
    // sharing" comment) means an incident anywhere on this trunk now
    // correctly propagates to both lines as `MatchScope::SharedSegment` --
    // more accurate than before, not a regression: passengers on both lines
    // are genuinely affected by an incident on their shared approach. This
    // must still NOT extend to `tfw-city-line.toml` (formerly part of
    // `tfw-valley-lines-south.toml`): the City Line only touches this
    // trunk at Radyr itself, via its own dedicated
    // `tfw-valley-radyr-junction` segment, not at Pontypridd or any of the
    // other five stations on `tfw-valley-taff-trunk` (Cathays, Llandaf,
    // Taffs Well, Treforest, Treforest Estate).
    #[test]
    fn valley_taff_trunk_shared_segment_propagates_after_split() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "AW-14",
            "Signal failure at Pontypridd",
            "Signal failure causing delays to Transport for Wales services.",
            &["AW"],
            &["PPD"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "tfw-valley-merthyr".to_string(),
                "tfw-valley-rhondda".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment",
                m.line.id
            );
        }
    }

    // southeastern-main-line was the first Southeastern file in this
    // catalogue (Batch 5, Task 5.1). At the time it shared no segment with
    // any already-curated line — thameslink-core.toml overlaps at LBG by
    // station only, not by segment. That's still true of `seml-weald`
    // specifically (Task 5.3's southeastern-highspeed.toml overlaps this
    // line only at AFK, by station, on its own `hs1-ashford` segment - see
    // afk_station_overlap_matches_both_seml_and_hs1_as_independent_exclusive_segments
    // below), so an incident on a `seml-weald` station untouched by any
    // other file should still stay exclusive to this line alone.
    //
    // Kent/Sussex batch: this test used to fire at Tonbridge (TON), but
    // southeastern-hastings-line.toml now also has a station there (see
    // afk_station_overlap_matches_both_seml_and_hs1_as_independent_exclusive_segments's
    // own sibling tests for that station-overlap pattern) - TON is no
    // longer a station this file has all to itself. Moved to Marden (MRN),
    // a `seml-weald` station still untouched by any other file (grepped
    // before making this change), to keep testing what this test is
    // actually meant to test: a genuinely exclusive segment not
    // propagating anywhere.
    #[test]
    fn seml_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-1",
            "Signal failure at Marden",
            "Signal failure causing delays to Southeastern services.",
            &["SE"],
            &["MRN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["southeastern-main-line".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // southeastern-chatham (Batch 5, Task 5.2) shares no segment with any
    // other already-curated line. Its DVP overlap with
    // southeastern-main-line.toml is station-only (the two lines approach
    // Dover Priory from physically different directions), documented in
    // southeastern-chatham.toml's own header comment. Since Task 5.3
    // (southeastern-highspeed.toml) reused `chatham-medway`/
    // `chatham-coastal` verbatim as a genuine shared trunk (see
    // hs1_chatham_medway_shared_segment_incident_propagates_to_both_lines
    // below), this test now exercises a `chatham-medway` station the new
    // file doesn't touch (Meopham, between Longfield and Sole Street -
    // west of Strood, where the Javelin's North Kent pattern joins), to
    // confirm the untouched part of chatham-medway still stays exclusive
    // of southeastern-highspeed.toml.
    //
    // Updated (Southeastern real-world-sanity review): southeastern-
    // sheerness-line.toml (a new file from that review) also reuses
    // `chatham-medway` verbatim for its own VIC-Sheerness through service as
    // far as Sittingbourne, and its own FNR-SOR stretch includes Meopham -
    // so this incident now also matches that file, and both lines report
    // SharedSegment for this exact station.
    #[test]
    fn chatham_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-2",
            "Signal failure at Meopham",
            "Signal failure causing delays to Southeastern services.",
            &["SE"],
            &["MEP"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "southeastern-chatham".to_string(),
                "southeastern-sheerness-line".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment",
                m.line.id
            );
        }
    }

    // The two branches within this one file (chatham-coastal via Ramsgate,
    // chatham-dover via Canterbury East) are both exclusive to
    // southeastern-chatham - an incident on the Dover branch shouldn't
    // pull in southeastern-main-line even though both lines terminate at
    // DVP (station overlap only, not a shared segment; see the file's own
    // header comment).
    #[test]
    fn chatham_dover_branch_incident_does_not_propagate_to_seml() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-3",
            "Points failure at Adisham",
            "Points failure causing delays to Southeastern services.",
            &["SE"],
            &["ADM"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["southeastern-chatham".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // southeastern-highspeed (Batch 5, Task 5.3) is the domestic "Javelin"
    // HS1 service. Its St Pancras - Stratford International - Ebbsfleet
    // International trunk (`hs1-domestic`) is purpose-built high-speed
    // infrastructure no other curated line touches, so an incident there
    // should stay exclusive to this file - mirrors
    // seml_exclusive_segment_incident_does_not_propagate above.
    #[test]
    fn hs1_domestic_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-4",
            "Overhead line problem at Stratford International",
            "An overhead line problem is causing delays to Southeastern high speed services.",
            &["SE"],
            &["SFA"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["southeastern-highspeed".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Ashford International (AFK) is on both southeastern-main-line.toml
    // (`seml-weald`) and southeastern-highspeed.toml (`hs1-ashford`) -
    // deliberately different segment names, because HS1 reaches Ashford via
    // its own purpose-built alignment through the North Downs Tunnel, not
    // via SEML's classic Sevenoaks/Tonbridge route. Per this task's brief
    // and both files' header comments, that's station overlap, not a
    // shared trunk: an AFK incident should match both lines independently,
    // each still scoped ExclusiveSegment, never SharedSegment.
    //
    // Kent/Sussex batch: southeastern-maidstone-east.toml and
    // southeastern-canterbury-west.toml both also terminate/junction at AFK
    // (their own `maidstone-east-line`/`canterbury-west-line` segments,
    // neither reusing `seml-weald` or `hs1-ashford`) - same station-overlap
    // treatment, added here rather than left to silently under-match.
    #[test]
    fn afk_station_overlap_matches_both_seml_and_hs1_as_independent_exclusive_segments() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-5",
            "Signal failure at Ashford International",
            "Signal failure causing delays to Southeastern services.",
            &["SE"],
            &["AFK"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "southeastern-main-line".to_string(),
                "southeastern-highspeed".to_string(),
                "southeastern-maidstone-east".to_string(),
                "southeastern-canterbury-west".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment, not shared",
                m.line.id
            );
        }
    }

    // Past Ashford, the Javelin runs over the same physical track
    // southeastern-main-line.toml documents as `seml-coast` (there is only
    // one route from Ashford to Dover via Folkestone). This file
    // deliberately does NOT reuse that segment name, though: `is_shared`
    // treats an entire segment name as shared the moment two files use it,
    // and this file doesn't call at every `seml-coast` station (e.g. it
    // skips Westenhanger/Sandling) - reusing the name verbatim would
    // therefore also mark those untouched stations SharedSegment. So this
    // is kept as station overlap on this file's own `hs1-ashford` segment
    // (see the file's header comment) - both lines still match a
    // Folkestone Central incident, but each independently as
    // ExclusiveSegment.
    #[test]
    fn hs1_ashford_station_overlap_matches_both_seml_and_hs1_as_independent_exclusive_segments() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-6",
            "Points failure at Folkestone Central",
            "Points failure causing delays to Southeastern services.",
            &["SE"],
            &["FKC"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "southeastern-main-line".to_string(),
                "southeastern-highspeed".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment, not shared",
                m.line.id
            );
        }
    }

    // Same reasoning again for the North Kent pattern: from Strood onward
    // this file's `hs1-northkent` segment runs over the same physical
    // track as southeastern-chatham.toml's `chatham-medway`/
    // `chatham-coastal`. Originally the name wasn't reused (this file
    // doesn't touch e.g. Longfield/Meopham/Sole Street), so it was modelled
    // as station overlap, not a shared trunk.
    //
    // REVIEW FIX (shared-segment structural review, review2-shared-
    // segments): resolved by splitting the segment at Strood (the actual
    // physical boundary - this file's own Gravesend approach is genuinely
    // exclusive, but SOO onward is genuinely the same track as
    // southeastern-chatham.toml) rather than declining to share the whole
    // stretch. SOO through RAM is now `strood-ramsgate-corridor`, reused
    // verbatim by both files - see southeastern-chatham.toml's own header
    // comment. A Ramsgate incident is therefore now a genuine SharedSegment
    // between southeastern-chatham and southeastern-highspeed.
    //
    // Kent/Sussex batch: southeastern-canterbury-west.toml also terminates
    // at RAM (its own `canterbury-west-line` segment, approached from
    // Ashford/Canterbury West rather than Faversham/Margate) - untouched by
    // this review, stays independently ExclusiveSegment.
    #[test]
    fn hs1_northkent_ramsgate_incident_matches_chatham_shared_and_canterbury_west_exclusive() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-7",
            "Signal failure at Ramsgate",
            "Signal failure causing delays to Southeastern services.",
            &["SE"],
            &["RAM"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "southeastern-chatham".to_string(),
                "southeastern-highspeed".to_string(),
                "southeastern-canterbury-west".to_string(),
            ])
        );
        for m in &matches {
            let expected = if m.line.id == "southeastern-canterbury-west" {
                MatchScope::ExclusiveSegment
            } else {
                MatchScope::SharedSegment
            };
            assert_eq!(m.scope, expected, "{} should be {:?}", m.line.id, expected);
        }
    }

    // southeastern-bexleyheath and southeastern-dartford-loop (a split of
    // the former southeastern-metro-north-kent, Batch 5 Task 5.4, per a
    // later data-driven line-definition audit) each cover one of the
    // Bexleyheath line/Dartford Loop line, both diverging from a shared
    // London Bridge-Lewisham trunk (`southeastern-lewisham-corridor`,
    // reused verbatim by both files - see each file's own SEGMENT NAMING
    // comment). Per the pre-split file's own header comment (FINDING 2),
    // research for that task could NOT confirm the gap analysis's premise
    // that Thameslink genuinely shares that trunk under normal service - so
    // no OTHER sibling file uses `southeastern-lewisham-corridor`, and an
    // incident on this line's own exclusive Bexleyheath branch (past the
    // Lewisham junction) should stay exclusive to this line alone. Mirrors
    // swr_exclusive_segment_incident_does_not_propagate and
    // elizabeth_branch_incident_stays_on_its_branch above.
    #[test]
    fn bexleyheath_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-8",
            "Points failure at Bexleyheath",
            "Points failure causing delays to Southeastern services.",
            &["SE"],
            &["BXH"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["southeastern-bexleyheath".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Same for the Dartford Loop branch (diverging from the shared trunk at
    // Hither Green rather than at Lewisham itself, and now its own separate
    // file, southeastern-dartford-loop.toml) - an incident on it should
    // also stay exclusive to this line, and shouldn't spuriously pull in
    // the Bexleyheath line's own file either (the two branches use
    // different segment names, `bexleyheath-branch` vs
    // `dartford-loop-branch`, despite sharing the same file before the
    // split and the same trunk segment today).
    #[test]
    fn dartford_loop_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-9",
            "Signal failure at Sidcup",
            "Signal failure causing delays to Southeastern services.",
            &["SE"],
            &["SID"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["southeastern-dartford-loop".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // LBG is also thameslink-core.toml's own terminus (its own segment
    // ends there too) and southeastern-main-line.toml's own `seml-london`
    // station, but per the pre-split senk file's header comment that's
    // station overlap only, not a shared trunk - same judgment
    // southeastern-main-line.toml already made for LBG/Thameslink. Mirrors
    // afk_station_overlap_matches_both_seml_and_hs1_as_independent_exclusive_segments
    // above.
    //
    // Task 5.5 (southeastern-hayes-line.toml) added a fourth LBG overlap:
    // its own `hayes-london` segment also calls at LBG (see that file's own
    // header comment for why it does NOT reuse senk's
    // `southeastern-lewisham-corridor` name despite passing through the
    // same station - the Hayes line's own calling pattern skips New
    // Cross/St Johns, so the two runs aren't confirmed to share physical
    // track for that stretch).
    //
    // Updated by Task 5.6 (southern-brighton-main-line.toml): that file also
    // has a station at LBG (its own `southern-bml-north` segment, named as a
    // courtesy hand-off for Task 5.14's thameslink-southern.toml, not yet a
    // real cross-file shared trunk - see that file's own header comment), so
    // it now joins this set as a fifth independent exclusive-segment match.
    //
    // Updated by Task 5.9 (southern-oxted-uckfield.toml): that file also has
    // a station at LBG (its own `oxted-london-bridge-approach` segment - the
    // usual London terminus for its Uckfield branch service, see that
    // file's own header comment for why this is station overlap, not a
    // shared trunk, with every other line here), so it now joins this set
    // as a sixth independent exclusive-segment match.
    //
    // Updated by Task 5.14 (thameslink-southern.toml): that file's own
    // Brighton branch also meets London Bridge here. An earlier draft
    // reused southern-brighton-main-line.toml's own `southern-bml-north`
    // segment name here, asserting a genuine SharedSegment pair - on
    // review this was withdrawn (thetrainline.com, the source relied on to
    // clear COMMON.md's bar, is not one of its four approved second-source
    // categories and doesn't attest physical track sharing anyway - see
    // thameslink-southern.toml's own BRIGHTON BRANCH header comment for the
    // full writeup). thameslink-southern now uses its own segment name
    // here (`thameslink-brighton`), so it joins this set as a seventh
    // independent ExclusiveSegment station-overlap match, same treatment as
    // every other line in this set.
    //
    // Updated by the southeastern-metro-north-kent split
    // (southeastern-bexleyheath.toml/southeastern-dartford-loop.toml, per a
    // data-driven line-definition audit): the former single senk file's own
    // LBG entry (`southeastern-lewisham-corridor`) is now duplicated
    // verbatim across BOTH of these new files (the same shared London
    // throat both lines still cross before diverging), so this set now has
    // an eighth independent match - but unlike every other line here, these
    // two are NOT independent ExclusiveSegment matches of each other: they
    // share the literal segment name, so the registry correctly promotes
    // BOTH to SharedSegment for this incident (mirrors
    // swr_shared_trunk_incident_propagates's per-family SharedSegment
    // shape). Every other line in this set keeps its own distinct segment
    // name at LBG and stays ExclusiveSegment.
    //
    // Kent/Sussex batch: southeastern-north-kent.toml (its own
    // `southeastern-north-kent` segment, the Greenwich-line approach) also
    // calls at LBG - originally station overlap, a ninth independent match.
    // (southeastern-maidstone-east.toml does NOT touch LBG - its own London
    // approach is via Herne Hill/Bromley South, the same Victoria-side
    // alignment southeastern-chatham.toml already models, which never
    // reaches London Bridge.)
    //
    // Updated (real-world sanity review): thameslink-rainham.toml's own
    // Luton-Rainham route also runs the Thameslink core's full length
    // through London Bridge (its own `thameslink-rainham` segment) before
    // diverging towards Greenwich - a tenth independent ExclusiveSegment
    // match.
    //
    // REVIEW FIX (shared-segment structural review, review2-shared-
    // segments): a cross-file review found southeastern-main-line.toml,
    // southeastern-hayes-line.toml and southeastern-north-kent.toml had each
    // kept their own separate name for this exact Charing Cross/Cannon
    // Street-Waterloo East-London Bridge-New Cross-St Johns-Lewisham
    // stretch, even though it is the same physical four-track approach
    // southeastern-bexleyheath.toml/southeastern-dartford-loop.toml already
    // correctly share as `southeastern-lewisham-corridor`. All three have
    // now been fixed to reuse that name too (see each file's own header
    // comment) - so this set now has FIVE files sharing
    // `southeastern-lewisham-corridor` at LBG (bexleyheath, dartford-loop,
    // main-line, hayes-line, north-kent), all SharedSegment together.
    // thameslink-core and thameslink-rainham each keep their own distinct
    // segment name here (`thameslink-core`/`thameslink-rainham`, untouched
    // by this review) and stay independently ExclusiveSegment. southern-
    // brighton-main-line and thameslink-southern are ALSO now a
    // SharedSegment pair with each other at LBG (their own separate
    // `brighton-main-line-north` unification - see Group 3 of the same
    // review, southern-brighton-main-line.toml's own SEGMENT NAMING
    // comment), independent of the southeastern-lewisham-corridor family.
    // southern-oxted-uckfield's own `oxted-london-bridge-approach` is
    // untouched by this review and stays independently ExclusiveSegment.
    //
    // Updated (Southern real-world-sanity review): southern-metro-crystal-
    // palace.toml also calls at LBG, on its own exclusive
    // `southern-metro-beckenham-approach` segment (not reused by
    // southern-metro-sutton.toml, which doesn't reach LBG at all) - an
    // eleventh independent ExclusiveSegment match.
    //
    // The set below and the function name cover eleven lines in total: five
    // of them (southeastern-{bexleyheath,dartford-loop,main-line,hayes-line,
    // north-kent}) share southeastern-lewisham-corridor as one SharedSegment
    // family, southern-brighton-main-line and thameslink-southern are a
    // second, independent SharedSegment pair, and the remaining four
    // (thameslink-core, thameslink-rainham, southern-oxted-uckfield,
    // southern-metro-crystal-palace) are each their own independent
    // ExclusiveSegment match.
    #[test]
    fn lbg_station_overlap_spans_ten_lines_bexleyheath_and_dartford_loop_share_the_trunk() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-10",
            "Signal failure at London Bridge",
            "Signal failure causing delays to Southeastern services.",
            &["SE"],
            &["LBG"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "southeastern-bexleyheath".to_string(),
                "southeastern-dartford-loop".to_string(),
                "thameslink-core".to_string(),
                "southeastern-main-line".to_string(),
                "southeastern-hayes-line".to_string(),
                "southern-brighton-main-line".to_string(),
                "southern-oxted-uckfield".to_string(),
                "thameslink-southern".to_string(),
                "southeastern-north-kent".to_string(),
                "thameslink-rainham".to_string(),
                "southern-metro-crystal-palace".to_string(),
            ])
        );
        for m in &matches {
            let expected = match m.line.id.as_str() {
                "southeastern-bexleyheath"
                | "southeastern-dartford-loop"
                | "southeastern-main-line"
                | "southeastern-hayes-line"
                | "southeastern-north-kent"
                | "southern-brighton-main-line"
                | "thameslink-southern" => MatchScope::SharedSegment,
                _ => MatchScope::ExclusiveSegment,
            };
            assert_eq!(m.scope, expected, "{} should be {:?}", m.line.id, expected);
        }
    }

    // Task 5.5 (southeastern-hayes-line.toml). An incident on the Hayes
    // line's own exclusive branch (past Lewisham, e.g. West Wickham) should
    // stay exclusive to this line - mirrors
    // swr_exclusive_segment_incident_does_not_propagate and
    // elizabeth_branch_incident_stays_on_its_branch above.
    #[test]
    fn hayes_branch_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-11",
            "Signal failure at West Wickham",
            "Signal failure causing delays to Southeastern services.",
            &["SE"],
            &["WWI"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["southeastern-hayes-line".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 5.5 (southeastern-hayes-line.toml). Lewisham (LEW) is a station
    // overlap between this file's own `hayes-london` segment and the
    // pre-split senk file's `southeastern-lewisham-corridor` - two
    // different segment names for the same station, per this file's own
    // header comment (not a shared trunk, since the Hayes line's own
    // calling pattern diverges from senk's before Lewisham).
    //
    // Updated by Task 5.3 (southeastern-main-line.toml, station-catalogue-
    // completeness plan): that file's own research confirmed New Cross, St
    // Johns, Lewisham and Hither Green all sit on the South Eastern Main
    // Line's own physical alignment toward Orpington (not just on the
    // Dartford Loop/Bexleyheath corridor's distinct tracks), so it now adds
    // Lewisham too, on its own `seml-london` segment - a third independent
    // exclusive-segment station overlap here, same treatment as every other
    // line in this set.
    //
    // Updated by the southeastern-metro-north-kent split
    // (southeastern-bexleyheath.toml/southeastern-dartford-loop.toml, per a
    // data-driven line-definition audit): LEW is the Bexleyheath line's own
    // diverging junction, so it stays on `southeastern-lewisham-corridor`
    // in BOTH new files (the same shared trunk each still crosses up to and
    // including Lewisham). That originally gave a fourth match here, with
    // southeastern-hayes-line/southeastern-main-line each using their own
    // distinct segment name at LEW (station overlap only).
    //
    // REVIEW FIX (shared-segment structural review, review2-shared-
    // segments): southeastern-hayes-line.toml and southeastern-main-
    // line.toml have both now been fixed to reuse
    // `southeastern-lewisham-corridor` at LEW too (see each file's own
    // header comment) - so all FOUR lines here now share the literal
    // segment name and the registry correctly promotes all four to
    // SharedSegment (mirrors swr_shared_trunk_incident_propagates's
    // per-family SharedSegment shape; see also
    // lbg_station_overlap_spans_nine_lines_bexleyheath_and_dartford_loop_share_the_trunk
    // above for the same pattern at London Bridge). southeastern-north-
    // kent.toml does not reach LEW at all (its own exclusive Greenwich-line
    // stretch diverges earlier, at New Cross), so it is not in this set.
    #[test]
    fn lew_station_overlap_matches_four_lines_bexleyheath_and_dartford_loop_share_the_trunk() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-12",
            "Signal failure at Lewisham",
            "Signal failure causing delays to Southeastern services.",
            &["SE"],
            &["LEW"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "southeastern-hayes-line".to_string(),
                "southeastern-bexleyheath".to_string(),
                "southeastern-dartford-loop".to_string(),
                "southeastern-main-line".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment on southeastern-lewisham-corridor",
                m.line.id
            );
        }
    }

    // REGRESSION TEST for this batch's final-review fix: sibling-line names
    // in an incident's text must not veto a genuine station hit.
    //
    // `is_excluded` is a HARD VETO evaluated in `lines_affected_by` BEFORE
    // `match_one` ever looks at `affected_stations`, so an
    // `excluded_keywords` entry naming a sibling line suppresses that file
    // even when the incident lists a CRS genuinely on it. Before this fix,
    // southeastern-hayes-line.toml excluded "Dartford Loop line" and the
    // pre-split southeastern-metro-north-kent.toml excluded "Hayes line",
    // so a real incident naming BOTH routes and listing a station both
    // files list (LEW - Lewisham, where the two corridors diverge, and also
    // CHX/LBG) vetoed BOTH files at once and returned zero Southeastern
    // matches - the exact multi-line incident these two files were written
    // to model. The vetoes have been removed from both files'
    // `excluded_keywords`; the station-CRS path already disambiguates this
    // correctly, as
    // lew_station_overlap_matches_four_lines_bexleyheath_and_dartford_loop_share_the_trunk
    // above shows for the no-line-names-in-text case. Neither
    // southeastern-bexleyheath.toml nor southeastern-dartford-loop.toml
    // (the senk split) excludes the other's own line name either, for the
    // same reason - see each file's own `excluded_keywords` comment.
    //
    // The veto MECHANISM itself is unchanged and still proven by
    // excluded_keyword_vetoes_match above (a genuinely foreign service on a
    // line that shares no station with the excluding file) - only the
    // specific data entries that misapplied it to station-sharing siblings
    // were removed.
    //
    // Updated by Task 5.3 (southeastern-main-line.toml): that file now also
    // lists LEW (see the lew_station_overlap... update above) and its own
    // `excluded_keywords` is just ["Hastings line"], which this incident's
    // text doesn't contain, so it joins this set as a match with no veto
    // risk.
    //
    // Updated by the southeastern-metro-north-kent split
    // (southeastern-bexleyheath.toml/southeastern-dartford-loop.toml, per a
    // data-driven line-definition audit): both new files still list LEW on
    // their shared `southeastern-lewisham-corridor` segment, so this
    // incident matches four lines. Updated again by the shared-segment
    // structural review (review2-shared-segments): southeastern-hayes-
    // line.toml and southeastern-main-line.toml now also reuse
    // `southeastern-lewisham-corridor` at LEW (see
    // lew_station_overlap_matches_four_lines_bexleyheath_and_dartford_loop_share_the_trunk
    // above for the full write-up), so all four lines are now promoted to
    // SharedSegment together, not just the bexleyheath/dartford-loop pair.
    #[test]
    fn sibling_line_names_no_longer_veto_a_shared_station_hit() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-13",
            "Disruption between London Bridge and Lewisham",
            "Disruption between London Bridge and Lewisham affecting the Hayes line and the Dartford Loop line.",
            &["SE"],
            &["LEW"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "southeastern-hayes-line".to_string(),
                "southeastern-bexleyheath".to_string(),
                "southeastern-dartford-loop".to_string(),
                "southeastern-main-line".to_string(),
            ]),
            "all named/overlapping lines list LEW and must all match; before the fix each vetoed the other and this was empty"
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment on southeastern-lewisham-corridor",
                m.line.id
            );
        }
    }

    // Task 5.6 (southern-brighton-main-line.toml). An incident on this
    // line's own exclusive `southern-bml-south` segment (past the ECR
    // junction) should stay exclusive to this line alone - mirrors
    // swr_exclusive_segment_incident_does_not_propagate and
    // elizabeth_branch_incident_stays_on_its_branch above. Uses Hassocks
    // (HSK) rather than Brighton (BTN) itself: Task 5.7
    // (southern-coastway-east.toml) added its own station at BTN too (a
    // real station overlap, since Coastway East also originates there -
    // see that file's own header comment), so BTN alone no longer proves
    // "no other already-curated line touches this station" the way it did
    // when this test was first written - that overlap is now covered
    // separately by
    // btn_station_overlap_matches_coastway_east_and_brighton_main_line_as_independent_exclusive_segments
    // below. HSK remains exclusive to `southern-bml-south`.
    //
    // Updated by Task 5.14 (thameslink-southern.toml): that file's own
    // Brighton branch also calls at Hassocks, originally on its own
    // `thameslink-brighton` segment - deliberately NOT the same name as this
    // line's own `southern-bml-south` at the time (see thameslink-
    // southern.toml's own PAST EAST CROYDON header comment history for why
    // that lead was originally documented but not acted on).
    //
    // REVIEW FIX (shared-segment structural review, review2-shared-
    // segments): that decision was reversed by a later cross-file review -
    // `southern-bml-south` (renamed `brighton-main-line-south`) is now
    // reused verbatim by thameslink-southern.toml too, so this is now a
    // genuine SharedSegment pair, not two independent ExclusiveSegment
    // matches - see both files' own updated header comments.
    #[test]
    fn southern_bml_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SN-1",
            "Signal failure at Hassocks",
            "Signal failure causing delays to Southern services.",
            &["SN"],
            &["HSK"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "southern-brighton-main-line".to_string(),
                "thameslink-southern".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment on brighton-main-line-south",
                m.line.id
            );
        }
    }

    // Task 5.6. NOTE ON WHAT THIS ACTUALLY PROVES (corrected after review -
    // the original comment here overclaimed): an incident tagged with the
    // `GX` operator at Preston Park - one of Gatwick Express's own
    // (Wikipedia-only-sourced, see southern-brighton-main-line.toml's own
    // header comment) peak-only calling points - still resolves to
    // `southern-brighton-main-line`, ExclusiveSegment. But this is a
    // station-hit match: `match_one`'s Tier 1 path matches on
    // `line.has_station(crs)` alone (see `common::LineDefinition::
    // has_station`) and never consults `operators`, so this test would pass
    // identically even if `GX` were never added to this line's `operators`
    // list - PRP is already this line's own station regardless of who's
    // asking. It does NOT exercise the `operators` field or prove the
    // fold-in decision "works" in the sense the file's own comment claims.
    // What it does confirm: a `GX`-tagged incident at a real Brighton Main
    // Line station isn't accidentally excluded or misrouted by this line's
    // matching logic - a narrower but still real assurance.
    //
    // The `operators` field's actual role - LDBWS sample classification via
    // `belongs_to_line` - is exercised by a separate test,
    // `belongs_to_line_gatwick_express_operator_folds_in_via_operators_list`
    // in `aggregation.rs`, which WOULD fail without `GX` in `operators`.
    //
    // Updated by Task 5.14 (thameslink-southern.toml): that file's own
    // Brighton branch also calls at Preston Park - originally its own
    // `thameslink-brighton` segment, a second independent ExclusiveSegment
    // station-overlap match.
    //
    // REVIEW FIX (shared-segment structural review, review2-shared-
    // segments): now a genuine SharedSegment pair on `brighton-main-line-
    // south` - see southern_bml_exclusive_segment_incident_does_not_propagate
    // above for the full write-up.
    #[test]
    fn southern_bml_station_hit_matches_regardless_of_gx_or_sn_operator_tag() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GX-1",
            "Delays at Preston Park",
            "Gatwick Express services are delayed at Preston Park.",
            &["GX"],
            &["PRP"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "southern-brighton-main-line".to_string(),
                "thameslink-southern".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment on brighton-main-line-south",
                m.line.id
            );
        }
    }

    // Task 5.7 (southern-coastway-east.toml). An incident on this line's
    // own exclusive `coastway-east-hastings` segment (past the Lewes
    // junction) should stay exclusive to this line alone - mirrors
    // swr_exclusive_segment_incident_does_not_propagate and
    // elizabeth_branch_incident_stays_on_its_branch above. This line is a
    // genuinely standalone route (no sibling file shares any of its
    // segment names - see that file's own header comment), so this also
    // confirms no accidental cross-file match: no shared-segment
    // propagation test is added for this line, per COMMON.md's own
    // "skip only for a genuinely standalone line" exception.
    #[test]
    fn coastway_east_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SN-2",
            "Signal failure at Eastbourne",
            "Signal failure causing delays to Southern services.",
            &["SN"],
            &["EBN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["southern-coastway-east".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 5.7, updated by Task 5.8. Brighton (BTN) is also
    // southern-brighton-main-line.toml's own terminus, but per this file's
    // own header comment that's station overlap only (the two lines
    // diverge immediately east of Brighton onto physically different
    // routes, and use different segment names - `coastway-east-brighton`
    // here vs `southern-bml-victoria`/`southern-bml-south` there) - same
    // judgment call as the LBG overlaps exercised above. Task 5.8
    // (southern-coastway-west.toml) added a third line at this same
    // station (its own `coastway-west-brighton` segment, diverging west
    // out of Brighton) - see that file's own header comment. Confirms an
    // incident at Brighton matches all three lines independently, each
    // still scoped ExclusiveSegment, never SharedSegment.
    // Updated by Task 5.14 (thameslink-southern.toml): that file's own
    // Brighton branch also terminates at Brighton - originally its own
    // `thameslink-brighton` segment, deliberately NOT the same name as
    // southern-brighton-main-line.toml's own `southern-bml-south` here (see
    // that file's own PAST EAST CROYDON header comment history for why the
    // southern-bml-south sharing lead found past East Croydon was
    // originally documented but not acted on).
    //
    // REVIEW FIX (shared-segment structural review, review2-shared-
    // segments): that decision was reversed - southern-brighton-main-
    // line.toml and thameslink-southern.toml now share `brighton-main-
    // line-south` verbatim (see southern_bml_exclusive_segment_incident_
    // does_not_propagate above), so those two are now SharedSegment with
    // each other here. southern-coastway-east.toml's/southern-coastway-
    // west.toml's own segment names are untouched by this review and stay
    // independently ExclusiveSegment.
    #[test]
    fn btn_station_overlap_matches_coastway_east_and_brighton_main_line_as_independent_exclusive_segments()
     {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SN-3",
            "Signal failure at Brighton",
            "Signal failure causing delays to Southern services.",
            &["SN"],
            &["BTN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "southern-coastway-east".to_string(),
                "southern-brighton-main-line".to_string(),
                "southern-coastway-west".to_string(),
                "thameslink-southern".to_string(),
            ])
        );
        for m in &matches {
            let expected = if m.line.id == "southern-brighton-main-line"
                || m.line.id == "thameslink-southern"
            {
                MatchScope::SharedSegment
            } else {
                MatchScope::ExclusiveSegment
            };
            assert_eq!(m.scope, expected, "{} should be {:?}", m.line.id, expected);
        }
    }

    // Task 5.8 (southern-coastway-west.toml). An incident on this line's
    // own exclusive `coastway-west-brighton` segment (a station no other
    // curated file touches) should stay exclusive to this line alone -
    // mirrors swr_exclusive_segment_incident_does_not_propagate and
    // coastway_east_exclusive_segment_incident_does_not_propagate above.
    // This line shares no segment name with any sibling file (its two
    // real overlaps - Brighton with southern-brighton-main-line.toml/
    // southern-coastway-east.toml, and Havant/Portsmouth with
    // swr-portsmouth-direct.toml - are both deliberately station overlap
    // only, per this file's own header comment), so no SharedSegment
    // propagation test is added for this line, per COMMON.md's own "skip
    // only for a genuinely standalone line" exception - the two station-
    // overlap tests below exercise both real overlaps instead.
    #[test]
    fn coastway_west_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SN-4",
            "Signal failure at Chichester",
            "Signal failure causing delays to Southern services.",
            &["SN"],
            &["CCH"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["southern-coastway-west".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 5.8. Havant (HAV) is also swr-portsmouth-direct.toml's own
    // station (on that file's `swr-portsmouth-direct` segment). Per this
    // file's own STATION-OVERLAP AT HAVANT/PORTSMOUTH header comment: this
    // is genuine physical track-sharing (Southern's West Coastway stopping
    // service and SWR's Portsmouth Direct service both run Havant-
    // Bedhampton-Hilsea-Fratton-Portsmouth into Portsmouth), but per the
    // cross-operator precedent xc-south-coast.toml/xc-manchester.toml
    // already set, segment names are only reused between sibling lines of
    // the SAME operator - SN and SW are different operators, so this is
    // treated as station overlap, not a shared trunk, same judgment call
    // as the AFK/Ramsgate overlaps between Southeastern and HS1 above.
    // Confirms an incident at Havant matches both lines independently,
    // each still scoped ExclusiveSegment, never SharedSegment.
    #[test]
    fn hav_station_overlap_matches_coastway_west_and_swr_portsmouth_direct_as_independent_exclusive_segments()
     {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SN-5",
            "Signal failure at Havant",
            "Signal failure causing delays to services.",
            &["SN"],
            &["HAV"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "southern-coastway-west".to_string(),
                "swr-portsmouth-direct".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment, not shared",
                m.line.id
            );
        }
    }

    // Task 5.9 (southern-oxted-uckfield.toml). An incident on this line's
    // own exclusive `oxted-uckfield-branch` segment (past the Hurst Green
    // junction) should stay exclusive to this line alone - mirrors
    // swr_exclusive_segment_incident_does_not_propagate and
    // elizabeth_branch_incident_stays_on_its_branch above. This line shares
    // no segment name with any sibling file (its three real overlaps - VIC,
    // LBG and ECR, all station overlap only per this file's own header
    // comment - are exercised by the two tests below and by the updated LBG
    // test above), so no SharedSegment propagation test is added for this
    // line, per COMMON.md's own "skip only for a genuinely standalone line"
    // exception.
    #[test]
    fn oxted_uckfield_branch_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SN-6",
            "Signal failure at Buxted",
            "Signal failure causing delays to Southern services.",
            &["SN"],
            &["BXD"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["southern-oxted-uckfield".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 5.9. Same for the East Grinstead branch (the other branch past
    // Hurst Green Junction) - an incident there should also stay exclusive
    // to this line. Per this file's own header comment, East Grinstead is
    // confirmed as Southern's own Oxted line terminus (not Thameslink
    // territory, despite the gap analysis grouping it with Thameslink's
    // southern branches) - no Thameslink sibling file exists yet, so this
    // stays a plain exclusive-segment case for now; see that file's own
    // HAND-OFF NOTE for what a future Thameslink southern-branches file
    // should re-check.
    #[test]
    fn oxted_east_grinstead_branch_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SN-7",
            "Points failure at Lingfield",
            "Points failure causing delays to Southern services.",
            &["SN"],
            &["LFD"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["southern-oxted-uckfield".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 5.9. Victoria (VIC) is also southern-brighton-main-line.toml's
    // own terminus and southeastern-chatham.toml's own station. Per this
    // file's own STATION OVERLAP AT VIC / LBG / ECR header comment: whether
    // Oxted line services physically share fast/slow tracks with those
    // other services out of Victoria was not confirmed to COMMON.md's bar,
    // so this is treated as station overlap only (this file's own
    // `oxted-victoria-approach` segment, not reusing either sibling's
    // segment name) - same judgment call as the AFK/Ramsgate/LBG overlaps
    // exercised elsewhere in this module. Confirms an incident at Victoria
    // matches all three lines independently, each still scoped
    // ExclusiveSegment, never SharedSegment.
    //
    // Kent/Sussex batch: southeastern-maidstone-east.toml also terminates at
    // VIC - originally its own `maidstone-east-victoria` segment, a fourth
    // independent station-overlap match.
    //
    // REVIEW FIX (shared-segment structural review, review2-shared-
    // segments): southeastern-chatham.toml's and southeastern-maidstone-
    // east.toml's own Victoria approaches turned out to be an IDENTICAL
    // twelve-station list, so both now share `chatham-maidstone-victoria`
    // verbatim - a genuine SharedSegment between those two specifically
    // (see southeastern-chatham.toml's own header comment). southern-
    // brighton-main-line.toml's own `southern-bml-victoria` and southern-
    // oxted-uckfield.toml's own `oxted-victoria-approach` are untouched by
    // this review and stay independently ExclusiveSegment.
    //
    // Updated by the Southeastern real-world-sanity review: southeastern-
    // sheerness-line.toml (new) also terminates at Victoria, reusing
    // `chatham-maidstone-victoria` verbatim (see that file's own SEGMENT
    // NAMING comment, updated to match southeastern-chatham.toml's
    // shared-segments-review split) - a fourth genuine SharedSegment member.
    //
    // Updated by the Southern real-world-sanity review: two more new files,
    // southern-metro-crystal-palace.toml and southern-metro-sutton.toml,
    // also terminate at Victoria, sharing their own `southern-metro-
    // victoria-trunk` segment verbatim with EACH OTHER - a separate,
    // independent SharedSegment pair, station overlap only against every
    // other family here.
    #[test]
    fn vic_station_overlap_matches_brighton_main_line_chatham_and_oxted_uckfield_as_independent_exclusive_segments()
     {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SN-8",
            "Signal failure at Victoria",
            "Signal failure causing delays to Southern services.",
            &["SN"],
            &["VIC"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "southern-brighton-main-line".to_string(),
                "southeastern-chatham".to_string(),
                "southern-oxted-uckfield".to_string(),
                "southeastern-maidstone-east".to_string(),
                "southeastern-sheerness-line".to_string(),
                "southern-metro-crystal-palace".to_string(),
                "southern-metro-sutton".to_string(),
            ])
        );
        for m in &matches {
            let expected = if m.line.id == "southeastern-chatham"
                || m.line.id == "southeastern-maidstone-east"
                || m.line.id == "southeastern-sheerness-line"
                || m.line.id == "southern-metro-crystal-palace"
                || m.line.id == "southern-metro-sutton"
            {
                MatchScope::SharedSegment
            } else {
                MatchScope::ExclusiveSegment
            };
            assert_eq!(m.scope, expected, "{} should be {:?}", m.line.id, expected);
        }
    }

    // Task 5.10 (great-northern-kings-lynn.toml). No shared-segment test is
    // added for the LNER pairing documented in that file's own LNER HAND-OFF
    // comment (`gn-ecml-slow-lines`) - `lines/lner-ecml.toml` (Batch 6) is
    // being written in a separate, parallel git worktree and does not exist
    // here, so a test asserting it would fail. Add that test once both
    // files exist and Batch 6 confirms whether it reuses the segment name.
    //
    // Originally this test also covered the direct Peterborough branch
    // (using Huntingdon) to show it stayed exclusive to this line. Task 5.13
    // (thameslink-cambridge.toml) confirmed genuine track-sharing on that
    // exact branch instead (see gn_peterborough_branch_shared_trunk_incident_
    // propagates_to_thameslink_cambridge below) - every station on that
    // branch is now genuinely shared, so there is no longer an exclusive
    // proof point left on it. That half of this test is retired in favour of
    // the new shared-trunk test, which asserts the real current behaviour.
    //
    // Same reasoning for the Cambridge/King's Lynn branch (the other branch
    // past Hitchin): Baldock (BDK) is no longer exclusive to this line
    // either, since Task 5.13's own semi-fast Cambridge service also stops
    // there (station overlap only, not a shared trunk - see
    // bdk_station_overlap_matches_great_northern_kings_lynn_and_thameslink_cambridge_as_independent_exclusive_segments
    // below). Meldreth (MEL), which that file's own MEL/STH/FXN EXCLUSION
    // header comment confirms Thameslink's semi-fast service skips
    // entirely, remains genuinely untouched by any other curated line and
    // takes over as this test's proof point.
    #[test]
    fn gn_kings_lynn_cambridge_branch_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GN-2",
            "Points failure at Meldreth",
            "Points failure causing delays to GN train services.",
            &["GN"],
            &["MEL"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["great-northern-kings-lynn".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 5.10. Cambridge (CBG) is also xc-stansted.toml's own terminus,
    // reached via a physically distinct route (Ely/March, not Hitchin/
    // Royston) - per that file's own STATION OVERLAP comment, this is
    // station overlap only, not a shared trunk. Confirms an incident there
    // matches both lines independently, each still scoped ExclusiveSegment,
    // never SharedSegment - same pattern as
    // vic_station_overlap_matches_brighton_main_line_chatham_and_oxted_uckfield_as_independent_exclusive_segments
    // above.
    //
    // Updated by Task 5.13 (thameslink-cambridge.toml): that file's own
    // Cambridge Line branch also terminates at CBG (its own
    // `thameslink-cambridge-branch` segment - see that file's own OVERLAP
    // (d) header comment), so it now joins this set as a third independent
    // exclusive-segment match.
    #[test]
    fn cbg_station_overlap_matches_great_northern_kings_lynn_and_xc_stansted_as_independent_exclusive_segments()
     {
        // Cambridge is also greater-anglia-west-anglia.toml's (`waml-mainline`)
        // and greater-anglia-breckland-line.toml's (`breckland-line`, orig.
        // bundled in greater-anglia-norfolk-branches.toml -- since split by
        // brand, real-world sanity review) own terminus (both Batch 2) --
        // two more independent ExclusiveSegment matches by the same
        // station-overlap pattern already established by
        // west_anglia_cambridge_is_station_overlap_only_with_xc_stansted.
        //
        // Updated by the Wales/East Anglia batch: `greater-anglia-ipswich-
        // cambridge.toml` also terminates at Cambridge (its own
        // `greater-anglia-ipswich-cambridge` segment) -- a sixth
        // independent ExclusiveSegment match.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GN-3",
            "Signal failure at Cambridge",
            "Signal failure causing delays to train services.",
            &["GN"],
            &["CBG"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "great-northern-kings-lynn".to_string(),
                "xc-stansted".to_string(),
                "thameslink-cambridge".to_string(),
                "greater-anglia-west-anglia".to_string(),
                "greater-anglia-breckland-line".to_string(),
                "greater-anglia-ipswich-cambridge".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment, not shared",
                m.line.id
            );
        }
    }

    // Task 5.11 (great-northern-suburban.toml). This file's research
    // (documented in its own SHARED-TRUNK RESEARCH FINDING header comment)
    // confirmed the Moorgate suburban service physically joins the East
    // Coast Main Line at Finsbury Park and runs over the same
    // `gn-ecml-slow-lines` corridor `great-northern-kings-lynn.toml` already
    // documents as far as Welwyn Garden City - but with a different calling
    // pattern (Moorgate stops at extra local stations that file's semi-fast
    // service skips), so - mirroring `southeastern-highspeed.toml`'s own
    // decision for the same "same track, different calling pattern"
    // situation - the segment name is deliberately NOT reused. This file is
    // therefore genuinely standalone with respect to cross-file segment
    // sharing (no `SharedSegment` propagation test is added, per COMMON.md's
    // own exception for a standalone line). This test confirms an incident
    // on this line's own exclusive `gn-moorgate-hertford-branch` segment
    // (Winchmore Hill - not a station either sibling GN file touches) stays
    // exclusive to this line alone - mirrors
    // swr_exclusive_segment_incident_does_not_propagate and
    // elizabeth_branch_incident_stays_on_its_branch above.
    #[test]
    fn gn_suburban_hertford_branch_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GN-4",
            "Signal failure at Winchmore Hill",
            "Signal failure causing delays to GN train services.",
            &["GN"],
            &["WIH"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["great-northern-suburban".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 5.11. Same for the Welwyn Garden City branch's own local-only
    // stops (the other branch past Alexandra Palace) - an incident at
    // Oakleigh Park, a station `great-northern-kings-lynn.toml`'s semi-fast
    // service never calls at, should also stay exclusive to this line, and
    // shouldn't spuriously pull in the Hertford Loop branch's own segment
    // name either (the two branches use different segment names despite
    // being the same file/line).
    #[test]
    fn gn_suburban_wgc_branch_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GN-5",
            "Points failure at Oakleigh Park",
            "Points failure causing delays to GN train services.",
            &["GN"],
            &["OKL"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["great-northern-suburban".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 5.11. Finsbury Park (FPK) is also `great-northern-kings-lynn.toml`'s
    // own `gn-ecml-slow-lines` station - per this file's own SHARED-TRUNK
    // RESEARCH FINDING header comment, this is a genuine physical overlap
    // (both services' trains run over the same East Coast Main Line slow
    // lines here) but with different calling patterns, so it's deliberately
    // kept as station overlap only, not a shared segment name - same
    // judgment call as the AFK/Ramsgate/LBG overlaps between Southeastern
    // and HS1 exercised above. Confirms an incident at Finsbury Park matches
    // both lines independently, each still scoped ExclusiveSegment, never
    // SharedSegment.
    //
    // Updated by Task 5.13 (thameslink-cambridge.toml): that file also has a
    // station at FPK (its own `thameslink-cambridge-peterborough-trunk`
    // segment - the Canal Tunnels' connection to the ECML, physically the
    // FAST lines here rather than `gn-ecml-slow-lines`, per that file's own
    // OVERLAP (a) header comment), so it now joins this set as a third
    // independent exclusive-segment match.
    #[test]
    fn fpk_station_overlap_matches_great_northern_suburban_and_kings_lynn_as_independent_exclusive_segments()
     {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GN-6",
            "Signal failure at Finsbury Park",
            "Signal failure causing delays to GN train services.",
            &["GN"],
            &["FPK"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "great-northern-suburban".to_string(),
                "great-northern-kings-lynn".to_string(),
                "thameslink-cambridge".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment, not shared",
                m.line.id
            );
        }
    }

    // Task 5.12 (thameslink-bedford.toml). This file's `mml-bedford-
    // st-pancras` segment (Kentish Town through Bedford) is named and
    // documented, per this file's own header comment, as the segment
    // Batch 7's EMR Midland Main Line file is required to cite and reuse
    // verbatim for its own Bedford-St Pancras section - but that sibling
    // file doesn't exist in this worktree yet, so no SharedSegment test for
    // that pairing is added here (would fail to compile/pass without the
    // sibling). Until that file exists and reuses the name, this segment is
    // exclusive to this line alone: an incident at Harpenden, a station on
    // that segment untouched by any other curated line, should stay
    // exclusive to this line - mirrors
    // swr_exclusive_segment_incident_does_not_propagate and
    // elizabeth_branch_incident_stays_on_its_branch above.
    // Updated (real-world sanity review): Harpenden is also on
    // thameslink-rainham.toml's own Luton-St Pancras stretch, reused
    // verbatim from this file (its own `thameslink-rainham` segment) -
    // station overlap only, both independently ExclusiveSegment. The name
    // ("...does_not_propagate") now refers to this station staying off
    // every OTHER curated line (EMR's own Bedford-St Pancras service skips
    // it - see ltn_lut_bdm_station_overlap_... below), not to zero overlap.
    #[test]
    fn thameslink_bedford_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TL-1",
            "Signal failure at Harpenden",
            "Signal failure causing delays to train services.",
            &["TL"],
            &["HPD"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "thameslink-bedford".to_string(),
                "thameslink-rainham".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment, not shared",
                m.line.id
            );
        }
    }

    // Task 5.12. St Pancras International (STP) is where this file's
    // northern branch meets thameslink-core.toml's own terminus, and is
    // also southeastern-highspeed.toml's own terminus (`hs1-domestic`).
    // Per this file's own SEGMENT NAMING header comment, this is
    // deliberately kept as station overlap only in every direction: this
    // file's own `mml-bedford-st-pancras` segment name is NOT reused by
    // thameslink-core (whose `thameslink-core` segment also covers
    // Farringdon/City Thameslink/Blackfriars/London Bridge, none of which
    // this branch file touches - reusing the name verbatim would
    // incorrectly mark those untouched stations SharedSegment too, exactly
    // the trap southeastern-highspeed.toml's own header comment already
    // flags for `seml-coast`/`chatham-medway`). Confirms an incident at STP
    // matches all three lines independently, each still scoped
    // ExclusiveSegment, never SharedSegment - mirrors
    // afk_station_overlap_matches_both_seml_and_hs1_as_independent_exclusive_segments
    // above.
    //
    // Updated by Task 5.13 (thameslink-cambridge.toml): that file's own
    // Cambridge/Peterborough branch also meets the core at STP (its own
    // `thameslink-cambridge-peterborough-trunk` segment, diverging into the
    // Canal Tunnels towards the ECML rather than north up the Midland Main
    // Line like thameslink-bedford - see that file's own STP header
    // comment), so it now joins this set as a fourth independent
    // exclusive-segment match.
    #[test]
    fn stp_station_overlap_matches_thameslink_core_bedford_and_highspeed_as_independent_exclusive_segments()
     {
        // St Pancras is also emr-connect.toml's and emr-midland-main-line.toml's
        // own terminus (both Batch 7, merged separately), which genuinely
        // share track London-Bedford-ward and correspondingly share the
        // `emr-mml-south` segment name with EACH OTHER -- SharedSegment
        // between those two specifically, while staying independent
        // ExclusiveSegment matches relative to the four Thameslink/HS1
        // lines, which use entirely distinct segment names.
        //
        // Updated (real-world sanity review): thameslink-rainham.toml's own
        // Luton-Rainham route also meets the core at STP (its own
        // `thameslink-rainham` segment) -- a seventh independent
        // exclusive-segment match.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TL-2",
            "Signal failure at St Pancras International",
            "Signal failure causing delays to train services.",
            &["TL"],
            &["STP"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "thameslink-core".to_string(),
                "thameslink-bedford".to_string(),
                "southeastern-highspeed".to_string(),
                "thameslink-cambridge".to_string(),
                "emr-connect".to_string(),
                "emr-midland-main-line".to_string(),
                "thameslink-rainham".to_string(),
            ])
        );
        for m in &matches {
            let expected = match m.line.id.as_str() {
                "emr-connect" | "emr-midland-main-line" => MatchScope::SharedSegment,
                _ => MatchScope::ExclusiveSegment,
            };
            assert_eq!(m.scope, expected, "{} scope mismatch", m.line.id);
        }
    }

    // Cross-batch follow-up (resolved): `lines/emr-midland-main-line.toml`'s
    // "Bedford-St Pancras: cross-batch dependency ruling" header comment
    // confirms Luton Airport Parkway, Luton and Bedford are genuine
    // station-overlap-only stations between the two EMR files
    // (`emr-midland-main-line` and `emr-connect`, which genuinely share
    // `emr-mml-south`'s calling pattern with EACH OTHER and so report
    // SharedSegment for one another) and `thameslink-bedford.toml`'s
    // `mml-bedford-st-pancras` segment (which calls at 13 additional local
    // stations neither EMR service stops at, so it does NOT share the
    // segment name and reports ExclusiveSegment). Mirrors the STP case
    // already exercised by
    // `stp_station_overlap_matches_thameslink_core_bedford_and_highspeed_as_independent_exclusive_segments`
    // above; this test covers the three remaining overlap stations.
    //
    // Updated by the Midlands EMR/WMR/LNWR sanity review: `lines/lnwr-
    // marston-vale-line.toml` (a new file from that review) also terminates
    // at Bedford, on its own exclusive `lnwr-marston-vale` segment (a
    // physically distinct branch, west towards Bletchley) -- station-
    // overlap only, so BDM specifically now also matches that line as a
    // fourth ExclusiveSegment line, while LTN/LUT are unaffected.
    //
    // Updated (real-world sanity review): thameslink-rainham.toml reuses
    // thameslink-bedford.toml's own Luton/Luton Airport Parkway stations
    // verbatim (its own route starts at Luton, one station south of
    // Bedford) -- so LTN and LUT now also match thameslink-rainham,
    // ExclusiveSegment, while BDM (Bedford itself, north of Luton, not on
    // thameslink-rainham's own route) does not gain that match -- it gains
    // the lnwr-marston-vale-line match described above instead.
    #[test]
    fn ltn_lut_bdm_station_overlap_between_emr_and_thameslink_bedford_stays_exclusive_for_thameslink()
     {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        for (crs, station_name, extra_lines) in [
            (
                "LTN",
                "Luton Airport Parkway",
                vec!["thameslink-rainham".to_string()],
            ),
            ("LUT", "Luton", vec!["thameslink-rainham".to_string()]),
            ("BDM", "Bedford", vec!["lnwr-marston-vale-line".to_string()]),
        ] {
            let inc = incident(
                &format!("EMR-TL-{crs}"),
                &format!("Signal failure at {station_name}"),
                &format!("Signal failure causing delays to train services at {station_name}."),
                &["EM"],
                &[crs],
            );
            let matches = lines_affected_by(&inc, &lines, &registry);
            let by_id: HashMap<String, MatchScope> = matches
                .iter()
                .map(|m| (m.line.id.clone(), m.scope))
                .collect();
            let mut expected = HashSet::from([
                "emr-midland-main-line".to_string(),
                "emr-connect".to_string(),
                "thameslink-bedford".to_string(),
            ]);
            expected.extend(extra_lines.iter().cloned());
            assert_eq!(
                by_id.keys().cloned().collect::<HashSet<_>>(),
                expected,
                "unexpected match set for {crs}"
            );
            assert_eq!(
                by_id.get("emr-midland-main-line"),
                Some(&MatchScope::SharedSegment),
                "{crs}"
            );
            assert_eq!(
                by_id.get("emr-connect"),
                Some(&MatchScope::SharedSegment),
                "{crs}"
            );
            assert_eq!(
                by_id.get("thameslink-bedford"),
                Some(&MatchScope::ExclusiveSegment),
                "{crs}"
            );
            for extra in &extra_lines {
                assert_eq!(
                    by_id.get(extra.as_str()),
                    Some(&MatchScope::ExclusiveSegment),
                    "{crs}: {extra}"
                );
            }
        }
    }

    // Task 5.13 (thameslink-cambridge.toml). Per that file's own OVERLAP (c)
    // header comment: the direct Peterborough branch (Arlesey, Biggleswade,
    // Sandy, St Neots, Huntingdon, Peterborough) is a genuine shared trunk
    // with great-northern-kings-lynn.toml's own `gn-peterborough-branch`
    // segment - Stevenage station's own Wikipedia services section confirms
    // this line runs "2 tph to Peterborough (all stations)", an identical
    // calling pattern to that file's own complete station list for the
    // branch (unlike the Cambridge Line branch, where this line's semi-fast
    // pattern skips Meldreth/Shepreth/Foxton - see
    // bdk_station_overlap_matches_great_northern_kings_lynn_and_thameslink_cambridge_as_independent_exclusive_segments
    // below). This replaces the previous exclusive-segment expectation for
    // Huntingdon (now genuinely shared) - mirrors
    // swr_shared_trunk_incident_propagates above.
    #[test]
    fn gn_peterborough_branch_shared_trunk_incident_propagates_to_thameslink_cambridge() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "GN-1",
            "Signal failure at Huntingdon",
            "Signal failure causing delays to GN train services.",
            &["GN"],
            &["HUN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "great-northern-kings-lynn".to_string(),
                "thameslink-cambridge".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment",
                m.line.id
            );
        }
    }

    // Task 5.13. Baldock (BDK) is on this line's own Cambridge Line branch
    // (`thameslink-cambridge-branch`) and also
    // great-northern-kings-lynn.toml's own `gn-cambridge-kings-lynn-branch`
    // station - per that file's own OVERLAP (d) header comment, this is
    // station overlap only, not a shared trunk, because this line's
    // semi-fast Cambridge service skips Meldreth/Shepreth/Foxton that GN's
    // own segment treats as consecutive stops (the same "same track,
    // different calling pattern" situation as the FPK/Stevenage overlaps
    // above). Confirms an incident at Baldock matches both lines
    // independently, each still scoped ExclusiveSegment, never
    // SharedSegment - mirrors
    // cbg_station_overlap_matches_great_northern_kings_lynn_and_xc_stansted_as_independent_exclusive_segments
    // above.
    #[test]
    fn bdk_station_overlap_matches_great_northern_kings_lynn_and_thameslink_cambridge_as_independent_exclusive_segments()
     {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TL-3",
            "Points failure at Baldock",
            "Points failure causing delays to train services.",
            &["TL"],
            &["BDK"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "great-northern-kings-lynn".to_string(),
                "thameslink-cambridge".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment, not shared",
                m.line.id
            );
        }
    }

    // Task 5.14 (thameslink-southern.toml). An incident on this line's own
    // exclusive Catford Loop stretch (`thameslink-sevenoaks-catford`,
    // between Elephant & Castle and the rejoin with the Chatham Main Line
    // at Shortlands - see that segment below) should stay exclusive to
    // this line alone - mirrors swr_exclusive_segment_incident_does_not_propagate
    // and elizabeth_branch_incident_stays_on_its_branch above.
    #[test]
    fn thameslink_sevenoaks_catford_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TL-4",
            "Signal failure at Catford",
            "Signal failure causing delays to train services.",
            &["TL"],
            &["CTF"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["thameslink-southern".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 5.14. The Sutton Loop (`thameslink-sutton-loop`) is exclusive to
    // this line everywhere except the single Wimbledon station overlap
    // (see wim_station_overlap_matches_swr_trunk_and_thameslink_southern_
    // as_independent_segments below) - an incident elsewhere on the loop
    // should stay exclusive, mirroring the Catford Loop test above.
    #[test]
    fn thameslink_sutton_loop_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TL-5",
            "Signal failure at Sutton Common",
            "Signal failure causing delays to train services.",
            &["TL"],
            &["SUC"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["thameslink-southern".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 5.14. Genuine physical track sharing exists here (the Sevenoaks
    // branch's Catford Loop stretch rejoins the Chatham Main Line at
    // Shortlands and shares track with southeastern-chatham.toml's own
    // approach as far as Swanley - see thameslink-southern.toml's own
    // header comment). Originally NOT modelled as a SharedSegment:
    // southeastern-chatham.toml's own OLD `chatham-london` segment bundled
    // this stretch together with its own Victoria-Herne Hill approach
    // (which this file's Sevenoaks branch never touches), so reusing that
    // name verbatim would have incorrectly also marked Victoria/BKJ/Herne
    // Hill as SharedSegment.
    //
    // REVIEW FIX (shared-segment structural review, review2-shared-
    // segments): resolved by splitting southeastern-chatham.toml's segment
    // at Shortlands instead of declining to share - SRT through SAY is now
    // `chatham-maidstone-thameslink-swanley`, reused verbatim by this file,
    // southeastern-chatham.toml AND southeastern-maidstone-east.toml (which
    // also turned out to run this identical stretch, previously its own
    // separately-named `maidstone-east-victoria`). All three now genuinely
    // SharedSegment at Swanley - see southeastern-chatham.toml's own header
    // comment for the full write-up.
    //
    // Updated by the Southeastern real-world-sanity review: southeastern-
    // sheerness-line.toml (new) also reuses `chatham-maidstone-thameslink-
    // swanley` verbatim for its own SRT-SAY stretch (see that file's own
    // SEGMENT NAMING comment, updated to match southeastern-chatham.toml's
    // shared-segments-review split) - a fourth genuine SharedSegment member
    // at Swanley.
    #[test]
    fn say_station_overlap_matches_chatham_and_thameslink_southern_as_independent_exclusive_segments()
     {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TL-6",
            "Signal failure at Swanley",
            "Signal failure causing delays to train services.",
            &["TL"],
            &["SAY"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "southeastern-chatham".to_string(),
                "thameslink-southern".to_string(),
                "southeastern-maidstone-east".to_string(),
                "southeastern-sheerness-line".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment on chatham-maidstone-thameslink-swanley",
                m.line.id
            );
        }
    }

    // Task 5.14. Originally REVISED after review to decline the flagged
    // Task 5.6 coordination (London Bridge/East Croydon): an earlier draft
    // reused `southern-bml-north` here and asserted a genuine SharedSegment
    // pair, but the only non-Wikipedia source found at the time
    // (thetrainline.com) is not one of COMMON.md's four approved
    // second-source categories and only shows service-existence, not
    // physical track sharing - so the claim was withdrawn.
    //
    // REVIEW FIX (shared-segment structural review, review2-shared-
    // segments): reinstated on different grounds - see thameslink-
    // southern.toml's own BRIGHTON BRANCH header comment and southern-
    // brighton-main-line.toml's own SEGMENT NAMING comment for the full
    // write-up. `brighton-main-line-north` (renamed from
    // `southern-bml-north`) is now reused verbatim by this file too, so an
    // incident here is a genuine SharedSegment pair with
    // southern-brighton-main-line.toml. southern-oxted-uckfield.toml's own
    // `oxted-trunk` is untouched by this review and stays independently
    // ExclusiveSegment.
    #[test]
    fn ecr_station_overlap_matches_brighton_main_line_and_oxted_uckfield_as_independent_exclusive_segments()
     {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TL-7",
            "Signal failure at East Croydon",
            "Signal failure causing delays to train services.",
            &["TL"],
            &["ECR"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "thameslink-southern".to_string(),
                "southern-brighton-main-line".to_string(),
                "southern-oxted-uckfield".to_string(),
            ])
        );
        for m in &matches {
            let expected = if m.line.id == "southern-oxted-uckfield" {
                MatchScope::ExclusiveSegment
            } else {
                MatchScope::SharedSegment
            };
            assert_eq!(m.scope, expected, "{} should be {:?}", m.line.id, expected);
        }
    }

    // Task 5.14. Blackfriars is this file's own core-boundary junction for
    // the Sevenoaks/Sutton Loop branches (`thameslink-southern-trunk`)
    // and also thameslink-core.toml's own station (`thameslink-core`) -
    // station overlap only, same judgment call as every other core-boundary
    // overlap in this batch (STP in thameslink-bedford.toml/
    // thameslink-cambridge.toml, LBG above) - mirrors
    // stp_station_overlap_matches_thameslink_core_bedford_and_highspeed_as_independent_exclusive_segments
    // above.
    //
    // Updated (real-world sanity review): thameslink-rainham.toml's own
    // Luton-Rainham route also runs the Thameslink core's full length,
    // including Blackfriars (its own `thameslink-rainham` segment) - a
    // third independent ExclusiveSegment match.
    #[test]
    fn bfr_station_overlap_matches_thameslink_core_and_thameslink_southern_as_independent_exclusive_segments()
     {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TL-8",
            "Signal failure at Blackfriars",
            "Signal failure causing delays to train services.",
            &["TL"],
            &["BFR"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "thameslink-core".to_string(),
                "thameslink-southern".to_string(),
                "thameslink-rainham".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment, not shared",
                m.line.id
            );
        }
    }

    // Task 5.14. Wimbledon is this file's own Sutton Loop station
    // (`thameslink-sutton-loop`, reached via Haydons Road) and also all
    // FIVE SWR files' own shared `swr-trunk-waterloo` station (reached via
    // Clapham Junction) - a physically distinct approach, so station
    // overlap only against the SWR group, not a further member of their own
    // shared trunk. Confirms an incident at Wimbledon still propagates
    // across the SWR lines as SharedSegment (mirrors
    // swr_shared_trunk_incident_propagates above) while this file's own
    // match stays independently ExclusiveSegment.
    //
    // Updated when lines/swr-kingston-loop.toml and lines/swr-chessington.toml
    // were added: those two model SWR's suburban slow-line corridor and also
    // call at Wimbledon on the same `swr-trunk-waterloo` segment, so the SWR
    // side of this assertion grew from three lines to five. This test is the
    // one that proves the two new files needed NO edit to the three existing
    // swr-*.toml files -- the shared-segment mechanism is name-based, and WIM
    // already carried the right name there.
    //
    // Updated again by the Wessex/Thames-Valley/Isle-of-Wight batch:
    // swr-west-of-england.toml also reuses `swr-trunk-waterloo` verbatim for
    // WIM (a real shared approach as far as Basingstoke/Worting Junction —
    // see that file's own segment-naming comment), growing the SWR side to
    // six. swr-windsor-lines.toml (this same batch) does NOT call at
    // Wimbledon at all — its own route runs via Vauxhall and Clapham
    // Junction's separate Windsor-lines platforms, never via Wimbledon — so
    // it is correctly absent here.
    //
    // Updated by the SE/SWR-loops batch: swr-new-guildford.toml also reuses
    // `swr-trunk-waterloo` verbatim for WIM (its own SEGMENTS diagram: WAT -
    // CLJ - WIM - SUR is the shared Waterloo approach before the
    // New Guildford line diverges), growing the SWR side to seven.
    // swr-chertsey-loop.toml and swr-hounslow-loop.toml (same batch) do NOT
    // call at Wimbledon — both diverge from the Waterloo trunk before
    // reaching it — so they are correctly absent here.
    //
    // Updated again by the SWR suburban-gap batch: swr-shepperton-
    // branch.toml, swr-hampton-court-branch.toml and swr-epsom-mole-
    // valley.toml also reuse `swr-trunk-waterloo` verbatim for WIM (each
    // file's own SEGMENTS diagram runs via Wimbledon before diverging
    // further out), growing the SWR side to ten. swr-waterloo-reading.toml
    // (same batch) does NOT call at Wimbledon — like swr-windsor-lines.toml
    // before it, its own route runs via Vauxhall/Richmond, never via
    // Wimbledon — so it is correctly absent here too.
    #[test]
    fn wim_station_overlap_matches_swr_trunk_and_thameslink_southern_as_independent_segments() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TL-9",
            "Signal failure at Wimbledon",
            "Signal failure causing delays to train services.",
            &["TL"],
            &["WIM"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "swr-south-west-main".to_string(),
                "swr-portsmouth-direct".to_string(),
                "swr-alton".to_string(),
                "swr-kingston-loop".to_string(),
                "swr-chessington".to_string(),
                "swr-west-of-england".to_string(),
                "swr-new-guildford".to_string(),
                "swr-shepperton-branch".to_string(),
                "swr-hampton-court-branch".to_string(),
                "swr-epsom-mole-valley".to_string(),
                "thameslink-southern".to_string(),
            ])
        );
        for m in &matches {
            if m.line.id == "thameslink-southern" {
                assert_eq!(
                    m.scope,
                    MatchScope::ExclusiveSegment,
                    "thameslink-southern should be ExclusiveSegment (different segment name)"
                );
            } else {
                assert_eq!(
                    m.scope,
                    MatchScope::SharedSegment,
                    "{} should be SharedSegment",
                    m.line.id
                );
            }
        }
    }

    // Task 5.14. Sevenoaks is this file's own branch terminus
    // (`thameslink-sevenoaks-otford`, reached via Bat & Ball) and also
    // southeastern-main-line.toml's own station (`seml-weald`, reached via
    // Dunton Green) - two physically distinct approaches per Sevenoaks
    // station's own Wikipedia article (see thameslink-southern.toml's own
    // SEV comment), so station overlap only, not a shared segment.
    #[test]
    fn sev_station_overlap_matches_seml_and_thameslink_southern_as_independent_exclusive_segments()
    {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TL-10",
            "Signal failure at Sevenoaks",
            "Signal failure causing delays to train services.",
            &["TL"],
            &["SEV"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "southeastern-main-line".to_string(),
                "thameslink-southern".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment, not shared",
                m.line.id
            );
        }
    }

    // Task 10.1 (ScotRail Central Belt): no sibling ScotRail line exists yet
    // in `lines/` to share `scotrail-central-belt`/
    // `scotrail-central-belt-edinburgh-throat` with, so there is no
    // shared-segment propagation to assert today -- see
    // `lines/scotrail-central-belt.toml`'s own comments for the segment-
    // naming groundwork left for Task 10.2. Only the exclusive-segment
    // non-propagation assertion applies for now, mirroring
    // `swr_exclusive_segment_incident_does_not_propagate`.
    #[test]
    fn scotrail_central_belt_exclusive_segment_incident_does_not_propagate() {
        // Falkirk High is also lumo.toml's own station (merged separately,
        // Batch 10), on its exclusive `lumo-glasgow` segment -- station-level
        // overlap, distinct segment names, both stay ExclusiveSegment.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-1",
            "Signal failure at Falkirk High",
            "Signal failure causing delays to ScotRail services at Falkirk High.",
            &["SR"],
            &["FKK"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-central-belt".to_string(), "lumo".to_string()])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    // Task 10.2 (ScotRail Glasgow Suburban), later split (line-definition
    // audit, 2026-09-21) into `scotrail-north-clyde.toml`/`scotrail-
    // argyle.toml`: Milngavie sits exclusively on `scotrail-north-
    // clyde.toml`'s own `scotrail-north-clyde-milngavie-branch` segment
    // (see that file's own scope-boundary note on why the Argyle Line's
    // real but unmodelled reach onto this branch isn't a shared segment),
    // and no other `lines/*.toml` file touches it -- so there is no
    // shared-segment propagation to assert today, mirroring
    // `scotrail_central_belt_exclusive_segment_incident_does_not_propagate`.
    // Only the exclusive-segment non-propagation assertion applies for now.
    #[test]
    fn scotrail_north_clyde_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-2",
            "Signal failure at Milngavie",
            "Signal failure causing delays to ScotRail services at Milngavie.",
            &["SR"],
            &["MLN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-north-clyde".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 10.3 (ScotRail Ayrshire Coast): `scotrail-ayrshire-stranraer`
    // (the Girvan/Stranraer branch) is not touched by any other
    // `lines/*.toml` file, so it stays an exclusive-segment non-
    // propagation assertion, mirroring
    // `scotrail_central_belt_exclusive_segment_incident_does_not_propagate`.
    #[test]
    fn scotrail_ayrshire_stranraer_branch_incident_does_not_propagate_alt() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-3b",
            "Signal failure at Irvine",
            "Signal failure causing delays to ScotRail services at Irvine.",
            &["SR"],
            &["IRV"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-ayrshire".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Greater Glasgow batch: `lines/scotrail-largs-ardrossan.toml` reuses
    // `scotrail-ayrshire.toml`'s own `scotrail-ayrshire-glasgow-ayr`
    // segment name verbatim for its shared Glasgow Central/Paisley Gilmour
    // Street/Johnstone/Kilwinning approach (both files' Largs/Ardrossan
    // and Ayr branches physically share this stretch as far as
    // Kilwinning, per `lines/scotrail-largs-ardrossan.toml`'s own
    // sourcing) -- a genuine shared trunk, superseding the previous
    // `scotrail_ayrshire_glasgow_ayr_trunk_incident_does_not_propagate`
    // exclusive-segment assertion for this same station. Mirrors
    // `overground_canonbury_curve_incident_propagates_to_mildmay_and_windrush`.
    #[test]
    fn scotrail_ayrshire_kilwinning_trunk_incident_propagates_to_largs_ardrossan() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-3",
            "Signal failure at Kilwinning",
            "Signal failure causing delays to ScotRail services at Kilwinning.",
            &["SR"],
            &["KWN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "scotrail-ayrshire".to_string(),
                "scotrail-largs-ardrossan".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment",
                m.line.id
            );
        }
    }

    #[test]
    fn scotrail_ayrshire_stranraer_branch_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-4",
            "Level crossing fault at Girvan",
            "Level crossing fault causing delays to ScotRail services at Girvan.",
            &["SR"],
            &["GIR"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-ayrshire".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 10.4 (ScotRail Fife Circle + Borders Railway): originally one
    // bundled file with two genuinely separate routes; a line-definition
    // audit (2026-09-21) split it into `scotrail-fife-circle.toml` and
    // `scotrail-borders-railway.toml`, each with its own distinct segment
    // names (`scotrail-fife-circle*` / `scotrail-borders`), neither of
    // which is shared with any other `lines/*.toml` file today (see
    // `lines/scotrail-fife-circle.toml`'s own comments on why the
    // Edinburgh Waverley/Haymarket overlap with `scotrail-central-belt`
    // isn't modelled as a shared segment) -- so, mirroring Task 10.3's
    // two-exclusive-segments treatment, one exclusive-segment
    // non-propagation assertion per route, not a shared-segment one.
    #[test]
    fn scotrail_fife_circle_exclusive_segment_incident_does_not_propagate() {
        // Kirkcaldy is also lner-ecml.toml's own station (its own
        // `ecml-aberdeen` segment, merged separately), on the Edinburgh-
        // Aberdeen main line via the Forth Bridge -- station-level overlap,
        // distinct segment names, both stay ExclusiveSegment.
        //
        // Updated by the Scotland real-world-sanity review: the new
        // scotrail-edinburgh-aberdeen.toml also calls at Kirkcaldy, reusing
        // `scotrail-fife-circle` verbatim (the last station its own route
        // shares with scotrail-fife-circle.toml before diverging) - a
        // genuine SharedSegment pair, so this incident now also matches
        // that file, and both flip to SharedSegment here.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-5",
            "Signal failure at Kirkcaldy",
            "Signal failure causing delays to ScotRail services at Kirkcaldy.",
            &["SR"],
            &["KDY"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "scotrail-fife-circle".to_string(),
                "lner-ecml".to_string(),
                "scotrail-edinburgh-aberdeen".to_string(),
            ])
        );
        for m in &matches {
            let expected = match m.line.id.as_str() {
                "scotrail-fife-circle" | "scotrail-edinburgh-aberdeen" => MatchScope::SharedSegment,
                _ => MatchScope::ExclusiveSegment,
            };
            assert_eq!(m.scope, expected, "{} should be {:?}", m.line.id, expected);
        }
    }

    #[test]
    fn scotrail_borders_railway_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-6",
            "Points failure at Galashiels",
            "Points failure causing delays to ScotRail services at Galashiels.",
            &["SR"],
            &["GAL"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-borders-railway".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 10.5 (ScotRail Highland Main Line): no sibling `lines/*.toml`
    // file touches this line's `scotrail-highland-main-line` segment yet
    // (see `lines/scotrail-highland-main-line.toml`'s own comments on the
    // Inverness-area segment-naming handoff left open for Tasks
    // 10.6/10.7/10.10), so there is no shared-segment propagation to
    // assert today -- only exclusive-segment non-propagation, mirroring
    // `scotrail_central_belt_exclusive_segment_incident_does_not_propagate`.
    // The incident is placed at Kingussie (on the line's exclusive
    // Perth-Carrbridge segment), not Inverness, since Inverness's own
    // segment fate is deliberately left open for a later task to decide.
    #[test]
    fn scotrail_highland_main_line_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-7",
            "Signal failure at Kingussie",
            "Signal failure causing delays to ScotRail services at Kingussie.",
            &["SR"],
            &["KIN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-highland-main-line".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 10.6 (ScotRail Far North Line): the Wick and Thurso branches
    // each get their own exclusive segment
    // (`scotrail-far-north-wick`/`scotrail-far-north-thurso`), used only by
    // this one file, so an incident on either branch should stay local to
    // `scotrail-far-north` with `ExclusiveSegment` scope -- mirroring
    // `swr_exclusive_segment_incident_does_not_propagate`.
    //
    // This file also tags Inverness-Dingwall as
    // `scotrail-inverness-dingwall-trunk`, a segment name Task 10.7's
    // Kyle of Lochalsh Line (`lines/scotrail-kyle.toml`) now independently
    // confirmed and reused for its own Inverness-Dingwall stations -- see
    // `scotrail_shared_inverness_dingwall_trunk_incident_propagates` below
    // for the resulting cross-file `SharedSegment` propagation assertion.
    // The same-file trunk-vs-branch structure this file also has
    // (Inverness-Georgemas Junction shared by both the Wick and Thurso
    // branches) is separately asserted directly via `SegmentRegistry` in
    // `crates/aggregator/src/segments.rs`, since that particular sharing
    // is internal to one line/file and `MatchScope::SharedSegment` only
    // applies across distinct line files.
    #[test]
    fn scotrail_far_north_wick_branch_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-8",
            "Points failure at Wick",
            "Points failure causing delays to ScotRail services at Wick.",
            &["SR"],
            &["WCK"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-far-north".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn scotrail_far_north_thurso_branch_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-9",
            "Signal failure at Thurso",
            "Signal failure causing delays to ScotRail services at Thurso.",
            &["SR"],
            &["THS"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-far-north".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 10.7 (ScotRail Kyle of Lochalsh Line): this line's own exclusive
    // track west of Dingwall Junction (Garve through Kyle of Lochalsh) is
    // tagged `scotrail-kyle-exclusive`, used only by this one file, so an
    // incident there should stay local to `scotrail-kyle` with
    // `ExclusiveSegment` scope -- mirroring
    // `swr_exclusive_segment_incident_does_not_propagate`.
    #[test]
    fn scotrail_kyle_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-10",
            "Landslip near Strathcarron",
            "Landslip causing delays to ScotRail services at Strathcarron.",
            &["SR"],
            &["STC"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["scotrail-kyle".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 10.7 (ScotRail Kyle of Lochalsh Line): the load-bearing
    // shared-trunk test this Batch 10 pairing exists for. Task 10.6
    // (`scotrail-far-north.toml`) reserved the segment name
    // `scotrail-inverness-dingwall-trunk` for the Inverness-Dingwall
    // stretch both lines' services physically run over; this file
    // independently re-confirmed that shared-track claim (see
    // `lines/scotrail-kyle.toml`'s own Sources comments) and reused the
    // exact same segment name. Now that both files exist,
    // `SegmentRegistry::is_shared` correctly reports the segment shared
    // between two distinct line IDs, so an incident at Dingwall (a
    // station on that shared segment) should match BOTH
    // `scotrail-far-north` and `scotrail-kyle`, each with
    // `MatchScope::SharedSegment` -- mirroring
    // `swr_shared_trunk_incident_propagates`.
    #[test]
    fn scotrail_shared_inverness_dingwall_trunk_incident_propagates() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-11",
            "Signal failure at Dingwall",
            "Signal failure causing delays to ScotRail services at Dingwall.",
            &["SR"],
            &["DIN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "scotrail-far-north".to_string(),
                "scotrail-kyle".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment",
                m.line.id
            );
        }
    }

    // Task 10.8/10.9 (ScotRail West Highland Line pairing): the
    // load-bearing shared-trunk test this Batch 10 pairing exists for,
    // mirroring `scotrail_shared_inverness_dingwall_trunk_incident_propagates`.
    // Task 10.8 (`scotrail-west-highland-fort-william.toml`) reserved the
    // segment name `scotrail-west-highland-glasgow-crianlarich` for the
    // Glasgow-Crianlarich stretch both West Highland Line branches
    // physically run over (combined trains splitting/joining at
    // Crianlarich); Task 10.9 (`scotrail-west-highland-oban.toml`)
    // independently re-confirmed that shared-track claim against a fresh
    // fetch of Wikipedia's "West Highland Line" and "Crianlarich railway
    // station" articles (see that file's own verification-note comments)
    // and reused the exact same segment name. `SegmentRegistry::is_shared`
    // now correctly reports the segment shared between two distinct line
    // IDs, so an incident at Ardlui (a station on that shared segment)
    // matches BOTH `scotrail-west-highland-fort-william` and
    // `scotrail-west-highland-oban`, each with `MatchScope::SharedSegment`.
    #[test]
    fn scotrail_west_highland_shares_glasgow_crianlarich_trunk_incident_propagates() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-12",
            "Landslip near Ardlui",
            "Landslip causing delays to ScotRail services at Ardlui.",
            &["SR"],
            &["AUI"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "scotrail-west-highland-fort-william".to_string(),
                "scotrail-west-highland-oban".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment",
                m.line.id
            );
        }
    }

    // Task 10.8: this line's own exclusive track between Crianlarich and
    // Fort William (tagged `scotrail-west-highland-fort-william-exclusive`,
    // used only by this one file) should stay local to
    // `scotrail-west-highland-fort-william` with `ExclusiveSegment` scope --
    // mirroring `scotrail_ayrshire_glasgow_ayr_trunk_incident_does_not_propagate`.
    #[test]
    fn scotrail_west_highland_fort_william_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-13",
            "Signal failure at Tulloch",
            "Signal failure causing delays to ScotRail services at Tulloch.",
            &["SR"],
            &["TUL"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-west-highland-fort-william".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 10.8: the Mallaig extension beyond Fort William (tagged
    // `scotrail-west-highland-mallaig`, used only by this one file) is a
    // second, distinct exclusive segment within the same file -- mirroring
    // `scotrail_ayrshire_stranraer_branch_incident_does_not_propagate`'s
    // identical trunk+branch treatment.
    #[test]
    fn scotrail_west_highland_mallaig_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-14",
            "Landslip near Glenfinnan",
            "Landslip causing delays to ScotRail services at Glenfinnan.",
            &["SR"],
            &["GLF"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-west-highland-fort-william".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 10.8/10.9: unlike the Crianlarich reservation above, this
    // line's Glasgow-area sharing with `scotrail-north-clyde.toml`'s own
    // `scotrail-north-clyde-west-trunk` segment (Dalmuir - Dumbarton
    // Central) is a REAL, already-merged sibling -- see this line's own
    // Sources comments for the independent verification.
    // `scotrail-north-clyde.toml` is the North Clyde Line split successor
    // of the former `scotrail-glasgow-suburban.toml` (line-definition
    // audit, 2026-09-21); this segment was renamed from
    // `scotrail-glasgow-suburban-west-trunk` as part of that split, with
    // both West Highland files updated to match. Task 10.9
    // (`scotrail-west-highland-oban.toml`) independently confirmed that
    // Oban services also call at Dumbarton Central before diverging near
    // Craigendoran Junction (Dumbarton Central's own Wikipedia article
    // explicitly names "trains ... between Glasgow and Oban and Mallaig")
    // and reused this exact segment name for its own DMR/DBC entries too,
    // making this a genuine three-way shared segment. An incident at
    // Dumbarton Central should therefore match ALL THREE of
    // `scotrail-north-clyde`, `scotrail-west-highland-fort-william` and
    // `scotrail-west-highland-oban` with `MatchScope::SharedSegment`,
    // mirroring `scotrail_shared_inverness_dingwall_trunk_incident_propagates`.
    #[test]
    fn scotrail_west_highland_shares_north_clyde_west_trunk_incident_propagates() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-15",
            "Signal failure at Dumbarton Central",
            "Signal failure causing delays to ScotRail services at Dumbarton Central.",
            &["SR"],
            &["DBC"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "scotrail-north-clyde".to_string(),
                "scotrail-west-highland-fort-william".to_string(),
                "scotrail-west-highland-oban".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment",
                m.line.id
            );
        }
    }

    // Post-review fix round 2: unlike Dumbarton Central above, Hyndland
    // (and Dalreoch/Partick alongside it) is NOT physically traversed by
    // West Highland Line trains -- they run non-stop between Glasgow
    // Queen Street and Dumbarton Central. Hyndland/Dalreoch/Partick were
    // previously mistakenly tagged onto the same
    // `scotrail-glasgow-suburban-west-trunk` segment name as DMR/DBC,
    // which made `SegmentRegistry::is_shared` (name-keyed, not
    // station-keyed) incorrectly report an incident here as shared with
    // the West Highland lines too. They are now retagged onto
    // `scotrail-north-clyde-west-approach`, exclusive to this file --
    // see `lines/scotrail-north-clyde.toml`'s own HYN comment for the
    // full explanation. An incident at Hyndland should therefore match
    // only `scotrail-north-clyde`, with `ExclusiveSegment` scope.
    #[test]
    fn scotrail_north_clyde_west_approach_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-15B",
            "Signal failure at Hyndland",
            "Signal failure causing delays to ScotRail services at Hyndland.",
            &["SR"],
            &["HYN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-north-clyde".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 3.1 (station-catalogue-completeness plan), FILL-IN piece:
    // Kilpatrick (KPT) is one of the 19 previously-missing minor stations
    // added to the former `scotrail-glasgow-suburban.toml` by this task,
    // now `scotrail-north-clyde.toml` after the 2026-09-21 line-definition
    // audit's split. It sits on the Dalmuir-Dumbarton Central stretch
    // itself, so (per that file's own KPT comment) it inherits the
    // genuinely-shared `scotrail-north-clyde-west-trunk` segment rather
    // than the exclusive `west-approach` segment most of the other 18 new
    // stations use. Before Task 3.1, `has_station("KPT")` returned false
    // for this line and an incident there was invisible to the matcher
    // entirely. Neither West Highland sibling file lists KPT itself (their
    // own stations skip straight from DMR to DBC, per their own Sources
    // comments: "WHL trains run non-stop" over this stretch), so unlike
    // `scotrail_west_highland_shares_north_clyde_west_trunk_incident_propagates`
    // (which uses the pre-existing, all-three-files DBC station) this
    // incident only station-matches `scotrail-north-clyde` itself -- but
    // its scope is still correctly `SharedSegment`, because
    // `SegmentRegistry::is_shared` keys on the `west-trunk` segment NAME,
    // which the West Highland files do reuse, not on which specific
    // stations carry it. This proves the segment-name inheritance is
    // correct for a station that didn't exist in the catalogue at all
    // until Task 3.1.
    #[test]
    fn scotrail_north_clyde_new_kilpatrick_station_on_shared_west_trunk_segment() {
        let lines = load_line("scotrail-north-clyde");
        let north_clyde = lines
            .get("scotrail-north-clyde")
            .expect("scotrail-north-clyde line should exist");
        assert!(
            north_clyde.has_station("KPT"),
            "Kilpatrick (KPT) should now be a station on scotrail-north-clyde"
        );

        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-17",
            "Signal failure at Kilpatrick",
            "Signal failure causing delays to ScotRail services at Kilpatrick.",
            &["SR"],
            &["KPT"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-north-clyde".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::SharedSegment);
    }

    // Task 3.1, BRANCH-RESEARCH piece: the previously-unmodelled Lanarkshire
    // branch group (Whifflet spur, Hamilton Circle, Larkhall branch, Lanark
    // branch) added to the former `scotrail-glasgow-suburban.toml` by this
    // task, now `scotrail-argyle.toml` after the 2026-09-21 line-definition
    // audit's split (the whole Lanarkshire branch group is Argyle Line
    // territory). Whifflet (WFF) itself is a real junction (the Coatbridge
    // Central terminus spur diverges there), on the exclusive
    // `scotrail-argyle-whifflet-branch` segment -- not shared with any
    // sibling `lines/*.toml` file (grepped clean before Task 3.1, see that
    // file's own Task 3.1 BRANCH-RESEARCH comment). Mirrors
    // `scotrail_north_clyde_exclusive_segment_incident_does_not_propagate`
    // but for a station that didn't exist in the catalogue at all until
    // Task 3.1.
    #[test]
    fn scotrail_argyle_new_whifflet_branch_incident_does_not_propagate() {
        let lines = load_line("scotrail-argyle");
        let argyle = lines
            .get("scotrail-argyle")
            .expect("scotrail-argyle line should exist");
        assert!(
            argyle.has_station("WFF"),
            "Whifflet (WFF) should now be a station on scotrail-argyle"
        );

        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-18",
            "Signal failure at Whifflet",
            "Signal failure causing delays to ScotRail services at Whifflet.",
            &["SR"],
            &["WFF"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["scotrail-argyle".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 10.9 (ScotRail West Highland Line, Oban arm): this line's own
    // exclusive track beyond Crianlarich (tagged
    // `scotrail-west-highland-oban-exclusive`, used only by this one file)
    // should stay local to `scotrail-west-highland-oban` with
    // `ExclusiveSegment` scope -- mirroring
    // `scotrail_west_highland_fort_william_exclusive_segment_incident_does_not_propagate`.
    #[test]
    fn scotrail_west_highland_oban_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-16",
            "Signal failure at Taynuilt",
            "Signal failure causing delays to ScotRail services at Taynuilt.",
            &["SR"],
            &["TAY"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-west-highland-oban".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 10.9 (fix round 1, post-review): GLQ (Glasgow Queen Street) is
    // reused verbatim as `scotrail-west-highland-glasgow-terminus` by both
    // `scotrail-west-highland-fort-william` and
    // `scotrail-west-highland-oban` -- see both files' own GLQ comments
    // for why (the "combined trains ... splitting at Crianlarich" sourcing
    // means the shared corridor genuinely starts at GLQ itself, not just
    // from Dalmuir onward). GLQ is ALSO a real `[[stations]]` entry in
    // `scotrail-central-belt.toml` (its own exclusive
    // `scotrail-central-belt` segment) and `scotrail-north-clyde.toml`
    // (its own `scotrail-north-clyde-core` segment, the North Clyde Line
    // split successor of the former `scotrail-glasgow-suburban.toml`) --
    // both genuinely different physical platform groups/services at the
    // same named station, unaffected by this fix.
    //
    // UPDATE (added alongside `lines/scotrail-bathgate.toml`): that file's
    // own GLQ entry reuses `scotrail-north-clyde.toml`'s exact
    // `scotrail-north-clyde-core` segment name (a genuine shared fact --
    // see that file's own sourcing), so North Clyde's own scope at GLQ
    // changes from `ExclusiveSegment` to `SharedSegment` too, and
    // `scotrail-bathgate` itself now also matches here. So an incident at
    // GLQ correctly matches all five lines, with three different scopes:
    // the two West Highland lines get `SharedSegment` on their own
    // `scotrail-west-highland-glasgow-terminus` segment; North Clyde and
    // Bathgate get `SharedSegment` on their own, separate
    // `scotrail-north-clyde-core` segment; Central Belt and Lumo stay
    // `ExclusiveSegment` on their own unrelated segments.
    #[test]
    fn scotrail_west_highland_shares_glasgow_terminus_incident_propagates() {
        // Glasgow Queen Street is also lumo.toml's own terminus (merged
        // separately, Batch 10), on its exclusive `lumo-glasgow` segment --
        // an independent match, ExclusiveSegment (station overlap,
        // distinct segment name, not part of either shared trunk below).
        //
        // Greater Glasgow batch: `scotrail-cumbernauld.toml` (Low Level
        // platforms) and `scotrail-maryhill.toml` (High Level platforms)
        // both also list GLQ as their own terminus, each on its own
        // exclusive segment name -- the same station-overlap pattern as
        // lumo above, not a claimed shared trunk with anything here (see
        // both files' own segment-naming coordination notes).
        //
        // Updated (Scotland real-world-sanity review): the new scotrail-
        // glasgow-aberdeen.toml also terminates at GLQ, on its own distinct
        // exclusive `scotrail-glasgow-aberdeen-approach` segment -- the same
        // station-overlap pattern, an independent ExclusiveSegment match.
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-17",
            "Points failure at Glasgow Queen Street",
            "Points failure causing delays to ScotRail services at Glasgow Queen Street.",
            &["SR"],
            &["GLQ"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "scotrail-central-belt".to_string(),
                "scotrail-north-clyde".to_string(),
                "scotrail-bathgate".to_string(),
                "scotrail-west-highland-fort-william".to_string(),
                "scotrail-west-highland-oban".to_string(),
                "lumo".to_string(),
                "scotrail-cumbernauld".to_string(),
                "scotrail-maryhill".to_string(),
                "scotrail-glasgow-aberdeen".to_string(),
            ])
        );
        for m in &matches {
            let expected = match m.line.id.as_str() {
                "scotrail-west-highland-fort-william"
                | "scotrail-west-highland-oban"
                | "scotrail-north-clyde"
                | "scotrail-bathgate" => MatchScope::SharedSegment,
                _ => MatchScope::ExclusiveSegment,
            };
            assert_eq!(m.scope, expected, "{} scope mismatch", m.line.id);
        }
    }

    // Task 10.10 (ScotRail Aberdeen - Inverness Line): this line's own
    // exclusive track (Aberdeen through Inverness Airport, tagged
    // `scotrail-aberdeen-inverness`) is used only by this one file, so an
    // incident there should stay local with `ExclusiveSegment` scope --
    // mirroring `swr_exclusive_segment_incident_does_not_propagate`.
    #[test]
    fn scotrail_aberdeen_inverness_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-18",
            "Signal failure at Elgin",
            "Signal failure causing delays to ScotRail services at Elgin.",
            &["SR"],
            &["ELG"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-aberdeen-inverness".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 10.10 (ScotRail Aberdeen - Inverness Line): the load-bearing
    // shared-trunk test this pairing exists for. Task 10.5
    // (`scotrail-highland-main-line.toml`) reserved the segment name
    // `scotrail-highland-inverness-approach` for its own Inverness station
    // entry, deliberately not assuming a shared segment on
    // platform-sharing evidence alone; this file independently confirmed
    // genuine track-sharing (Wikipedia's Millburn Junction detail,
    // cross-checked against railwaycodes.org.uk's own ELR database -- see
    // `lines/scotrail-aberdeen-inverness.toml`'s own Sources comments) and
    // reused the exact same segment name.
    //
    // Inverness is also a station on `scotrail-far-north` and
    // `scotrail-kyle` (tagged `scotrail-inverness-dingwall-trunk`, that
    // pair's own independently-confirmed shared trunk from Task 10.6/10.7)
    // -- `LineDefinition::has_station` matches on station presence
    // regardless of segment name, so an incident at Inverness hits all
    // four lines, mirroring
    // `scotrail_west_highland_shares_glasgow_terminus_incident_propagates`'s
    // precedent for a station where two independent shared-trunk pairs
    // happen to converge. Since Inverness sits on a genuine cross-file
    // shared segment for *both* pairs (not merely a same-line exclusive
    // segment for either), all four lines get `MatchScope::SharedSegment`
    // here -- unlike the Glasgow Queen Street case, where only two of the
    // four lines had a cross-file shared segment at that station.
    #[test]
    fn scotrail_shared_highland_inverness_approach_incident_propagates() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-19",
            "Points failure at Inverness",
            "Points failure causing delays to ScotRail services at Inverness.",
            &["SR"],
            &["INV"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "scotrail-highland-main-line".to_string(),
                "scotrail-aberdeen-inverness".to_string(),
                "scotrail-far-north".to_string(),
                "scotrail-kyle".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment",
                m.line.id
            );
        }
    }

    // Follow-up task closing the Batch 10 gap flagged in
    // `scotrail-central-belt.toml` (Shotts and Bathgate had no dedicated
    // files). `scotrail-shotts.toml` has no internal branching -- one
    // exclusive segment, `scotrail-shotts-exclusive`, covering everything
    // from Glasgow Central to Slateford. An incident at Shotts itself (the
    // line's own namesake mid-corridor station) should therefore match
    // only `scotrail-shotts`, with `ExclusiveSegment` scope, mirroring
    // `scotrail_north_clyde_exclusive_segment_incident_does_not_propagate`.
    #[test]
    fn scotrail_shotts_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-20",
            "Signal failure at Shotts",
            "Signal failure causing delays to ScotRail services at Shotts.",
            &["SR"],
            &["SHS"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["scotrail-shotts".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 3.1 (station-catalogue-completeness plan), fix round 1
    // post-review: the former `scotrail-glasgow-suburban.toml`'s Task 3.1
    // FILL-IN piece added Uddingston (UDD) and Bellshill (BLH), both of
    // which were already `[[stations]]` entries in this file
    // (`scotrail-shotts.toml`) -- an undisclosed cross-file collision the
    // review caught, since this file's own pre-existing CBL comment had
    // explicitly pre-flagged this exact scenario. Verified as genuine
    // physical track sharing, not coincidental station-name overlap:
    // Wikipedia's "Shotts line" article states "Until Holytown Junction
    // the line [is] used by Argyle Line services", i.e. Argyle Line
    // services (now `scotrail-argyle.toml`, this file's own sibling after
    // the 2026-09-21 line-definition audit split the former bundled file)
    // physically run over the same Uddingston-Bellshill stretch this
    // file's Shotts-branded services use. Both files' UDD/BLH entries were
    // retagged onto a new shared segment, `scotrail-uddingston-bellshill-
    // trunk` -- see both files' own UDD/BLH comments for the full sourcing.
    // An incident at Bellshill should therefore match both
    // `scotrail-shotts` and `scotrail-argyle` with `MatchScope::
    // SharedSegment`, mirroring
    // `scotrail_shared_inverness_dingwall_trunk_incident_propagates`.
    #[test]
    fn scotrail_uddingston_bellshill_trunk_shares_argyle_incident_propagates() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-21",
            "Signal failure at Bellshill",
            "Signal failure causing delays to ScotRail services at Bellshill.",
            &["SR"],
            &["BLH"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-shotts".to_string(), "scotrail-argyle".to_string()])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment",
                m.line.id
            );
        }
    }

    // `scotrail-bathgate.toml`'s own middle section (Drumgelloch through
    // Edinburgh Park) is genuinely exclusive to that file today -- no other
    // `lines/*.toml` file has a station entry there. An incident at
    // Bathgate itself (the line's own namesake town) should therefore
    // match only `scotrail-bathgate`, with `ExclusiveSegment` scope,
    // mirroring `scotrail_shotts_exclusive_segment_incident_does_not_propagate`.
    #[test]
    fn scotrail_bathgate_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-21",
            "Points failure at Bathgate",
            "Points failure causing delays to ScotRail services at Bathgate.",
            &["SR"],
            &["BHG"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-bathgate".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `scotrail-central-belt.toml`'s own Haymarket comment pre-emptively
    // reserved `scotrail-central-belt-edinburgh-throat` for whoever
    // eventually modelled the Shotts and Bathgate routings, on the sourced
    // basis that both genuinely rejoin the Falkirk High routing's own
    // track before Haymarket and Edinburgh Waverley (RAILSCOT/Wikipedia
    // "Airdrie-Bathgate rail link" and "Edinburgh-Bathgate line" for
    // Bathgate's own Newbridge Junction rejoin; Wikipedia "Haymarket
    // railway station", independently, for Shotts's own 1853 Slateford
    // connection). `scotrail-shotts.toml` and `scotrail-bathgate.toml` both
    // reuse this exact segment name for their own Haymarket/Edinburgh
    // Waverley entries, so an incident at Haymarket should match all three
    // of `scotrail-central-belt`, `scotrail-shotts` and `scotrail-bathgate`
    // with `MatchScope::SharedSegment`, mirroring
    // `scotrail_west_highland_shares_north_clyde_west_trunk_incident_propagates`'s
    // three-way shared-segment shape.
    //
    // Haymarket is also a real, major interchange for several other
    // already-merged lines with no track-sharing claim sourced against this
    // throat (`scotrail-fife-circle`, the Fife Circle Line split successor
    // of the former `scotrail-fife-borders.toml`; `tpe-anglo-scottish`;
    // `lner-ecml`; `lumo`) -- each of those stays `ExclusiveSegment` on its
    // own, unrelated segment, mirroring
    // `scotrail_west_highland_shares_glasgow_terminus_incident_propagates`'s
    // mixed-scope shape at a heavily-overlapped hub station.
    //
    // UPDATE (added alongside `lines/scotrail-stirling-dunblane.toml`,
    // Central Scotland/Edinburgh gap-coverage batch): that file's own
    // Edinburgh to Dunblane Line (via Falkirk Grahamston) genuinely shares
    // this same Edinburgh-Haymarket-Linlithgow-Polmont approach with the
    // Falkirk High routing, diverging only at Polmont -- sourced
    // independently from the Shotts/Bathgate claim above, via Polmont's own
    // Wikipedia page (both routes share an identical previous station,
    // Linlithgow) and Falkirk Grahamston's own page (confirms the Edinburgh
    // to Dunblane Line calls there, not Falkirk High). It reuses this exact
    // segment name for its own Edinburgh Waverley/Haymarket entries, so an
    // incident at Haymarket now also matches `scotrail-stirling-dunblane`
    // with `MatchScope::SharedSegment`.
    //
    // Updated by the Scotland real-world-sanity review: the new
    // scotrail-edinburgh-aberdeen.toml reuses `scotrail-fife-circle-throat`
    // verbatim at Haymarket (from scotrail-fife-circle.toml), so it now also
    // matches with SharedSegment - and since sharing is evaluated per
    // segment NAME catalogue-wide (that name is now used by three files:
    // fife-circle, edinburgh-aberdeen, and scotrail-levenmouth.toml
    // elsewhere), `scotrail-fife-circle` itself flips here from
    // ExclusiveSegment to SharedSegment too. wcml-scotland.toml (new) also
    // calls at Haymarket, on its own exclusive `wcml-scotland-edinburgh`
    // segment (used nowhere else) - an independent ExclusiveSegment match.
    #[test]
    fn scotrail_central_belt_edinburgh_throat_shared_incident_propagates() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-22",
            "Overhead line damage at Haymarket",
            "Overhead line damage causing delays to ScotRail services at Haymarket.",
            &["SR"],
            &["HYM"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "scotrail-central-belt".to_string(),
                "scotrail-shotts".to_string(),
                "scotrail-bathgate".to_string(),
                "scotrail-fife-circle".to_string(),
                "scotrail-stirling-dunblane".to_string(),
                "tpe-anglo-scottish".to_string(),
                "lner-ecml".to_string(),
                "lumo".to_string(),
                "scotrail-edinburgh-aberdeen".to_string(),
                "wcml-scotland".to_string(),
            ])
        );
        for m in &matches {
            let expected = match m.line.id.as_str() {
                "scotrail-central-belt"
                | "scotrail-shotts"
                | "scotrail-bathgate"
                | "scotrail-stirling-dunblane"
                | "scotrail-fife-circle"
                | "scotrail-edinburgh-aberdeen" => MatchScope::SharedSegment,
                _ => MatchScope::ExclusiveSegment,
            };
            assert_eq!(m.scope, expected, "{} scope mismatch", m.line.id);
        }
    }

    // `lines/scotrail-north-berwick.toml` (Central Scotland/Edinburgh
    // gap-coverage batch): a genuinely standalone branch today -- no other
    // `lines/*.toml` file has a station entry at Drem, Longniddry,
    // Prestonpans, Wallyford or Musselburgh. An incident at North Berwick
    // itself should match only this line, `ExclusiveSegment`, mirroring
    // `scotrail_ayrshire_stranraer_branch_incident_does_not_propagate`.
    #[test]
    fn scotrail_north_berwick_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-28",
            "Signal failure at North Berwick",
            "Signal failure causing delays to ScotRail services at North Berwick.",
            &["SR"],
            &["NBW"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-north-berwick".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `lines/scotrail-stirling-dunblane.toml`'s own exclusive stretch
    // (Falkirk Grahamston through Dunblane) originally had no sibling file
    // -- the Croy Line and Cumbernauld Line, which genuinely share this
    // stretch in real life, were documented-but-unmodelled gaps (see that
    // file's own scope notes).
    //
    // Updated (Scotland real-world-sanity review): the new scotrail-
    // glasgow-aberdeen.toml also calls at Dunblane (not its own terminus,
    // but a genuine through station on its own Glasgow-Aberdeen spine),
    // reusing `scotrail-stirling-dunblane` verbatim - a genuine SharedSegment
    // pair, mirroring the Kirkcaldy/Haymarket precedent above for the other
    // new inter-city spine files.
    #[test]
    fn scotrail_stirling_dunblane_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-29",
            "Points failure at Dunblane",
            "Points failure causing delays to ScotRail services at Dunblane.",
            &["SR"],
            &["DBL"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "scotrail-stirling-dunblane".to_string(),
                "scotrail-glasgow-aberdeen".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment",
                m.line.id
            );
        }
    }

    // Integration-merge reconciliation: `scotrail-stirling-dunblane.toml`
    // and `scotrail-cumbernauld.toml` both list Falkirk Grahamston (FKG)
    // and Camelon (CMO) on the literal `scotrail-cumbernauld-falkirk-tail`
    // segment (a genuine shared trunk -- both routes converge here per
    // each file's own sourced comments), so an incident there should
    // propagate to both as SharedSegment. Mirrors
    // `scotrail_springburn_spur_incident_propagates_to_cumbernauld_and_north_clyde`.
    #[test]
    fn scotrail_cumbernauld_falkirk_tail_incident_propagates_to_stirling_dunblane() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-32",
            "Signal failure at Falkirk Grahamston",
            "Signal failure causing delays to ScotRail services at Falkirk Grahamston.",
            &["SR"],
            &["FKG"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "scotrail-cumbernauld".to_string(),
                "scotrail-stirling-dunblane".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment",
                m.line.id
            );
        }
    }

    // `lines/scotrail-levenmouth.toml`'s own Glenrothes with Thornton
    // (Thornton Junction) entry is a deliberate station-overlap-only
    // choice, not a shared segment: `scotrail-fife-circle.toml` (split out
    // of the former `scotrail-fife-borders.toml` by a later data-driven
    // catalogue audit) tags this same station with its own plain
    // `scotrail-fife-circle` segment name, which also covers Kirkcaldy and
    // the rest of that file's own loop --
    // reusing it here for just this one station would incorrectly mark
    // that whole loop as shared with this branch too (see
    // `lines/scotrail-levenmouth.toml`'s own comment on this station for
    // the full reasoning). An incident there should therefore match both
    // lines, each with its own `MatchScope::ExclusiveSegment`, mirroring
    // `xc_manchester_station_overlap_with_wmr_snow_hill_stays_exclusive_each_line`'s
    // station-overlap-without-segment-sharing shape.
    // Updated by the Scotland real-world-sanity review: the new
    // scotrail-edinburgh-aberdeen.toml reuses the plain `scotrail-fife-
    // circle` segment name verbatim (see the Kirkcaldy test above) - since
    // sharing is evaluated per segment NAME catalogue-wide, not per
    // station, `scotrail-fife-circle`'s OWN GLT entry now also reports
    // SharedSegment here even though scotrail-edinburgh-aberdeen has no
    // station at Glenrothes with Thornton itself. scotrail-levenmouth's own
    // distinct segment name is unaffected and stays ExclusiveSegment.
    #[test]
    fn scotrail_levenmouth_station_overlap_with_fife_borders_stays_exclusive_each_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-30",
            "Points failure at Glenrothes with Thornton",
            "Points failure causing delays to ScotRail services at Glenrothes with Thornton.",
            &["SR"],
            &["GLT"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "scotrail-fife-circle".to_string(),
                "scotrail-levenmouth".to_string(),
            ])
        );
        // REVIEW2 FIX (item 2, 2026-09-21): `scotrail-fife-circle` at GLT
        // used to show as SharedSegment, but only as a side effect of the
        // whole-route-segment over-sharing bug this review fixed:
        // `scotrail-edinburgh-aberdeen.toml` used to reuse the broad
        // `scotrail-fife-circle` segment name for its own SGL..KDY coastal
        // stretch, which made the segment NAME shared across two files even
        // though neither of scotrail-edinburgh-aberdeen's own stations is
        // GLT (an inland-loop-only station). That file now uses a narrower
        // `scotrail-fife-circle-coastal` name instead (see
        // scotrail-fife-circle.toml's own top-of-file comment), so
        // `scotrail-fife-circle` is once again used by only one file --
        // correctly ExclusiveSegment here, matching `scotrail-levenmouth`.
        // `scotrail-levenmouth.toml`'s own GLT entry has always used its own
        // `scotrail-levenmouth` segment name, not `scotrail-fife-circle`, so
        // this incident matches both lines by station overlap only, neither
        // sharing a segment with the other.
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    // `lines/scotrail-levenmouth.toml`'s own exclusive branch (Cameron
    // Bridge, Leven) has no sibling file. An incident at Leven itself, the
    // branch's own terminus, should match only this line, `ExclusiveSegment`,
    // mirroring `scotrail_bathgate_exclusive_segment_incident_does_not_propagate`.
    #[test]
    fn scotrail_levenmouth_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-31",
            "Level crossing fault at Leven",
            "Level crossing fault causing delays to ScotRail services at Leven.",
            &["SR"],
            &["LEV"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-levenmouth".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `scotrail-north-clyde.toml`'s own Bellgrove comment already named
    // Bathgate as one of the three eastbound splits from its North Clyde
    // core trackage (`scotrail-north-clyde.toml` is the North Clyde Line
    // split successor of the former `scotrail-glasgow-suburban.toml`).
    // `scotrail-bathgate.toml` reuses that file's own
    // `scotrail-north-clyde-core` segment name for its own Charing
    // Cross/Glasgow Queen Street/Bellgrove entries, so an incident at
    // Charing Cross should match both `scotrail-north-clyde` and
    // `scotrail-bathgate`, each with `MatchScope::SharedSegment`, mirroring
    // `scotrail_shared_inverness_dingwall_trunk_incident_propagates`'s
    // two-way shared-segment shape. Charing Cross (unlike Glasgow Queen
    // Street itself, which several unrelated lines also touch -- see
    // `scotrail_west_highland_shares_glasgow_terminus_incident_propagates`)
    // is not a `[[stations]]` entry in any other `lines/*.toml` file, so
    // this test's own `matched_ids` stays a clean two-line set.
    #[test]
    fn scotrail_bathgate_shares_north_clyde_core_incident_propagates() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-23",
            "Signal failure at Charing Cross",
            "Signal failure causing delays to ScotRail services at Charing Cross.",
            &["SR"],
            &["CHC"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "scotrail-north-clyde".to_string(),
                "scotrail-bathgate".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment",
                m.line.id
            );
        }
    }

    // `scotrail-north-clyde.toml`'s own Airdrie comment already noted that
    // real electrified track continues beyond its own North Clyde terminus
    // towards Bathgate. `scotrail-bathgate.toml` reuses that file's own
    // `scotrail-north-clyde-airdrie-branch` segment name for its own
    // Airdrie entry (the junction-in-service-pattern where Bathgate-bound
    // trains continue past the North Clyde terminus pattern), so an
    // incident at Airdrie should match both `scotrail-north-clyde` and
    // `scotrail-bathgate`, each with `MatchScope::SharedSegment`, mirroring
    // `scotrail_bathgate_shares_north_clyde_core_incident_propagates`
    // immediately above.
    #[test]
    fn scotrail_bathgate_shares_north_clyde_airdrie_branch_incident_propagates() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-24",
            "Points failure at Airdrie",
            "Points failure causing delays to ScotRail services at Airdrie.",
            &["SR"],
            &["ADR"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "scotrail-north-clyde".to_string(),
                "scotrail-bathgate".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment",
                m.line.id
            );
        }
    }

    // Task 10.2 (chiltern-main-line.toml catalogue-completeness pass).
    // Sudbury & Harrow Road, Princes Risborough and a full run of other
    // Marylebone-approach stations were previously omitted from this file
    // as "minor intermediate calls" per its own stale header comment.
    // Princes Risborough needed particular care: confirmed (see that
    // station's own comment in lines/chiltern-main-line.toml) to be
    // genuinely on this line's Marylebone-Bicester North-Banbury route,
    // not only the separate Oxford or Aylesbury routes.
    #[test]
    fn chiltern_main_line_has_previously_omitted_marylebone_approach_stations() {
        let lines = load_line("chiltern-main-line");
        let line = lines
            .get("chiltern-main-line")
            .expect("chiltern-main-line should exist");
        assert!(
            line.has_station("SUD"),
            "chiltern-main-line should now include Sudbury & Harrow Road (SUD)"
        );
        assert!(
            line.has_station("PRR"),
            "chiltern-main-line should now include Princes Risborough (PRR)"
        );
    }

    // Task 10.2, continued: Lapworth, Widney Manor, Olton, Acocks Green,
    // Tyseley, Small Heath and Bordesley were previously omitted on the
    // Hatton -> Birmingham approach (see that stretch's own comments in
    // lines/chiltern-main-line.toml). Widney Manor, Olton and Acocks Green
    // also appear in `wmr-snow-hill.toml`'s own independently-worked
    // "omitted" list (Batch 9, a separate isolated worktree this task
    // couldn't see or coordinate with live), but per the Task 10.2
    // controller ruling this file deliberately keeps them on its own
    // existing `chiltern-birmingham-approach` segment rather than
    // guessing at whatever segment name that concurrent agent picks -- so,
    // as this catalogue is loaded in this worktree today, no sibling line
    // shares that segment name yet. Skip the second (shared-segment)
    // assertion, and say so here, per the Testing convention: "Skip this
    // second assertion... when the new station's segment has no sibling."
    #[test]
    fn chiltern_main_line_has_previously_omitted_birmingham_approach_stations() {
        let lines = load_line("chiltern-main-line");
        let line = lines
            .get("chiltern-main-line")
            .expect("chiltern-main-line should exist");
        for crs in ["LPW", "WMR", "OLT", "ACG", "TYS", "SMA", "BBS"] {
            assert!(
                line.has_station(crs),
                "chiltern-main-line should now include {crs}"
            );
        }
    }

    // Station-catalogue-completeness plan, Task 5.1, FILL-IN piece:
    // southeastern-chatham.toml previously omitted the four suburban/
    // slow-line stations between Herne Hill and Bromley South (West
    // Dulwich, Sydenham Hill, Penge East, Kent House) for unconfirmed
    // fast/slow calling pattern reasons. They're now confirmed and
    // inserted - originally on `chatham-london`, renamed to
    // `chatham-maidstone-victoria` by the shared-segment structural review
    // (review2-shared-segments; see southeastern-chatham.toml's own header
    // comment) - this is a regression guard that `has_station` picks them
    // up via `LineDefinition::from_dir` (i.e. the TOML actually parses and
    // the stations aren't silently dropped or misspelled).
    #[test]
    fn chatham_fillin_suburban_stations_are_now_modelled() {
        let lines = load_line("southeastern-chatham");
        let chatham = lines
            .get("southeastern-chatham")
            .expect("southeastern-chatham line should exist");
        for crs in ["WDU", "SYH", "PNE", "KTH"] {
            assert!(
                chatham.has_station(crs),
                "southeastern-chatham should now have station {crs}"
            );
            assert_eq!(
                chatham.segment_for(crs),
                Some("chatham-maidstone-victoria"),
                "{crs} should be on chatham-maidstone-victoria"
            );
        }
    }

    // Station-catalogue-completeness plan, Task 5.1, BRANCH-RESEARCH
    // piece: the Ramsgate-onward coastal-loop continuation (Minster,
    // Sandwich, Deal, Walmer, Martin Mill) is now confirmed and modelled
    // on a new `chatham-deal` segment (see southeastern-chatham.toml's
    // own header comment for why this is a new segment rather than a
    // continuation of `chatham-coastal`). `chatham-deal` isn't reused by
    // any other file in the catalogue (grepped `lines/*.toml` before
    // picking the name), so per this task's recipe a shared-segment
    // MatchScope assertion is skipped - instead this mirrors
    // `chatham_exclusive_segment_incident_does_not_propagate` above and
    // confirms an incident on the new branch stays ExclusiveSegment and
    // scoped to this file alone, same as the rest of this file's
    // segments.
    #[test]
    fn chatham_deal_branch_stations_are_now_modelled_and_stay_exclusive() {
        let lines = load_all_lines();
        let chatham = lines
            .get("southeastern-chatham")
            .expect("southeastern-chatham line should exist");
        for crs in ["MSR", "SDW", "DEA", "WAM", "MTM"] {
            assert!(
                chatham.has_station(crs),
                "southeastern-chatham should now have station {crs}"
            );
            assert_eq!(
                chatham.segment_for(crs),
                Some("chatham-deal"),
                "{crs} should be on chatham-deal"
            );
        }

        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-8",
            "Signal failure at Deal",
            "Signal failure causing delays to Southeastern services.",
            &["SE"],
            &["DEA"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["southeastern-chatham".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Station-catalogue-completeness plan, Task 5.3:
    // southeastern-main-line.toml previously omitted the suburban
    // stopping-pattern stations between London Bridge and Orpington (New
    // Cross, St Johns, Lewisham, Hither Green, Grove Park, Chislehurst,
    // Petts Wood) pending route-diagram confirmation. They're now
    // confirmed and inserted - originally all on one `seml-london` segment,
    // split by the shared-segment structural review (review2-shared-
    // segments) into the genuinely shared CHX-LEW stretch (NWX/SAJ/LEW, now
    // `southeastern-lewisham-corridor`) and this file's own exclusive
    // continuation past Lewisham (HGR/GRP/CIT/PET, now
    // `seml-orpington-branch`) - see southeastern-main-line.toml's own
    // header comment. This is a regression guard that `has_station`/
    // `segment_for` pick them up via `LineDefinition::from_dir` (i.e. the
    // TOML actually parses and the stations aren't silently dropped or
    // misspelled).
    #[test]
    fn seml_fillin_suburban_stations_are_now_modelled() {
        let lines = load_line("southeastern-main-line");
        let seml = lines
            .get("southeastern-main-line")
            .expect("southeastern-main-line should exist");
        for crs in ["NWX", "SAJ", "LEW"] {
            assert!(
                seml.has_station(crs),
                "southeastern-main-line should now have station {crs}"
            );
            assert_eq!(
                seml.segment_for(crs),
                Some("southeastern-lewisham-corridor"),
                "{crs} should be on southeastern-lewisham-corridor"
            );
        }
        for crs in ["HGR", "GRP", "CIT", "PET"] {
            assert!(
                seml.has_station(crs),
                "southeastern-main-line should now have station {crs}"
            );
            assert_eq!(
                seml.segment_for(crs),
                Some("seml-orpington-branch"),
                "{crs} should be on seml-orpington-branch"
            );
        }
    }

    // New Cross (NWX) is a station overlap between this file's own
    // `seml-london`, overground-windrush.toml's own
    // `overground-windrush-new-cross` (a different route entirely, the
    // London Overground Windrush line's own New Cross terminus branch), and
    // - since the southeastern-metro-north-kent split
    // (southeastern-bexleyheath.toml/southeastern-dartford-loop.toml, per a
    // data-driven line-definition audit) - BOTH of those two new files'
    // shared `southeastern-lewisham-corridor` trunk segment (NWX sits
    // before the Lewisham fork, so it's still common to both). This file
    // and overground-windrush.toml each use their own distinct segment
    // name here and stay independently ExclusiveSegment (mirrors
    // lbg_station_overlap_spans_nine_lines_bexleyheath_and_dartford_loop_share_the_trunk
    // above), but southeastern-bexleyheath and southeastern-dartford-loop
    // share the literal segment name at NWX, so the registry correctly
    // promotes both of those two to SharedSegment.
    //
    // Kent/Sussex batch: southeastern-north-kent.toml also USED TO have a
    // station at New Cross - originally its own `southeastern-north-kent`
    // segment, the point this file's and southeastern-metro-north-kent.
    // toml's own NWX comments both already flagged as where "a
    // differently-aligned North Kent Line route ... diverges" - a fifth
    // independent match at the time.
    //
    // REVIEW FIX (shared-segment structural review, review2-shared-
    // segments): southeastern-main-line.toml and southeastern-north-
    // kent.toml were both fixed to reuse `southeastern-lewisham-corridor`
    // at NWX too (see each file's own header comment), so this set briefly
    // had FOUR lines sharing the literal segment name (main-line,
    // bexleyheath, dartford-loop, north-kent), all promoted to
    // SharedSegment together.
    //
    // REVIEW2 FIX (2026-09-21): southeastern-north-kent.toml's own NWX
    // entry has been removed entirely - fresh research found the Greenwich
    // line this file models genuinely diverges from the South Eastern Main
    // Line at North Kent East Junction, BEFORE New Cross (between London
    // Bridge and New Cross), not "just past" it, so this file's own trains
    // never physically call at New Cross at all; see that file's own DEP
    // entry comment for the full write-up. This set is back down to THREE
    // lines sharing `southeastern-lewisham-corridor` at NWX (main-line,
    // bexleyheath, dartford-loop). overground-windrush.toml's own
    // `overground-windrush-new-cross` is untouched by either review (a
    // different route entirely) and stays independently ExclusiveSegment.
    #[test]
    fn nwx_station_overlap_matches_four_lines_bexleyheath_and_dartford_loop_share_the_trunk() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-14",
            "Signal failure at New Cross",
            "Signal failure causing delays to Southeastern services.",
            &["SE"],
            &["NWX"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "southeastern-main-line".to_string(),
                "southeastern-bexleyheath".to_string(),
                "southeastern-dartford-loop".to_string(),
                "overground-windrush".to_string(),
            ])
        );
        for m in &matches {
            let expected = if m.line.id == "overground-windrush" {
                MatchScope::ExclusiveSegment
            } else {
                MatchScope::SharedSegment
            };
            assert_eq!(m.scope, expected, "{} should be {:?}", m.line.id, expected);
        }
    }

    // Hither Green (HGR) is a station overlap between this file's own
    // `seml-london` and southeastern-dartford-loop.toml's own
    // `dartford-loop-branch` (the Dartford Loop line's own exclusive tracks
    // diverge AT Hither Green, per southeastern-main-line.toml's own header
    // comment - the station itself is shared, the tracks beyond it are
    // not). HGR does NOT touch southeastern-bexleyheath.toml at all - that
    // line's own branch has already diverged from the shared trunk earlier,
    // at Lewisham (see the senk-split comment on this station in
    // southeastern-dartford-loop.toml itself). Confirms an incident there
    // matches both lines independently, each still scoped ExclusiveSegment,
    // never SharedSegment.
    #[test]
    fn hgr_station_overlap_matches_seml_and_dartford_loop_as_independent_exclusive_segments() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-15",
            "Signal failure at Hither Green",
            "Signal failure causing delays to Southeastern services.",
            &["SE"],
            &["HGR"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "southeastern-main-line".to_string(),
                "southeastern-dartford-loop".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment, not shared",
                m.line.id
            );
        }
    }

    // Chislehurst and Petts Wood are new to the catalogue - no other file
    // models them, so an incident there should stay exclusive to
    // southeastern-main-line alone. Mirrors
    // chatham_deal_branch_stations_are_now_modelled_and_stay_exclusive
    // above.
    //
    // Kent/Sussex batch: Grove Park (GRP) is no longer exclusive to this
    // set - southeastern-bromley-north.toml now also has a station there
    // (its own `southeastern-bromley-north` segment, the point this file's
    // own GRP comment already flagged as where "the Bromley North line ...
    // diverges") - moved to its own case below with that additional match.
    #[test]
    fn seml_grove_park_chislehurst_petts_wood_are_exclusive_to_seml() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        for crs in ["CIT", "PET"] {
            let inc = incident(
                "SE-16",
                "Signal failure",
                "Signal failure causing delays to Southeastern services.",
                &["SE"],
                &[crs],
            );
            let matches = lines_affected_by(&inc, &lines, &registry);
            let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
            assert_eq!(
                matched_ids,
                HashSet::from(["southeastern-main-line".to_string()]),
                "{crs} should match only southeastern-main-line"
            );
            assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
        }
    }

    // Kent/Sussex batch: Grove Park (GRP) is a station overlap between this
    // file's own `seml-london` and southeastern-bromley-north.toml's own
    // `southeastern-bromley-north` segment - confirms an incident there
    // matches both lines independently, each still scoped ExclusiveSegment,
    // never SharedSegment.
    #[test]
    fn grp_station_overlap_matches_seml_and_bromley_north_as_independent_exclusive_segments() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SE-16B",
            "Signal failure at Grove Park",
            "Signal failure causing delays to Southeastern services.",
            &["SE"],
            &["GRP"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "southeastern-main-line".to_string(),
                "southeastern-bromley-north".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment, not shared",
                m.line.id
            );
        }
    }

    // Station-catalogue-completeness plan, Task 5.4
    // (southern-brighton-main-line.toml): a fresh route-diagram pass
    // confirmed Clapham Junction (CLJ) and Wivelsfield (WVF) as genuinely
    // missing, currently-Southern-served stations - see each entry's own
    // comment in the TOML file for sourcing. Regression guard: the TOML
    // actually parses and both stations are picked up via
    // `LineDefinition::from_dir` on their expected segment.
    #[test]
    fn southern_brighton_main_line_fillin_stations_are_now_modelled() {
        let lines = load_line("southern-brighton-main-line");
        let bml = lines
            .get("southern-brighton-main-line")
            .expect("southern-brighton-main-line should exist");
        assert!(
            bml.has_station("CLJ"),
            "southern-brighton-main-line should now have station CLJ"
        );
        assert_eq!(
            bml.segment_for("CLJ"),
            Some("southern-bml-victoria"),
            "CLJ should be on southern-bml-victoria"
        );
        assert!(
            bml.has_station("WVF"),
            "southern-brighton-main-line should now have station WVF"
        );
        assert_eq!(
            bml.segment_for("WVF"),
            Some("brighton-main-line-central"),
            "WVF should be on brighton-main-line-central (originally \
             brighton-main-line-south, renamed from southern-bml-south by \
             the shared-segment structural review, review2-shared-segments; \
             review2 item 3 later split brighton-main-line-south at Three \
             Bridges into brighton-main-line-south (GTW/TBD only) and \
             brighton-main-line-central (HHE onward, including WVF) to \
             restore byte-identical sharing with thameslink-southern.toml \
             once that file's own Balcombe/BAB entry was found to diverge - \
             see southern-brighton-main-line.toml's own header/TBD-entry \
             comments)"
        );
    }

    // Clapham Junction (CLJ) turns out to be a station overlap across
    // several SWR files (swr-south-west-main.toml, swr-portsmouth-
    // direct.toml, swr-alton.toml, swr-kingston-loop.toml,
    // swr-chessington.toml, and -- added by the Wessex/Thames-Valley/
    // Isle-of-Wight batch -- swr-west-of-england.toml and
    // swr-windsor-lines.toml) that all share the literal
    // `swr-trunk-waterloo` segment name there (a genuine shared physical
    // trunk out of Waterloo), so those seven should resolve as
    // SharedSegment together; the two Overground files each use their own
    // exclusive segment name (`overground-windrush-clapham-branch`,
    // `overground-mildmay-clapham-branch`) and this file's own new
    // `southern-bml-victoria` is likewise unique to it (grepped
    // `lines/*.toml` before picking the name) - all three of those stay
    // ExclusiveSegment, independent of the SWR septet and of each other.
    // Mirrors the mixed shared/exclusive pattern already exercised elsewhere
    // in this file (e.g. the LBG/DVP multi-file overlaps), just with more
    // lines at once.
    //
    // Updated by the SE/SWR-loops batch: swr-chertsey-loop.toml,
    // swr-hounslow-loop.toml and swr-new-guildford.toml (three more new
    // files) all also reuse `swr-trunk-waterloo` verbatim at CLJ (each
    // file's own SEGMENTS diagram documents the same Waterloo-Clapham
    // Junction approach before diverging), growing the SharedSegment side
    // from seven SWR files to ten. The three Overground/Southern exclusive
    // matches are unaffected.
    //
    // Updated again by the SWR suburban-gap batch: swr-waterloo-
    // reading.toml, swr-shepperton-branch.toml, swr-hampton-court-
    // branch.toml and swr-epsom-mole-valley.toml (four more new files) all
    // also reuse `swr-trunk-waterloo` verbatim at CLJ, growing the
    // SharedSegment side from ten SWR files to fourteen. The three
    // Overground/Southern exclusive matches remain unaffected.
    //
    // Updated by the Southern real-world-sanity review: two new files,
    // southern-metro-crystal-palace.toml and southern-metro-sutton.toml,
    // also call at CLJ, sharing their own `southern-metro-victoria-trunk`
    // segment verbatim with EACH OTHER (a separate SharedSegment pair,
    // independent of the fourteen-strong SWR family) - station overlap only
    // against every other line here. Both join the SharedSegment side (of
    // their own pair), growing the total match count to seventeen.
    #[test]
    fn clj_station_overlap_matches_swr_trunk_shared_and_overground_and_bml_as_mixed_scope() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SN-1",
            "Signal failure at Clapham Junction",
            "Signal failure causing delays to services.",
            &["SN"],
            &["CLJ"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "swr-south-west-main".to_string(),
                "swr-portsmouth-direct".to_string(),
                "swr-alton".to_string(),
                "swr-kingston-loop".to_string(),
                "swr-chessington".to_string(),
                "swr-west-of-england".to_string(),
                "swr-windsor-lines".to_string(),
                "swr-chertsey-loop".to_string(),
                "swr-hounslow-loop".to_string(),
                "swr-new-guildford".to_string(),
                "swr-waterloo-reading".to_string(),
                "swr-shepperton-branch".to_string(),
                "swr-hampton-court-branch".to_string(),
                "swr-epsom-mole-valley".to_string(),
                "overground-windrush".to_string(),
                "overground-mildmay".to_string(),
                "southern-brighton-main-line".to_string(),
                "southern-metro-crystal-palace".to_string(),
                "southern-metro-sutton".to_string(),
            ])
        );
        for m in &matches {
            let expected = if [
                "swr-south-west-main",
                "swr-portsmouth-direct",
                "swr-alton",
                "swr-kingston-loop",
                "swr-chessington",
                "swr-west-of-england",
                "swr-windsor-lines",
                "swr-chertsey-loop",
                "swr-hounslow-loop",
                "swr-new-guildford",
                "swr-waterloo-reading",
                "swr-shepperton-branch",
                "swr-hampton-court-branch",
                "swr-epsom-mole-valley",
                "southern-metro-crystal-palace",
                "southern-metro-sutton",
            ]
            .contains(&m.line.id.as_str())
            {
                MatchScope::SharedSegment
            } else {
                MatchScope::ExclusiveSegment
            };
            assert_eq!(m.scope, expected, "{} should be {:?}", m.line.id, expected);
        }
    }

    // Task 4.1 (station-catalogue completeness): `tfw-cambrian.toml`
    // previously omitted every coast-branch request-stop halt for brevity.
    // A dozen of those (Penhelig, Tonfanau, Llwyngwril, Llanaber, Talybont,
    // Dyffryn Ardudwy, Llanbedr, Pensarn, Llandanwg, Tygwyn, Talsarnau,
    // Llandecwyn, Penychain) are confirmed currently-open stations and are
    // now individually listed. Before this fix, `has_station` returned
    // false for all of them, so `/stations/<crs>` would have silently
    // returned an empty disruption list instead of resolving to this line.
    // Llangelynin, by contrast, is confirmed closed (disused since 1991)
    // and is deliberately still absent -- asserted here too as a guard
    // against it being mistakenly re-added later.
    #[test]
    fn tfw_cambrian_has_station_includes_newly_added_coast_request_stops() {
        let lines = load_line("tfw-cambrian");
        let cambrian = lines
            .get("tfw-cambrian")
            .expect("tfw-cambrian line should exist");
        for crs in [
            "PHG", "TNF", "LLW", "LLA", "TLB", "DYF", "LBR", "PES", "LDN", "TYG", "TAL", "LLC",
            "PNC",
        ] {
            assert!(
                cambrian.has_station(crs),
                "tfw-cambrian should now list {crs}"
            );
        }
        assert!(
            !cambrian.has_station("LGY"),
            "Llangelynin is closed (disused since 1991) and should not be listed"
        );
    }

    // `tfw-cambrian-coast` is not (yet) a cross-file shared segment -- no
    // other line in the catalogue reaches the Cambrian Coast branch (see
    // `lines/tfw-cambrian.toml`'s own bundled-file comment) -- so unlike
    // `swr_shared_trunk_incident_propagates`, there is no sibling line to
    // assert a `MatchScope::SharedSegment` against for a newly-added
    // station here. This test instead confirms an incident at one of the
    // newly-added stations (Tonfanau) resolves to `tfw-cambrian` alone,
    // with `MatchScope::ExclusiveSegment`, exactly like the rest of the
    // coast branch already did before this fix.
    #[test]
    fn tfw_cambrian_new_coast_station_incident_resolves_exclusive_segment() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TFW-CAM-1",
            "Points failure at Tonfanau",
            "Points failure causing delays to Transport for Wales services at Tonfanau.",
            &["AW"],
            &["TNF"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["tfw-cambrian".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 4.5 (station-catalogue completeness): `tfw-north-wales-coast.toml`
    // previously omitted 9 minor intermediate calls -- Shotton, Conwy,
    // Penmaenmawr, Llanfairfechan, and (on Anglesey, after Bangor)
    // Llanfairpwll, Bodorgan, Tŷ Croes, Rhosneigr and Valley -- for brevity.
    // All 9 are confirmed currently-open stations and are now individually
    // listed. Before this fix, `has_station` returned false for all of
    // them, so `/stations/<crs>` would have silently returned an empty
    // disruption list instead of resolving to this line. Llandudno (a
    // different line's branch territory) and the file's own closed-station
    // exclusions (e.g. Menai Bridge, Gaerwen) are deliberately still
    // absent -- asserted here too as a guard against them being mistakenly
    // added later.
    #[test]
    fn tfw_north_wales_coast_has_station_includes_newly_added_minor_calls() {
        let lines = load_line("tfw-north-wales-coast");
        let nwc = lines
            .get("tfw-north-wales-coast")
            .expect("tfw-north-wales-coast line should exist");
        for crs in [
            "SHT", "CNW", "PMW", "LLF", "LPG", "BOR", "TYC", "RHO", "VAL",
        ] {
            assert!(
                nwc.has_station(crs),
                "tfw-north-wales-coast should now list {crs}"
            );
        }
        assert!(
            !nwc.has_station("LLD"),
            "Llandudno itself is a different line's branch territory and should not be listed"
        );
    }

    // `tfw-north-wales-coast`'s Chester-Holyhead segment (`tfw-north-wales-
    // coast`) is, as of the 2026-09-21 real-world-sanity review, a genuine
    // cross-file shared segment with `wcml-north-wales.toml` (reversing
    // this file's own former station-overlap-only ruling at Chester -- see
    // this file's own "Cross-batch note" comment above Chester). Segment
    // sharing is indexed by name across the whole catalogue, not
    // per-station, so an incident at Conwy -- a station
    // `wcml-north-wales.toml` doesn't separately list -- still resolves to
    // `tfw-north-wales-coast` alone (no other file has a CNW entry to
    // match), but classified `MatchScope::SharedSegment`, not
    // `ExclusiveSegment`: this station genuinely sits on the same
    // physical, Avanti-shared main line as the 8 stations both files list,
    // even though Avanti's own limited-stop service doesn't call here.
    // This test's own name is kept (its previous conclusion has been
    // superseded, not its subject) so a future editor who greps for it by
    // name still lands on this exact case; see
    // `north_wales_coast_own_stretch_incident_is_shared_segment_but_
    // single_match` above for the twin assertion at a different station,
    // and `llj_station_overlap_matches_both_lines_as_exclusive`/
    // `chester_station_overlap_matches_both_lines_as_exclusive` for the
    // cases where `wcml-north-wales.toml` genuinely does have a matching
    // entry too.
    #[test]
    fn tfw_north_wales_coast_new_station_incident_resolves_exclusive_segment() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "TFW-NWC-1",
            "Signalling fault at Conwy",
            "Signalling fault causing delays to Transport for Wales services at Conwy.",
            &["AW"],
            &["CNW"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["tfw-north-wales-coast".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::SharedSegment);
    }

    // Task 2.3: `northern-calder-valley.toml`'s previously-missing
    // intermediate halts. Mytholmroyd (MYT) sits between Sowerby Bridge and
    // Hebden Bridge on this file's sole segment, `northern-calder-valley`,
    // which per that file's own top-of-file comment has no shared-trunk
    // sibling anywhere in this catalogue - so there is no SharedSegment
    // assertion to make here, only that `has_station` picks up the new
    // station and that an incident there stays exclusive, mirroring
    // `calder_valley_exclusive_segment_incident_does_not_propagate` above.
    #[test]
    fn calder_valley_mytholmroyd_has_station_and_stays_exclusive() {
        let lines = load_line("northern-calder-valley");
        let calder_valley = lines
            .get("northern-calder-valley")
            .expect("northern-calder-valley should load");
        assert!(
            calder_valley.has_station("MYT"),
            "northern-calder-valley should now list Mytholmroyd (MYT)"
        );
        assert_eq!(
            calder_valley.segment_for("MYT"),
            Some("northern-calder-valley")
        );

        let all_lines = load_all_lines();
        let registry = SegmentRegistry::new(&all_lines);
        let inc = incident(
            "NT-10",
            "Signal failure at Mytholmroyd",
            "Signal failure causing delays on the Calder Valley Line at Mytholmroyd.",
            &["NT"],
            &["MYT"],
        );
        let matches = lines_affected_by(&inc, &all_lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["northern-calder-valley".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 2.4: `northern-cumbrian-coast.toml`'s previously-missing
    // intermediate halts. Sellafield (SEL) sits between St Bees/Corkickle
    // and Whitehaven on this file's sole non-junction segment,
    // `northern-cumbrian-coast`, which (per a fresh grep of `lines/*.toml`
    // while writing this test) is not shared by any sibling line in this
    // catalogue - so there is no SharedSegment assertion to make here, only
    // that `has_station` picks up the new station and that an incident
    // there stays exclusive to this line.
    #[test]
    fn cumbrian_coast_sellafield_has_station_and_stays_exclusive() {
        let lines = load_line("northern-cumbrian-coast");
        let cumbrian_coast = lines
            .get("northern-cumbrian-coast")
            .expect("northern-cumbrian-coast should load");
        assert!(
            cumbrian_coast.has_station("SEL"),
            "northern-cumbrian-coast should now list Sellafield (SEL)"
        );
        assert_eq!(
            cumbrian_coast.segment_for("SEL"),
            Some("northern-cumbrian-coast")
        );

        let all_lines = load_all_lines();
        let registry = SegmentRegistry::new(&all_lines);
        let inc = incident(
            "NT-11",
            "Trespass incident at Sellafield",
            "Trespass incident causing delays on the Cumbrian Coast Line at Sellafield.",
            &["NT"],
            &["SEL"],
        );
        let matches = lines_affected_by(&inc, &all_lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["northern-cumbrian-coast".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 2.6: `northern-furness.toml`'s previously-missing intermediate
    // halts. Grange-over-Sands (GOS) sits between Carnforth and Ulverston on
    // this file's exclusive `northern-furness-branch` segment, which (per a
    // fresh grep of `lines/*.toml` while writing this test) is not shared by
    // any sibling line in this catalogue - so there is no SharedSegment
    // assertion to make here, only that `has_station` picks up the new
    // station and that an incident there stays exclusive to this line.
    #[test]
    fn furness_grange_over_sands_has_station_and_stays_exclusive() {
        let lines = load_line("northern-furness");
        let furness = lines
            .get("northern-furness")
            .expect("northern-furness should load");
        assert!(
            furness.has_station("GOS"),
            "northern-furness should now list Grange-over-Sands (GOS)"
        );
        assert_eq!(furness.segment_for("GOS"), Some("northern-furness-branch"));

        let all_lines = load_all_lines();
        let registry = SegmentRegistry::new(&all_lines);
        let inc = incident(
            "NT-12",
            "Signal failure at Grange-over-Sands",
            "Signal failure causing delays on the Furness Line at Grange-over-Sands.",
            &["NT"],
            &["GOS"],
        );
        let matches = lines_affected_by(&inc, &all_lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["northern-furness".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 2.7: `northern-hope-valley.toml`'s previously-missing intermediate
    // halts. Bamford (BAM) sits between Hope and Hathersage on this file's
    // sole segment, `northern-hope-valley`, which (per a grep of `lines/
    // *.toml` for the literal `segment = "northern-hope-valley"` value while
    // writing this test) is not shared by any sibling line - so there is no
    // SharedSegment assertion to make here, only that `has_station` picks up
    // the new station and that an incident there stays exclusive. This is
    // deliberately distinct from `emr-regional.toml`, which correctly
    // excludes these same physical stations for EMR's own fast service - see
    // this file's own comment and Task 7.3.
    #[test]
    fn hope_valley_bamford_has_station_and_stays_exclusive() {
        let lines = load_line("northern-hope-valley");
        let hope_valley = lines
            .get("northern-hope-valley")
            .expect("northern-hope-valley should load");
        assert!(
            hope_valley.has_station("BAM"),
            "northern-hope-valley should now list Bamford (BAM)"
        );
        assert_eq!(hope_valley.segment_for("BAM"), Some("northern-hope-valley"));

        let all_lines = load_all_lines();
        let registry = SegmentRegistry::new(&all_lines);
        let inc = incident(
            "NT-13",
            "Signal failure at Bamford",
            "Signal failure causing delays on the Hope Valley Line at Bamford.",
            &["NT"],
            &["BAM"],
        );
        let matches = lines_affected_by(&inc, &all_lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["northern-hope-valley".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 2.8: `northern-lakes.toml`'s previously-missing intermediate
    // halts. Burneside (BUD) sits between Kendal and Staveley on this file's
    // sole segment, `northern-lakes`, which (per a grep of `lines/*.toml`
    // while writing this test) is not shared by any sibling line - so there
    // is no SharedSegment assertion to make here, only that `has_station`
    // picks up the new station and that an incident there stays exclusive.
    #[test]
    fn lakes_burneside_has_station_and_stays_exclusive() {
        let lines = load_line("northern-lakes");
        let lakes = lines
            .get("northern-lakes")
            .expect("northern-lakes should load");
        assert!(
            lakes.has_station("BUD"),
            "northern-lakes should now list Burneside (BUD)"
        );
        assert_eq!(lakes.segment_for("BUD"), Some("northern-lakes"));

        let all_lines = load_all_lines();
        let registry = SegmentRegistry::new(&all_lines);
        let inc = incident(
            "NT-14",
            "Signal failure at Burneside",
            "Signal failure causing delays on the Lakes Line at Burneside.",
            &["NT"],
            &["BUD"],
        );
        let matches = lines_affected_by(&inc, &all_lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["northern-lakes".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 2.9: `northern-tyne-valley.toml`'s previously-missing intermediate
    // halts. Prudhoe (PRU) sits between Stocksfield and Wylam on this file's
    // sole segment, `northern-tyne-valley`, which (per a grep of `lines/
    // *.toml` while writing this test) is not shared by any sibling line -
    // so there is no SharedSegment assertion to make here, only that
    // `has_station` picks up the new station and that an incident there
    // stays exclusive.
    #[test]
    fn tyne_valley_prudhoe_has_station_and_stays_exclusive() {
        let lines = load_line("northern-tyne-valley");
        let tyne_valley = lines
            .get("northern-tyne-valley")
            .expect("northern-tyne-valley should load");
        assert!(
            tyne_valley.has_station("PRU"),
            "northern-tyne-valley should now list Prudhoe (PRU)"
        );
        assert_eq!(tyne_valley.segment_for("PRU"), Some("northern-tyne-valley"));

        let all_lines = load_all_lines();
        let registry = SegmentRegistry::new(&all_lines);
        let inc = incident(
            "NT-15",
            "Signal failure at Prudhoe",
            "Signal failure causing delays on the Tyne Valley Line at Prudhoe.",
            &["NT"],
            &["PRU"],
        );
        let matches = lines_affected_by(&inc, &all_lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["northern-tyne-valley".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Task 9.1 (2026-09-01) fill-in for `lines/wmr-snow-hill.toml`: verify
    // newly-added stations (previously omitted as "minor calls") are now
    // recognised by `has_station`. Bordesley is a good pick since it's the
    // one whose segment placement (shared `wmr-snow-hill-trunk`, not either
    // exclusive branch) needed live-source confirmation, not just an
    // insertion.
    #[test]
    fn wmr_snow_hill_recognises_newly_added_stations() {
        let lines = load_line("wmr-snow-hill");
        let line = lines
            .get("wmr-snow-hill")
            .expect("wmr-snow-hill should load");
        for crs in [
            "BBS", "SMA", "ACG", "OLT", "WMR", "SRI", "HLG", "YRD", "WYT", "EWD", "WDE", "DZY",
            "WWW", "WMC", "STY", "BKD", "HAG", "LYE", "CRA", "OHL", "ROW", "LGG", "THW", "JEQ",
        ] {
            assert!(
                line.has_station(crs),
                "{crs} should now be recognised on wmr-snow-hill"
            );
        }
        // Fernhill Heath was named in this task's brief but confirmed closed
        // since 1965 (never reopened) against two independent sources, so it
        // deliberately stays out.
        assert!(
            !line.has_station("FNH"),
            "Fernhill Heath is closed and must not be added"
        );
    }

    // None of wmr-snow-hill.toml's three segments (`wmr-snow-hill-trunk`,
    // `wmr-snow-hill-dorridge`, `wmr-snow-hill-stratford`) are shared with
    // any other file's segment tag in the catalogue -- confirmed both by the
    // file's own header comment (no segment-name collision found by grep)
    // and by the task controller's own pre-check. `chiltern-main-line.toml`
    // (Task 10.2, a parallel worktree neither task could see) independently
    // added Bordesley too, on its own exclusive `chiltern-birmingham-approach`
    // segment -- station-level overlap only, same "overlap is fine,
    // segment-sharing is a deliberate choice" precedent as
    // `lnwr_birmingham_crewe_exclusive_segment_incident_does_not_propagate`
    // above, so still ExclusiveSegment for both.
    #[test]
    fn wmr_snow_hill_bordesley_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "WMR-1",
            "Signal failure at Bordesley",
            "Signal failure causing delays to West Midlands Railway services.",
            &["LM"],
            &["BBS"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "wmr-snow-hill".to_string(),
                "chiltern-main-line".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    // Regression coverage for incident 7F69B9D781A941AD8305FECCE3ACAA43
    // ("Disruption between New Malden and Raynes Park"), confirmed live on
    // 2026-09-04/05: the incident's only real `affectedOperators` entry is
    // South Western Railway (SW), but the description's ticket-acceptance
    // clause naming CrossCountry as an alternative route used to produce a
    // spurious `cross-country` KeywordOnly match. That spurious match then
    // deleted every South Western line's correct OperatorOnly match
    // outright (the unscoped `has_precise`/`retain` bug, Decision 1) and,
    // separately, should never have matched at all (the ungated Tier-2
    // keyword acceptance bug, Decision 2). Both fixes are required for this
    // test to pass: Decision 2 alone stops `cross-country` from matching;
    // Decision 1 alone stops the SW deletion but would still leave
    // `cross-country` matching alongside SW.
    #[test]
    fn real_incident_sw_disruption_not_misattributed_to_cross_country() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "7F69B9D781A941AD8305FECCE3ACAA43",
            "Disruption between New Malden and Raynes Park",
            "Disruption is affecting South Western Railway services between \
             New Malden and Raynes Park due to a signalling fault. Trains \
             may be cancelled, delayed or revised. Your ticket is also \
             being accepted on the following alternative routes: \
             CrossCountry services between Reading and Bournemouth.",
            &["SW"],
            &[],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(
            !matched_ids.contains("cross-country"),
            "spurious ticket-acceptance CrossCountry mention must not match: {matched_ids:?}"
        );
        for id in [
            "swr-south-west-main",
            "swr-kingston-loop",
            "swr-chessington",
            "swr-portsmouth-direct",
            "swr-alton",
        ] {
            assert!(
                matched_ids.contains(id),
                "{id} should match (real SW attribution): {matched_ids:?}"
            );
        }
        for m in &matches {
            if m.line.id.starts_with("swr-") {
                assert_eq!(
                    m.scope,
                    MatchScope::OperatorOnly,
                    "{} should match via OperatorOnly",
                    m.line.id
                );
            }
        }
    }

    // Decision 1, generalized beyond the real incident: an incident naming
    // two unrelated operators, where only one of them (Grand Central, GC)
    // gets a precise (keyword) match on one of its lines. Hull Trains
    // (HT)'s only line has no textual keyword hit at all, just the
    // structured operator overlap, so it should still surface as
    // OperatorOnly -- Decision 1's per-operator rescoping must not let
    // GC's precise match strip HT's OperatorOnly match just because they
    // co-occur in the same incident.
    #[test]
    fn decision1_precise_match_for_one_operator_does_not_strip_operator_only_for_another() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "D1-1",
            "Grand Central service delayed",
            "A Grand Central service was delayed due to a points failure \
             near Sunderland.",
            &["GC", "HT"],
            &[],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let by_id: HashMap<String, MatchScope> = matches
            .iter()
            .map(|m| (m.line.id.clone(), m.scope))
            .collect();
        assert_eq!(
            by_id.get("grand-central"),
            Some(&MatchScope::KeywordOnly),
            "grand-central should get the precise keyword match: {by_id:?}"
        );
        assert_eq!(
            by_id.get("hull-trains"),
            Some(&MatchScope::OperatorOnly),
            "hull-trains's operator-only match must survive GC's unrelated \
             precise match: {by_id:?}"
        );
    }

    // Decision 1 must not regress the original, intended same-operator
    // suppression: an SW incident that both tags `operators = ["SW"]`
    // system-wide *and* names one specific SW route keyword ("Portsmouth
    // Direct") should still treat the other SW lines' OperatorOnly matches
    // as superseded by the named line -- this is the behavior the
    // pre-existing comment always claimed to implement, and Decision 1
    // must only remove the cross-operator leak, not this same-operator
    // intent.
    #[test]
    fn decision1_same_operator_precise_match_still_suppresses_other_operator_only_matches() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "D1-2",
            "Portsmouth Direct line disruption",
            "Disruption on the Portsmouth Direct line, between Guildford \
             and Havant, due to a signal failure.",
            &["SW"],
            &[],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(
            matched_ids.contains("swr-portsmouth-direct"),
            "the named line should still match: {matched_ids:?}"
        );
        for id in [
            "swr-south-west-main",
            "swr-kingston-loop",
            "swr-chessington",
            "swr-alton",
        ] {
            assert!(
                !matched_ids.contains(id),
                "{id}'s OperatorOnly match should still be suppressed by \
                 swr-portsmouth-direct's precise match: {matched_ids:?}"
            );
        }
    }

    // Decision 2 in isolation: an SW incident with an incidental, unrelated
    // Grand Central brand mention should not match grand-central at all --
    // `incident.operators` positively excludes GC, so the keyword hit must
    // be rejected outright, not merely demoted.
    #[test]
    fn decision2_contradicted_keyword_hit_does_not_match_at_all() {
        let lines = load_line("grand-central");
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "D2-1",
            "South Western Railway disruption",
            "A South Western Railway service between London Waterloo and \
             Southampton is disrupted due to a points failure. Elsewhere, \
             a Grand Central service was also affected by a separate fault \
             near Sunderland.",
            &["SW"],
            &[],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert!(
            !matched_ids.contains("grand-central"),
            "incidental Grand Central mention contradicted by \
             incident.operators = [SW] must not match: {matched_ids:?}"
        );
    }

    // Decision 2 must not regress a genuine brand-only incident: when
    // `incident.operators` actually names the brand's own operator code,
    // the keyword hit is not contradicted and should match exactly as
    // before.
    #[test]
    fn decision2_genuine_brand_only_incident_still_matches() {
        let lines = load_line("hull-trains");
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "D2-2",
            "Hull Trains service cancelled",
            "A Hull Trains service between London King's Cross and Hull \
             has been cancelled due to a fault.",
            &["HT"],
            &[],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].line.id, "hull-trains");
        assert_eq!(matches[0].scope, MatchScope::KeywordOnly);
    }

    // Decision 2 must not regress the case cross-country.toml's own
    // comment documents: "Cross Country Route" names real
    // Birmingham-Bristol infrastructure, not the CrossCountry brand, and
    // must keep matching via keyword alone when the incident's structured
    // operators list agrees (XC).
    #[test]
    fn decision2_cross_country_route_infrastructure_mention_still_matches() {
        let lines = load_line("cross-country");
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "D2-3",
            "Cross Country Route disruption",
            "Disruption on the Cross Country Route between Birmingham and \
             Bristol due to a landslip.",
            &["XC"],
            &[],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].line.id, "cross-country");
        assert_eq!(matches[0].scope, MatchScope::KeywordOnly);
    }

    // ---------------------------------------------------------------
    // `LineMatcher` -- the wrapper `api` uses to fill
    // `incidents.affected_lines` at ingest. These tests are the pure,
    // database-free half of the incident archive's Line-filter regression
    // (the other half, `search_incidents` actually returning the row, is
    // an `#[ignore]`d database test in
    // `api::data::queries::incident_search_query_tests`).
    // ---------------------------------------------------------------

    fn full_catalogue_matcher() -> LineMatcher {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../lines");
        let lines = LineDefinition::from_dir(&dir).expect("lines/ directory should parse");
        LineMatcher::new(&lines)
    }

    /// The exact incident this defect was found with: a real production
    /// Elizabeth line row (`10D1F12529CA43BAB244C131D3087FD8`) that the
    /// archive's Line filter could not find, because the filter matched on
    /// `affected_stations` and RDM's feed gives no station codes at all.
    /// Its text and operator list are copied verbatim from the live
    /// `GET /public/incidents?operator=XR` response of 2026-09-17.
    #[test]
    fn affected_line_ids_finds_the_elizabeth_line_from_a_real_incident_with_no_station_codes() {
        let matcher = full_catalogue_matcher();
        let inc = incident(
            "10D1F12529CA43BAB244C131D3087FD8",
            "Residual disruption to Elizabeth line services between Shenfield and Romford",
            "Following an earlier fault with the signalling system between Shenfield and \
             Romford, all lines have now reopened. Residual delays of up to 15 minutes are \
             expected.",
            &["XR"],
            // The point of the whole exercise: no CRS codes, because the
            // Knowledgebase Incidents schema has no field to carry them.
            &[],
        );

        assert_eq!(
            matcher.affected_line_ids(&inc),
            vec!["elizabeth-line".to_string()],
            "the matcher attributes this to the Elizabeth line by keyword, with no station \
             codes involved -- and to the Elizabeth line ONLY: its two branch lines share the \
             XR operator code but got no keyword hit, so the cross-line OperatorOnly filter \
             correctly drops them rather than claiming a Shenfield-branch-specific incident \
             affects Heathrow too"
        );
    }

    /// The false-positive direction. `lines/elizabeth-line.toml`'s
    /// `match_keywords` contains "Elizabeth line", which appears verbatim
    /// in the ticket-acceptance boilerplate other operators' incidents
    /// routinely carry. The feed's own structured operator list is what
    /// stops that becoming a match -- the same gate
    /// `decision2_*` above cover for `lines_affected_by`, asserted here at
    /// the level the archive actually stores.
    #[test]
    fn affected_line_ids_does_not_attribute_a_ticket_acceptance_mention_to_the_named_line() {
        let matcher = full_catalogue_matcher();
        let inc = incident(
            "TICKET-ACCEPTANCE",
            "Disruption between Woking and Basingstoke",
            "A fault with the signalling system is causing delays. Your ticket is also valid \
             on Elizabeth line services between Paddington and Reading.",
            &["SW"],
            &[],
        );

        let ids = matcher.affected_line_ids(&inc);
        assert!(
            !ids.contains(&"elizabeth-line".to_string()),
            "an Elizabeth line mention inside a South Western incident's ticket-acceptance \
             clause must not put this incident in the Elizabeth line's archive: {ids:?}"
        );
        assert!(
            ids.iter().any(|id| id.starts_with("swr-")),
            "it must still be attributed to South Western's own lines: {ids:?}"
        );
    }

    /// The boundary of the test above, stated explicitly so nobody reads
    /// that one as proof of more than it shows. The keyword tier's only
    /// guard against a ticket-acceptance mention is the feed's own
    /// structured operator list (`match_one`'s `contradicted` check), so
    /// with an EMPTY `operators` list there is no guard and the mention
    /// does match. That is pre-existing live-status behaviour, unchanged by
    /// storing the answer; this test exists so the next person to look at a
    /// surprising archive row finds the reason here rather than rediscovering
    /// it. Fixing it means narrowing the keyword tier in `match_one`, which
    /// would change live status too and belongs in its own change.
    #[test]
    fn affected_line_ids_has_no_guard_against_a_ticket_mention_when_the_feed_names_no_operator() {
        let matcher = full_catalogue_matcher();
        let inc = incident(
            "TICKET-ACCEPTANCE-NO-OPERATORS",
            "Disruption between Woking and Basingstoke",
            "Your ticket is also valid on Elizabeth line services between Paddington and \
             Reading.",
            &[],
            &[],
        );

        assert!(
            matcher
                .affected_line_ids(&inc)
                .contains(&"elizabeth-line".to_string()),
            "documenting the known gap: with no structured operator list there is nothing to \
             contradict a bare keyword hit, so the mention matches"
        );
    }

    /// Sorted and deduped, because the value is written to a database
    /// column and compared for equality by the backfill's "did anything
    /// change" check -- `lines_affected_by` itself iterates a `HashMap`
    /// and so returns an arbitrary order.
    #[test]
    fn affected_line_ids_is_sorted_and_free_of_duplicates() {
        let matcher = full_catalogue_matcher();
        let inc = incident(
            "SORTED",
            "South Western Railway disruption",
            "Delays to South Western Railway services.",
            &["SW"],
            &[],
        );

        let ids = matcher.affected_line_ids(&inc);
        assert!(ids.len() > 1, "expected several SWR lines: {ids:?}");
        let mut sorted = ids.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(ids, sorted, "must come back sorted and deduped: {ids:?}");
    }

    #[test]
    fn knows_line_accepts_a_catalogue_id_and_rejects_anything_else() {
        let matcher = full_catalogue_matcher();
        assert!(matcher.knows_line("elizabeth-line"));
        assert!(!matcher.knows_line("not-a-line"));
        // Guards the archive route's 400-vs-empty-page distinction against
        // a caller passing a TfL line id, which this catalogue does not
        // contain (see the 2026-09-16 TfL archive spec).
        assert!(!matcher.knows_line("tfl-elizabeth"));
    }

    // North West England line-coverage audit (2026-09-21):
    // `lines/northern-glossop-hadfield.toml` and `lines/northern-rose-
    // hill.toml` genuinely share track from Manchester Piccadilly to Guide
    // Bridge (segment `northern-guide-bridge`, reused verbatim between the
    // two files) before diverging -- mirrors `swr_shared_trunk_incident_propagates`.
    #[test]
    fn northern_guide_bridge_incident_propagates_to_glossop_hadfield_and_rose_hill() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "NT-16",
            "Signal failure at Guide Bridge",
            "Signal failure causing delays to Northern services at Guide Bridge.",
            &["NT"],
            &["GUI"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let by_id: HashMap<String, MatchScope> = matches
            .iter()
            .map(|m| (m.line.id.clone(), m.scope))
            .collect();
        assert_eq!(
            by_id.get("northern-glossop-hadfield"),
            Some(&MatchScope::SharedSegment)
        );
        assert_eq!(
            by_id.get("northern-rose-hill"),
            Some(&MatchScope::SharedSegment)
        );
    }

    #[test]
    fn northern_glossop_hadfield_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "NT-17",
            "Signal failure at Broadbottom",
            "Signal failure causing delays on the Glossop/Hadfield Line at Broadbottom.",
            &["NT"],
            &["BDB"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["northern-glossop-hadfield".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn northern_rose_hill_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "NT-18",
            "Signal failure at Rose Hill Marple",
            "Signal failure causing delays on the Rose Hill Marple Line at Rose Hill Marple.",
            &["NT"],
            &["RSH"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["northern-rose-hill".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn northern_buxton_line_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "NT-19",
            "Signal failure at Whaley Bridge",
            "Signal failure causing delays on the Buxton Line at Whaley Bridge.",
            &["NT"],
            &["WBR"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["northern-buxton-line".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Stockport is now touched by many different files' own separate
    // segments (wcml-manchester, xc-manchester, northern-hope-valley,
    // tpe-south, emr-regional, and now northern-buxton-line and
    // northern-mid-cheshire too) -- station overlap only throughout, per
    // the precedent already established by `wcml-manchester.toml`'s own
    // comment. Mirrors
    // `emr_regional_stockport_and_hope_valley_both_match_without_over_propagating`.
    #[test]
    fn northern_buxton_line_and_mid_cheshire_both_match_stockport_without_over_propagating() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "NT-20",
            "Overhead line damage at Stockport",
            "Overhead line damage causing delays to Northern services at Stockport.",
            &["NT"],
            &["SPT"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let by_id: HashMap<String, MatchScope> = matches
            .iter()
            .map(|m| (m.line.id.clone(), m.scope))
            .collect();
        assert_eq!(
            by_id.get("northern-buxton-line"),
            Some(&MatchScope::ExclusiveSegment)
        );
        assert_eq!(
            by_id.get("northern-mid-cheshire"),
            Some(&MatchScope::ExclusiveSegment)
        );
    }

    #[test]
    fn northern_mid_cheshire_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "NT-21",
            "Signal failure at Northwich",
            "Signal failure causing delays on the Mid-Cheshire Line at Northwich.",
            &["NT"],
            &["NWI"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["northern-mid-cheshire".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn northern_east_lancashire_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "NT-22",
            "Points failure at Accrington",
            "Points failure causing delays on the East Lancashire Line at Accrington.",
            &["NT"],
            &["ACR"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["northern-east-lancashire".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // (Blackburn's own overlap between this line and `northern-clitheroe` is
    // exercised by `clitheroe_exclusive_segment_incident_does_not_propagate`
    // above, updated for this file's addition -- not duplicated here.)

    // Kirkham & Wesham is genuinely shared track between
    // `lines/northern-blackpool-south.toml` and `northern-blackpool.toml`,
    // but a coarse-granularity mismatch (that file's own segment spans its
    // entire route, not just this stretch) means no segment name is reused
    // -- see `northern-blackpool-south.toml`'s own top-of-file comment.
    // Mirrors `emr_regional_stockport_and_hope_valley_both_match_without_over_propagating`.
    #[test]
    fn northern_blackpool_south_kirkham_and_wesham_matches_both_files_without_over_propagating() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "NT-24",
            "Signal failure at Kirkham & Wesham",
            "Signal failure causing delays to Northern services at Kirkham & Wesham.",
            &["NT"],
            &["KKM"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let by_id: HashMap<String, MatchScope> = matches
            .iter()
            .map(|m| (m.line.id.clone(), m.scope))
            .collect();
        assert_eq!(
            by_id.get("northern-blackpool"),
            Some(&MatchScope::ExclusiveSegment)
        );
        assert_eq!(
            by_id.get("northern-blackpool-south"),
            Some(&MatchScope::ExclusiveSegment)
        );
    }

    #[test]
    fn northern_blackpool_south_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "NT-25",
            "Signal failure at Lytham",
            "Signal failure causing delays on the South Fylde Line at Lytham.",
            &["NT"],
            &["LTM"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["northern-blackpool-south".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Regression guard for the documented Salwick oddity in
    // `lines/northern-blackpool-south.toml`: Salwick sits geographically on
    // the Preston-Kirkham & Wesham stretch shared with
    // `northern-blackpool.toml`, but that file does not itself list Salwick
    // as a station, so an incident there must NOT be reported as affecting
    // `northern-blackpool` -- guards against someone "fixing" this by
    // relabelling Salwick onto the shared `northern-blackpool` segment
    // without also adding it to that file.
    #[test]
    fn northern_blackpool_south_salwick_stays_exclusive_not_shared_with_blackpool() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "NT-26",
            "Signal failure at Salwick",
            "Signal failure causing delays on the South Fylde Line at Salwick.",
            &["NT"],
            &["SLW"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["northern-blackpool-south".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    #[test]
    fn northern_bentham_line_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "NT-27",
            "Signal failure at Bentham",
            "Signal failure causing delays on the Bentham Line at Bentham.",
            &["NT"],
            &["BEN"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["northern-bentham-line".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Greater Glasgow batch: `scotrail-glasgow-south-western.toml` and
    // `scotrail-east-kilbride.toml` share the `scotrail-gsw-approach`
    // segment (Glasgow Central, Crossmyloof, Pollokshaws West) -- a
    // genuine shared trunk, sourced in both files' own comments (the East
    // Kilbride branch diverges from the GSW main line at Busby Junction,
    // just past Pollokshaws West). Mirrors
    // `overground_canonbury_curve_incident_propagates_to_mildmay_and_windrush`.
    #[test]
    fn scotrail_gsw_approach_incident_propagates_to_gsw_and_east_kilbride() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-GSW-1",
            "Points failure at Pollokshaws West",
            "Points failure causing delays to ScotRail services at Pollokshaws West.",
            &["SR"],
            &["PWW"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "scotrail-glasgow-south-western".to_string(),
                "scotrail-east-kilbride".to_string()
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment",
                m.line.id
            );
        }
    }

    // `scotrail-gsw-nith-valley` (Kilmarnock south to Carlisle) is not
    // touched by any other `lines/*.toml` file, so it stays an
    // exclusive-segment non-propagation assertion.
    #[test]
    fn scotrail_gsw_nith_valley_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-GSW-2",
            "Signal failure at Dumfries",
            "Signal failure causing delays to ScotRail services at Dumfries.",
            &["SR"],
            &["DMF"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-glasgow-south-western".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `scotrail-east-kilbride-branch` (Thornliebank onward) is exclusive
    // to `scotrail-east-kilbride.toml`.
    #[test]
    fn scotrail_east_kilbride_branch_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-EK-1",
            "Overhead line fault at Hairmyres",
            "Overhead line fault causing delays to ScotRail services at Hairmyres.",
            &["SR"],
            &["HMY"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-east-kilbride".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Greater Glasgow batch: `scotrail-inverclyde.toml` is a standalone
    // addition today (no other file touches its stations), mirroring
    // `scotrail_ayrshire_stranraer_branch_incident_does_not_propagate`.
    #[test]
    fn scotrail_inverclyde_gourock_branch_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-INV-1",
            "Signal failure at Gourock",
            "Signal failure causing delays to ScotRail services at Gourock.",
            &["SR"],
            &["GRK"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-inverclyde".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Greater Glasgow batch: `scotrail-cumbernauld.toml` deliberately does
    // not reuse `scotrail-north-clyde.toml`'s segment names for the
    // Springburn overlap (that file lives in a different, unmerged
    // worktree today -- see this file's own segment-naming coordination
    // note), so this was originally an exclusive-segment assertion.
    //
    // Updated (Scotland real-world-sanity review): scotrail-argyle.toml also
    // terminates its own Whifflet-spur service at Cumbernauld, deliberately
    // reusing `scotrail-cumbernauld-branch` verbatim from this file (see
    // that file's own CUB comment: same physical track, different `role`
    // describing each line's own service pattern there) - a genuine
    // SharedSegment pair.
    #[test]
    fn scotrail_cumbernauld_branch_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-CUM-1",
            "Points failure at Cumbernauld",
            "Points failure causing delays to ScotRail services at Cumbernauld.",
            &["SR"],
            &["CUB"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "scotrail-cumbernauld".to_string(),
                "scotrail-argyle".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment",
                m.line.id
            );
        }
    }

    // Integration-merge reconciliation: `scotrail-cumbernauld.toml` and
    // `scotrail-north-clyde.toml` both list Springburn (SPR) on the
    // literal `scotrail-north-clyde-springburn-spur` segment (a genuine,
    // single-station shared trunk -- the reversal point documented in both
    // files' own SPR comments), so an incident there should propagate to
    // both as SharedSegment. Mirrors `swr_shared_trunk_incident_propagates`.
    #[test]
    fn scotrail_springburn_spur_incident_propagates_to_cumbernauld_and_north_clyde() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-CUM-2",
            "Points failure at Springburn",
            "Points failure causing delays to ScotRail services at Springburn.",
            &["SR"],
            &["SPR"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "scotrail-cumbernauld".to_string(),
                "scotrail-north-clyde".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::SharedSegment,
                "{} should be SharedSegment",
                m.line.id
            );
        }
    }

    // Greater Glasgow batch: `scotrail-maryhill.toml` is a standalone
    // addition today.
    #[test]
    fn scotrail_maryhill_line_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-MYH-1",
            "Signal failure at Maryhill",
            "Signal failure causing delays to ScotRail services at Maryhill.",
            &["SR"],
            &["MYH"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-maryhill".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Greater Glasgow batch: `scotrail-cathcart-circle.toml`'s Neilston
    // branch is exclusive to that file (Barrhead, not Neilston, is the
    // station shared with `scotrail-glasgow-south-western.toml` -- see
    // that file's own file-level comment on why the two clusters are
    // kept separate).
    #[test]
    fn scotrail_cathcart_neilston_branch_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-CC-1",
            "Signal failure at Neilston",
            "Signal failure causing delays to ScotRail services at Neilston.",
            &["SR"],
            &["NEI"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-cathcart-circle".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // Greater Glasgow batch: Barrhead belongs to
    // `scotrail-glasgow-south-western.toml`, not the Cathcart Circle
    // cluster -- guards the file-level ruling documented in
    // `lines/scotrail-cathcart-circle.toml` against regressing.
    #[test]
    fn scotrail_barrhead_is_glasgow_south_western_not_cathcart_circle() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "SR-GSW-3",
            "Points failure at Barrhead",
            "Points failure causing delays to ScotRail services at Barrhead.",
            &["SR"],
            &["BRR"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["scotrail-glasgow-south-western".to_string()])
        );
    }

    // Midlands EMR/WMR/LNWR sanity review: new-line regression guards.
    //
    // `lines/emr-crewe-derby.toml` -- Uttoxeter is on the exclusive
    // `emr-crewe-derby-west` segment, west of the shared-with-`emr-mml-
    // derby` Derby junction, so it should not propagate to any other line.
    #[test]
    fn emr_crewe_derby_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "EM-CD-1",
            "Points failure at Uttoxeter",
            "Points failure causing delays to services at Uttoxeter.",
            &["EM"],
            &["UTT"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["emr-crewe-derby".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `lines/emr-lincoln-peterborough.toml` -- Ruskington is on the
    // exclusive `emr-lincoln-peterborough` segment, not shared with any
    // other line. (Sleaford itself, one station further along this same
    // segment, is ALSO a genuine `emr-poacher.toml` station -- the Poacher
    // Line's own Grantham-Skegness route also runs via Sleaford, a real
    // crossing point between the two lines, discovered while writing this
    // test. Station-overlap only, no segment shared; Ruskington is used
    // here instead purely so this test demonstrates the single-line case.)
    #[test]
    fn emr_lincoln_peterborough_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "EM-LP-1",
            "Points failure at Ruskington",
            "Points failure causing delays to services at Ruskington.",
            &["EM"],
            &["RKT"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["emr-lincoln-peterborough".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `lines/emr-barton-line.toml` -- Goxhill is exclusive to this line's
    // own `emr-barton-line` segment.
    #[test]
    fn emr_barton_line_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "EM-BL-1",
            "Points failure at Goxhill",
            "Points failure causing delays to services at Goxhill.",
            &["EM"],
            &["GOX"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(matched_ids, HashSet::from(["emr-barton-line".to_string()]));
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `lines/emr-derwent-valley.toml` (re-scoped, Midlands EMR/WMR/LNWR
    // sanity review) -- Market Rasen is exclusive to this line's own
    // `emr-derwent-valley-grimsby` segment.
    #[test]
    fn emr_derwent_valley_market_rasen_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "EM-DV-1",
            "Points failure at Market Rasen",
            "Points failure causing delays to services at Market Rasen.",
            &["EM"],
            &["MKR"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["emr-derwent-valley".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `lines/wmr-malvern-line.toml` -- Great Malvern is exclusive to this
    // line's own `wmr-malvern-line` segment.
    //
    // Updated by the GWR/southwest sanity review: gwr-cotswold.toml's own
    // extension also terminates at Great Malvern, on its own exclusive
    // `gwr-cotswold` segment - station overlap only, a second independent
    // ExclusiveSegment match.
    #[test]
    fn wmr_malvern_line_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LM-ML-1",
            "Points failure at Great Malvern",
            "Points failure causing delays to services at Great Malvern.",
            &["LM"],
            &["GMV"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["wmr-malvern-line".to_string(), "gwr-cotswold".to_string()])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }

    // `lines/lnwr-marston-vale-line.toml` -- Ridgmont is exclusive to this
    // line's own `lnwr-marston-vale` segment.
    #[test]
    fn lnwr_marston_vale_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LM-MV-1",
            "Points failure at Ridgmont",
            "Points failure causing delays to services at Ridgmont.",
            &["LM"],
            &["RID"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["lnwr-marston-vale-line".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `lines/lnwr-stafford-crewe.toml` -- Stone (Staffs) is exclusive to
    // this line's own `lnwr-stafford-crewe` segment.
    #[test]
    fn lnwr_stafford_crewe_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LM-SC-1",
            "Points failure at Stone",
            "Points failure causing delays to services at Stone.",
            &["LM"],
            &["SNE"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["lnwr-stafford-crewe".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `lines/wmr-stourbridge-town.toml` -- the whole two-station shuttle is
    // exclusive to this file.
    #[test]
    fn wmr_stourbridge_town_exclusive_segment_incident_does_not_propagate() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LM-ST-1",
            "Points failure at Stourbridge Town",
            "Points failure causing delays to services at Stourbridge Town.",
            &["LM"],
            &["SBT"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from(["wmr-stourbridge-town".to_string()])
        );
        assert_eq!(matches[0].scope, MatchScope::ExclusiveSegment);
    }

    // `lines/wmr-nuneaton-coventry.toml` (extended) and `lines/wmr-snow-
    // hill.toml` (extended) both now reach Leamington Spa, alongside the
    // two files that already modelled it (`xc-south-coast.toml`,
    // `chiltern-main-line.toml`) -- four independent physical approaches
    // into the same station, none sharing a segment name with any other
    // (see each file's own ruling comment), so all four should match this
    // incident independently as ExclusiveSegment.
    #[test]
    fn leamington_spa_four_way_station_overlap_stays_exclusive_each_line() {
        let lines = load_all_lines();
        let registry = SegmentRegistry::new(&lines);
        let inc = incident(
            "LM-LMS-1",
            "Signal failure at Leamington Spa",
            "Signal failure causing delays to services at Leamington Spa.",
            &["LM"],
            &["LMS"],
        );
        let matches = lines_affected_by(&inc, &lines, &registry);
        let matched_ids: HashSet<String> = matches.iter().map(|m| m.line.id.clone()).collect();
        assert_eq!(
            matched_ids,
            HashSet::from([
                "xc-south-coast".to_string(),
                "chiltern-main-line".to_string(),
                "wmr-nuneaton-coventry".to_string(),
                "wmr-snow-hill".to_string(),
            ])
        );
        for m in &matches {
            assert_eq!(
                m.scope,
                MatchScope::ExclusiveSegment,
                "{} should be ExclusiveSegment",
                m.line.id
            );
        }
    }
}
