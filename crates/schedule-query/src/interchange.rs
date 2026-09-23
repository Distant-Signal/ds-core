//! Shared, pure interchange lookups: minimum same-station change time,
//! CRS-sibling TIPLOCs (different platform groups of one physical
//! station), and valid cross-CRS fixed links at a given date/time. Both
//! Connection Scan (Phase 3) and RAPTOR (Phase 4) call these functions
//! directly while independently implementing their own scan/relaxation
//! logic on top -- see this plan's Judgment Call 2 for why this is a live
//! lookup layer, not synthetic connections baked into
//! [`crate::connections::Connection`].
//!
//! Real interchange rules and sentinel values, independently re-verified
//! this pass against the sibling `Distant-Signal-MCP` project's own
//! `src/timetable/plan/interchange.ts` (re-cloned directly for this plan's
//! research pass, not carried forward from an earlier, unverified pass):
//! the MSN member's minimum-change-time field defaults to 5 minutes when
//! absent (the modal real value), and two specific values (98, 99) are
//! sentinels meaning "not a real rail interchange" (a bus/coach stand),
//! never a literal duration -- see [`minimum_change_time`]'s own doc
//! comment for the full reasoning that project's own Task 1 investigation
//! established and this app inherits unchanged.

use std::collections::HashMap;

use chrono::NaiveDate;

/// Every other TIPLOC sharing this one's CRS code -- a different platform
/// group of the same physical station (the sibling's own real example:
/// Wimbledon's `WDON`/`WIMBLDN`/`WDNLUL` all share CRS `WIM`). Deliberately
/// does not collapse siblings into one node: a caller changing between them
/// still owes the real minimum-change-time cost, charged at the TIPLOC
/// actually being boarded at -- Phase 3/4's own scan logic is where that
/// charge is applied, not here.
#[derive(Debug, Clone)]
pub struct InterchangeData {
    /// TIPLOC -> raw minimum-change-time minutes, straight from
    /// `stanox_crs.change_time_minutes` (Phase 1) -- `None`/absent means
    /// "no MSN record matched this TIPLOC at all," genuinely different
    /// from a present sentinel or the modal 5 (see Phase 1's own Judgment
    /// Call 3).
    pub change_time_by_tiploc: HashMap<String, i32>,
    pub tiploc_to_crs: HashMap<String, String>,
    pub crs_to_tiplocs: HashMap<String, Vec<String>>,
    /// CRS -> every fixed link departing FROM it (ALF's `O` field),
    /// unfiltered by date/time -- [`fixed_links_from`] applies the
    /// date/time filter at lookup time.
    pub fixed_links_from_crs: HashMap<String, Vec<FixedLink>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixedLink {
    pub mode: String,
    pub to_crs: String,
    pub minutes: i32,
    /// Raw "HHMM", 4 ASCII digits.
    pub valid_from: String,
    pub valid_to: String,
    /// Raw 7-char '0'/'1' bitmask, Monday-first.
    pub days_mask: String,
}

/// The result of looking up a station's minimum same-train-to-different-train
/// change time. `Finite` is real minutes; `NoInterchange` is the two real
/// sentinel values (98, 99) -- no candidate change duration can ever meet or
/// beat "no interchange possible here."
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeTime {
    Finite(u32),
    NoInterchange,
}

impl ChangeTime {
    /// `true` if `available_minutes` is enough to make this change --
    /// callers compare with `>=`, not `>`: CIF publishes its minimum as the
    /// shortest connection that IS timetabled to work, not the shortest one
    /// that fails.
    pub fn allows(self, available_minutes: u32) -> bool {
        match self {
            ChangeTime::Finite(minimum) => available_minutes >= minimum,
            ChangeTime::NoInterchange => false,
        }
    }
}

/// Used whenever a TIPLOC's MSN record carries no change-time value at all
/// -- the modal value among stations that do carry one.
const DEFAULT_CHANGE_TIME: u32 = 5;

/// Raw MSN change-time values that appear against a bus/coach stand, never
/// a genuine rail interchange -- independently re-confirmed this pass
/// against the sibling project's own investigation (`interchange.ts:9-29`):
/// every station carrying either value in a real extract was traced to a
/// bus/coach stand (airport transfer stops, a market place), never a
/// station where a passenger changes between two trains.
const NO_INTERCHANGE_SENTINELS: [i32; 2] = [98, 99];

/// The shortest same-station change this app's own interchange data
/// considers valid at `tiploc`. Defaults to [`DEFAULT_CHANGE_TIME`] when
/// `tiploc` has no MSN record at all (a genuine gap, not a sentinel).
///
/// `tiploc` is normalized (via [`crate::normalize_tiploc`]) before the
/// lookup -- defense-in-depth against a caller passing a still-padded,
/// 7-character schedule-body TIPLOC while `change_time_by_tiploc` is keyed
/// on the bare form (see [`InterchangeData::change_time_by_tiploc`]'s own
/// doc comment). Callers should still normalize, and all of them do, but
/// correctness must not depend on their remembering to.
pub fn minimum_change_time(data: &InterchangeData, tiploc: &str) -> ChangeTime {
    let tiploc = crate::normalize_tiploc(tiploc);
    match data.change_time_by_tiploc.get(tiploc) {
        None => ChangeTime::Finite(DEFAULT_CHANGE_TIME),
        Some(raw) if NO_INTERCHANGE_SENTINELS.contains(raw) => ChangeTime::NoInterchange,
        Some(raw) => ChangeTime::Finite((*raw).max(0) as u32),
    }
}

/// Every other TIPLOC sharing `tiploc`'s own CRS code, excluding `tiploc`
/// itself. Empty when `tiploc` has no CRS at all (a junction-only TIPLOC
/// with no MSN record -- the same population [`minimum_change_time`]
/// defaults for) or is the only TIPLOC recorded against its CRS.
///
/// `tiploc` is normalized (via [`crate::normalize_tiploc`]) before the
/// lookup, for the same defense-in-depth reason [`minimum_change_time`]
/// normalizes -- `tiploc_to_crs` is keyed on the bare form.
pub fn sibling_tiplocs<'a>(data: &'a InterchangeData, tiploc: &str) -> Vec<&'a str> {
    let tiploc = crate::normalize_tiploc(tiploc);
    let Some(crs) = data.tiploc_to_crs.get(tiploc) else {
        return Vec::new();
    };
    data.crs_to_tiplocs
        .get(crs)
        .map(|tiplocs| {
            tiplocs
                .iter()
                .filter(|candidate| candidate.as_str() != tiploc)
                .map(String::as_str)
                .collect()
        })
        .unwrap_or_default()
}

/// Monday-first day-of-week index (0=Monday..6=Sunday) for `date` -- same
/// convention `schedule_query::records::BasicSchedule::days_of_week` uses
/// for the CIF `SCHEDULE` member's own bitmask, applied here to ALF's
/// `days_mask` for consistency across this crate.
fn day_index(date: NaiveDate) -> usize {
    use chrono::Datelike;
    date.weekday().num_days_from_monday() as usize
}

fn clock_minutes(time_min: u32) -> u32 {
    time_min % 1440
}

fn to_hhmm(minutes: u32) -> String {
    format!("{:02}{:02}", minutes / 60, minutes % 60)
}

/// The fixed links (tube, walk, transfer, bus, ferry) actually usable from
/// `from_crs` at `date`/`time_min` -- ALF's raw rows, resolved down to the
/// ones that genuinely apply. `time_min` may exceed 1440 (an overnight
/// connection's own `arrival_min` convention, see [`crate::connections::Connection`]);
/// it is reduced to a same-day clock position before comparison, since a
/// fixed link's validity window is defined against the clock, not a
/// running total.
///
/// A row applies only when its day mask matches `date`'s day of week AND
/// `time_min`'s clock time falls inside its `valid_from`/`valid_to` window
/// (both bounds inclusive). Where several rows exist for the same
/// destination CRS and both are valid, only the shortest is returned --
/// callers must never be handed a slower option when a faster one is also
/// timetabled at the same moment (the real Euston↔King's Cross
/// tube-vs-transfer case, §0.4 of the design spec).
///
/// Both the candidate scan order and the returned `Vec`'s order are made
/// deterministic by sorting on `(to_crs, minutes, mode)`: `candidates` is
/// sorted before the shortest-per-destination scan so an exact-`minutes`
/// tie always resolves the same way regardless of the order Postgres
/// happened to return `fixed_links` rows in (that table's own `SELECT` has
/// no `ORDER BY` -- see `crates/api/src/data/trip_planning.rs`'s
/// `fetch_interchange_data`), and the final result is sorted again since
/// `shortest_by_destination` is itself a `HashMap`, whose iteration order
/// is otherwise randomized per-process.
pub fn fixed_links_from<'a>(
    data: &'a InterchangeData,
    from_crs: &str,
    date: NaiveDate,
    time_min: u32,
) -> Vec<&'a FixedLink> {
    let clock = to_hhmm(clock_minutes(time_min));
    let day = day_index(date);

    let Some(candidates) = data.fixed_links_from_crs.get(from_crs) else {
        return Vec::new();
    };

    fn sort_key(link: &FixedLink) -> (&str, i32, &str) {
        (link.to_crs.as_str(), link.minutes, link.mode.as_str())
    }

    let mut sorted_candidates: Vec<&FixedLink> = candidates.iter().collect();
    sorted_candidates.sort_by(|a, b| sort_key(a).cmp(&sort_key(b)));

    let mut shortest_by_destination: HashMap<&str, &FixedLink> = HashMap::new();
    for link in sorted_candidates {
        let Some(day_flag) = link.days_mask.as_bytes().get(day) else {
            continue;
        };
        if *day_flag != b'1' {
            continue;
        }
        if !(link.valid_from.as_str() <= clock.as_str() && clock.as_str() <= link.valid_to.as_str())
        {
            continue;
        }
        match shortest_by_destination.get(link.to_crs.as_str()) {
            Some(existing) if existing.minutes <= link.minutes => {}
            _ => {
                shortest_by_destination.insert(&link.to_crs, link);
            }
        }
    }
    let mut result: Vec<&FixedLink> = shortest_by_destination.into_values().collect();
    result.sort_by(|a, b| sort_key(a).cmp(&sort_key(b)));
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_data() -> InterchangeData {
        InterchangeData {
            change_time_by_tiploc: HashMap::new(),
            tiploc_to_crs: HashMap::new(),
            crs_to_tiplocs: HashMap::new(),
            fixed_links_from_crs: HashMap::new(),
        }
    }

    #[test]
    fn a_tiploc_with_no_msn_record_defaults_to_five_minutes() {
        assert_eq!(
            minimum_change_time(&empty_data(), "ANYTPL"),
            ChangeTime::Finite(5)
        );
    }

    #[test]
    fn a_recorded_finite_value_is_used_directly() {
        let mut data = empty_data();
        data.change_time_by_tiploc.insert("EUSTON".to_string(), 8);
        assert_eq!(minimum_change_time(&data, "EUSTON"), ChangeTime::Finite(8));
    }

    #[test]
    fn a_98_or_99_sentinel_means_no_interchange_possible() {
        let mut data = empty_data();
        data.change_time_by_tiploc.insert("BUSSTOP".to_string(), 99);
        assert_eq!(
            minimum_change_time(&data, "BUSSTOP"),
            ChangeTime::NoInterchange
        );
        assert!(!ChangeTime::NoInterchange.allows(10_000));
    }

    #[test]
    fn allows_uses_greater_than_or_equal_not_strictly_greater() {
        assert!(ChangeTime::Finite(5).allows(5));
        assert!(!ChangeTime::Finite(5).allows(4));
    }

    #[test]
    fn sibling_tiplocs_excludes_itself_and_returns_other_crs_members() {
        let mut data = empty_data();
        data.tiploc_to_crs
            .insert("WDON".to_string(), "WIM".to_string());
        data.tiploc_to_crs
            .insert("WIMBLDN".to_string(), "WIM".to_string());
        data.crs_to_tiplocs.insert(
            "WIM".to_string(),
            vec![
                "WDON".to_string(),
                "WIMBLDN".to_string(),
                "WDNLUL".to_string(),
            ],
        );
        let mut siblings = sibling_tiplocs(&data, "WDON");
        siblings.sort_unstable();
        assert_eq!(siblings, vec!["WDNLUL", "WIMBLDN"]);
    }

    #[test]
    fn sibling_tiplocs_is_empty_for_a_tiploc_with_no_crs() {
        assert!(sibling_tiplocs(&empty_data(), "JUNCTION").is_empty());
    }

    fn link(
        to_crs: &str,
        minutes: i32,
        valid_from: &str,
        valid_to: &str,
        days_mask: &str,
    ) -> FixedLink {
        FixedLink {
            mode: "TUBE".to_string(),
            to_crs: to_crs.to_string(),
            minutes,
            valid_from: valid_from.to_string(),
            valid_to: valid_to.to_string(),
            days_mask: days_mask.to_string(),
        }
    }

    fn monday() -> NaiveDate {
        // 2026-08-31 is independently confirmed a Monday elsewhere in this
        // codebase's own tests (schedule_query::records's own BasicSchedule
        // doc comment).
        NaiveDate::from_ymd_opt(2026, 8, 31).unwrap()
    }

    #[test]
    fn a_link_valid_on_this_day_and_time_is_returned() {
        let mut data = empty_data();
        data.fixed_links_from_crs.insert(
            "EUS".to_string(),
            vec![link("KGX", 5, "0500", "2359", "1111111")],
        );
        let links = fixed_links_from(&data, "EUS", monday(), 8 * 60);
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].to_crs, "KGX");
    }

    #[test]
    fn a_link_excluded_by_day_mask_is_not_returned() {
        let mut data = empty_data();
        // Sunday-only (index 6).
        data.fixed_links_from_crs.insert(
            "EUS".to_string(),
            vec![link("KGX", 5, "0000", "2359", "0000001")],
        );
        assert!(fixed_links_from(&data, "EUS", monday(), 8 * 60).is_empty());
    }

    #[test]
    fn a_link_excluded_by_time_window_is_not_returned() {
        let mut data = empty_data();
        data.fixed_links_from_crs.insert(
            "EUS".to_string(),
            vec![link("KGX", 5, "0500", "0800", "1111111")],
        );
        assert!(fixed_links_from(&data, "EUS", monday(), 9 * 60).is_empty());
    }

    #[test]
    fn an_overnight_time_min_past_1440_is_reduced_to_clock_time_first() {
        let mut data = empty_data();
        data.fixed_links_from_crs.insert(
            "EUS".to_string(),
            vec![link("KGX", 5, "0500", "2359", "1111111")],
        );
        // 1440 + 8*60 = day-2 08:00 -- must still match a same-clock-time window.
        let links = fixed_links_from(&data, "EUS", monday(), 1440 + 8 * 60);
        assert_eq!(links.len(), 1);
    }

    #[test]
    fn only_the_shortest_of_several_valid_rows_to_the_same_destination_is_returned() {
        // The real Euston<->King's Cross case: tube (5 min) and transfer
        // (15 min) both valid at the same moment -- only the faster wins.
        let mut data = empty_data();
        data.fixed_links_from_crs.insert(
            "EUS".to_string(),
            vec![
                link("KGX", 15, "0000", "2359", "1111111"),
                link("KGX", 5, "0500", "2359", "1111111"),
            ],
        );
        let links = fixed_links_from(&data, "EUS", monday(), 8 * 60);
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].minutes, 5);
    }
}
