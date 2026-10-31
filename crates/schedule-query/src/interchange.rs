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

use crate::operator::OperatorId;
use chrono::NaiveDate;

/// Every other TIPLOC sharing this one's CRS code -- a different platform
/// group of the same physical station (the sibling's own real example:
/// Wimbledon's `WDON`/`WIMBLDN`/`WDNLUL` all share CRS `WIM`). Deliberately
/// does not collapse siblings into one node: a caller changing between them
/// still owes the real minimum-change-time cost, charged at the TIPLOC
/// actually being boarded at -- Phase 3/4's own scan logic is where that
/// charge is applied, not here.
#[derive(Debug, Clone, Default)]
pub struct InterchangeData {
    /// TIPLOC -> raw minimum-change-time minutes, straight from
    /// `stanox_crs.change_time_minutes` (Phase 1) -- `None`/absent means
    /// "no MSN record matched this TIPLOC at all," genuinely different
    /// from a present sentinel or the modal 5 (see Phase 1's own Judgment
    /// Call 3).
    pub change_time_by_tiploc: HashMap<String, i32>,
    pub tiploc_to_crs: HashMap<String, String>,
    pub crs_to_tiplocs: HashMap<String, Vec<String>>,
    /// CRS -> every fixed link departing FROM it, unfiltered by date/time
    /// -- [`fixed_links_from`] applies the date/time filter at lookup time.
    /// An ALF row is a link between its `O` and `D` both ways, so build
    /// this with [`index_fixed_links_both_ways`], not from `O` alone.
    pub fixed_links_from_crs: HashMap<String, Vec<FixedLink>>,
    /// The extra change time owed when changing to or from a bus or ferry
    /// (see [`ModalChangeBuffer`]). Empty (no extra time) by default.
    pub modal_change: ModalChangeBuffer,
}

/// The extra minimum change time a change costs on a bus or ferry side:
/// `minutes` for alighting from a service in `road_or_water_uids` to make a
/// change, and `minutes` again for boarding one at a change -- so a
/// bus-to-train or train-to-ferry change costs the station's own minimum
/// change time plus `minutes`, and a bus-to-bus change plus twice
/// `minutes`. Never charged at the journey's origin or destination.
///
/// Rail minimum change times assume a platform-to-platform walk; a coach
/// stand or a ferry berth is usually further away and its services less
/// punctual. `api` fills this from `TRIP_PLAN_ROAD_WATER_CHANGE_MINUTES`
/// (default 5). See docs/superpowers/specs/2026-10-06-tiploc-locations-design.md.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ModalChangeBuffer {
    /// UIDs of the bus and ship services in the graph.
    pub road_or_water_uids: std::collections::HashSet<String>,
    pub minutes: u32,
}

impl ModalChangeBuffer {
    /// The buffer owed for one side of a change on `uid`'s service: its
    /// `minutes` for a bus or ferry, 0 for a train.
    pub fn extra_for(&self, uid: &str) -> u32 {
        if self.minutes > 0 && self.road_or_water_uids.contains(uid) {
            self.minutes
        } else {
            0
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FixedLink {
    pub mode: String,
    pub to_crs: String,
    pub minutes: i32,
    /// Raw "HHMM", 4 ASCII digits.
    pub valid_from: String,
    pub valid_to: String,
    /// Raw 7-char '0'/'1' bitmask, Monday-first.
    pub days_mask: String,
    /// Who runs it (planner operator filtering spec §5.1): `LT` for a
    /// `TUBE` link and a `TRANSFER` between a Tube pair, a DS operator for
    /// an attributed `METRO` link, else [`OperatorId::UNKNOWN`]. Set by
    /// the api's graph build; a parsed row starts unknown.
    pub operator: OperatorId,
    /// [`OperatorId::UNKNOWN`] for an ordinary link. Otherwise the link is
    /// a stand-in, usable only by a search whose operator filter rules
    /// that operator out: the estimated walks between London Underground
    /// pairs, used only when the Tube is (planner operator filtering spec
    /// §5.1).
    pub only_without: OperatorId,
}

impl FixedLink {
    /// A walk (`WALK`): never blocked by an operator filter.
    #[must_use]
    pub fn is_walk(&self) -> bool {
        self.mode == "WALK"
    }

    /// A stand-in link ([`Self::only_without`]): never used without an
    /// operator filter that rules its operator out.
    #[must_use]
    pub fn is_stand_in(&self) -> bool {
        !self.only_without.is_unknown()
    }
}

/// Indexes ALF fixed-link rows (`(from_crs, link)`) by the CRS they leave
/// from, in BOTH directions: each row also yields its reverse, `to_crs` ->
/// `from_crs` with the same mode, minutes, time window and day mask.
///
/// The ALF lists each link once, not once per direction. Its companion FLF
/// member spells the same links out as "ADDITIONAL LINK: TUBE BETWEEN CHX
/// AND EUS IN 18 MINUTES", and in the real file (RJTTF995ALF, 2026-10-20:
/// 4,651 rows over 1,308 mode/origin/destination triples) not one triple
/// has its reverse listed. Indexing the rows one way only left about half
/// of every link unusable: `EUS -> VXH` by Tube worked, `VXH -> EUS` did
/// not, so Vauxhall to Euston went round by Waterloo East and Charing
/// Cross. The reverse keeps the row's validity window, read against the
/// clock time the link is started at, as the forward row is.
///
/// An exact duplicate (a row listed twice, or a reverse the source already
/// lists) is kept once. `fixed_links_from` still picks the shortest valid
/// link per destination, so a source that does list a direction with its
/// own minutes is honoured where that is quicker.
///
/// Each CRS's links come back sorted (see
/// [`InterchangeData::sort_fixed_links`]), which the lookups rely on for
/// speed: indexed both ways a London terminus has twice the links, and the
/// planner looks them up on every improved arrival there.
pub fn index_fixed_links_both_ways(
    rows: impl IntoIterator<Item = (String, FixedLink)>,
) -> HashMap<String, Vec<FixedLink>> {
    let mut seen: std::collections::HashSet<(String, FixedLink)> = std::collections::HashSet::new();
    let mut by_from: HashMap<String, Vec<FixedLink>> = HashMap::new();
    for (from_crs, link) in rows {
        let reverse_from = link.to_crs.clone();
        let reverse = FixedLink {
            to_crs: from_crs.clone(),
            ..link.clone()
        };
        for (from, link) in [(from_crs, link), (reverse_from, reverse)] {
            if seen.insert((from.clone(), link.clone())) {
                by_from.entry(from).or_default().push(link);
            }
        }
    }
    for links in by_from.values_mut() {
        links.sort_by(link_order);
    }
    by_from
}

/// The order [`fixed_links_from`] reads a CRS's links in: by destination,
/// then minutes, then mode (its tie-break), then the rest so that equal
/// rows sit together.
fn link_order(a: &FixedLink, b: &FixedLink) -> std::cmp::Ordering {
    (
        &a.to_crs,
        a.minutes,
        &a.mode,
        &a.valid_from,
        &a.valid_to,
        &a.days_mask,
    )
        .cmp(&(
            &b.to_crs,
            b.minutes,
            &b.mode,
            &b.valid_from,
            &b.valid_to,
            &b.days_mask,
        ))
}

/// `(to_crs, minutes, mode)`: [`fixed_links_from`]'s order and tie-break.
fn lookup_key(link: &FixedLink) -> (&str, i32, &str) {
    (link.to_crs.as_str(), link.minutes, link.mode.as_str())
}

fn is_lookup_sorted(links: &[FixedLink]) -> bool {
    links.is_sorted_by(|a, b| lookup_key(a) <= lookup_key(b))
}

impl InterchangeData {
    /// Sorts every CRS's fixed links into lookup order. Lookups work on
    /// unsorted lists too, but sort a copy each time; call this after
    /// adding links to an index (as `api` does after its stop <-> station
    /// walks).
    pub fn sort_fixed_links(&mut self) {
        for links in self.fixed_links_from_crs.values_mut() {
            links.sort_by(link_order);
        }
    }
}

/// Every fixed link from `from_crs` to `to_crs`, whatever its validity.
/// Sorted by minutes when the index is sorted.
pub fn fixed_links_between<'a>(
    data: &'a InterchangeData,
    from_crs: &str,
    to_crs: &'a str,
) -> impl Iterator<Item = &'a FixedLink> + 'a {
    let links: &'a [FixedLink] = data
        .fixed_links_from_crs
        .get(from_crs)
        .map_or(&[], Vec::as_slice);
    let links = if is_lookup_sorted(links) {
        let start = links.partition_point(|link| link.to_crs.as_str() < to_crs);
        let end = start + links[start..].partition_point(|link| link.to_crs.as_str() <= to_crs);
        &links[start..end]
    } else {
        links
    };
    links.iter().filter(move |link| link.to_crs == to_crs)
}

/// The link [`fixed_links_from`] would take from `from_crs` to `to_crs` at
/// `date`/`time_min`: the shortest one valid then, or `None`.
#[expect(
    clippy::similar_names,
    reason = "the similar names are distinct domain terms"
)]
pub fn fixed_link_between<'a>(
    data: &'a InterchangeData,
    from_crs: &str,
    to_crs: &'a str,
    date: NaiveDate,
    time_min: u32,
) -> Option<&'a FixedLink> {
    fixed_link_between_where(data, from_crs, to_crs, date, time_min, |link| {
        !link.is_stand_in()
    })
}

/// [`fixed_link_between`] among the links `allow` accepts (an operator
/// filter's: see [`fixed_links_from_where`]).
#[expect(
    clippy::similar_names,
    reason = "the similar names are distinct domain terms"
)]
pub fn fixed_link_between_where<'a>(
    data: &'a InterchangeData,
    from_crs: &str,
    to_crs: &'a str,
    date: NaiveDate,
    time_min: u32,
    allow: impl Fn(&FixedLink) -> bool,
) -> Option<&'a FixedLink> {
    let clock = hhmm_bytes(clock_minutes(time_min));
    let day = day_index(date);
    fixed_links_between(data, from_crs, to_crs)
        .filter(|link| allow(link) && applies(link, day, clock))
        .min_by(|a, b| lookup_key(a).cmp(&lookup_key(b)))
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
#[expect(
    clippy::cast_sign_loss,
    reason = "the value is clamped to >= 0 first, and minute values fit easily"
)]
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

/// `minutes` (a clock position, under 1440) as the ASCII bytes of `HHMM`,
/// which compare as the time does against an ALF row's `valid_from` and
/// `valid_to`.
#[expect(
    clippy::cast_possible_truncation,
    reason = "each value is a single decimal digit"
)]
fn hhmm_bytes(minutes: u32) -> [u8; 4] {
    let (hours, minutes) = (minutes / 60, minutes % 60);
    [
        b'0' + (hours / 10) as u8,
        b'0' + (hours % 10) as u8,
        b'0' + (minutes / 10) as u8,
        b'0' + (minutes % 10) as u8,
    ]
}

/// Whether `link` runs on weekday `day` (0 = Monday) at `clock` (`HHMM`
/// bytes): its day-mask flag is `'1'` and `clock` is inside its window,
/// both ends inclusive.
fn applies(link: &FixedLink, day: usize, clock: [u8; 4]) -> bool {
    link.days_mask.as_bytes().get(day) == Some(&b'1')
        && link.valid_from.as_bytes() <= clock.as_slice()
        && clock.as_slice() <= link.valid_to.as_bytes()
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
/// Both the candidate scan order and the returned `Vec`'s order are
/// deterministic, `(to_crs, minutes, mode)`: an exact-`minutes` tie always
/// resolves the same way regardless of the order Postgres happened to
/// return `fixed_links` rows in (that table's own `SELECT` has no `ORDER
/// BY` -- see `crates/api/src/data/trip_planning.rs`'s
/// `fetch_interchange_data`). An index already in that order (see
/// [`InterchangeData::sort_fixed_links`]) is read as it is; any other is
/// sorted per call.
#[expect(
    clippy::similar_names,
    reason = "the similar names are distinct domain terms"
)]
pub fn fixed_links_from<'a>(
    data: &'a InterchangeData,
    from_crs: &str,
    date: NaiveDate,
    time_min: u32,
) -> Vec<&'a FixedLink> {
    fixed_links_from_where(data, from_crs, date, time_min, |link| !link.is_stand_in())
}

/// [`fixed_links_from`] among the links `allow` accepts: the shortest
/// ALLOWED link to each destination. A search with an operator filter
/// passes its rule here rather than dropping the chosen link afterwards,
/// which would lose a longer link it may use (a walk where the Tube is
/// ruled out). [`fixed_links_from`] itself accepts every link but the
/// stand-ins ([`FixedLink::only_without`]).
#[expect(
    clippy::similar_names,
    reason = "the similar names are distinct domain terms"
)]
pub fn fixed_links_from_where<'a>(
    data: &'a InterchangeData,
    from_crs: &str,
    date: NaiveDate,
    time_min: u32,
    allow: impl Fn(&FixedLink) -> bool,
) -> Vec<&'a FixedLink> {
    let clock = hhmm_bytes(clock_minutes(time_min));
    let day = day_index(date);

    let Some(candidates) = data.fixed_links_from_crs.get(from_crs) else {
        return Vec::new();
    };

    // In `(to_crs, minutes, mode)` order the first valid link to each
    // destination is its shortest, and the result comes out sorted. An
    // index from `index_fixed_links_both_ways` or `sort_fixed_links` is
    // already in that order; anything else is sorted here first.
    let mut result: Vec<&FixedLink> = Vec::new();
    let mut take = |link: &'a FixedLink| {
        if allow(link)
            && applies(link, day, clock)
            && result.last().is_none_or(|last| last.to_crs != link.to_crs)
        {
            result.push(link);
        }
    };
    if is_lookup_sorted(candidates) {
        candidates.iter().for_each(&mut take);
    } else {
        let mut sorted: Vec<&FixedLink> = candidates.iter().collect();
        sorted.sort_by(|a, b| lookup_key(a).cmp(&lookup_key(b)));
        sorted.into_iter().for_each(&mut take);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_data() -> InterchangeData {
        InterchangeData {
            modal_change: ModalChangeBuffer::default(),
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
            only_without: OperatorId::UNKNOWN,
            operator: OperatorId::UNKNOWN,
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
    fn a_link_listed_once_is_indexed_both_ways() {
        // The real ALF row `M=TUBE,O=EUS,D=VXH,T=22,S=0700,E=1900,R=1111110`
        // is the only Euston <-> Vauxhall row; Vauxhall must reach Euston too.
        let data = InterchangeData {
            fixed_links_from_crs: index_fixed_links_both_ways([(
                "EUS".to_string(),
                link("VXH", 22, "0700", "1900", "1111110"),
            )]),
            ..empty_data()
        };
        let back = fixed_links_from(&data, "VXH", monday(), 10 * 60);
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].to_crs, "EUS");
        assert_eq!(back[0].minutes, 22);
        assert_eq!(back[0].mode, "TUBE");
        let forward = fixed_links_from(&data, "EUS", monday(), 10 * 60);
        assert_eq!(forward[0].to_crs, "VXH");
        // The reverse keeps the row's window and days: not on a Sunday,
        // not after 19:00.
        let sunday = NaiveDate::from_ymd_opt(2026, 9, 6).unwrap();
        assert!(fixed_links_from(&data, "VXH", sunday, 10 * 60).is_empty());
        assert!(fixed_links_from(&data, "VXH", monday(), 20 * 60).is_empty());
    }

    #[test]
    fn duplicate_rows_and_already_listed_reverses_are_kept_once() {
        let index = index_fixed_links_both_ways([
            (
                "VIC".to_string(),
                link("WAT", 17, "0700", "1900", "1111110"),
            ),
            (
                "VIC".to_string(),
                link("WAT", 17, "0700", "1900", "1111110"),
            ),
            (
                "WAT".to_string(),
                link("VIC", 17, "0700", "1900", "1111110"),
            ),
            // A reverse with its own minutes is a different link: both stay.
            (
                "WAT".to_string(),
                link("VIC", 15, "0700", "1900", "1111110"),
            ),
        ]);
        // 17 (listed twice, and listed back) once each way; 15 both ways.
        assert_eq!(index["VIC"].len(), 2);
        assert_eq!(index["WAT"].len(), 2);
        let data = InterchangeData {
            fixed_links_from_crs: index,
            ..empty_data()
        };
        let quickest = fixed_links_from(&data, "WAT", monday(), 10 * 60);
        assert_eq!(quickest[0].minutes, 15);
    }

    #[test]
    fn sorted_and_unsorted_indexes_look_up_the_same_links() {
        let rows = vec![
            link("KGX", 15, "0000", "2359", "1111111"),
            link("VIC", 9, "0700", "1900", "1111110"),
            link("KGX", 5, "0500", "2359", "1111111"),
            link("STP", 5, "0700", "1900", "1111110"),
            link("KGX", 10, "1901", "2359", "1111110"),
            link("VIC", 14, "1901", "2359", "1111110"),
        ];
        let unsorted = InterchangeData {
            fixed_links_from_crs: HashMap::from([("EUS".to_string(), rows)]),
            ..empty_data()
        };
        let mut sorted = unsorted.clone();
        sorted.sort_fixed_links();
        assert_ne!(
            sorted.fixed_links_from_crs["EUS"],
            unsorted.fixed_links_from_crs["EUS"]
        );
        for minute in [6 * 60, 8 * 60, 19 * 60 + 30, 1440 + 8 * 60] {
            let from_sorted = fixed_links_from(&sorted, "EUS", monday(), minute);
            assert_eq!(
                from_sorted,
                fixed_links_from(&unsorted, "EUS", monday(), minute)
            );
            for to in ["KGX", "VIC", "STP", "ZZZ"] {
                let between = fixed_link_between(&sorted, "EUS", to, monday(), minute);
                assert_eq!(
                    between,
                    fixed_link_between(&unsorted, "EUS", to, monday(), minute)
                );
                assert_eq!(
                    between,
                    from_sorted.iter().find(|l| l.to_crs == to).copied()
                );
            }
        }
        let at_eight: Vec<i32> = fixed_links_from(&sorted, "EUS", monday(), 8 * 60)
            .iter()
            .map(|l| l.minutes)
            .collect();
        // KGX 5 (not 15), STP 5, VIC 9, in destination order.
        assert_eq!(at_eight, [5, 5, 9]);
        let mut kgx: Vec<i32> = fixed_links_between(&unsorted, "EUS", "KGX")
            .map(|l| l.minutes)
            .collect();
        kgx.sort_unstable();
        assert_eq!(kgx, [5, 10, 15]);
        let sorted_kgx: Vec<i32> = fixed_links_between(&sorted, "EUS", "KGX")
            .map(|l| l.minutes)
            .collect();
        assert_eq!(sorted_kgx, [5, 10, 15]);
        assert_eq!(fixed_links_between(&sorted, "VXH", "KGX").count(), 0);
    }

    #[test]
    fn a_pair_with_no_link_either_way_gains_none() {
        let index = index_fixed_links_both_ways([(
            "EUS".to_string(),
            link("VXH", 22, "0700", "1900", "1111110"),
        )]);
        assert!(!index.contains_key("VIC"));
        assert!(index["VXH"].iter().all(|l| l.to_crs == "EUS"));
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
