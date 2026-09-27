//! Pure logic for computing which stations `poller-ldbws` should sample,
//! independent of any HTTP/DB concern so it's testable without either.

use common::LineDefinition;
use std::collections::{BTreeSet, HashMap, HashSet};

/// Deduplicated, sorted union of every line's `sample_stations` CRS codes.
/// Sorted so the returned list (and therefore `poller-ldbws`'s poll order)
/// is deterministic across runs, not dependent on `Vec<LineDefinition>`
/// iteration order.
pub fn dedup_sample_stations(lines: &[LineDefinition]) -> Vec<String> {
    let mut set = BTreeSet::new();
    for line in lines {
        for crs in &line.sample_stations {
            set.insert(crs.clone());
        }
    }
    set.into_iter().collect()
}

/// Operator knobs (LEG-18) that narrow the sample-station list. Both are
/// off by default, and with both off [`select_sample_stations`] returns
/// exactly [`dedup_sample_stations`]'s list. `poller-ldbws` sends them as
/// query parameters on `GET /private/sample-stations`; see
/// `routes/samples.rs` for why they live there rather than in `api`'s own
/// config.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SampleSelection {
    /// Only sample lines at least one user has pinned.
    pub pinned_lines_only: bool,
    /// At most this many stations. `None` (or 0) means no cap.
    pub max_stations: Option<usize>,
}

impl SampleSelection {
    /// True when neither knob is set: the full, unfiltered list is wanted
    /// and no pin data needs to be read.
    pub fn is_unrestricted(&self) -> bool {
        !self.pinned_lines_only && self.max_stations.unwrap_or(0) == 0
    }
}

/// [`dedup_sample_stations`] narrowed by `selection`. `pin_counts` maps a
/// line id to how many users have pinned it (lines absent from it are
/// unpinned); it is ignored when `selection` is unrestricted.
///
/// The cap is line-fair rather than alphabetical: lines are ordered
/// most-pinned first (then by id), and stations are taken round-robin --
/// every line's first sample station, then every line's second, and so on
/// -- until the cap is reached. A cap therefore thins out each line's
/// coverage before it drops any line entirely, and drops unpinned lines
/// before pinned ones. The result is sorted, like the unrestricted list.
pub fn select_sample_stations(
    lines: &[LineDefinition],
    pin_counts: &HashMap<String, i64>,
    selection: SampleSelection,
) -> Vec<String> {
    if selection.is_unrestricted() {
        return dedup_sample_stations(lines);
    }

    let mut ordered: Vec<(&LineDefinition, i64)> = lines
        .iter()
        .map(|line| (line, pin_counts.get(&line.id).copied().unwrap_or(0)))
        .filter(|(_, pins)| !selection.pinned_lines_only || *pins > 0)
        .collect();
    ordered.sort_by(|(a, a_pins), (b, b_pins)| b_pins.cmp(a_pins).then_with(|| a.id.cmp(&b.id)));

    let cap = match selection.max_stations {
        Some(cap) if cap > 0 => cap,
        _ => usize::MAX,
    };
    let deepest = ordered
        .iter()
        .map(|(line, _)| line.sample_stations.len())
        .max()
        .unwrap_or(0);

    let mut chosen = HashSet::new();
    'rounds: for depth in 0..deepest {
        for (line, _) in &ordered {
            if chosen.len() >= cap {
                break 'rounds;
            }
            if let Some(crs) = line.sample_stations.get(depth) {
                chosen.insert(crs.clone());
            }
        }
    }

    let mut stations: Vec<String> = chosen.into_iter().collect();
    stations.sort();
    stations
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line_with_samples(id: &str, sample_stations: &[&str]) -> LineDefinition {
        LineDefinition {
            id: id.to_string(),
            name: id.to_string(),
            mode: "national-rail".to_string(),
            category: "main-line".to_string(),
            operators: vec![],
            stations: vec![],
            sample_stations: sample_stations.iter().map(|s| s.to_string()).collect(),
            match_keywords: vec![],
            excluded_keywords: vec![],
            severity_overrides: Default::default(),
            destination_crs_filter: vec![],
            headcode_prefixes: vec![],
            full_coverage_enabled: false,
        }
    }

    #[test]
    fn empty_lines_produce_empty_list() {
        assert_eq!(dedup_sample_stations(&[]), Vec::<String>::new());
    }

    #[test]
    fn single_line_returns_its_stations_sorted() {
        let lines = vec![line_with_samples("wcml", &["EUS", "MKC", "BHM"])];
        assert_eq!(dedup_sample_stations(&lines), vec!["BHM", "EUS", "MKC"]);
    }

    #[test]
    fn overlapping_stations_across_lines_are_deduplicated() {
        let lines = vec![
            line_with_samples("swr-main", &["WAT", "WOK", "BSK"]),
            line_with_samples("swr-portsmouth", &["WAT", "WOK", "PMH"]),
        ];
        assert_eq!(
            dedup_sample_stations(&lines),
            vec!["BSK", "PMH", "WAT", "WOK"]
        );
    }

    fn pins(entries: &[(&str, i64)]) -> HashMap<String, i64> {
        entries
            .iter()
            .map(|(id, count)| (id.to_string(), *count))
            .collect()
    }

    fn catalogue() -> Vec<LineDefinition> {
        vec![
            line_with_samples("anglia", &["NRW", "COL", "IPS"]),
            line_with_samples("swr-main", &["WAT", "WOK", "BSK"]),
            line_with_samples("swr-portsmouth", &["WAT", "WOK", "PMH"]),
            line_with_samples("wcml", &["EUS", "MKC", "BHM"]),
        ]
    }

    #[test]
    fn the_default_selection_is_exactly_the_deduplicated_list() {
        let lines = catalogue();
        // Pin data must not matter when no knob is set.
        let pin_counts = pins(&[("wcml", 3)]);
        assert!(SampleSelection::default().is_unrestricted());
        assert_eq!(
            select_sample_stations(&lines, &pin_counts, SampleSelection::default()),
            dedup_sample_stations(&lines)
        );
        // A zero cap is "no cap", not "no stations".
        let zero_cap = SampleSelection {
            pinned_lines_only: false,
            max_stations: Some(0),
        };
        assert!(zero_cap.is_unrestricted());
        assert_eq!(
            select_sample_stations(&lines, &pin_counts, zero_cap),
            dedup_sample_stations(&lines)
        );
    }

    #[test]
    fn a_cap_at_or_above_the_list_size_changes_nothing() {
        let lines = catalogue();
        let full = dedup_sample_stations(&lines);
        for cap in [full.len(), full.len() + 1, 10_000] {
            let selection = SampleSelection {
                pinned_lines_only: false,
                max_stations: Some(cap),
            };
            assert_eq!(select_sample_stations(&lines, &pins(&[]), selection), full);
        }
    }

    #[test]
    fn a_cap_keeps_every_line_covered_before_adding_second_stations() {
        let lines = catalogue();
        let selection = SampleSelection {
            pinned_lines_only: false,
            max_stations: Some(4),
        };
        // Round one, by line id: anglia NRW, swr-main WAT, swr-portsmouth
        // WAT (already chosen), wcml EUS; round two starts with anglia COL.
        assert_eq!(
            select_sample_stations(&lines, &pins(&[]), selection),
            vec!["COL", "EUS", "NRW", "WAT"]
        );
    }

    #[test]
    fn a_cap_serves_the_most_pinned_lines_first() {
        let lines = catalogue();
        let selection = SampleSelection {
            pinned_lines_only: false,
            max_stations: Some(2),
        };
        assert_eq!(
            select_sample_stations(&lines, &pins(&[("wcml", 5), ("swr-main", 1)]), selection),
            vec!["EUS", "WAT"]
        );
    }

    #[test]
    fn pinned_lines_only_drops_every_unpinned_line() {
        let lines = catalogue();
        let selection = SampleSelection {
            pinned_lines_only: true,
            max_stations: None,
        };
        assert_eq!(
            select_sample_stations(
                &lines,
                &pins(&[("swr-portsmouth", 1), ("unknown", 2)]),
                selection
            ),
            vec!["PMH", "WAT", "WOK"]
        );
        // Nobody has pinned anything: nothing to sample.
        assert!(select_sample_stations(&lines, &pins(&[]), selection).is_empty());
    }

    #[test]
    fn pinned_lines_only_and_a_cap_combine() {
        let lines = catalogue();
        let selection = SampleSelection {
            pinned_lines_only: true,
            max_stations: Some(3),
        };
        assert_eq!(
            select_sample_stations(&lines, &pins(&[("wcml", 1), ("anglia", 2)]), selection),
            vec!["COL", "EUS", "NRW"]
        );
    }
}
