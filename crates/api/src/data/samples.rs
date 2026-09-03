//! Pure logic for computing which stations `poller-ldbws` should sample,
//! independent of any HTTP/DB concern so it's testable without either.

use common::LineDefinition;
use std::collections::BTreeSet;

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
            exclusive_segments: vec![],
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
}
