//! Bus stops' and ferry terminals' parent stations: the curated
//! `reference-data/tiploc-parent-stations.csv`, and the walking time
//! between a stop and its parent.
//!
//! `schedule-reference` reads the CSV's parents (its third parent rule,
//! after "same TIPLOC" and "nearest within 400 m"); `api` reads its
//! `walk_minutes` and builds the planner's stop <-> parent walking links
//! from the published `tiploc_locations` rows, timing each by
//! [`walk_minutes`]. Both read the same compiled-in file through this
//! module. See docs/superpowers/specs/2026-10-06-tiploc-locations-design.md
//! ("Walking links to the parent station").

use std::collections::BTreeMap;

/// `reference-data/tiploc-parent-stations.csv`, compiled in.
pub const CURATED_PARENTS_CSV: &str =
    include_str!("../../../reference-data/tiploc-parent-stations.csv");

/// Walking pace assumed over an MSN grid distance: 80 m a minute (4.8
/// km/h), the usual planning figure for a walk with luggage.
pub const WALK_METRES_PER_MINUTE: i32 = 80;
/// Added to every walk: getting off the stand or out of the terminal and
/// into the station. MSN grid references are only 100 m squares, so this
/// also covers their rounding.
pub const WALK_ACCESS_MINUTES: i32 = 2;
/// The shortest walk [`walk_minutes`] gives, even for a stop in the
/// station's own grid square.
pub const MIN_WALK_MINUTES: i32 = 3;
/// The longest walk [`walk_minutes`] gives from a distance. Parents found by
/// proximity are within 400 m, so this only caps a long curated link; one
/// that really takes longer says so in the CSV's `walk_minutes`.
pub const MAX_WALK_MINUTES: i32 = 15;
/// The largest `walk_minutes` the CSV may give.
pub const MAX_CURATED_WALK_MINUTES: i32 = 60;

/// One curated row: the parent station and, optionally, a walking time
/// that overrides [`walk_minutes`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CuratedParent {
    pub parent_crs: String,
    pub walk_minutes: Option<i32>,
}

/// The walk between a stop and its parent station: the curated
/// `walk_minutes` when the CSV gives one for this same parent, else from
/// the grid distance (`ceil(distance / 80 m) + 2`, clamped to `3..=15`),
/// else, with no distance known (a stop or station with no MSN grid
/// reference), the cautious [`MAX_WALK_MINUTES`].
pub fn walk_minutes(distance_m: Option<i32>, curated: Option<i32>) -> i32 {
    if let Some(minutes) = curated {
        return minutes;
    }
    distance_m.map_or(MAX_WALK_MINUTES, |distance| {
        let distance = distance.max(0);
        let walking = (distance + WALK_METRES_PER_MINUTE - 1) / WALK_METRES_PER_MINUTE;
        (walking + WALK_ACCESS_MINUTES).clamp(MIN_WALK_MINUTES, MAX_WALK_MINUTES)
    })
}

/// TIPLOC -> curated parent from `csv` (`tiploc,parent_crs,walk_minutes,note`).
/// Comment (`#`) and blank lines and the header are skipped; a malformed row
/// is skipped too (the checked-in file's own test proves it has none). An
/// empty `walk_minutes` means "from the distance"; a non-numeric one, or one
/// outside `1..=60`, makes the row malformed.
pub fn parse_curated_parents(csv: &str) -> BTreeMap<String, CuratedParent> {
    csv.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter_map(|line| {
            let mut fields = line.splitn(4, ',');
            let tiploc = fields.next()?.trim().to_ascii_uppercase();
            let crs = fields.next()?.trim().to_ascii_uppercase();
            let minutes = fields.next()?.trim();
            let walk_minutes = if minutes.is_empty() {
                None
            } else {
                Some(
                    minutes
                        .parse::<i32>()
                        .ok()
                        .filter(|m| (1..=MAX_CURATED_WALK_MINUTES).contains(m))?,
                )
            };
            let valid = !tiploc.is_empty()
                && tiploc.len() <= 7
                && tiploc.chars().all(|c| c.is_ascii_alphanumeric())
                && crs.len() == 3
                && crs.chars().all(|c| c.is_ascii_uppercase());
            valid.then_some((
                tiploc,
                CuratedParent {
                    parent_crs: crs,
                    walk_minutes,
                },
            ))
        })
        .collect()
}

/// The checked-in curated parents.
pub fn curated_parents() -> BTreeMap<String, CuratedParent> {
    parse_curated_parents(CURATED_PARENTS_CSV)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn walking_time_comes_from_the_grid_distance() {
        // Same grid square: the 3-minute floor (0 m walk + 2 access).
        assert_eq!(walk_minutes(Some(0), None), 3);
        // One square diagonally (141 m): 2 + 2.
        assert_eq!(walk_minutes(Some(141), None), 4);
        // Heathrow Terminal 2's stop, 200 m: 3 + 2.
        assert_eq!(walk_minutes(Some(200), None), 5);
        // Exactly 80 m a minute rounds no further up.
        assert_eq!(walk_minutes(Some(160), None), 4);
        // The proximity limit, 400 m: 5 + 2.
        assert_eq!(walk_minutes(Some(400), None), 7);
        // A long curated link is capped...
        assert_eq!(walk_minutes(Some(943), None), 14);
        assert_eq!(walk_minutes(Some(5_000), None), 15);
        // ...unknown distance is cautious...
        assert_eq!(walk_minutes(None, None), 15);
        // ...and a curated time wins over both.
        assert_eq!(walk_minutes(Some(412), Some(8)), 8);
        assert_eq!(walk_minutes(None, Some(25)), 25);
        assert_eq!(walk_minutes(Some(-5), None), 3);
    }

    #[test]
    fn the_checked_in_csv_parses_completely() {
        let parents = curated_parents();
        let rows = CURATED_PARENTS_CSV
            .lines()
            .filter(|line| !line.trim().is_empty() && !line.starts_with('#'))
            .count();
        // Every data row (all but the header) parsed.
        assert_eq!(parents.len(), rows - 1);
        assert_eq!(
            parents.get("HTRBUS3"),
            Some(&CuratedParent {
                parent_crs: "HXX".to_string(),
                walk_minutes: Some(8),
            })
        );
    }

    #[test]
    fn curated_rows_are_validated() {
        let parents = parse_curated_parents(
            "# comment\n\
             tiploc,parent_crs,walk_minutes,note\n\
             GOOD,ABC,,fine, with a comma\n\
             TIMED,ABD,12,explicit minutes\n\
             BAD-ONE,ABC,,x\n\
             OK2,abcd,,x\n\
             NOMIN,ABC,soon,x\n\
             TOOLONG,ABC,61,x\n\
             ZERO,ABC,0,x\n\
             SHORT,ABC\n\
             low,xyz,,lower case is upper-cased\n",
        );
        let parent = |crs: &str, walk_minutes| CuratedParent {
            parent_crs: crs.to_string(),
            walk_minutes,
        };
        assert_eq!(
            parents.into_iter().collect::<Vec<_>>(),
            vec![
                ("GOOD".to_string(), parent("ABC", None)),
                ("LOW".to_string(), parent("XYZ", None)),
                ("TIMED".to_string(), parent("ABD", Some(12))),
            ]
        );
    }
}
