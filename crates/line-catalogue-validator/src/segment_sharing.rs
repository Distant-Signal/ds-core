//! Segment-sharing rules (lines/SCHEMA.md, "Segments"), checked against a
//! checked-in baseline of the violations the catalogue already had.
//!
//! The matcher (`common::matcher`) calls an incident at a station
//! `ExclusiveSegment` when the station's segment has one user, and
//! `SharedSegment` when several lines use it. That only works when shared
//! track carries one segment name in every file that runs over it, and
//! junction stations sit on the shared segment. Three shapes break it:
//!
//! - `exclusive-on-several-lines <CRS> <line>...`: the station is on a
//!   single-user segment in two or more files, so it is "exclusive" to
//!   each of them at once. Either the same track under two names (unify
//!   the names) or two routes meeting at a station (a junction: give it a
//!   narrow shared segment, as `swr-brockenhurst-junction` does).
//! - `junction-on-exclusive-segment <line> <CRS>`: a `role = "junction"`
//!   station another line also lists, on a single-user segment.
//! - `single-user-shared-segment <segment>`: a segment named as shared
//!   track (a `junction`, `trunk` or `approach` word in its name) that
//!   only one line uses, usually because a sibling spells it differently.
//!
//! Violations listed in the baseline are reported as one warning; a new
//! one is an error, so the catalogue cannot get worse, and a baseline
//! entry that no longer occurs is an error too, so a fix also shrinks the
//! baseline. Some baselined entries are deliberate (two physically
//! separate routes that only share a station building); a reviewed new
//! one of those goes into the baseline with
//! `cargo run -p line-catalogue-validator -- --write-segment-baseline`.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use crate::checks::{Finding, LoadedLine, Severity};

/// The baseline, relative to the catalogue directory. Not `*.toml`, so the
/// line glob never reads it.
pub(crate) const BASELINE_FILE: &str = "segment-sharing-baseline.txt";

/// Words that name a segment as shared track.
const SHARED_WORDS: [&str; 3] = ["junction", "trunk", "approach"];

#[derive(Debug)]
pub(crate) struct Violation {
    /// Stable key, one line of the baseline file.
    pub key: String,
    pub path: PathBuf,
    pub message: String,
}

/// Every segment-sharing violation in the catalogue, sorted by key.
pub(crate) fn violations(lines: &[LoadedLine]) -> Vec<Violation> {
    let mut users: HashMap<&str, BTreeSet<&str>> = HashMap::new();
    let mut listed_by: HashMap<&str, BTreeSet<&str>> = HashMap::new();
    for line in lines {
        let id = line.definition.id.as_str();
        for station in &line.definition.stations {
            listed_by
                .entry(station.crs.as_str())
                .or_default()
                .insert(id);
            if let Some(segment) = station.segment.as_deref() {
                users.entry(segment).or_default().insert(id);
            }
        }
    }
    let single_user = |segment: &str| users.get(segment).is_some_and(|u| u.len() == 1);

    let mut out = Vec::new();
    // CRS -> (line id -> its single-user segment there, and the file).
    let mut exclusive_at: BTreeMap<&str, BTreeMap<&str, (&str, &Path)>> = BTreeMap::new();
    for line in lines {
        let id = line.definition.id.as_str();
        for station in &line.definition.stations {
            let Some(segment) = station.segment.as_deref() else {
                continue;
            };
            if !single_user(segment) {
                continue;
            }
            exclusive_at
                .entry(station.crs.as_str())
                .or_default()
                .insert(id, (segment, line.path.as_path()));
            let others = listed_by
                .get(station.crs.as_str())
                .is_some_and(|ids| ids.len() > 1);
            if station.role == "junction" && others {
                out.push(Violation {
                    key: format!("junction-on-exclusive-segment {id} {}", station.crs),
                    path: line.path.clone(),
                    message: format!(
                        "junction {} is on `{segment}`, which only this line uses, although \
                         other lines list the station -- lines/SCHEMA.md: a junction belongs \
                         to the shared segment",
                        station.crs
                    ),
                });
            }
        }
    }
    for (crs, by_line) in &exclusive_at {
        if by_line.len() < 2 {
            continue;
        }
        let ids: Vec<&str> = by_line.keys().copied().collect();
        let detail: Vec<String> = by_line
            .iter()
            .map(|(id, (segment, _))| format!("{id} (`{segment}`)"))
            .collect();
        let path = by_line
            .values()
            .next()
            .map(|(_, p)| p.to_path_buf())
            .unwrap_or_default();
        out.push(Violation {
            key: format!("exclusive-on-several-lines {crs} {}", ids.join(" ")),
            path,
            message: format!(
                "{crs} is on a single-user segment in several lines: {} -- one track gets one \
                 segment name in every file that runs over it, and a junction a shared one \
                 (lines/SCHEMA.md, Segments)",
                detail.join(", ")
            ),
        });
    }
    let mut named_shared: Vec<(&str, &str)> = users
        .iter()
        .filter(|(segment, u)| {
            u.len() == 1 && segment.split('-').any(|word| SHARED_WORDS.contains(&word))
        })
        .filter_map(|(segment, u)| u.first().map(|id| (*segment, *id)))
        .collect();
    named_shared.sort_unstable();
    for (segment, id) in named_shared {
        let path = lines
            .iter()
            .find(|l| l.definition.id == id)
            .map(|l| l.path.clone())
            .unwrap_or_default();
        out.push(Violation {
            key: format!("single-user-shared-segment {segment}"),
            path,
            message: format!(
                "segment `{segment}` is named as shared track but only {id} uses it -- reuse \
                 the sibling files' name for that track, or rename it"
            ),
        });
    }
    out.sort_by(|a, b| a.key.cmp(&b.key));
    out
}

/// The baseline's keys: non-empty lines that are not `#` comments.
fn parse_baseline(text: &str) -> BTreeSet<String> {
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(ToString::to_string)
        .collect()
}

/// Findings against the baseline in `lines_dir` (module docs).
pub(crate) fn validate(lines_dir: &Path, lines: &[LoadedLine]) -> Vec<Finding> {
    let path = lines_dir.join(BASELINE_FILE);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) => {
            return vec![Finding {
                path,
                line_no: None,
                severity: Severity::Error,
                message: format!(
                    "cannot read the segment-sharing baseline ({e}); write it with \
                     `cargo run -p line-catalogue-validator -- --write-segment-baseline`"
                ),
            }];
        }
    };
    compare(&path, &text, violations(lines))
}

fn compare(path: &Path, baseline_text: &str, found: Vec<Violation>) -> Vec<Finding> {
    let baseline = parse_baseline(baseline_text);
    let found_keys: BTreeSet<String> = found.iter().map(|v| v.key.clone()).collect();
    let mut findings = Vec::new();
    let mut known = 0;
    for violation in found {
        if baseline.contains(&violation.key) {
            known += 1;
            continue;
        }
        findings.push(Finding {
            path: violation.path,
            line_no: None,
            severity: Severity::Error,
            message: format!(
                "{} [new segment-sharing violation `{}`; fix it, or if it is deliberate add \
                 that key to {}]",
                violation.message,
                violation.key,
                path.display()
            ),
        });
    }
    for stale in baseline.difference(&found_keys) {
        let line_no = baseline_text
            .lines()
            .position(|l| l.trim() == stale)
            .map(|i| i + 1);
        findings.push(Finding {
            path: path.to_path_buf(),
            line_no,
            severity: Severity::Error,
            message: format!(
                "baseline entry `{stale}` no longer occurs -- delete that line (or rewrite the \
                 file with --write-segment-baseline)"
            ),
        });
    }
    if known > 0 {
        findings.push(Finding {
            path: path.to_path_buf(),
            line_no: None,
            severity: Severity::Warning,
            message: format!(
                "{known} known segment-sharing violation(s) are baselined here, see \
                 lines/SCHEMA.md, Segments; fix them and delete their lines"
            ),
        });
    }
    findings
}

/// The baseline file's text for these violations.
pub(crate) fn render_baseline(found: &[Violation]) -> String {
    let mut text = String::from(
        "# Segment-sharing violations the catalogue already has, one key per line.\n\
         # line-catalogue-validator fails on any violation not listed here, and on a\n\
         # listed one that no longer occurs. See crates/line-catalogue-validator/src/\n\
         # segment_sharing.rs and lines/SCHEMA.md (Segments). Rewrite with\n\
         # `cargo run -p line-catalogue-validator -- --write-segment-baseline`.\n",
    );
    for violation in found {
        text.push_str(&violation.key);
        text.push('\n');
    }
    text
}

/// Rewrites the baseline with every current violation. Returns how many.
pub(crate) fn write_baseline(lines_dir: &Path, lines: &[LoadedLine]) -> anyhow::Result<usize> {
    let found = violations(lines);
    std::fs::write(lines_dir.join(BASELINE_FILE), render_baseline(&found))?;
    Ok(found.len())
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;

    use super::*;

    fn line(id: &str, stations: &[(&str, &str, Option<&str>)]) -> LoadedLine {
        let mut raw = format!(
            "id = \"{id}\"\nname = \"{id}\"\nmode = \"national-rail\"\n\
             category = \"regional\"\noperators = [\"XC\"]\n"
        );
        for (crs, segment, role) in stations {
            write!(
                raw,
                "[[stations]]\ncrs = \"{crs}\"\nsegment = \"{segment}\"\n"
            )
            .unwrap();
            if let Some(role) = role {
                writeln!(raw, "role = \"{role}\"").unwrap();
            }
        }
        LoadedLine {
            path: PathBuf::from(format!("lines/{id}.toml")),
            definition: common::LineDefinition::from_toml(&raw, Path::new("test.toml"))
                .expect("parses"),
            raw,
        }
    }

    fn keys(lines: &[LoadedLine]) -> Vec<String> {
        violations(lines).into_iter().map(|v| v.key).collect()
    }

    #[test]
    fn a_shared_trunk_with_its_junction_on_it_is_clean() {
        let lines = [
            line(
                "a",
                &[
                    ("WAT", "trunk-x", None),
                    ("WOK", "trunk-x", Some("junction")),
                    ("BSK", "a-own", None),
                ],
            ),
            line(
                "b",
                &[
                    ("WAT", "trunk-x", None),
                    ("WOK", "trunk-x", Some("junction")),
                    ("GLD", "b-own", None),
                ],
            ),
        ];
        assert_eq!(keys(&lines), [] as [String; 0]);
    }

    #[test]
    fn the_same_track_under_two_names_is_reported() {
        let lines = [
            line(
                "a",
                &[("FPK", "a-own", Some("junction")), ("SVG", "a-own", None)],
            ),
            line(
                "b",
                &[("FPK", "b-own", Some("junction")), ("SVG", "b-trunk", None)],
            ),
        ];
        assert_eq!(
            keys(&lines),
            [
                "exclusive-on-several-lines FPK a b",
                "exclusive-on-several-lines SVG a b",
                "junction-on-exclusive-segment a FPK",
                "junction-on-exclusive-segment b FPK",
                "single-user-shared-segment b-trunk",
            ]
        );
    }

    #[test]
    fn a_junction_no_other_line_lists_is_not_reported() {
        let lines = [line("a", &[("EPH", "a-own", Some("junction"))])];
        assert_eq!(keys(&lines), [] as [String; 0]);
    }

    #[test]
    fn baselined_violations_warn_and_new_or_stale_ones_fail() {
        let lines = [
            line("a", &[("FPK", "a-own", None)]),
            line("b", &[("FPK", "b-own", None)]),
        ];
        let path = Path::new("lines/segment-sharing-baseline.txt");
        let findings = compare(
            path,
            "# header\nexclusive-on-several-lines FPK a b\n",
            violations(&lines),
        );
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert_eq!(findings[0].severity, Severity::Warning);

        let findings = compare(path, "", violations(&lines));
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert_eq!(findings[0].severity, Severity::Error);
        assert!(findings[0].message.contains("FPK"), "{findings:?}");

        let findings = compare(
            path,
            "\nsingle-user-shared-segment gone-trunk\n",
            Vec::new(),
        );
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert_eq!(findings[0].severity, Severity::Error);
        assert_eq!(findings[0].line_no, Some(2));
        assert!(findings[0].message.contains("gone-trunk"), "{findings:?}");
    }

    #[test]
    fn the_baseline_round_trips() {
        let lines = [
            line("a", &[("FPK", "a-own", None)]),
            line("b", &[("FPK", "b-own", None)]),
        ];
        let text = render_baseline(&violations(&lines));
        assert_eq!(
            parse_baseline(&text),
            BTreeSet::from(["exclusive-on-several-lines FPK a b".to_string()])
        );
    }

    #[test]
    fn the_real_catalogue_matches_its_baseline() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../lines");
        let lines = crate::checks::load_all(&dir).expect("lines/ loads");
        let errors: Vec<String> = validate(&dir, &lines)
            .into_iter()
            .filter(|f| f.severity == Severity::Error)
            .map(|f| f.to_string())
            .collect();
        assert!(errors.is_empty(), "{errors:#?}");
    }
}
