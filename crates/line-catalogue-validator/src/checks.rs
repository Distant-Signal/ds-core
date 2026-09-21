//! The actual comparison logic: `lines/*.toml` content vs a
//! [`ReferenceData`]. Deliberately takes no opinion on where `ReferenceData`
//! came from -- see `reference.rs`'s module doc -- so this module is
//! exercised identically by both tiers and by this crate's own unit tests
//! (no CSV file or network access needed to test the comparison rules
//! themselves).

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use common::LineDefinition;

use crate::reference::ReferenceData;

/// One loaded `lines/*.toml` file, kept alongside its raw text so findings
/// can cite a real line number (`common::LineDefinition::from_file`
/// discards the source text once parsed, and TOML line numbers aren't
/// otherwise recoverable from the parsed struct).
pub struct LoadedLine {
    pub path: PathBuf,
    pub definition: LineDefinition,
    pub raw: String,
}

pub fn load_all(lines_dir: &Path) -> anyhow::Result<Vec<LoadedLine>> {
    let pattern = format!("{}/*.toml", lines_dir.display());
    let mut out = Vec::new();
    for entry in glob::glob(&pattern)? {
        let path = entry?;
        let raw = std::fs::read_to_string(&path)?;
        let definition = LineDefinition::from_file(&path)?;
        out.push(LoadedLine {
            path,
            definition,
            raw,
        });
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    /// Fails the build.
    Error,
    /// Printed, does not affect the exit code -- see
    /// `reference-data/line-catalogue-validation.md`'s "Known
    /// limitations" section for why a CRS/TIPLOC mismatch specifically is
    /// a warning, not an error.
    Warning,
}

#[derive(Debug)]
pub struct Finding {
    pub path: PathBuf,
    pub line_no: Option<usize>,
    pub severity: Severity,
    pub message: String,
}

impl std::fmt::Display for Finding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let tag = match self.severity {
            Severity::Error => "error",
            Severity::Warning => "warning",
        };
        match self.line_no {
            Some(n) => write!(f, "{}:{n}: {tag}: {}", self.path.display(), self.message),
            None => write!(f, "{}: {tag}: {}", self.path.display(), self.message),
        }
    }
}

/// 1-based line numbers of every line in `raw` whose trimmed content
/// starts with `field = "` (e.g. `crs = "EUS"`), in file order. Relies on
/// `[[stations]]` entries always writing `crs`/`tiploc` as a bare
/// `key = "value"` line (true for every `lines/*.toml` file today --
/// `lines/SCHEMA.md`'s own worked example is written this way, and no
/// file inlines a station as a one-line table) to line each value up
/// positionally with `LineDefinition.stations`'s parsed order.
fn field_line_numbers(raw: &str, field: &str) -> Vec<usize> {
    let needle = format!("{field} = \"");
    raw.lines()
        .enumerate()
        .filter(|(_, line)| line.trim_start().starts_with(&needle))
        .map(|(i, _)| i + 1)
        .collect()
}

fn operators_line_number(raw: &str) -> Option<usize> {
    raw.lines()
        .position(|line| line.trim_start().starts_with("operators ="))
        .map(|i| i + 1)
}

pub fn validate_lines(lines: &[LoadedLine], reference: &ReferenceData) -> Vec<Finding> {
    let mut findings = Vec::new();

    for line in lines {
        let crs_lines = field_line_numbers(&line.raw, "crs");
        let tiploc_lines = field_line_numbers(&line.raw, "tiploc");
        let operators_line = operators_line_number(&line.raw);

        for (idx, station) in line.definition.stations.iter().enumerate() {
            let crs_line_no = crs_lines.get(idx).copied();
            if !reference.known_crs(&station.crs) {
                findings.push(Finding {
                    path: line.path.clone(),
                    line_no: crs_line_no,
                    severity: Severity::Error,
                    message: format!(
                        "unknown CRS code \"{}\" (station #{}) -- not found in \
                         reference-data/crs-tiploc.csv; either a typo or a code that's never \
                         been issued",
                        station.crs,
                        idx + 1
                    ),
                });
                continue;
            }

            if let Some(tiploc) = &station.tiploc {
                match reference.tiploc_matches(&station.crs, tiploc) {
                    Some(true) | None => {}
                    Some(false) => {
                        // Best-effort: find *a* tiploc line, not
                        // necessarily this station's, when counts drift
                        // (e.g. a station missing its optional tiploc).
                        let tiploc_line_no = tiploc_lines.get(
                            line.definition.stations[..idx]
                                .iter()
                                .filter(|s| s.tiploc.is_some())
                                .count(),
                        );
                        let known = reference
                            .crs_to_tiploc
                            .get(&station.crs)
                            .map(|set| {
                                let mut v: Vec<_> = set.iter().cloned().collect();
                                v.sort();
                                v.join(", ")
                            })
                            .unwrap_or_default();
                        findings.push(Finding {
                            path: line.path.clone(),
                            line_no: tiploc_line_no.copied().or(crs_line_no),
                            severity: Severity::Warning,
                            message: format!(
                                "CRS \"{}\" and tiploc \"{tiploc}\" (station #{}) are not a \
                                 known pairing in reference-data/crs-tiploc.csv (known TIPLOCs \
                                 for {}: [{known}]) -- tiploc is documentation-only per \
                                 lines/SCHEMA.md, so this is non-blocking; see \
                                 reference-data/line-catalogue-validation.md's \"Known \
                                 limitations\" before assuming this is wrong",
                                station.crs,
                                idx + 1,
                                station.crs,
                            ),
                        });
                    }
                }
            }
        }

        for operator in &line.definition.operators {
            if !reference.known_operator(operator) {
                findings.push(Finding {
                    path: line.path.clone(),
                    line_no: operators_line,
                    severity: Severity::Error,
                    message: format!(
                        "unknown operator code \"{operator}\" -- not found in \
                         reference-data/toc-codes.csv's currently-valid ATOC code list"
                    ),
                });
            }
        }
    }

    findings
}

/// Stretch goal: informational-only coverage gaps -- currently-valid ATOC
/// codes that no `lines/*.toml` file's `operators` references at all.
/// Never affects the exit code (see `main.rs`).
///
/// Station-level coverage gaps (real CRS codes with no line referencing
/// them) are deliberately not computed here: `reference-data/crs-tiploc.csv`
/// carries every location railwaycodes.org.uk has ever issued a CRS for --
/// including long-closed stations and non-passenger pseudo-codes (`X`-
/// prefixed) it does not distinguish from currently-open ones (see that
/// file's provenance doc) -- so a bare set-difference against 4,500+ CRS
/// codes would be overwhelmingly noise (this catalogue intentionally
/// covers ~110 lines, not every minor request stop in Great Britain) with
/// no reliable way from this data alone to scope it down to "currently-open
/// passenger stations" as the task suggests. Rather than ship a report
/// that's mostly noise, this is left as a documented gap: a genuine
/// open/closed status column would need Network Rail's own CORPUS data
/// (the live tier's territory, see `reference.rs`), not this vendored
/// snapshot.
pub fn unused_operator_codes(
    lines: &[LoadedLine],
    reference: &ReferenceData,
) -> Vec<(String, String)> {
    let used: BTreeSet<&str> = lines
        .iter()
        .flat_map(|l| l.definition.operators.iter().map(String::as_str))
        .collect();

    let mut gaps: Vec<(String, String)> = reference
        .toc_codes
        .iter()
        .filter(|(code, _)| !used.contains(code.as_str()))
        .map(|(code, name)| (code.clone(), name.clone()))
        .collect();
    gaps.sort();
    gaps
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn reference_with(crs_tiploc: &[(&str, &[&str])], tocs: &[&str]) -> ReferenceData {
        let mut data = ReferenceData::default();
        for (crs, tiplocs) in crs_tiploc {
            data.crs_to_name.insert(crs.to_string(), crs.to_string());
            data.crs_to_tiploc.insert(
                crs.to_string(),
                tiplocs
                    .iter()
                    .map(|t| t.to_string())
                    .collect::<HashSet<_>>(),
            );
        }
        for toc in tocs {
            data.toc_codes.insert(toc.to_string(), toc.to_string());
        }
        data
    }

    fn write_line_file(dir: &Path, id: &str, body: &str) -> PathBuf {
        let path = dir.join(format!("{id}.toml"));
        std::fs::write(&path, body).unwrap();
        path
    }

    const HEADER: &str = r#"
id = "test-line"
name = "Test Line"
mode = "national-rail"
category = "regional"
operators = ["XC"]
"#;

    #[test]
    fn unknown_crs_is_a_hard_error() {
        let dir = tempfile_dir();
        let body = format!("{HEADER}\n[[stations]]\ncrs = \"ZZZ\"\n");
        let expected_line = body
            .lines()
            .position(|l| l.trim_start().starts_with("crs = \""))
            .map(|i| i + 1);
        write_line_file(&dir, "a", &body);
        let lines = load_all(&dir).unwrap();
        let reference = reference_with(&[], &["XC"]);
        let findings = validate_lines(&lines, &reference);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].severity, Severity::Error);
        assert!(findings[0].message.contains("ZZZ"));
        assert_eq!(findings[0].line_no, expected_line);
    }

    #[test]
    fn known_crs_with_no_tiploc_field_passes_clean() {
        let dir = tempfile_dir();
        write_line_file(
            &dir,
            "a",
            &format!("{HEADER}\n[[stations]]\ncrs = \"EUS\"\n"),
        );
        let lines = load_all(&dir).unwrap();
        let reference = reference_with(&[("EUS", &["EUSTON"])], &["XC"]);
        let findings = validate_lines(&lines, &reference);
        assert!(findings.is_empty());
    }

    #[test]
    fn mismatched_tiploc_is_a_warning_not_an_error() {
        let dir = tempfile_dir();
        write_line_file(
            &dir,
            "a",
            &format!("{HEADER}\n[[stations]]\ncrs = \"EUS\"\ntiploc = \"WRONG\"\n"),
        );
        let lines = load_all(&dir).unwrap();
        let reference = reference_with(&[("EUS", &["EUSTON"])], &["XC"]);
        let findings = validate_lines(&lines, &reference);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].severity, Severity::Warning);
    }

    #[test]
    fn crs_known_but_no_tiploc_data_at_all_never_flags_a_given_tiploc() {
        let dir = tempfile_dir();
        write_line_file(
            &dir,
            "a",
            &format!("{HEADER}\n[[stations]]\ncrs = \"EUS\"\ntiploc = \"ANYTHING\"\n"),
        );
        let lines = load_all(&dir).unwrap();
        let reference = reference_with(&[("EUS", &[])], &["XC"]);
        let findings = validate_lines(&lines, &reference);
        assert!(findings.is_empty());
    }

    #[test]
    fn unknown_operator_is_a_hard_error() {
        let dir = tempfile_dir();
        write_line_file(
            &dir,
            "a",
            "id = \"t\"\nname = \"T\"\nmode = \"national-rail\"\ncategory = \"regional\"\noperators = [\"ZZ\"]\n\n[[stations]]\ncrs = \"EUS\"\n",
        );
        let lines = load_all(&dir).unwrap();
        let reference = reference_with(&[("EUS", &["EUSTON"])], &["XC"]);
        let findings = validate_lines(&lines, &reference);
        assert_eq!(findings.len(), 1);
        assert!(findings[0].message.contains("ZZ"));
    }

    #[test]
    fn unused_operator_codes_reports_codes_no_line_references() {
        let dir = tempfile_dir();
        write_line_file(
            &dir,
            "a",
            &format!("{HEADER}\n[[stations]]\ncrs = \"EUS\"\n"),
        );
        let lines = load_all(&dir).unwrap();
        let reference = reference_with(&[("EUS", &["EUSTON"])], &["XC", "GW"]);
        let gaps = unused_operator_codes(&lines, &reference);
        assert_eq!(gaps, vec![("GW".to_string(), "GW".to_string())]);
    }

    /// A per-test tempdir under the crate's own `target/` -- avoids a new
    /// dev-dependency (`tempfile`) purely for these tests.
    fn tempfile_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "line-catalogue-validator-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}
