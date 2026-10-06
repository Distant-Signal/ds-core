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
pub(crate) struct LoadedLine {
    pub path: PathBuf,
    pub definition: LineDefinition,
    pub raw: String,
}

pub(crate) fn load_all(lines_dir: &Path) -> anyhow::Result<Vec<LoadedLine>> {
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
pub(crate) enum Severity {
    /// Fails the build.
    Error,
    /// Printed, does not affect the exit code -- see
    /// `reference-data/line-catalogue-validation.md`'s "Known
    /// limitations" section for why a CRS/TIPLOC mismatch specifically is
    /// a warning, not an error.
    Warning,
}

#[derive(Debug)]
pub(crate) struct Finding {
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

pub(crate) fn validate_lines(lines: &[LoadedLine], reference: &ReferenceData) -> Vec<Finding> {
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

        // poller-ldbws asks LDBWS for exactly these CRS codes, so one that
        // isn't among this line's own stations is either a typo (LDBWS
        // answers "Invalid crs code supplied" every poll -- "ANV" for
        // Andover's "ADV" did this in prod) or samples another line's
        // service.
        let sample_line = line
            .raw
            .lines()
            .position(|l| l.trim_start().starts_with("sample_stations ="))
            .map(|i| i + 1);
        for sample in &line.definition.sample_stations {
            if !line.definition.stations.iter().any(|s| &s.crs == sample) {
                findings.push(Finding {
                    path: line.path.clone(),
                    line_no: sample_line,
                    severity: Severity::Error,
                    message: format!(
                        "sample station \"{sample}\" is not one of this line's own stations -- \
                         poller-ldbws polls this CRS for the line's status, so it must be listed \
                         under [[stations]]"
                    ),
                });
            }
        }

        // The aggregator's `belongs_to_line` compares each LDBWS departure's
        // *destination* against this list, so it names where services
        // terminate -- often beyond the line's own catalogued stations
        // (Elizabeth line trains to Shenfield, TPE Hull trains to
        // Manchester, WCML Scotland trains to Euston). So it is NOT required
        // to be a subset of [[stations]]; it only has to be a real CRS, since
        // a typo'd code silently matches no departure and drops the line's
        // LDBWS sample to nothing.
        let filter_line = line
            .raw
            .lines()
            .position(|l| l.trim_start().starts_with("destination_crs_filter ="))
            .map(|i| i + 1);
        for destination in &line.definition.destination_crs_filter {
            if !reference.known_crs(destination) {
                findings.push(Finding {
                    path: line.path.clone(),
                    line_no: filter_line,
                    severity: Severity::Error,
                    message: format!(
                        "unknown CRS code \"{destination}\" in destination_crs_filter -- not \
                         found in reference-data/crs-tiploc.csv; a typo here matches no LDBWS \
                         departure, so the line's sampled status silently loses every service"
                    ),
                });
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

/// Checks the generated `lines/generated/pass-through.toml`
/// (`scripts/generate-pass-through.py`, `common::pass_through`) against the
/// catalogue and the reference data, without touching a database.
///
/// Errors: the file is missing or does not parse, names a line the
/// catalogue does not have, or carries a CRS code
/// `reference-data/crs-tiploc.csv` does not know. Warnings (stale after a
/// catalogue edit; the loader skips them, and regenerating fixes them): a
/// leg whose ends are not consecutive stations of its line, or a passed
/// station that is one of the line's own stations.
pub(crate) fn validate_pass_through(
    lines_dir: &Path,
    lines: &[LoadedLine],
    reference: &ReferenceData,
) -> Vec<Finding> {
    let path = lines_dir.join(common::pass_through::PASS_THROUGH_FILE);
    let error = |line_no: Option<usize>, message: String| Finding {
        path: path.clone(),
        line_no,
        severity: Severity::Error,
        message,
    };
    let raw = match std::fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(e) => {
            return vec![error(
                None,
                format!(
                    "cannot read the generated pass-through file ({e}); generate it with \
                     scripts/generate-pass-through.py (lines/SCHEMA.md)"
                ),
            )];
        }
    };
    let file = match common::pass_through::PassThrough::parse(&raw) {
        Ok(file) => file,
        Err(e) => return vec![error(None, format!("does not parse: {e:#}"))],
    };
    let line_no = |needle: &str| {
        raw.lines()
            .position(|l| l.trim_start().starts_with(needle))
            .map(|i| i + 1)
    };
    let mut findings = Vec::new();
    for (line_id, legs) in &file.lines {
        let header = format!("[lines.{line_id}]");
        let Some(line) = lines.iter().find(|l| l.definition.id == *line_id) else {
            findings.push(error(
                line_no(&header),
                format!("names line {line_id:?}, which no lines/*.toml defines"),
            ));
            continue;
        };
        let stations: Vec<&str> = line
            .definition
            .stations
            .iter()
            .map(|s| s.crs.as_str())
            .collect();
        for leg in legs {
            let key = format!("{}-{}", leg.from, leg.to);
            let at = line_no(&format!("{key} ="));
            for crs in [&leg.from, &leg.to].into_iter().chain(&leg.via) {
                if !reference.known_crs(crs) {
                    findings.push(error(
                        at,
                        format!(
                            "{line_id} {key}: unknown CRS code {crs:?} (not in \
                             reference-data/crs-tiploc.csv)"
                        ),
                    ));
                }
            }
            let consecutive = stations
                .windows(2)
                .any(|pair| pair[0] == leg.from && pair[1] == leg.to);
            if !consecutive {
                findings.push(Finding {
                    path: path.clone(),
                    line_no: at,
                    severity: Severity::Warning,
                    message: format!(
                        "{line_id} {key}: not two consecutive stations of the line any more \
                         -- stale; regenerate with scripts/generate-pass-through.py"
                    ),
                });
            }
            for crs in leg
                .via
                .iter()
                .filter(|crs| stations.contains(&crs.as_str()))
            {
                findings.push(Finding {
                    path: path.clone(),
                    line_no: at,
                    severity: Severity::Warning,
                    message: format!(
                        "{line_id} {key}: {crs} is one of the line's own stations -- stale; \
                         regenerate with scripts/generate-pass-through.py"
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
/// carries every CRS in Network Rail's CORPUS -- including freight,
/// engineering and Underground/bus pseudo-codes (`X`/`Z`/`Q`-prefixed) and
/// locations with no passenger service, which it does not distinguish from
/// currently-open stations (see that file's provenance doc) -- so a bare
/// set-difference against 4,100+ CRS codes would be overwhelmingly noise
/// (this catalogue intentionally covers ~240 line files, not every minor
/// request stop in Great Britain) with no reliable way from this data alone
/// to scope it down to "currently-open passenger stations" as the task
/// suggests. Rather than ship a report that's mostly noise, this is left as
/// a documented gap: CORPUS has no open/closed or passenger-service column
/// either, so it would need another source (e.g. the Knowledgebase
/// Stations feed).
pub(crate) fn unused_operator_codes(
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
            data.crs_to_name
                .insert((*crs).to_string(), (*crs).to_string());
            data.crs_to_tiploc.insert(
                (*crs).to_string(),
                tiplocs
                    .iter()
                    .map(ToString::to_string)
                    .collect::<HashSet<_>>(),
            );
        }
        for toc in tocs {
            data.toc_codes
                .insert((*toc).to_string(), (*toc).to_string());
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
    fn sample_station_not_on_the_line_is_a_hard_error() {
        let dir = tempfile_dir();
        // `sample_stations` is a top-level key, so it goes before [[stations]].
        let body = format!(
            "{HEADER}sample_stations = [\"EUS\", \"ANV\"]\n\n[[stations]]\ncrs = \"EUS\"\n"
        );
        let expected_line = body
            .lines()
            .position(|l| l.starts_with("sample_stations ="))
            .map(|i| i + 1);
        write_line_file(&dir, "a", &body);
        let lines = load_all(&dir).unwrap();
        let reference = reference_with(&[("EUS", &["EUSTON"])], &["XC"]);
        let findings = validate_lines(&lines, &reference);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].severity, Severity::Error);
        assert!(findings[0].message.contains("\"ANV\""));
        assert_eq!(findings[0].line_no, expected_line);
    }

    #[test]
    fn unknown_destination_filter_crs_is_a_hard_error() {
        let dir = tempfile_dir();
        let body = format!(
            "{HEADER}destination_crs_filter = [\"EUS\", \"ZZZ\"]\n\n[[stations]]\ncrs = \"EUS\"\n"
        );
        let expected_line = body
            .lines()
            .position(|l| l.starts_with("destination_crs_filter ="))
            .map(|i| i + 1);
        write_line_file(&dir, "a", &body);
        let lines = load_all(&dir).unwrap();
        let reference = reference_with(&[("EUS", &["EUSTON"])], &["XC"]);
        let findings = validate_lines(&lines, &reference);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].severity, Severity::Error);
        assert!(findings[0].message.contains("\"ZZZ\""));
        assert_eq!(findings[0].line_no, expected_line);
    }

    /// A destination beyond the line's own stations is normal (through
    /// services terminating elsewhere) -- only an unknown code is flagged.
    #[test]
    fn destination_filter_crs_off_the_line_but_known_passes_clean() {
        let dir = tempfile_dir();
        write_line_file(
            &dir,
            "a",
            &format!("{HEADER}destination_crs_filter = [\"MAN\"]\n\n[[stations]]\ncrs = \"EUS\"\n"),
        );
        let lines = load_all(&dir).unwrap();
        let reference = reference_with(&[("EUS", &["EUSTON"]), ("MAN", &["MNCRPIC"])], &["XC"]);
        let findings = validate_lines(&lines, &reference);
        assert!(findings.is_empty(), "{findings:?}");
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

    fn write_pass_through(dir: &Path, body: &str) {
        std::fs::create_dir_all(dir.join("generated")).unwrap();
        std::fs::write(dir.join(common::pass_through::PASS_THROUGH_FILE), body).unwrap();
    }

    fn pass_through_findings(body: Option<&str>) -> Vec<Finding> {
        let dir = tempfile_dir();
        let line = format!(
            "{HEADER}\n[[stations]]\ncrs = \"LBG\"\n[[stations]]\ncrs = \"ECR\"\n\
             [[stations]]\ncrs = \"GTW\"\n"
        );
        write_line_file(&dir, "test-line", &line);
        if let Some(body) = body {
            write_pass_through(&dir, body);
        }
        let lines = load_all(&dir).unwrap();
        let reference = reference_with(
            &[
                ("LBG", &[]),
                ("ECR", &[]),
                ("GTW", &[]),
                ("NXG", &[]),
                ("NWD", &[]),
            ],
            &["XC"],
        );
        validate_pass_through(&dir, &lines, &reference)
    }

    const PASS_THROUGH_HEADER: &str = "source_dates = [\"2026-10-07\"]\n";

    #[test]
    fn a_valid_pass_through_file_passes_clean() {
        let body =
            format!("{PASS_THROUGH_HEADER}[lines.test-line]\nLBG-ECR = [\"NXG\", \"NWD\"]\n");
        assert!(pass_through_findings(Some(&body)).is_empty());
    }

    #[test]
    fn a_missing_or_malformed_pass_through_file_is_a_hard_error() {
        for body in [
            None,
            Some("not toml ["),
            Some("[lines.test-line]\nLBG-ECR = [\"NXG\"]\n"),
            Some("source_dates = []\n[lines.test-line]\nLBG = [\"NXG\"]\n"),
        ] {
            let findings = pass_through_findings(body);
            assert_eq!(findings.len(), 1, "{body:?}: {findings:?}");
            assert_eq!(findings[0].severity, Severity::Error);
        }
    }

    #[test]
    fn unknown_lines_and_crs_in_the_pass_through_file_are_hard_errors() {
        let body = format!("{PASS_THROUGH_HEADER}[lines.no-such-line]\nLBG-ECR = [\"NXG\"]\n");
        let findings = pass_through_findings(Some(&body));
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].severity, Severity::Error);
        assert!(findings[0].message.contains("no-such-line"));
        assert_eq!(findings[0].line_no, Some(2));

        let body = format!("{PASS_THROUGH_HEADER}[breaks]\nno-such-line = [\"LBG-ECR\"]\n");
        let findings = pass_through_findings(Some(&body));
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].severity, Severity::Error);

        let body = format!("{PASS_THROUGH_HEADER}[lines.test-line]\nLBG-ECR = [\"ZZZ\"]\n");
        let findings = pass_through_findings(Some(&body));
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].severity, Severity::Error);
        assert!(findings[0].message.contains("ZZZ"));
        assert_eq!(findings[0].line_no, Some(3));
    }

    #[test]
    fn stale_pass_through_legs_are_warnings() {
        // Not consecutive (the catalogue gained or reordered a station),
        // and a passed station that is now one of the line's own stops.
        let body = format!(
            "{PASS_THROUGH_HEADER}[lines.test-line]\nLBG-GTW = [\"NXG\"]\nECR-GTW = [\"LBG\"]\n"
        );
        let findings = pass_through_findings(Some(&body));
        assert_eq!(findings.len(), 2, "{findings:?}");
        assert!(findings.iter().all(|f| f.severity == Severity::Warning));
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
