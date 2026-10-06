//! Stations a catalogue line's trains run through without the catalogue
//! listing them, from `lines/generated/pass-through.toml`.
//!
//! A line's `[[stations]]` are the stops worth showing, so they skip the
//! stations its trains pass (or call at) between them: the Brighton Main
//! Line lists London Bridge and East Croydon, but its trains run through New
//! Cross Gate, Sydenham and Norwood Junction in between. An incident
//! "between New Cross Gate and Norwood Junction" names no station of the
//! line, so the 2026-10-06 misses study found about 60% of the lines missed
//! while the incident showed elsewhere were lines that pass through a named
//! section without stopping, or whose catalogue does not list the section's
//! ends.
//!
//! `scripts/generate-pass-through.py` derives, from the CIF timetable, every
//! station on the path between each pair of consecutive catalogue stations,
//! and writes the generated file (checked in; regenerated at each timetable
//! change, see lines/SCHEMA.md). [`LineDefinition::from_dir`] attaches it to
//! the catalogue as [`LineDefinition::pass_through`], which ONLY the
//! incident matcher reads ([`LineDefinition::holds_place`]): it is never
//! serialised, never a stop, never sampled and never in a segment.
//!
//! [`LineDefinition::from_dir`]: crate::LineDefinition::from_dir
//! [`LineDefinition::pass_through`]: crate::LineDefinition::pass_through
//! [`LineDefinition::holds_place`]: crate::LineDefinition::holds_place

use std::collections::BTreeMap;
use std::path::Path;

use serde::Deserialize;

/// Where the generated file lives, relative to the catalogue directory.
pub const PASS_THROUGH_FILE: &str = "generated/pass-through.toml";

/// The stations one line's trains run through between two consecutive
/// catalogue stations, `from` and `to`, in order from `from`; or, with
/// `runs` false (a "break"), a pair none of the line's trains runs between
/// directly: a branch boundary in the catalogue's station order, which the
/// matcher never treats as track.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PassThroughLeg {
    pub from: String,
    pub to: String,
    pub via: Vec<String>,
    pub runs: bool,
}

/// The parsed generated file.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PassThrough {
    /// The timetable dates the file was generated from (its header says
    /// when to regenerate it).
    pub source_dates: Vec<String>,
    /// Line id -> its legs, in file order, then its breaks.
    pub lines: BTreeMap<String, Vec<PassThroughLeg>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFile {
    source_dates: Vec<String>,
    #[serde(default)]
    lines: BTreeMap<String, toml::Table>,
    #[serde(default)]
    breaks: BTreeMap<String, Vec<String>>,
}

/// `"FROM-TO"` as two CRS codes.
fn leg_ends(line_id: &str, key: &str) -> anyhow::Result<(String, String)> {
    let Some((from, to)) = key.split_once('-') else {
        anyhow::bail!("line {line_id}: leg {key:?} is not FROM-TO");
    };
    for crs in [from, to] {
        ensure_crs(line_id, key, crs)?;
    }
    Ok((from.to_string(), to.to_string()))
}

fn ensure_crs(line_id: &str, key: &str, crs: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        crs.len() == 3 && crs.chars().all(|c| c.is_ascii_uppercase()),
        "line {line_id}: leg {key}: {crs:?} is not a CRS code"
    );
    Ok(())
}

impl PassThrough {
    /// Parses the generated file's text: `source_dates`, then one
    /// `[lines.<id>]` table per line with one `FROM-TO = ["CRS", ...]` key
    /// per leg, then a `[breaks]` table, `<id> = ["FROM-TO", ...]`. Errors
    /// on anything else, so a hand-edited or truncated file fails loudly
    /// (CI: line-catalogue-validator).
    pub fn parse(text: &str) -> anyhow::Result<Self> {
        let raw: RawFile = toml::from_str(text)?;
        let mut lines = BTreeMap::new();
        for (line_id, table) in raw.lines {
            let mut legs = Vec::with_capacity(table.len());
            for (key, value) in table {
                let (from, to) = leg_ends(&line_id, &key)?;
                let via: Vec<String> = value
                    .as_array()
                    .ok_or_else(|| anyhow::anyhow!("line {line_id}: leg {key} is not a list"))?
                    .iter()
                    .map(|crs| {
                        crs.as_str().map(str::to_string).ok_or_else(|| {
                            anyhow::anyhow!("line {line_id}: leg {key} has a non-string entry")
                        })
                    })
                    .collect::<anyhow::Result<_>>()?;
                for crs in &via {
                    ensure_crs(&line_id, &key, crs)?;
                }
                legs.push(PassThroughLeg {
                    from,
                    to,
                    via,
                    runs: true,
                });
            }
            lines.insert(line_id, legs);
        }
        for (line_id, pairs) in raw.breaks {
            let legs: &mut Vec<PassThroughLeg> = lines.entry(line_id.clone()).or_default();
            for key in pairs {
                let (from, to) = leg_ends(&line_id, &key)?;
                legs.push(PassThroughLeg {
                    from,
                    to,
                    via: Vec::new(),
                    runs: false,
                });
            }
        }
        Ok(Self {
            source_dates: raw.source_dates,
            lines,
        })
    }

    /// Reads `<lines_dir>/generated/pass-through.toml`; `None` when there
    /// is no such file (a test catalogue, or a deployment from before the
    /// file existed: the matcher then works from catalogue stations alone,
    /// exactly as before).
    pub fn load(lines_dir: &Path) -> anyhow::Result<Option<Self>> {
        let path = lines_dir.join(PASS_THROUGH_FILE);
        match std::fs::read_to_string(&path) {
            Ok(text) => Self::parse(&text)
                .map(Some)
                .map_err(|e| e.context(format!("parsing {}", path.display()))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(anyhow::Error::from(e).context(format!("reading {}", path.display()))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_generated_format() {
        let file = PassThrough::parse(
            "# GENERATED\nsource_dates = [\"2026-10-07\"]\n\n\
             [lines.southern-brighton-main-line]\nLBG-ECR = [\"NXG\", \"SYD\", \"NWD\"]\n\
             TBD-HHE = [\"BAB\"]\n",
        )
        .expect("parses");
        assert_eq!(file.source_dates, ["2026-10-07"]);
        let legs = &file.lines["southern-brighton-main-line"];
        assert_eq!(legs.len(), 2);
        assert_eq!(legs[0].from, "LBG");
        assert_eq!(legs[0].to, "ECR");
        assert_eq!(legs[0].via, ["NXG", "SYD", "NWD"]);
        assert!(legs.iter().all(|leg| leg.runs));
    }

    #[test]
    fn parses_breaks() {
        let file = PassThrough::parse(
            "source_dates = [\"2026-10-07\"]\n[lines.bml]\nLBG-ECR = [\"NXG\"]\n\
             [breaks]\nbml = [\"CLJ-LBG\"]\nother = [\"AAA-BBB\"]\n",
        )
        .expect("parses");
        let bml = &file.lines["bml"];
        assert_eq!(bml.len(), 2);
        assert!(bml[0].runs);
        assert!(!bml[1].runs);
        assert_eq!((bml[1].from.as_str(), bml[1].to.as_str()), ("CLJ", "LBG"));
        assert!(bml[1].via.is_empty());
        assert_eq!(file.lines["other"].len(), 1);
        assert!(PassThrough::parse("source_dates = []\n[breaks]\nbml = [\"CLJLBG\"]\n").is_err());
        assert!(PassThrough::parse("source_dates = []\n[breaks]\nbml = \"CLJ-LBG\"\n").is_err());
    }

    #[test]
    fn rejects_malformed_files() {
        for text in [
            "",
            "source_dates = []\nsurprise = 1\n",
            "source_dates = []\n[lines.x]\nLBGECR = [\"NXG\"]\n",
            "source_dates = []\n[lines.x]\nLBG-ECR = \"NXG\"\n",
            "source_dates = []\n[lines.x]\nLBG-ECR = [1]\n",
            "source_dates = []\n[lines.x]\nLBG-ECR = [\"nxg\"]\n",
            "source_dates = []\n[lines.x]\nLBG-ECRX = [\"NXG\"]\n",
        ] {
            assert!(PassThrough::parse(text).is_err(), "{text:?}");
        }
    }

    #[test]
    fn a_missing_file_is_none() {
        let dir = std::env::temp_dir().join(format!("pass-through-none-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        assert_eq!(PassThrough::load(&dir).expect("loads"), None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_checked_in_file_parses() {
        let dir = crate::manifest_dir!().join("../../lines");
        let file = PassThrough::load(&dir)
            .expect("lines/generated/pass-through.toml should parse")
            .expect("lines/generated/pass-through.toml should exist");
        assert!(!file.source_dates.is_empty());
        assert!(!file.lines.is_empty());
    }
}
