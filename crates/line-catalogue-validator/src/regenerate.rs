//! Regenerating the fast tier's vendored reference CSVs from Network Rail /
//! National Rail Knowledgebase data instead of the railwaycodes.org.uk
//! scrape (DQ13 / LEG-24).
//!
//! - `crs-tiploc.csv` from Network Rail's **CORPUS** extract
//!   (`CORPUSExtract.json`, the Rail Data Marketplace "NWR CORPUS" product,
//!   Open Government Licence v3.0 -- credited on `/attribution`). Run with
//!   `--regenerate-crs-tiploc-from-corpus <CORPUSExtract.json>`.
//! - `toc-codes.csv` from the **Knowledgebase Train Operating Company List**
//!   (the RDM feed `crates/poller-tocs` ingests into the `tocs` table). Run
//!   with `--regenerate-toc-codes-from-rdm-xml <saved feed response.xml>`.
//!
//! Both are pure functions of their input file, so a regeneration is
//! reproducible from the input alone; see
//! `reference-data/line-catalogue-validation.md` for where to get the
//! inputs and what each rule is for.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

/// One `TIPLOCDATA` row of `CORPUSExtract.json`. CORPUS pads every absent
/// value with a single space rather than omitting it or using `null`, so
/// every field is read leniently and trimmed. `NLC` and `STANOX` are read
/// as raw JSON values because extracts have carried them both as numbers
/// and as strings; see [`code_text`]. `UIC` and
/// `NLCDESC16` are ignored.
#[derive(Debug, Deserialize)]
struct RawCorpusRow {
    #[serde(rename = "NLC", default)]
    nlc: Option<serde_json::Value>,
    #[serde(rename = "STANOX", default)]
    stanox: Option<serde_json::Value>,
    #[serde(rename = "TIPLOC", default)]
    tiploc: Option<String>,
    #[serde(rename = "3ALPHA", default)]
    three_alpha: Option<String>,
    #[serde(rename = "NLCDESC", default)]
    nlc_desc: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CorpusExtract {
    #[serde(rename = "TIPLOCDATA")]
    tiploc_data: Vec<RawCorpusRow>,
}

pub use common::corpus_inference::{
    AmbiguousGroup, CorpusCrsTiploc, CorpusRow, CrsTiplocRow, Rule,
};

/// A JSON code (`NLC`/`STANOX`) as text: extracts have carried them both as
/// numbers and as strings. Anything else (a fraction, a negative number, an
/// array...) is not a usable code.
fn code_text(v: Option<&serde_json::Value>) -> Option<String> {
    match v? {
        serde_json::Value::Number(n) => n.as_u64().map(|n| n.to_string()),
        serde_json::Value::String(s) => Some(s.clone()),
        _ => None,
    }
}

/// Turns a CORPUS extract into `crs-tiploc.csv` rows by the shared
/// conservative inference ([`common::corpus_inference::infer_crs_tiploc`],
/// also used by `api`; its doc has the rules). Refuses an extract that
/// yields no CRS at all.
pub fn crs_tiploc_from_corpus(json: &[u8]) -> Result<CorpusCrsTiploc> {
    let extract: CorpusExtract =
        serde_json::from_slice(json).context("parsing CORPUS extract JSON")?;
    let rows: Vec<CorpusRow> = extract
        .tiploc_data
        .into_iter()
        .map(|row| CorpusRow {
            nlc: code_text(row.nlc.as_ref()),
            stanox: code_text(row.stanox.as_ref()),
            tiploc: row.tiploc,
            crs: row.three_alpha,
            nlc_desc: row.nlc_desc,
        })
        .collect();
    crs_tiploc_from_rows(&rows)
}

/// [`crs_tiploc_from_corpus`] for rows already read (e.g. from
/// `corpus_locations`, see `--regenerate-crs-tiploc-from-db`).
pub fn crs_tiploc_from_rows(rows: &[CorpusRow]) -> Result<CorpusCrsTiploc> {
    let result = common::corpus_inference::infer_crs_tiploc(rows);
    if result.rows.is_empty() {
        bail!(
            "CORPUS extract yielded zero CRS codes -- not a plausible real extract (wrong file, \
             or the TIPLOCDATA/3ALPHA field names changed); refusing to write an empty file"
        );
    }
    Ok(result)
}

/// A set of `(crs, tiploc)` pairs.
pub type CrsTiplocPairs = BTreeSet<(String, String)>;

/// Reads the `(crs, tiploc)` pairs (non-empty `tiploc` only) and the set of
/// CRS codes from an existing `crs-tiploc.csv`, normalised the way
/// `ReferenceData::from_vendored_csvs` normalises them.
pub fn read_crs_tiploc_pairs<R: std::io::Read>(
    reader: R,
) -> Result<(CrsTiplocPairs, BTreeSet<String>)> {
    #[derive(Deserialize)]
    struct Row {
        crs: String,
        tiploc: String,
    }
    let mut pairs = BTreeSet::new();
    let mut crs_codes = BTreeSet::new();
    for row in csv::Reader::from_reader(reader).deserialize::<Row>() {
        let row = row.context("parsing crs-tiploc CSV")?;
        let crs = row.crs.trim().to_ascii_uppercase();
        let tiploc = row.tiploc.trim().to_ascii_uppercase();
        crs_codes.insert(crs.clone());
        if !tiploc.is_empty() {
            pairs.insert((crs, tiploc));
        }
    }
    Ok((pairs, crs_codes))
}

/// Per-rule agreement between a new output and an existing file.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RuleAgreement {
    pub matched: usize,
    pub only_new: usize,
    /// TIPLOCs (not pairs) whose CRS set differs from the old file's.
    pub conflicts: usize,
}

/// Agreement between a freshly generated `crs-tiploc.csv` and an existing
/// one (normally the committed railwaycodes-derived snapshot).
#[derive(Debug, Default, Clone)]
pub struct Comparison {
    pub matched: BTreeSet<(String, String)>,
    /// Pairs the old file has and the new output lacks.
    pub only_old: BTreeSet<(String, String)>,
    /// Pairs the new output has and the old file lacks.
    pub only_new: BTreeSet<(String, String)>,
    /// TIPLOCs present in both files whose CRS sets differ:
    /// tiploc -> (old CRS set, new CRS set). Their pairs also appear in
    /// `only_old`/`only_new`.
    pub conflicts: BTreeMap<String, (BTreeSet<String>, BTreeSet<String>)>,
    pub by_rule: BTreeMap<Rule, RuleAgreement>,
    pub crs_only_old: BTreeSet<String>,
    pub crs_only_new: BTreeSet<String>,
}

fn crs_by_tiploc(pairs: &BTreeSet<(String, String)>) -> BTreeMap<&str, BTreeSet<String>> {
    let mut m: BTreeMap<&str, BTreeSet<String>> = BTreeMap::new();
    for (crs, tiploc) in pairs {
        m.entry(tiploc.as_str()).or_default().insert(crs.clone());
    }
    m
}

pub fn compare(
    new: &CorpusCrsTiploc,
    old_pairs: &BTreeSet<(String, String)>,
    old_crs: &BTreeSet<String>,
) -> Comparison {
    let new_pairs: BTreeSet<(String, String)> = new.report.pair_rules.keys().cloned().collect();
    let new_crs: BTreeSet<String> = new.rows.iter().map(|r| r.0.clone()).collect();
    let mut c = Comparison {
        matched: new_pairs.intersection(old_pairs).cloned().collect(),
        only_old: old_pairs.difference(&new_pairs).cloned().collect(),
        only_new: new_pairs.difference(old_pairs).cloned().collect(),
        crs_only_old: old_crs.difference(&new_crs).cloned().collect(),
        crs_only_new: new_crs.difference(old_crs).cloned().collect(),
        ..Comparison::default()
    };
    let old_by_tiploc = crs_by_tiploc(old_pairs);
    let new_by_tiploc = crs_by_tiploc(&new_pairs);
    for (tiploc, new_set) in &new_by_tiploc {
        if let Some(old_set) = old_by_tiploc.get(tiploc)
            && old_set != new_set
        {
            c.conflicts
                .insert((*tiploc).to_owned(), (old_set.clone(), new_set.clone()));
        }
    }
    for rule in Rule::ALL {
        c.by_rule.insert(rule, RuleAgreement::default());
    }
    for (pair, rule) in &new.report.pair_rules {
        let agreement = c.by_rule.entry(*rule).or_default();
        if old_pairs.contains(pair) {
            agreement.matched += 1;
        } else {
            agreement.only_new += 1;
        }
    }
    for tiploc in c.conflicts.keys() {
        // Every pair of one TIPLOC comes from the same rule.
        if let Some(rule) = new
            .report
            .pair_rules
            .iter()
            .find(|((_, t), _)| t == tiploc)
            .map(|(_, r)| *r)
        {
            c.by_rule.entry(rule).or_default().conflicts += 1;
        }
    }
    c
}

fn join(set: &BTreeSet<String>) -> String {
    set.iter().cloned().collect::<Vec<_>>().join(",")
}

fn write_groups(out: &mut String, title: &str, groups: &BTreeMap<String, AmbiguousGroup>) {
    use std::fmt::Write as _;
    let _ = writeln!(out, "\n{title}: {} group(s)", groups.len());
    for (key, g) in groups {
        let _ = writeln!(
            out,
            "  {key}: candidates {} -- TIPLOC(s) {}",
            join(&g.candidates),
            join(&g.tiplocs)
        );
    }
}

/// How much of the report [`render_report`] renders.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ReportDetail {
    /// Counts only.
    Summary,
    /// Counts, ambiguous groups and conflicts.
    Standard,
    /// Everything, including every pair only in one of the two files.
    Full,
}

/// Renders the regeneration report: counts per rule, ambiguous groups,
/// and (when given) the comparison against an existing file.
pub fn render_report(
    result: &CorpusCrsTiploc,
    comparison: Option<(&Path, &Comparison)>,
    detail: ReportDetail,
) -> String {
    use std::fmt::Write as _;
    let r = &result.report;
    let mut out = String::new();
    let _ = writeln!(out, "=== crs-tiploc.csv from CORPUS: inference report ===");
    let _ = writeln!(out, "output rows: {}", result.rows.len());
    for rule in Rule::ALL {
        let _ = writeln!(out, "pairs by {}: {}", rule.label(), r.pairs_by_rule(rule));
    }
    let ambiguous_tiplocs: usize = r.ambiguous.values().map(|g| g.tiplocs.len()).sum();
    let _ = writeln!(
        out,
        "CRS-less TIPLOCs left out as ambiguous (name matched several stations): {ambiguous_tiplocs}"
    );
    let _ = writeln!(
        out,
        "CRS-less TIPLOCs left out by name (at a station's STANOX, but a signal/junction/\
         sidings/... or another name): {}",
        r.left_out_by_name.len()
    );
    let _ = writeln!(
        out,
        "CRS-less TIPLOCs left out with no station at their STANOX: {}",
        r.left_out_no_candidate
    );
    if detail >= ReportDetail::Standard {
        let _ = writeln!(out, "\ninferred pairs:");
        for ((crs, tiploc), rule) in &r.pair_rules {
            if *rule != Rule::Direct {
                let desc = r.inferred_descs.get(tiploc).map_or("", String::as_str);
                let _ = writeln!(out, "  {crs},{tiploc} ({desc}) [{}]", rule.label());
            }
        }
        write_groups(&mut out, "ambiguous STANOX groups", &r.ambiguous);
    }
    if detail == ReportDetail::Full {
        let _ = writeln!(out, "\nleft out by name:");
        for (tiploc, desc) in &r.left_out_by_name {
            let _ = writeln!(out, "  {tiploc} ({desc})");
        }
    }

    if let Some((path, c)) = comparison {
        let _ = writeln!(out, "\n=== comparison with {} ===", path.display());
        let _ = writeln!(out, "pairs matched: {}", c.matched.len());
        let _ = writeln!(
            out,
            "pairs only in old file (still missing): {}",
            c.only_old.len()
        );
        let _ = writeln!(out, "pairs only in new output: {}", c.only_new.len());
        let _ = writeln!(
            out,
            "conflicts (TIPLOC in both, different CRS): {}",
            c.conflicts.len()
        );
        let _ = writeln!(
            out,
            "CRS codes only in old file: {}; only in new output: {}",
            c.crs_only_old.len(),
            c.crs_only_new.len()
        );
        let _ = writeln!(out, "by rule (matched / only-new / conflicting TIPLOCs):");
        for (rule, a) in &c.by_rule {
            let _ = writeln!(
                out,
                "  {}: {} / {} / {}",
                rule.label(),
                a.matched,
                a.only_new,
                a.conflicts
            );
        }
        if detail == ReportDetail::Summary {
            return out;
        }
        let _ = writeln!(out, "\nconflicts:");
        for (tiploc, (old, new)) in &c.conflicts {
            let rule = r
                .pair_rules
                .iter()
                .find(|((_, t), _)| t == tiploc)
                .map_or("?", |(_, rule)| rule.label());
            let _ = writeln!(
                out,
                "  {tiploc}: old {} -> new {} [{rule}]",
                join(old),
                join(new)
            );
        }
        if detail == ReportDetail::Full {
            let _ = writeln!(out, "\npairs only in old file:");
            for (crs, tiploc) in &c.only_old {
                let _ = writeln!(out, "  {crs},{tiploc}");
            }
            let _ = writeln!(out, "\npairs only in new output:");
            for pair in &c.only_new {
                let rule = r.pair_rules.get(pair).map_or("?", |rule| rule.label());
                let _ = writeln!(out, "  {},{} [{rule}]", pair.0, pair.1);
            }
            let _ = writeln!(
                out,
                "\nCRS codes only in old file: {}",
                join(&c.crs_only_old)
            );
            let _ = writeln!(
                out,
                "CRS codes only in new output: {}",
                join(&c.crs_only_new)
            );
        }
    }
    out
}

/// Turns a saved Knowledgebase Train Operating Company List XML response
/// into `toc-codes.csv` rows `(atoc_code, name)`, sorted by code. Every
/// operator in the feed is kept: it is the same list production's `tocs`
/// table holds, so "valid" here means "an operator code this app knows".
pub fn toc_codes_from_rdm_xml(xml: &str) -> Result<Vec<(String, String)>> {
    let tocs = crate::rdm_toc::parse_rdm_tocs(xml)?;
    let sorted: BTreeMap<String, String> = tocs.into_iter().collect();
    if sorted.is_empty() {
        bail!("TOC list XML yielded zero operators; refusing to write an empty file");
    }
    Ok(sorted.into_iter().collect())
}

pub fn write_crs_tiploc_csv(path: &Path, rows: &[CrsTiplocRow]) -> Result<()> {
    let mut w =
        csv::Writer::from_path(path).with_context(|| format!("writing {}", path.display()))?;
    w.write_record(["crs", "tiploc", "name"])?;
    for (crs, tiploc, name) in rows {
        w.write_record([crs, tiploc, name])?;
    }
    w.flush()?;
    Ok(())
}

pub fn write_toc_codes_csv(path: &Path, rows: &[(String, String)]) -> Result<()> {
    let mut w =
        csv::Writer::from_path(path).with_context(|| format!("writing {}", path.display()))?;
    w.write_record(["atoc_code", "name"])?;
    for (code, name) in rows {
        w.write_record([code, name])?;
    }
    w.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn corpus_rows_become_sorted_crs_tiploc_pairs() {
        // Real CORPUS shape: absent values are a single space.
        let json = br#"{"TIPLOCDATA":[
            {"NLC":123,"STANOX":"87219","TIPLOC":"CLPHMJC","3ALPHA":"CLJ","UIC":" ","NLCDESC":"CLAPHAM JUNCTION","NLCDESC16":" "},
            {"NLC":124,"STANOX":"87219","TIPLOC":"CLPHMJ1","3ALPHA":"CLJ","UIC":" ","NLCDESC":"CLAPHAM JN PLATFORM 1","NLCDESC16":" "},
            {"NLC":125,"STANOX":" ","TIPLOC":"FOOJN","3ALPHA":" ","UIC":" ","NLCDESC":"FOO JUNCTION","NLCDESC16":" "},
            {"NLC":126,"STANOX":" ","TIPLOC":" ","3ALPHA":"ABC","UIC":" ","NLCDESC":"NO TIPLOC STATION","NLCDESC16":" "},
            {"NLC":127,"STANOX":" ","TIPLOC":"ABWD","3ALPHA":"ABW","UIC":" ","NLCDESC":"ABBEY WOOD","NLCDESC16":" "},
            {"NLC":128,"STANOX":" ","TIPLOC":"ABWD","3ALPHA":"ABW","UIC":" ","NLCDESC":"ABBEY WOOD DUP","NLCDESC16":" "},
            {"NLC":129,"STANOX":" ","TIPLOC":"X","3ALPHA":"ab1","UIC":" ","NLCDESC":"JUNK","NLCDESC16":" "}
        ]}"#;
        let rows = crs_tiploc_from_corpus(json).unwrap().rows;
        let s = |a: &str, b: &str, c: &str| (a.to_owned(), b.to_owned(), c.to_owned());
        assert_eq!(
            rows,
            vec![
                s("ABC", "", "NO TIPLOC STATION"),
                s("ABW", "ABWD", "ABBEY WOOD"),
                s("CLJ", "CLPHMJ1", "CLAPHAM JN PLATFORM 1"),
                s("CLJ", "CLPHMJC", "CLAPHAM JN PLATFORM 1"),
            ]
        );
    }

    #[test]
    fn a_crs_with_a_tiploc_elsewhere_gets_no_bare_row() {
        let json = br#"{"TIPLOCDATA":[
            {"TIPLOC":" ","3ALPHA":"ABC","NLCDESC":"BARE"},
            {"TIPLOC":"ABCD","3ALPHA":"ABC","NLCDESC":"WITH TIPLOC"}
        ]}"#;
        let rows = crs_tiploc_from_corpus(json).unwrap().rows;
        assert_eq!(
            rows,
            vec![("ABC".into(), "ABCD".into(), "WITH TIPLOC".into())]
        );
    }

    #[test]
    fn an_extract_with_no_crs_is_refused() {
        let json = br#"{"TIPLOCDATA":[{"TIPLOC":"FOOJN","3ALPHA":" ","NLCDESC":"FOO"}]}"#;
        assert!(crs_tiploc_from_corpus(json).is_err());
    }

    fn pair(crs: &str, tiploc: &str) -> (String, String) {
        (crs.to_owned(), tiploc.to_owned())
    }

    fn set(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| (*s).to_owned()).collect()
    }

    fn s(a: &str, b: &str, c: &str) -> CrsTiplocRow {
        (a.to_owned(), b.to_owned(), c.to_owned())
    }

    /// Real rows from a CORPUS extract (blank values are one space):
    /// Clapham Junction's platform groups share its STANOX, and so do a
    /// pseudo-CRS carriage siding (`XCP`), a loop and a signal; signals and
    /// sidings sharing only its NLC prefix have their own STANOX.
    #[test]
    fn station_parts_inherit_crs_by_stanox_and_name() {
        let json = br#"{"TIPLOCDATA":[
            {"NLC":559500,"STANOX":"87219","TIPLOC":"CLPHMJN","3ALPHA":"CLJ","UIC":"55950","NLCDESC":"CLAPHAM JUNCTION LONDON","NLCDESC16":" "},
            {"NLC":559513,"STANOX":"87219","TIPLOC":"CLPHJCS","3ALPHA":"XCP","UIC":" ","NLCDESC":"BR CARRIAGE SIDINGS","NLCDESC16":" "},
            {"NLC":559518,"STANOX":"87219","TIPLOC":"CLPHMJ1","3ALPHA":" ","UIC":" ","NLCDESC":"CLAPHAM JUNCTION PLATS 0-2","NLCDESC16":" "},
            {"NLC":559569,"STANOX":87219,"TIPLOC":"CLPHMJC","3ALPHA":" ","UIC":" ","NLCDESC":"CLAPHAM JUNCTION (C)","NLCDESC16":" "},
            {"NLC":559572,"STANOX":"87219","TIPLOC":"CLPHMJW","3ALPHA":" ","UIC":" ","NLCDESC":"CLAPHAM JN (WINDSOR)","NLCDESC16":" "},
            {"NLC":559595,"STANOX":"87219","TIPLOC":"CLPHJLP","3ALPHA":" ","UIC":" ","NLCDESC":"CLAPHAM JUNCTION LOOP","NLCDESC16":" "},
            {"NLC":559526,"STANOX":"87309","TIPLOC":"CLPH149","3ALPHA":" ","UIC":" ","NLCDESC":"CLAPHAM JUNCTION SIGNAL W149","NLCDESC16":" "},
            {"NLC":559532,"STANOX":"87227","TIPLOC":"CLPHMMS","3ALPHA":" ","UIC":" ","NLCDESC":"CLAPHAM JN MIDDLE SDG","NLCDESC16":" "},
            {"NLC":557800,"STANOX":"87261","TIPLOC":"WIMBLDN","3ALPHA":"WIM","UIC":" ","NLCDESC":"WIMBLEDON","NLCDESC16":" "},
            {"NLC":557831,"STANOX":"87261","TIPLOC":"WIMB827","3ALPHA":" ","UIC":" ","NLCDESC":"WIMBLEDON SIGNAL VC827","NLCDESC16":" "},
            {"NLC":557802,"STANOX":"87261","TIPLOC":"WDON","3ALPHA":" ","UIC":" ","NLCDESC":"SOUTH WEST","NLCDESC16":" "},
            {"NLC":557801,"STANOX":"87261","TIPLOC":"WDONSS","3ALPHA":" ","UIC":" ","NLCDESC":"SOUTH SIDINGS","NLCDESC16":" "},
            {"NLC":999902,"STANOX":"87261","TIPLOC":"FAKECEN","3ALPHA":" ","UIC":" ","NLCDESC":"CENTRAL","NLCDESC16":" "},
            {"NLC":542600,"STANOX":"87201","TIPLOC":"VICTRIA","3ALPHA":"VIC","UIC":" ","NLCDESC":"VICTORIA LONDON","NLCDESC16":" "},
            {"NLC":542604,"STANOX":"87201","TIPLOC":"VICTRIE","3ALPHA":" ","UIC":" ","NLCDESC":"LONDON VICTORIA (E)","NLCDESC16":" "},
            {"NLC":696900,"STANOX":"52226","TIPLOC":"STFD","3ALPHA":"SRA","UIC":" ","NLCDESC":"STRATFORD","NLCDESC16":" "},
            {"NLC":696901,"STANOX":"52226","TIPLOC":"STFDCJ","3ALPHA":" ","UIC":" ","NLCDESC":"STRATFORD CENTRAL JUNCTION","NLCDESC16":" "},
            {"NLC":154000,"STANOX":"63631","TIPLOC":"STPANCI","3ALPHA":"SPX","UIC":" ","NLCDESC":"ST PANCRAS","NLCDESC16":" "},
            {"NLC":959200,"STANOX":"63631","TIPLOC":"STPADOM","3ALPHA":" ","UIC":" ","NLCDESC":"ST PANCRAS INTL (DOMESTIC)","NLCDESC16":" "},
            {"NLC":777700,"STANOX":"00000","TIPLOC":"ZEROA","3ALPHA":"ZZA","UIC":" ","NLCDESC":"ZERO","NLCDESC16":" "},
            {"NLC":888800,"STANOX":0,"TIPLOC":"ZEROB","3ALPHA":" ","UIC":" ","NLCDESC":"ZERO B","NLCDESC16":" "},
            {"NLC":" ","STANOX":" ","TIPLOC":"BLANKX","3ALPHA":" ","UIC":" ","NLCDESC":"BLANK","NLCDESC16":" "}
        ]}"#;
        let result = crs_tiploc_from_corpus(json).unwrap();
        assert_eq!(
            result.rows,
            vec![
                s("CLJ", "CLPHMJ1", "CLAPHAM JUNCTION LONDON"),
                s("CLJ", "CLPHMJC", "CLAPHAM JUNCTION LONDON"),
                s("CLJ", "CLPHMJN", "CLAPHAM JUNCTION LONDON"),
                s("CLJ", "CLPHMJW", "CLAPHAM JUNCTION LONDON"),
                // STPADOM sorts after STPANCI but is inferred, so the name
                // stays the primary TIPLOC's.
                s("SPX", "STPADOM", "ST PANCRAS"),
                s("SPX", "STPANCI", "ST PANCRAS"),
                s("SRA", "STFD", "STRATFORD"),
                s("VIC", "VICTRIA", "VICTORIA LONDON"),
                s("VIC", "VICTRIE", "VICTORIA LONDON"),
                s("WIM", "WDON", "WIMBLEDON"),
                s("WIM", "WIMBLDN", "WIMBLEDON"),
                s("XCP", "CLPHJCS", "BR CARRIAGE SIDINGS"),
                s("ZZA", "ZEROA", "ZERO"),
            ]
        );
        let r = &result.report;
        assert_eq!(r.pair_rules[&pair("CLJ", "CLPHMJN")], Rule::Direct);
        for t in ["CLPHMJ1", "CLPHMJC", "CLPHMJW"] {
            assert_eq!(r.pair_rules[&pair("CLJ", t)], Rule::StationName, "{t}");
        }
        assert_eq!(r.pair_rules[&pair("SPX", "STPADOM")], Rule::StationName);
        assert_eq!(r.pair_rules[&pair("VIC", "VICTRIE")], Rule::StationName);
        // Platform words only, same STANOX and NLC location.
        assert_eq!(r.pair_rules[&pair("WIM", "WDON")], Rule::StationQualifier);
        assert_eq!(r.pairs_by_rule(Rule::Direct), 7);
        assert_eq!(r.pairs_by_rule(Rule::StationName), 5);
        assert_eq!(r.pairs_by_rule(Rule::StationQualifier), 1);
        assert_eq!(r.inferred_descs["CLPHMJW"], "CLAPHAM JN (WINDSOR)");
        assert_eq!(r.inferred_descs["WDON"], "SOUTH WEST");
        // At a station's STANOX, but a loop, a signal, a junction, sidings,
        // and platform words from another NLC location.
        assert_eq!(
            r.left_out_by_name.keys().cloned().collect::<BTreeSet<_>>(),
            set(&["CLPHJLP", "FAKECEN", "STFDCJ", "WDONSS", "WIMB827"])
        );
        // CLPH149, CLPHMMS (own STANOX); ZEROB (zero STANOX is not a
        // group); BLANKX.
        assert_eq!(r.left_out_no_candidate, 4);
        assert!(r.ambiguous.is_empty());
    }

    #[test]
    fn ambiguous_names_assign_nothing_and_are_reported() {
        // Two stations at one STANOX where one's name is a prefix of the
        // other's: a TIPLOC naming the longer one matches both.
        let json = br#"{"TIPLOCDATA":[
            {"STANOX":"40000","TIPLOC":"FARR","3ALPHA":"AAA","NLCDESC":"FARRINGDON"},
            {"STANOX":"40000","TIPLOC":"FARREL","3ALPHA":"BBB","NLCDESC":"FARRINGDON EL"},
            {"STANOX":"40000","TIPLOC":"FARRELP","3ALPHA":" ","NLCDESC":"FARRINGDON EL PLATFORM"},
            {"STANOX":"40000","TIPLOC":"FARRX","3ALPHA":" ","NLCDESC":"FARRINGDON X"}
        ]}"#;
        let result = crs_tiploc_from_corpus(json).unwrap();
        let r = &result.report;
        assert_eq!(r.pair_rules[&pair("AAA", "FARRX")], Rule::StationName);
        assert!(!r.pair_rules.keys().any(|(_, t)| t == "FARRELP"));
        assert_eq!(
            r.ambiguous,
            BTreeMap::from([(
                "40000".to_owned(),
                AmbiguousGroup {
                    candidates: set(&["AAA", "BBB"]),
                    tiplocs: set(&["FARRELP"]),
                }
            )])
        );
        let text = render_report(&result, None, ReportDetail::Standard);
        assert!(text.contains("40000: candidates AAA,BBB -- TIPLOC(s) FARRELP"));
        assert!(text.contains("  AAA,FARRX (FARRINGDON X) [station part by name (same STANOX)]\n"));
        assert!(text.contains("left out as ambiguous (name matched several stations): 1"));
    }

    #[test]
    fn a_tiploc_with_a_direct_crs_on_any_row_is_never_inferred() {
        // The second row for PRIMARY has no 3ALPHA and names another
        // station at that station's STANOX; the direct CRS still wins.
        let json = br#"{"TIPLOCDATA":[
            {"STANOX":"60000","TIPLOC":"PRIMARY","3ALPHA":"PRI","NLCDESC":"P"},
            {"STANOX":"61000","TIPLOC":"OTHER","3ALPHA":"OTH","NLCDESC":"O"},
            {"STANOX":"61000","TIPLOC":"PRIMARY","3ALPHA":" ","NLCDESC":"O"}
        ]}"#;
        let result = crs_tiploc_from_corpus(json).unwrap();
        let pairs: Vec<_> = result.report.pair_rules.into_iter().collect();
        assert_eq!(
            pairs,
            vec![
                (pair("OTH", "OTHER"), Rule::Direct),
                (pair("PRI", "PRIMARY"), Rule::Direct),
            ]
        );
    }

    #[test]
    fn comparison_against_an_existing_csv_reports_agreement_by_rule() {
        let json = br#"{"TIPLOCDATA":[
            {"STANOX":"87219","TIPLOC":"CLPHMJN","3ALPHA":"CLJ","NLCDESC":"CLAPHAM JUNCTION LONDON"},
            {"STANOX":"87219","TIPLOC":"CLPHMJW","3ALPHA":" ","NLCDESC":"CLAPHAM JN (WINDSOR)"},
            {"STANOX":"12345","TIPLOC":"FOOBAR","3ALPHA":"FOO","NLCDESC":"FOO"},
            {"STANOX":"12345","TIPLOC":"FOOBARX","3ALPHA":" ","NLCDESC":"FOO EAST BAY"}
        ]}"#;
        let result = crs_tiploc_from_corpus(json).unwrap();
        // CRLF, like the committed snapshot, with a bare-CRS row.
        let old_csv = "crs,tiploc,name\r\n\
                       BAR,FOOBARX,Bar\r\n\
                       CLJ,CLPHMJN,Clapham Junction\r\n\
                       CLJ,CLPHMJW,Clapham Junction\r\n\
                       FOO,FOOBAR,Foo\r\n\
                       NOT,,No Tiploc\r\n\
                       OLD,OLDTIP,Old\r\n";
        let (old_pairs, old_crs) = read_crs_tiploc_pairs(old_csv.as_bytes()).unwrap();
        let c = compare(&result, &old_pairs, &old_crs);

        assert_eq!(
            c.matched,
            BTreeSet::from([
                pair("CLJ", "CLPHMJN"),
                pair("CLJ", "CLPHMJW"),
                pair("FOO", "FOOBAR")
            ])
        );
        assert_eq!(
            c.only_old,
            BTreeSet::from([pair("BAR", "FOOBARX"), pair("OLD", "OLDTIP")])
        );
        assert_eq!(c.only_new, BTreeSet::from([pair("FOO", "FOOBARX")]));
        assert_eq!(
            c.conflicts,
            BTreeMap::from([("FOOBARX".to_owned(), (set(&["BAR"]), set(&["FOO"])))])
        );
        assert_eq!(c.crs_only_old, set(&["BAR", "NOT", "OLD"]));
        assert!(c.crs_only_new.is_empty());
        let agreement = |matched, only_new, conflicts| RuleAgreement {
            matched,
            only_new,
            conflicts,
        };
        assert_eq!(c.by_rule[&Rule::Direct], agreement(2, 0, 0));
        assert_eq!(c.by_rule[&Rule::StationName], agreement(1, 1, 1));
        assert_eq!(c.by_rule[&Rule::StationQualifier], agreement(0, 0, 0));

        let path = Path::new("reference-data/crs-tiploc.csv");
        let summary = render_report(&result, Some((path, &c)), ReportDetail::Summary);
        assert!(summary.contains("pairs matched: 3"));
        assert!(summary.contains("pairs only in old file (still missing): 2"));
        assert!(summary.contains("pairs only in new output: 1"));
        assert!(summary.contains("conflicts (TIPLOC in both, different CRS): 1"));
        assert!(summary.contains("station part by name (same STANOX): 1 / 1 / 1"));
        assert!(!summary.contains("FOOBARX: old BAR"));
        let full = render_report(&result, Some((path, &c)), ReportDetail::Full);
        assert!(full.contains("FOOBARX: old BAR -> new FOO [station part by name (same STANOX)]"));
        assert!(full.contains("  OLD,OLDTIP\n"));
        assert!(full.contains("  FOO,FOOBARX [station part by name (same STANOX)]\n"));
    }

    #[test]
    fn rdm_toc_xml_becomes_sorted_toc_codes() {
        let xml = r#"<TrainOperatingCompanyList>
            <TrainOperatingCompany><AtocCode>xc</AtocCode><Name>CrossCountry</Name><LegalName>XC Trains Limited</LegalName></TrainOperatingCompany>
            <TrainOperatingCompany><AtocCode>AW</AtocCode><Name>Transport for Wales</Name><LegalName>TfW</LegalName></TrainOperatingCompany>
        </TrainOperatingCompanyList>"#;
        assert_eq!(
            toc_codes_from_rdm_xml(xml).unwrap(),
            vec![
                ("AW".into(), "Transport for Wales".into()),
                ("XC".into(), "CrossCountry".into()),
            ]
        );
    }
}
