//! Regenerating the fast tier's vendored reference CSVs from Network Rail /
//! National Rail Knowledgebase data instead of the railwaycodes.org.uk
//! scrape (DQ13 / LEG-24).
//!
//! - `crs-tiploc.csv` from Network Rail's **CORPUS** extract
//!   (`CORPUSExtract.json`, NRIL open-data licence -- the same licence
//!   `/attribution` already credits). Run with
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
/// and as strings; see [`nlc_location`] and [`stanox_key`]. `UIC` and
/// `NLCDESC16` are ignored.
#[derive(Debug, Deserialize)]
struct CorpusRow {
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
    tiploc_data: Vec<CorpusRow>,
}

/// One output row of `crs-tiploc.csv`: `(crs, tiploc, name)`, with an empty
/// `tiploc` only for a CRS that no CORPUS row pairs with any TIPLOC.
pub type CrsTiplocRow = (String, String, String);

/// Which cross-referencing rule produced a `(crs, tiploc)` pair. Rules are
/// tried in this order and the first that yields exactly one CRS wins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Rule {
    /// The TIPLOC's own CORPUS row carries a `3ALPHA`.
    Direct,
    /// Exactly one CRS among the rows sharing the TIPLOC's 4-digit NLC
    /// location prefix.
    NlcGroup,
    /// Exactly one CRS among the rows sharing the TIPLOC's STANOX.
    StanoxGroup,
}

impl Rule {
    pub const ALL: [Rule; 3] = [Rule::Direct, Rule::NlcGroup, Rule::StanoxGroup];

    pub fn label(self) -> &'static str {
        match self {
            Rule::Direct => "direct (own 3ALPHA)",
            Rule::NlcGroup => "NLC group (4-digit prefix)",
            Rule::StanoxGroup => "STANOX group",
        }
    }
}

/// A group (NLC location prefix or STANOX) that was consulted for at least
/// one CRS-less TIPLOC and held two or more distinct CRS codes, so it could
/// not settle any of them.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct AmbiguousGroup {
    pub candidates: BTreeSet<String>,
    /// The CRS-less TIPLOCs that consulted this group.
    pub tiplocs: BTreeSet<String>,
}

/// How [`crs_tiploc_from_corpus`] arrived at its output, for the
/// regeneration report. Not written to the CSV.
#[derive(Debug, Default, Clone)]
pub struct InferenceReport {
    /// Every `(crs, tiploc)` pair in the output and the rule behind it.
    pub pair_rules: BTreeMap<(String, String), Rule>,
    /// Ambiguous NLC groups, keyed by 4-digit prefix (prefixes joined with
    /// `+` for a TIPLOC whose rows carry several NLC locations).
    pub ambiguous_nlc: BTreeMap<String, AmbiguousGroup>,
    /// Ambiguous STANOX groups, keyed the same way.
    pub ambiguous_stanox: BTreeMap<String, AmbiguousGroup>,
    /// TIPLOCs assigned by the STANOX rule after their NLC group had been
    /// ambiguous (as opposed to empty). These are the least certain
    /// inferences: worth eyeballing.
    pub stanox_after_ambiguous_nlc: BTreeSet<String>,
    /// CRS-less TIPLOCs left out because neither group was decisive and at
    /// least one of them was ambiguous.
    pub left_out_ambiguous: BTreeSet<String>,
    /// CRS-less TIPLOCs left out because neither group had any CRS at all
    /// (junctions, sidings, depots: the great majority of CORPUS).
    pub left_out_no_candidate: usize,
}

impl InferenceReport {
    pub fn pairs_by_rule(&self, rule: Rule) -> usize {
        self.pair_rules.values().filter(|r| **r == rule).count()
    }
}

/// Output of [`crs_tiploc_from_corpus`].
#[derive(Debug, Clone)]
pub struct CorpusCrsTiploc {
    pub rows: Vec<CrsTiplocRow>,
    pub report: InferenceReport,
}

fn is_crs(s: &str) -> bool {
    s.len() == 3 && s.bytes().all(|b| b.is_ascii_uppercase())
}

fn is_tiploc(s: &str) -> bool {
    (2..=7).contains(&s.len())
        && s.bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
}

/// Digits of a numeric-or-string JSON value, trimmed; `None` for blank,
/// non-digit or absent values.
fn digits(v: Option<&serde_json::Value>) -> Option<String> {
    let s = match v? {
        serde_json::Value::Number(n) => n.as_u64()?.to_string(),
        serde_json::Value::String(s) => s.trim().to_owned(),
        _ => return None,
    };
    (!s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())).then_some(s)
}

/// The 4-digit location part of a 6-digit NLC (the last two digits
/// distinguish sub-locations of the same place). A numeric NLC lost its
/// leading zeros, so it is left-padded back to six digits first. An NLC of
/// more than six digits, or of all zeros, is not usable.
fn nlc_location(v: Option<&serde_json::Value>) -> Option<String> {
    let d = digits(v)?;
    if d.len() > 6 || d.bytes().all(|b| b == b'0') {
        return None;
    }
    Some(format!("{d:0>6}")[..4].to_owned())
}

/// A STANOX usable as a group key: blank and all-zero STANOX values are
/// placeholders that would lump unrelated locations together.
fn stanox_key(v: Option<&serde_json::Value>) -> Option<String> {
    digits(v).filter(|d| !d.bytes().all(|b| b == b'0'))
}

fn trimmed(s: Option<&str>) -> &str {
    s.unwrap_or("").trim()
}

/// Outcome of consulting one kind of group for one CRS-less TIPLOC.
enum GroupVerdict {
    One(String),
    Ambiguous(BTreeSet<String>),
    Nothing,
}

fn consult(keys: &BTreeSet<String>, index: &BTreeMap<String, BTreeSet<String>>) -> GroupVerdict {
    let candidates: BTreeSet<String> = keys
        .iter()
        .filter_map(|k| index.get(k))
        .flatten()
        .cloned()
        .collect();
    match candidates.len() {
        0 => GroupVerdict::Nothing,
        1 => GroupVerdict::One(candidates.into_iter().next().unwrap_or_default()),
        _ => GroupVerdict::Ambiguous(candidates),
    }
}

fn note_ambiguous(
    map: &mut BTreeMap<String, AmbiguousGroup>,
    keys: &BTreeSet<String>,
    candidates: BTreeSet<String>,
    tiploc: &str,
) {
    let key = keys.iter().cloned().collect::<Vec<_>>().join("+");
    let group = map.entry(key).or_default();
    group.candidates.extend(candidates);
    group.tiplocs.insert(tiploc.to_owned());
}

/// Turns a CORPUS extract into `crs-tiploc.csv` rows.
///
/// CORPUS fills `3ALPHA` only on a station's primary TIPLOC, so each
/// valid TIPLOC (2-7 uppercase letters/digits) gets its CRS by the first
/// of these rules that applies (see [`Rule`]):
///
/// 1. **Direct**: any of the TIPLOC's rows has a `3ALPHA` that is exactly
///    three uppercase letters -- every such CRS is kept, and no inference
///    is attempted.
/// 2. **NLC group**: otherwise, if the rows sharing the TIPLOC's 4-digit
///    NLC location prefix carry exactly one distinct CRS, that one.
/// 3. **STANOX group**: otherwise (no CRS in the NLC group, or more than
///    one), if the rows sharing its STANOX (blank/zero STANOX ignored)
///    carry exactly one distinct CRS, that one.
/// 4. Otherwise the TIPLOC is left out.
///
/// Group candidates come only from rows with a direct `3ALPHA` (including
/// ones without a usable TIPLOC), never from other inferences, so the
/// result does not depend on evaluation order.
///
/// Output contract (unchanged from the direct-only generator, so the
/// validator reads it as before): one row per distinct `(crs, tiploc)`
/// pair, one row with an empty `tiploc` for a CRS that ends up with no
/// TIPLOC; `name` is the `NLCDESC` of the lexicographically-first
/// *directly* paired TIPLOC row for that CRS (or of the first bare row),
/// so inferred pairs never change a station's name and the output does not
/// depend on CORPUS's row order; sorted by `crs` then `tiploc`.
pub fn crs_tiploc_from_corpus(json: &[u8]) -> Result<CorpusCrsTiploc> {
    let extract: CorpusExtract =
        serde_json::from_slice(json).context("parsing CORPUS extract JSON")?;

    // crs -> directly paired tiploc -> name.
    let mut direct: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    // crs -> name, for a CRS seen on a row without a usable TIPLOC.
    let mut bare: BTreeMap<String, String> = BTreeMap::new();
    // group key -> CRS codes seen directly in that group.
    let mut nlc_index: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut stanox_index: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    // tiploc -> (NLC locations, STANOX keys) over all of its rows.
    let mut tiploc_keys: BTreeMap<String, (BTreeSet<String>, BTreeSet<String>)> = BTreeMap::new();
    let mut tiplocs_with_crs: BTreeSet<String> = BTreeSet::new();

    for row in &extract.tiploc_data {
        let crs = trimmed(row.three_alpha.as_deref());
        let tiploc = trimmed(row.tiploc.as_deref());
        let nlc = nlc_location(row.nlc.as_ref());
        let stanox = stanox_key(row.stanox.as_ref());
        let has_crs = is_crs(crs);

        if has_crs {
            if let Some(n) = &nlc {
                nlc_index
                    .entry(n.clone())
                    .or_default()
                    .insert(crs.to_owned());
            }
            if let Some(s) = &stanox {
                stanox_index
                    .entry(s.clone())
                    .or_default()
                    .insert(crs.to_owned());
            }
            let name = trimmed(row.nlc_desc.as_deref()).to_owned();
            if is_tiploc(tiploc) {
                tiplocs_with_crs.insert(tiploc.to_owned());
                direct
                    .entry(crs.to_owned())
                    .or_default()
                    .entry(tiploc.to_owned())
                    .or_insert(name);
            } else {
                bare.entry(crs.to_owned()).or_insert(name);
            }
        }
        if is_tiploc(tiploc) {
            let keys = tiploc_keys.entry(tiploc.to_owned()).or_default();
            keys.0.extend(nlc);
            keys.1.extend(stanox);
        }
    }

    let mut report = InferenceReport::default();
    // crs -> tiploc, every pair whatever its rule.
    let mut pairs: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (crs, tiplocs) in &direct {
        for tiploc in tiplocs.keys() {
            pairs.entry(crs.clone()).or_default().insert(tiploc.clone());
            report
                .pair_rules
                .insert((crs.clone(), tiploc.clone()), Rule::Direct);
        }
    }

    for (tiploc, (nlcs, stanoxes)) in &tiploc_keys {
        if tiplocs_with_crs.contains(tiploc) {
            continue;
        }
        let nlc_verdict = consult(nlcs, &nlc_index);
        let (crs, rule) = match nlc_verdict {
            GroupVerdict::One(crs) => (Some(crs), Rule::NlcGroup),
            nlc_verdict => {
                let nlc_ambiguous = match nlc_verdict {
                    GroupVerdict::Ambiguous(c) => {
                        note_ambiguous(&mut report.ambiguous_nlc, nlcs, c, tiploc);
                        true
                    }
                    _ => false,
                };
                match consult(stanoxes, &stanox_index) {
                    GroupVerdict::One(crs) => {
                        if nlc_ambiguous {
                            report.stanox_after_ambiguous_nlc.insert(tiploc.clone());
                        }
                        (Some(crs), Rule::StanoxGroup)
                    }
                    GroupVerdict::Ambiguous(c) => {
                        note_ambiguous(&mut report.ambiguous_stanox, stanoxes, c, tiploc);
                        report.left_out_ambiguous.insert(tiploc.clone());
                        (None, Rule::StanoxGroup)
                    }
                    GroupVerdict::Nothing => {
                        if nlc_ambiguous {
                            report.left_out_ambiguous.insert(tiploc.clone());
                        } else {
                            report.left_out_no_candidate += 1;
                        }
                        (None, Rule::StanoxGroup)
                    }
                }
            }
        };
        if let Some(crs) = crs {
            pairs.entry(crs.clone()).or_default().insert(tiploc.clone());
            report.pair_rules.insert((crs, tiploc.clone()), rule);
        }
    }

    let crs_codes: BTreeSet<&String> = pairs.keys().chain(bare.keys()).collect();
    let mut rows = Vec::new();
    for crs in crs_codes {
        let name = direct
            .get(crs)
            .and_then(|t| t.values().next())
            .or_else(|| bare.get(crs))
            .cloned()
            .unwrap_or_default();
        match pairs.get(crs) {
            Some(tiplocs) => {
                for tiploc in tiplocs {
                    rows.push((crs.clone(), tiploc.clone(), name.clone()));
                }
            }
            None => rows.push((crs.clone(), String::new(), name)),
        }
    }
    if rows.is_empty() {
        bail!(
            "CORPUS extract yielded zero CRS codes -- not a plausible real extract (wrong file, \
             or the TIPLOCDATA/3ALPHA field names changed); refusing to write an empty file"
        );
    }
    Ok(CorpusCrsTiploc { rows, report })
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
    let _ = writeln!(
        out,
        "TIPLOCs assigned by STANOX after an ambiguous NLC group: {}{}",
        r.stanox_after_ambiguous_nlc.len(),
        if r.stanox_after_ambiguous_nlc.is_empty() {
            String::new()
        } else {
            format!(" ({})", join(&r.stanox_after_ambiguous_nlc))
        }
    );
    let _ = writeln!(
        out,
        "CRS-less TIPLOCs left out as ambiguous: {}",
        r.left_out_ambiguous.len()
    );
    let _ = writeln!(
        out,
        "CRS-less TIPLOCs left out with no candidate CRS: {}",
        r.left_out_no_candidate
    );
    if detail >= ReportDetail::Standard {
        write_groups(&mut out, "ambiguous NLC groups", &r.ambiguous_nlc);
        write_groups(&mut out, "ambiguous STANOX groups", &r.ambiguous_stanox);
    } else {
        let _ = writeln!(
            out,
            "ambiguous groups: {} NLC, {} STANOX",
            r.ambiguous_nlc.len(),
            r.ambiguous_stanox.len()
        );
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

    /// Direct, NLC inheritance (numeric, string and zero-padded NLCs) and
    /// STANOX fallback, in real CORPUS shape (blank values are one space).
    #[test]
    fn secondary_tiplocs_inherit_crs_by_nlc_then_stanox() {
        let json = br#"{"TIPLOCDATA":[
            {"NLC":548700,"STANOX":"87219","TIPLOC":"CLPHMJC","3ALPHA":"CLJ","UIC":" ","NLCDESC":"CLAPHAM JUNCTION","NLCDESC16":" "},
            {"NLC":548703,"STANOX":"87200","TIPLOC":"CLPHMJW","3ALPHA":" ","UIC":" ","NLCDESC":"CLAPHAM JN WINDSOR LINES","NLCDESC16":" "},
            {"NLC":"548709","STANOX":" ","TIPLOC":"CLAPHMW","3ALPHA":" ","UIC":" ","NLCDESC":"CLAPHAM WEST","NLCDESC16":" "},
            {"NLC":12300,"STANOX":" ","TIPLOC":"LZA","3ALPHA":"LZA","UIC":" ","NLCDESC":"LEADING ZERO","NLCDESC16":" "},
            {"NLC":"012301","STANOX":" ","TIPLOC":"LZAB","3ALPHA":" ","UIC":" ","NLCDESC":"LEADING ZERO B","NLCDESC16":" "},
            {"NLC":123400,"STANOX":"12345","TIPLOC":"FOOBAR","3ALPHA":"FOO","UIC":" ","NLCDESC":"FOO","NLCDESC16":" "},
            {"NLC":999900,"STANOX":12345,"TIPLOC":"FOOBARX","3ALPHA":" ","UIC":" ","NLCDESC":"FOO EXTRA","NLCDESC16":" "},
            {"NLC":777700,"STANOX":"00000","TIPLOC":"ZEROA","3ALPHA":"ZZA","UIC":" ","NLCDESC":"ZERO A","NLCDESC16":" "},
            {"NLC":888800,"STANOX":"00000","TIPLOC":"ZEROB","3ALPHA":" ","UIC":" ","NLCDESC":"ZERO B","NLCDESC16":" "},
            {"NLC":" ","STANOX":" ","TIPLOC":"BLANKX","3ALPHA":" ","UIC":" ","NLCDESC":"BLANK","NLCDESC16":" "},
            {"NLC":0,"STANOX":0,"TIPLOC":"NOUGHT","3ALPHA":" ","UIC":" ","NLCDESC":"ZERO NLC","NLCDESC16":" "}
        ]}"#;
        let result = crs_tiploc_from_corpus(json).unwrap();
        let s = |a: &str, b: &str, c: &str| (a.to_owned(), b.to_owned(), c.to_owned());
        assert_eq!(
            result.rows,
            vec![
                // CLAPHMW sorts first but is inferred, so the name stays
                // the primary TIPLOC's.
                s("CLJ", "CLAPHMW", "CLAPHAM JUNCTION"),
                s("CLJ", "CLPHMJC", "CLAPHAM JUNCTION"),
                s("CLJ", "CLPHMJW", "CLAPHAM JUNCTION"),
                s("FOO", "FOOBAR", "FOO"),
                s("FOO", "FOOBARX", "FOO"),
                s("LZA", "LZA", "LEADING ZERO"),
                s("LZA", "LZAB", "LEADING ZERO"),
                // The all-zero STANOX is not a group: ZEROB stays out.
                s("ZZA", "ZEROA", "ZERO A"),
            ]
        );
        let r = &result.report;
        assert_eq!(r.pair_rules[&pair("CLJ", "CLPHMJC")], Rule::Direct);
        assert_eq!(r.pair_rules[&pair("CLJ", "CLPHMJW")], Rule::NlcGroup);
        assert_eq!(r.pair_rules[&pair("CLJ", "CLAPHMW")], Rule::NlcGroup);
        assert_eq!(r.pair_rules[&pair("LZA", "LZAB")], Rule::NlcGroup);
        assert_eq!(r.pair_rules[&pair("FOO", "FOOBARX")], Rule::StanoxGroup);
        assert_eq!(r.pairs_by_rule(Rule::Direct), 4);
        assert_eq!(r.pairs_by_rule(Rule::NlcGroup), 3);
        assert_eq!(r.pairs_by_rule(Rule::StanoxGroup), 1);
        // ZEROB, BLANKX, NOUGHT.
        assert_eq!(r.left_out_no_candidate, 3);
        assert!(r.left_out_ambiguous.is_empty());
        assert!(r.ambiguous_nlc.is_empty() && r.ambiguous_stanox.is_empty());
    }

    #[test]
    fn ambiguous_groups_assign_nothing_and_are_reported() {
        let json = br#"{"TIPLOCDATA":[
            {"NLC":400000,"STANOX":"40000","TIPLOC":"AAAA","3ALPHA":"AAA","NLCDESC":"A"},
            {"NLC":400001,"STANOX":"40001","TIPLOC":"BBBB","3ALPHA":"BBB","NLCDESC":"B"},
            {"NLC":400002,"STANOX":"40002","TIPLOC":"AMBIG1","3ALPHA":" ","NLCDESC":"NLC AMBIGUOUS"},
            {"NLC":400003,"STANOX":"40000","TIPLOC":"AMBIG2","3ALPHA":" ","NLCDESC":"NLC AMBIGUOUS, STANOX NOT"},
            {"NLC":500000,"STANOX":"50000","TIPLOC":"CCCC","3ALPHA":"CCC","NLCDESC":"C"},
            {"NLC":510000,"STANOX":"50000","TIPLOC":"DDDD","3ALPHA":"DDD","NLCDESC":"D"},
            {"NLC":520000,"STANOX":"50000","TIPLOC":"AMBIG3","3ALPHA":" ","NLCDESC":"STANOX AMBIGUOUS"}
        ]}"#;
        let result = crs_tiploc_from_corpus(json).unwrap();
        let r = &result.report;
        let tiplocs: BTreeSet<&str> = result.rows.iter().map(|r| r.1.as_str()).collect();
        assert!(!tiplocs.contains("AMBIG1") && !tiplocs.contains("AMBIG3"));

        // An ambiguous NLC group falls through to STANOX, and a decisive
        // STANOX there is flagged as the least-certain kind of inference.
        assert_eq!(r.pair_rules[&pair("AAA", "AMBIG2")], Rule::StanoxGroup);
        assert_eq!(r.stanox_after_ambiguous_nlc, set(&["AMBIG2"]));

        assert_eq!(
            r.ambiguous_nlc,
            BTreeMap::from([(
                "4000".to_owned(),
                AmbiguousGroup {
                    candidates: set(&["AAA", "BBB"]),
                    tiplocs: set(&["AMBIG1", "AMBIG2"]),
                }
            )])
        );
        assert_eq!(
            r.ambiguous_stanox,
            BTreeMap::from([(
                "50000".to_owned(),
                AmbiguousGroup {
                    candidates: set(&["CCC", "DDD"]),
                    tiplocs: set(&["AMBIG3"]),
                }
            )])
        );
        assert_eq!(r.left_out_ambiguous, set(&["AMBIG1", "AMBIG3"]));
        assert_eq!(r.left_out_no_candidate, 0);

        let text = render_report(&result, None, ReportDetail::Standard);
        assert!(text.contains("4000: candidates AAA,BBB -- TIPLOC(s) AMBIG1,AMBIG2"));
        assert!(text.contains("50000: candidates CCC,DDD -- TIPLOC(s) AMBIG3"));
    }

    #[test]
    fn a_tiploc_with_a_direct_crs_on_any_row_is_never_inferred() {
        // The second row for PRIMARY has no 3ALPHA and sits in another
        // CRS's NLC group; the direct CRS on its first row still wins.
        let json = br#"{"TIPLOCDATA":[
            {"NLC":600000,"STANOX":" ","TIPLOC":"PRIMARY","3ALPHA":"PRI","NLCDESC":"P"},
            {"NLC":610000,"STANOX":" ","TIPLOC":"OTHER","3ALPHA":"OTH","NLCDESC":"O"},
            {"NLC":610001,"STANOX":" ","TIPLOC":"PRIMARY","3ALPHA":" ","NLCDESC":"P2"}
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
            {"NLC":548700,"STANOX":"87219","TIPLOC":"CLPHMJC","3ALPHA":"CLJ","NLCDESC":"CLAPHAM JUNCTION"},
            {"NLC":548703,"STANOX":" ","TIPLOC":"CLPHMJW","3ALPHA":" ","NLCDESC":"CLAPHAM JN W"},
            {"NLC":123400,"STANOX":"12345","TIPLOC":"FOOBAR","3ALPHA":"FOO","NLCDESC":"FOO"},
            {"NLC":999900,"STANOX":"12345","TIPLOC":"FOOBARX","3ALPHA":" ","NLCDESC":"FOO X"}
        ]}"#;
        let result = crs_tiploc_from_corpus(json).unwrap();
        // CRLF, like the committed snapshot, with a bare-CRS row.
        let old_csv = "crs,tiploc,name\r\n\
                       BAR,FOOBARX,Bar\r\n\
                       CLJ,CLPHMJC,Clapham Junction\r\n\
                       CLJ,CLPHMJW,Clapham Junction\r\n\
                       FOO,FOOBAR,Foo\r\n\
                       NOT,,No Tiploc\r\n\
                       OLD,OLDTIP,Old\r\n";
        let (old_pairs, old_crs) = read_crs_tiploc_pairs(old_csv.as_bytes()).unwrap();
        let c = compare(&result, &old_pairs, &old_crs);

        assert_eq!(
            c.matched,
            BTreeSet::from([
                pair("CLJ", "CLPHMJC"),
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
        assert_eq!(c.by_rule[&Rule::NlcGroup], agreement(1, 0, 0));
        assert_eq!(c.by_rule[&Rule::StanoxGroup], agreement(0, 1, 1));

        let path = Path::new("reference-data/crs-tiploc.csv");
        let summary = render_report(&result, Some((path, &c)), ReportDetail::Summary);
        assert!(summary.contains("pairs matched: 3"));
        assert!(summary.contains("pairs only in old file (still missing): 2"));
        assert!(summary.contains("pairs only in new output: 1"));
        assert!(summary.contains("conflicts (TIPLOC in both, different CRS): 1"));
        assert!(summary.contains("STANOX group: 0 / 1 / 1"));
        assert!(!summary.contains("FOOBARX: old BAR"));
        let full = render_report(&result, Some((path, &c)), ReportDetail::Full);
        assert!(full.contains("FOOBARX: old BAR -> new FOO [STANOX group]"));
        assert!(full.contains("  OLD,OLDTIP\n"));
        assert!(full.contains("  FOO,FOOBARX [STANOX group]\n"));
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
