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

/// Which rule produced a `(crs, tiploc)` pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Rule {
    /// The TIPLOC's own CORPUS row carries a `3ALPHA`.
    Direct,
    /// A CRS-less TIPLOC that is part of a station, named as such: it
    /// shares a STANOX with that station's `3ALPHA` row and its description
    /// is the station's name plus only platform-ish words
    /// (`CLAPHAM JN (WINDSOR)`, `VICTORIA PLAT 10`). See
    /// [`crs_tiploc_from_corpus`].
    StationName,
    /// A CRS-less TIPLOC that is part of a station, described only by
    /// platform-ish words (`CENTRAL`, `SOUTH WEST`, `NO 4 BAY PLATFORM`):
    /// it shares both the STANOX and the 4-digit NLC location with that
    /// station's `3ALPHA` row.
    StationQualifier,
}

impl Rule {
    pub const ALL: [Rule; 3] = [Rule::Direct, Rule::StationName, Rule::StationQualifier];

    pub fn label(self) -> &'static str {
        match self {
            Rule::Direct => "direct (own 3ALPHA)",
            Rule::StationName => "station part by name (same STANOX)",
            Rule::StationQualifier => "station part by platform words (same STANOX + NLC)",
        }
    }
}

/// A STANOX where one CRS-less TIPLOC's description matched two or more
/// distinct stations' names, so it could not be settled.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct AmbiguousGroup {
    pub candidates: BTreeSet<String>,
    /// The CRS-less TIPLOCs that matched more than one of them.
    pub tiplocs: BTreeSet<String>,
}

/// How [`crs_tiploc_from_corpus`] arrived at its output, for the
/// regeneration report. Not written to the CSV.
#[derive(Debug, Default, Clone)]
pub struct InferenceReport {
    /// Every `(crs, tiploc)` pair in the output and the rule behind it.
    pub pair_rules: BTreeMap<(String, String), Rule>,
    /// Description of each TIPLOC paired by an inference rule, for
    /// eyeballing the inferences.
    pub inferred_descs: BTreeMap<String, String>,
    /// CRS-less TIPLOCs whose description matched several stations at
    /// their STANOX, keyed by STANOX (joined with `+` for a TIPLOC whose
    /// rows carry several).
    pub ambiguous: BTreeMap<String, AmbiguousGroup>,
    /// CRS-less TIPLOCs that share a STANOX with a station but were left
    /// out because their description is not that station's name, or names
    /// it followed by a non-station word (signal, junction, sidings, depot,
    /// loop, ...): tiploc -> its description. Mostly signals and junctions
    /// at the station throat.
    pub left_out_by_name: BTreeMap<String, String>,
    /// CRS-less TIPLOCs left out because no station `3ALPHA` row shares
    /// their STANOX (junctions, sidings, depots: the great majority of
    /// CORPUS).
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

/// Abbreviations CORPUS descriptions use for the words the name checks
/// look at, mapped to one spelling.
const ABBREVIATIONS: &[(&str, &str)] = &[
    ("JN", "JUNCTION"),
    ("JCN", "JUNCTION"),
    ("JNC", "JUNCTION"),
    ("JNCT", "JUNCTION"),
    ("JCT", "JUNCTION"),
    ("JUNC", "JUNCTION"),
    ("JUNCTON", "JUNCTION"),
    ("SIG", "SIGNAL"),
    ("SIGS", "SIGNAL"),
    ("SDG", "SIDINGS"),
    ("SDGS", "SIDINGS"),
    ("SDNGS", "SIDINGS"),
    ("SIDNGS", "SIDINGS"),
    ("SIDING", "SIDINGS"),
    ("SIDDINGS", "SIDINGS"),
    ("CARR", "CARRIAGE"),
    ("XOVERS", "CROSSOVER"),
    ("XOVER", "CROSSOVER"),
    ("YD", "YARD"),
];

/// Words that mark a station's own `3ALPHA` row as not really a station
/// (a pseudo-CRS carriage siding, yard or depot such as `XCP` "BR CARRIAGE
/// SIDINGS" at Clapham Junction's STANOX), so it is never a candidate.
/// Deliberately excludes place-name words (`JUNCTION`, `ROAD`, `TOWN`...).
const NON_STATION_ANCHOR_WORDS: &[&str] = &[
    "BALLAST",
    "CARRIAGE",
    "CROSSOVER",
    "CSD",
    "DEPOT",
    "DEPOTS",
    "ENTRANCE",
    "ENTRY",
    "EXIT",
    "FREIGHT",
    "FRT",
    "GF",
    "GOODS",
    "HOLDING",
    "LC",
    "LOOP",
    "RECEPTION",
    "REV",
    "SHED",
    "SHEDS",
    "SIDINGS",
    "SIGNAL",
    "SST",
    "STABLING",
    "TMD",
    "TRAINCARE",
    "WASHER",
    "YARD",
];

/// Words that, following a station's name in a CRS-less TIPLOC's
/// description, mark it as infrastructure or a non-rail point near the
/// station rather than part of it: signals, junctions, sidings, depots,
/// loops, yards, crossovers, level crossings, ground frames, staff and
/// engineering locations, freight terminals, bus stops and the like.
const NON_STATION_SUFFIX_WORDS: &[&str] = &[
    "BALLAST",
    "BOX",
    "BRIDGE",
    "BUS",
    "CARRIAGE",
    "CE",
    "CENTRE",
    "CHORD",
    "CHS",
    "CROSSING",
    "CROSSOVER",
    "CS",
    "CSD",
    "CURVE",
    "DEPOT",
    "DEPOTS",
    "DMUD",
    "DOCK",
    "DOCKS",
    "DRIVERS",
    "EMUD",
    "ENG",
    "ENGOPS",
    "ENTRANCE",
    "ENTRY",
    "EXIT",
    "FRAME",
    "FREIGHT",
    "FRT",
    "FUEL",
    "FUELLING",
    "GDS",
    "GF",
    "GOODS",
    "GROUND",
    "HEADSHUNT",
    "HOLDING",
    "HQ",
    "JUNCTION",
    "LC",
    "LINE",
    "LINES",
    "LIP",
    "LOOP",
    "LOOPS",
    "MAINTENANCE",
    "MESS",
    "MUSEUM",
    "MUSM",
    "NECK",
    "OFFICE",
    "OTS",
    "PAYBILL",
    "PW",
    "PWAY",
    "QUARRY",
    "RECEPTION",
    "RELIEF",
    "REV",
    "ROAD",
    "SALES",
    "SB",
    "SERVICES",
    "SHED",
    "SHEDS",
    "SHIP",
    "SHUNT",
    "SIDINGS",
    "SIGNAL",
    "SIGNALLING",
    "SPUR",
    "SST",
    "STABLING",
    "STAFF",
    "STOP",
    "TERMINAL",
    "TERMINALS",
    "TMD",
    "TOWN",
    "TRAINCARE",
    "TUNNEL",
    "TURNBACK",
    "UPL",
    "VIADUCT",
    "WASH",
    "WASHER",
    "WELDERS",
    "WHARF",
    "WORKS",
    "XNG",
    "YARD",
];

/// Splits a CORPUS description into normalised words: uppercased, dots
/// dropped (so `L.C.` is `LC`), split on anything not a letter or digit,
/// runs of single letters joined (`C H S` is `CHS`, `B R` is `BR`), and
/// [`ABBREVIATIONS`] expanded.
fn words(desc: &str) -> Vec<String> {
    let cleaned = desc.to_ascii_uppercase().replace('.', "");
    let mut out: Vec<String> = Vec::new();
    let mut letter_run = String::new();
    let flush = |run: &mut String, out: &mut Vec<String>| {
        if !run.is_empty() {
            out.push(std::mem::take(run));
        }
    };
    for w in cleaned
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|w| !w.is_empty())
    {
        if w.len() == 1 && w.bytes().all(|b| b.is_ascii_uppercase()) {
            letter_run.push_str(w);
            continue;
        }
        flush(&mut letter_run, &mut out);
        out.push(w.to_owned());
    }
    flush(&mut letter_run, &mut out);
    for w in &mut out {
        if let Some((_, full)) = ABBREVIATIONS.iter().find(|(abbr, _)| abbr == w) {
            *w = (*full).to_owned();
        }
    }
    out
}

/// A station's name as matched against CRS-less TIPLOC descriptions: its
/// words, less a trailing `LONDON` (`CLAPHAM JUNCTION LONDON`,
/// `PADDINGTON LONDON`), or `None` if its description marks it as not a
/// station (see [`NON_STATION_ANCHOR_WORDS`]).
fn station_stem(desc: &str) -> Option<Vec<String>> {
    let mut w = words(desc);
    if w.iter()
        .any(|w| NON_STATION_ANCHOR_WORDS.contains(&w.as_str()))
    {
        return None;
    }
    if w.len() > 1 && w.last().is_some_and(|l| l == "LONDON") {
        w.pop();
    }
    (!w.is_empty()).then_some(w)
}

/// Whether `desc` names the station with stem `stem` (optionally after a
/// leading `LONDON`, as in `LONDON VICTORIA (E)`) and then only
/// platform-ish words: `CLAPHAM JN (WINDSOR)`, `VICTORIA PLAT 10`,
/// `BASINGSTOKE EAST BAY`, but not `CLAPHAM JN SIGNAL TVC147`,
/// `READING SOUTHERN JN` or `WIMBLEDON SIGNAL W1101`. A word of three or
/// more characters containing a digit (`W149`, `TVC587`, `150`) is a
/// signal number and also disqualifies.
fn names_station_part(desc: &[String], stem: &[String]) -> bool {
    let rest = desc.strip_prefix(stem).or_else(|| {
        desc.strip_prefix(["LONDON".to_owned()].as_slice())
            .and_then(|d| d.strip_prefix(stem))
    });
    let Some(rest) = rest else {
        return false;
    };
    !rest.iter().any(|w| {
        NON_STATION_SUFFIX_WORDS.contains(&w.as_str())
            || (w.len() >= 3 && w.bytes().any(|b| b.is_ascii_digit()))
    })
}

/// Words that, on their own, describe a part of a station: platform
/// groups and bays (`CENTRAL`, `EASTERN`, `SOUTH WEST`, `NO 4 BAY
/// PLATFORM`, `DOWN BAY`).
const PLATFORM_WORDS: &[&str] = &[
    "BAY",
    "BAYS",
    "CENTRAL",
    "DOWN",
    "EAST",
    "EASTERN",
    "FAST",
    "HIGH",
    "LOCAL",
    "LOW",
    "MAIN",
    "MIDDLE",
    "NO",
    "NORTH",
    "NORTHERN",
    "PLAT",
    "PLATFORM",
    "PLATFORMS",
    "PLATS",
    "SLOW",
    "SOUTH",
    "SOUTHERN",
    "SUBURBAN",
    "UP",
    "WEST",
    "WESTERN",
];

/// Whether `desc` is made only of [`PLATFORM_WORDS`], platform numbers of
/// at most two digits (`4`, `NO3`) and single letters (`C`).
fn is_platform_words_only(desc: &[String]) -> bool {
    !desc.is_empty()
        && desc.iter().all(|w| {
            PLATFORM_WORDS.contains(&w.as_str())
                || (w.len() <= 2 && w.bytes().all(|b| b.is_ascii_digit()))
                || w.strip_prefix("NO").is_some_and(|n| {
                    (1..=2).contains(&n.len()) && n.bytes().all(|b| b.is_ascii_digit())
                })
                || (w.len() == 1 && w.bytes().all(|b| b.is_ascii_uppercase()))
        })
}

/// A station-like `3ALPHA` row, as a candidate for CRS-less TIPLOCs at its
/// STANOX.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Station {
    stem: Vec<String>,
    nlc: Option<String>,
    crs: String,
}

/// One row of a CRS-less TIPLOC.
struct TiplocRow {
    stanox: Option<String>,
    nlc: Option<String>,
    words: Vec<String>,
    desc: String,
}

// The inference rule below is a deliberate, user-accepted trade-off
// (precision over recall), with its known costs recorded in
// reference-data/line-catalogue-validation.md, section "Decision
// (2026-09-28): conservative CORPUS inference". Read that before loosening
// it or editing the word lists above.

/// Turns a CORPUS extract into `crs-tiploc.csv` rows.
///
/// CORPUS fills `3ALPHA` only on a station's primary TIPLOC, so each
/// valid TIPLOC (2-7 uppercase letters/digits) gets its CRS by the first
/// of these rules that applies (see [`Rule`]):
///
/// 1. **Direct**: any of the TIPLOC's rows has a `3ALPHA` that is exactly
///    three uppercase letters -- every such CRS is kept, and no inference
///    is attempted.
/// 2. **Station part**: otherwise, the candidate stations for each of its
///    rows are the `3ALPHA` rows with the same STANOX (blank/zero STANOX
///    never match) whose own description does not mark them as a
///    yard/sidings/depot pseudo-station, and that the row's description
///    either
///    - **names**: the station's name (description less a trailing
///      `LONDON`, abbreviations normalised) followed only by platform-ish
///      words -- no signal, junction, sidings, depot, loop, yard,
///      crossover, level-crossing, freight or staff word, and no signal
///      number ([`Rule::StationName`]); or
///    - **qualifies**: the description is only platform words (`CENTRAL`,
///      `SOUTH WEST`, `NO 4 BAY PLATFORM`) and the row also shares the
///      station's 4-digit NLC location ([`Rule::StationQualifier`]).
///
///    Exactly one distinct CRS over all its rows: that one (by name if
///    any row named it). Several: left out and reported as ambiguous.
/// 3. Otherwise the TIPLOC is left out.
///
/// This recovers genuine platform-group TIPLOCs (Clapham Junction's
/// `CLPHMJC`/`CLPHMJW`/`CLPHMJM`/`CLPHMJ1`, London Bridge's `LNDNBDC`/
/// `LNDNBDE`, Victoria's `VICT9`..`VICT19`, St Pancras's `STPADOM`)
/// without handing station codes to the signals, junctions and sidings
/// that share a station's NLC prefix or even its STANOX. Looser rules were
/// measured on a real extract and rejected (see
/// `reference-data/line-catalogue-validation.md`): "the only CRS in the
/// 4-digit NLC group, else the only CRS at the STANOX" (4,613 inferences,
/// overwhelmingly signals, junctions, sidings and freight terminals), and
/// "NLC and STANOX groups agree" (132, still mostly junctions and signals).
///
/// Candidates come only from rows with a direct `3ALPHA` (including ones
/// without a usable TIPLOC), never from other inferences, so the result
/// does not depend on evaluation order.
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
    // STANOX -> every station-like 3ALPHA row there.
    let mut stations_at: BTreeMap<String, BTreeSet<Station>> = BTreeMap::new();
    let mut tiploc_rows: BTreeMap<String, Vec<TiplocRow>> = BTreeMap::new();
    let mut tiplocs_with_crs: BTreeSet<String> = BTreeSet::new();

    for row in &extract.tiploc_data {
        let crs = trimmed(row.three_alpha.as_deref());
        let tiploc = trimmed(row.tiploc.as_deref());
        let desc = trimmed(row.nlc_desc.as_deref());
        let stanox = stanox_key(row.stanox.as_ref());
        let nlc = nlc_location(row.nlc.as_ref());

        if is_crs(crs) {
            if let (Some(s), Some(stem)) = (&stanox, station_stem(desc)) {
                stations_at.entry(s.clone()).or_default().insert(Station {
                    stem,
                    nlc: nlc.clone(),
                    crs: crs.to_owned(),
                });
            }
            if is_tiploc(tiploc) {
                tiplocs_with_crs.insert(tiploc.to_owned());
                direct
                    .entry(crs.to_owned())
                    .or_default()
                    .entry(tiploc.to_owned())
                    .or_insert_with(|| desc.to_owned());
            } else {
                bare.entry(crs.to_owned())
                    .or_insert_with(|| desc.to_owned());
            }
        }
        if is_tiploc(tiploc) {
            tiploc_rows
                .entry(tiploc.to_owned())
                .or_default()
                .push(TiplocRow {
                    stanox,
                    nlc,
                    words: words(desc),
                    desc: desc.to_owned(),
                });
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

    for (tiploc, rows) in &tiploc_rows {
        if tiplocs_with_crs.contains(tiploc) {
            continue;
        }
        let mut stanoxes: BTreeSet<&str> = BTreeSet::new();
        let mut any_station = false;
        // crs -> (rule, description of the row that matched it).
        let mut candidates: BTreeMap<String, (Rule, &str)> = BTreeMap::new();
        for row in rows {
            let Some(stations) = row.stanox.as_ref().and_then(|s| stations_at.get(s)) else {
                continue;
            };
            any_station = true;
            stanoxes.extend(row.stanox.as_deref());
            for station in stations {
                let rule = if names_station_part(&row.words, &station.stem) {
                    Rule::StationName
                } else if row.nlc.is_some()
                    && row.nlc == station.nlc
                    && is_platform_words_only(&row.words)
                {
                    Rule::StationQualifier
                } else {
                    continue;
                };
                let entry = candidates
                    .entry(station.crs.clone())
                    .or_insert((rule, row.desc.as_str()));
                if rule < entry.0 {
                    *entry = (rule, row.desc.as_str());
                }
            }
        }
        if candidates.len() == 1 {
            if let Some((crs, (rule, desc))) = candidates.into_iter().next() {
                pairs.entry(crs.clone()).or_default().insert(tiploc.clone());
                report.pair_rules.insert((crs, tiploc.clone()), rule);
                report
                    .inferred_descs
                    .insert(tiploc.clone(), desc.to_owned());
            }
        } else if !candidates.is_empty() {
            let key = stanoxes.into_iter().collect::<Vec<_>>().join("+");
            let group = report.ambiguous.entry(key).or_default();
            group.candidates.extend(candidates.into_keys());
            group.tiplocs.insert(tiploc.clone());
        } else if any_station {
            let desc = rows.first().map(|r| r.desc.clone()).unwrap_or_default();
            report.left_out_by_name.insert(tiploc.clone(), desc);
        } else {
            report.left_out_no_candidate += 1;
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

    #[test]
    fn words_normalise_dots_letter_runs_and_abbreviations() {
        let w = |d: &str| words(d).join(" ");
        assert_eq!(w("CLAPHAM JN (WINDSOR)"), "CLAPHAM JUNCTION WINDSOR");
        assert_eq!(w("BRORA L.C."), "BRORA LC");
        assert_eq!(w("HAYES (KENT) C H S"), "HAYES KENT CHS");
        assert_eq!(
            w("VICTORIA  PLAT  9    (TPS USE)"),
            "VICTORIA PLAT 9 TPS USE"
        );
        assert_eq!(w("HARRINGAY UP REV SDGS"), "HARRINGAY UP REV SIDINGS");
        assert_eq!(w("PECKHAM RYE (C)"), "PECKHAM RYE C");

        let platform = |d: &str| is_platform_words_only(&words(d));
        for d in [
            "CENTRAL",
            "SOUTH WEST",
            "NO3 PLATFORM",
            "NO 4 BAY PLATFORM",
            "DOWN BAY",
        ] {
            assert!(platform(d), "{d}");
        }
        for d in [
            "L H S",
            "STATION FORECOURT",
            "DOWN BAY SIDING",
            "SIGNAL 570",
            "NO 123",
            "",
        ] {
            assert!(!platform(d), "{d}");
        }
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
