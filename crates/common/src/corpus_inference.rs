//! The conservative CORPUS CRS inference, shared by
//! `line-catalogue-validator` (regenerating `reference-data/crs-tiploc.csv`)
//! and `api` (the CORPUS-vs-timetable comparison and the off-by-default
//! runtime crosswalk fallback), so both apply exactly the same rule.
//!
//! Network Rail's CORPUS fills `3ALPHA` (the CRS) only on a station's
//! primary TIPLOC. [`infer_crs_tiploc`] gives each valid TIPLOC its CRS
//! directly, or -- for a CRS-less platform/bay TIPLOC of a station -- by the
//! user-accepted conservative rule recorded in
//! `reference-data/line-catalogue-validation.md`, section "Decision
//! (2026-09-28): conservative CORPUS inference" (precision over recall: a
//! wrong pair is worse than a missing one). Read that before loosening the
//! rule or editing the word lists below; the measurements behind them are in
//! that file's "Regenerating this snapshot" section.
//!
//! [`crosswalk`] narrows the result to what a runtime lookup can use: one
//! CRS per TIPLOC and one CRS per STANOX, ambiguous keys dropped.
//!
//! Everything here is a pure function of the rows, with no I/O.

use std::collections::{BTreeMap, BTreeSet};

/// One CORPUS `TIPLOCDATA` row, as far as the inference needs it. Values
/// may be untrimmed or blank (CORPUS pads absent values with a single
/// space), and an NLC or STANOX may have lost its leading zeros. Every field
/// is normalised here, so the raw extract and `corpus_locations` (already
/// normalised by `schedule-ingest`) give the same result.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CorpusRow {
    pub nlc: Option<String>,
    pub stanox: Option<String>,
    pub tiploc: Option<String>,
    /// `3ALPHA`.
    pub crs: Option<String>,
    /// `NLCDESC`.
    pub nlc_desc: Option<String>,
}

/// Bumped whenever [`infer_crs_tiploc`] or [`crosswalk`] can give a
/// different answer for the same rows, so a crosswalk stored by an older
/// build is rebuilt (see `api::data::corpus_crosswalk`).
pub const RULES_VERSION: i32 = 1;

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
    /// [`infer_crs_tiploc`].
    StationName,
    /// A CRS-less TIPLOC that is part of a station, described only by
    /// platform-ish words (`CENTRAL`, `SOUTH WEST`, `NO 4 BAY PLATFORM`):
    /// it shares both the STANOX and the 4-digit NLC location with that
    /// station's `3ALPHA` row.
    StationQualifier,
}

impl Rule {
    pub const ALL: [Rule; 3] = [Rule::Direct, Rule::StationName, Rule::StationQualifier];

    /// A stable machine name, as stored by `api`.
    pub fn as_str(self) -> &'static str {
        match self {
            Rule::Direct => "direct",
            Rule::StationName => "station_name",
            Rule::StationQualifier => "station_qualifier",
        }
    }

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

/// How [`infer_crs_tiploc`] arrived at its output, for the
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

/// Output of [`infer_crs_tiploc`].
#[derive(Debug, Clone)]
pub struct CorpusCrsTiploc {
    pub rows: Vec<CrsTiplocRow>,
    pub report: InferenceReport,
}

/// Exactly three uppercase ASCII letters.
pub fn is_crs(s: &str) -> bool {
    s.len() == 3 && s.bytes().all(|b| b.is_ascii_uppercase())
}

/// 2-7 uppercase ASCII letters or digits.
pub fn is_tiploc(s: &str) -> bool {
    (2..=7).contains(&s.len())
        && s.bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
}

/// A trimmed, non-empty, all-digit code; `None` otherwise.
fn digits(v: Option<&str>) -> Option<&str> {
    let s = v?.trim();
    (!s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())).then_some(s)
}

/// The 4-digit location part of a 6-digit NLC (the last two digits
/// distinguish sub-locations of the same place). An NLC that lost its
/// leading zeros (a JSON number) is left-padded back to six digits first.
/// An NLC of more than six digits, or of all zeros, is not usable.
pub fn nlc_location(nlc: Option<&str>) -> Option<String> {
    let d = digits(nlc)?;
    if d.len() > 6 || d.bytes().all(|b| b == b'0') {
        return None;
    }
    Some(format!("{d:0>6}")[..4].to_owned())
}

/// A STANOX usable as a group key, left-padded to five digits (a JSON-number
/// STANOX lost its leading zeros): blank and all-zero STANOX values are
/// placeholders that would lump unrelated locations together.
pub fn stanox_key(stanox: Option<&str>) -> Option<String> {
    digits(stanox)
        .filter(|d| !d.bytes().all(|b| b == b'0'))
        .map(|d| format!("{d:0>5}"))
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
pub fn infer_crs_tiploc(extract: &[CorpusRow]) -> CorpusCrsTiploc {
    // crs -> directly paired tiploc -> name.
    let mut direct: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    // crs -> name, for a CRS seen on a row without a usable TIPLOC.
    let mut bare: BTreeMap<String, String> = BTreeMap::new();
    // STANOX -> every station-like 3ALPHA row there.
    let mut stations_at: BTreeMap<String, BTreeSet<Station>> = BTreeMap::new();
    let mut tiploc_rows: BTreeMap<String, Vec<TiplocRow>> = BTreeMap::new();
    let mut tiplocs_with_crs: BTreeSet<String> = BTreeSet::new();

    for row in extract {
        let crs = trimmed(row.crs.as_deref());
        let tiploc = trimmed(row.tiploc.as_deref());
        let desc = trimmed(row.nlc_desc.as_deref());
        let stanox = stanox_key(row.stanox.as_deref());
        let nlc = nlc_location(row.nlc.as_deref());

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
    CorpusCrsTiploc { rows, report }
}

/// One TIPLOC's CRS as a runtime lookup can use it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TiplocCrs {
    pub tiploc: String,
    pub crs: String,
    pub rule: Rule,
    /// The TIPLOC's STANOX when all its rows agree on one usable STANOX.
    pub stanox: Option<String>,
    /// The station's CORPUS name (the `name` column of `crs-tiploc.csv`,
    /// so a platform TIPLOC carries its station's name).
    pub station_name: String,
}

/// One STANOX's CRS as a runtime lookup can use it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StanoxCrs {
    pub stanox: String,
    pub crs: String,
    /// A TIPLOC at this STANOX with that CRS: a directly paired one when
    /// there is one, else the alphabetically first.
    pub tiploc: String,
    pub station_name: String,
}

/// The unambiguous part of [`infer_crs_tiploc`]'s result, keyed for
/// lookups. Both lists are sorted by key.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Crosswalk {
    pub tiplocs: Vec<TiplocCrs>,
    pub stanoxes: Vec<StanoxCrs>,
}

/// Narrows `inferred` (computed from `rows`) to one CRS per key:
///
/// - a TIPLOC paired with exactly one CRS (a TIPLOC CORPUS gives several
///   `3ALPHA`s is left out);
/// - a STANOX whose kept TIPLOCs (those with a single STANOX of their own)
///   all carry the same CRS. A STANOX shared by two stations is left out:
///   the same "don't guess between stations" posture as the timetable's
///   own `stanox_crs` exclusions.
pub fn crosswalk(rows: &[CorpusRow], inferred: &CorpusCrsTiploc) -> Crosswalk {
    let mut stanoxes_of: BTreeMap<&str, BTreeSet<String>> = BTreeMap::new();
    for row in rows {
        let tiploc = trimmed(row.tiploc.as_deref());
        if !is_tiploc(tiploc) {
            continue;
        }
        let entry = stanoxes_of.entry(tiploc).or_default();
        if let Some(stanox) = stanox_key(row.stanox.as_deref()) {
            entry.insert(stanox);
        }
    }
    let name_of: BTreeMap<&str, &str> = inferred
        .rows
        .iter()
        .map(|(crs, _, name)| (crs.as_str(), name.as_str()))
        .collect();
    let mut crs_of: BTreeMap<&str, Vec<(&str, Rule)>> = BTreeMap::new();
    for ((crs, tiploc), rule) in &inferred.report.pair_rules {
        crs_of
            .entry(tiploc.as_str())
            .or_default()
            .push((crs.as_str(), *rule));
    }
    let name = |crs: &str| name_of.get(crs).copied().unwrap_or_default().to_owned();

    let mut out = Crosswalk::default();
    // stanox -> crs -> [(rule, tiploc)]
    type ByCrs<'a> = BTreeMap<&'a str, Vec<(Rule, &'a str)>>;
    let mut at_stanox: BTreeMap<String, ByCrs> = BTreeMap::new();
    for (tiploc, crs_list) in &crs_of {
        let [(crs, rule)] = crs_list.as_slice() else {
            continue;
        };
        let stanox = stanoxes_of
            .get(tiploc)
            .filter(|s| s.len() == 1)
            .and_then(|s| s.iter().next().cloned());
        if let Some(stanox) = &stanox {
            at_stanox
                .entry(stanox.clone())
                .or_default()
                .entry(crs)
                .or_default()
                .push((*rule, tiploc));
        }
        out.tiplocs.push(TiplocCrs {
            tiploc: (*tiploc).to_owned(),
            crs: (*crs).to_owned(),
            rule: *rule,
            stanox,
            station_name: name(crs),
        });
    }
    for (stanox, by_crs) in at_stanox {
        let mut by_crs = by_crs.into_iter();
        let (Some((crs, mut tiplocs)), None) = (by_crs.next(), by_crs.next()) else {
            continue;
        };
        // Direct sorts before the inference rules.
        tiplocs.sort();
        if let Some((_, tiploc)) = tiplocs.first() {
            out.stanoxes.push(StanoxCrs {
                stanox,
                crs: crs.to_owned(),
                tiploc: (*tiploc).to_owned(),
                station_name: name(crs),
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(nlc: &str, stanox: &str, tiploc: &str, crs: &str, desc: &str) -> CorpusRow {
        let opt = |s: &str| Some(s.to_owned());
        CorpusRow {
            nlc: opt(nlc),
            stanox: opt(stanox),
            tiploc: opt(tiploc),
            crs: opt(crs),
            nlc_desc: opt(desc),
        }
    }

    #[test]
    fn codes_are_normalised_whatever_their_padding() {
        assert_eq!(stanox_key(Some(" 4311 ")).as_deref(), Some("04311"));
        assert_eq!(stanox_key(Some("04311")).as_deref(), Some("04311"));
        assert_eq!(stanox_key(Some("00000")), None);
        assert_eq!(stanox_key(Some(" ")), None);
        assert_eq!(stanox_key(Some("X1")), None);
        assert_eq!(nlc_location(Some("700")).as_deref(), Some("0007"));
        assert_eq!(nlc_location(Some("559513")).as_deref(), Some("5595"));
        assert_eq!(nlc_location(Some("1234567")), None);
        assert_eq!(nlc_location(Some("000000")), None);
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

    /// The crosswalk keeps one CRS per TIPLOC and per STANOX: the platform
    /// TIPLOC inherits its station's CRS and name, a TIPLOC with two direct
    /// CRS codes and a STANOX shared by two stations are dropped.
    #[test]
    fn the_crosswalk_keeps_only_unambiguous_keys() {
        let rows = vec![
            row(
                "559500",
                "87219",
                "CLPHMJN",
                "CLJ",
                "CLAPHAM JUNCTION LONDON",
            ),
            row("559572", "87219", "CLPHMJW", " ", "CLAPHAM JN (WINDSOR)"),
            row("559595", "87219", "CLPHJLP", " ", "CLAPHAM JUNCTION LOOP"),
            row("100000", "11111", "TWOCRS", "AAA", "A"),
            row("100001", "11112", "TWOCRS", "BBB", "B"),
            row("200000", "22222", "ASHFKY", "AFK", "ASHFORD INTERNATIONAL"),
            row("200001", "22222", "ASHFKI", "ASI", "ASHFORD INTL EUROSTAR"),
            row("300000", " ", "NOSTNX", "NST", "NO STANOX"),
        ];
        let inferred = infer_crs_tiploc(&rows);
        let cw = crosswalk(&rows, &inferred);
        let tiplocs: Vec<(&str, &str, Rule, Option<&str>, &str)> = cw
            .tiplocs
            .iter()
            .map(|t| {
                (
                    t.tiploc.as_str(),
                    t.crs.as_str(),
                    t.rule,
                    t.stanox.as_deref(),
                    t.station_name.as_str(),
                )
            })
            .collect();
        assert_eq!(
            tiplocs,
            vec![
                (
                    "ASHFKI",
                    "ASI",
                    Rule::Direct,
                    Some("22222"),
                    "ASHFORD INTL EUROSTAR"
                ),
                (
                    "ASHFKY",
                    "AFK",
                    Rule::Direct,
                    Some("22222"),
                    "ASHFORD INTERNATIONAL"
                ),
                (
                    "CLPHMJN",
                    "CLJ",
                    Rule::Direct,
                    Some("87219"),
                    "CLAPHAM JUNCTION LONDON"
                ),
                (
                    "CLPHMJW",
                    "CLJ",
                    Rule::StationName,
                    Some("87219"),
                    "CLAPHAM JUNCTION LONDON"
                ),
                ("NOSTNX", "NST", Rule::Direct, None, "NO STANOX"),
            ]
        );
        // 22222 has two stations; 11111/11112 only a dropped TIPLOC.
        assert_eq!(
            cw.stanoxes,
            vec![StanoxCrs {
                stanox: "87219".to_owned(),
                crs: "CLJ".to_owned(),
                tiploc: "CLPHMJN".to_owned(),
                station_name: "CLAPHAM JUNCTION LONDON".to_owned(),
            }]
        );
    }

    #[test]
    fn nothing_in_nothing_out() {
        let inferred = infer_crs_tiploc(&[]);
        assert!(inferred.rows.is_empty());
        assert_eq!(crosswalk(&[], &inferred), Crosswalk::default());
    }
}
