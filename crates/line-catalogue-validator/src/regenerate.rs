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
/// every field is read as an optional string and trimmed. Only the three
/// fields this generator uses are declared; the rest (`NLC`, `STANOX`,
/// `UIC`, `NLCDESC16`) are ignored.
#[derive(Debug, Deserialize)]
struct CorpusRow {
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

fn is_crs(s: &str) -> bool {
    s.len() == 3 && s.bytes().all(|b| b.is_ascii_uppercase())
}

fn is_tiploc(s: &str) -> bool {
    (2..=7).contains(&s.len())
        && s.bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
}

/// Turns a CORPUS extract into `crs-tiploc.csv` rows, applying the same
/// output contract the railwaycodes-derived file had:
///
/// - a row is used only if its `3ALPHA` is exactly three uppercase letters
///   (CORPUS leaves it blank for the great majority of locations, which
///   have no CRS);
/// - its `TIPLOC` is kept only if it is 2-7 uppercase letters/digits;
/// - one output row per distinct `(crs, tiploc)` pair, and one row with an
///   empty `tiploc` for a CRS that no row pairs with a valid TIPLOC;
/// - `name` is the `NLCDESC` of the lexicographically-first TIPLOC row for
///   that CRS (or of the first row at all when there's no TIPLOC), so the
///   output does not depend on CORPUS's row order;
/// - sorted by `crs` then `tiploc`.
pub fn crs_tiploc_from_corpus(json: &[u8]) -> Result<Vec<CrsTiplocRow>> {
    let extract: CorpusExtract =
        serde_json::from_slice(json).context("parsing CORPUS extract JSON")?;

    // crs -> tiploc -> name (BTreeMap, so the first key is the smallest).
    let mut pairs: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    // crs -> name, for a CRS seen only without a usable TIPLOC.
    let mut bare: BTreeMap<String, String> = BTreeMap::new();

    for row in extract.tiploc_data {
        let crs = row.three_alpha.as_deref().unwrap_or("").trim();
        if !is_crs(crs) {
            continue;
        }
        let name = row.nlc_desc.as_deref().unwrap_or("").trim().to_owned();
        let tiploc = row.tiploc.as_deref().unwrap_or("").trim();
        if is_tiploc(tiploc) {
            pairs
                .entry(crs.to_owned())
                .or_default()
                .entry(tiploc.to_owned())
                .or_insert(name);
        } else {
            bare.entry(crs.to_owned()).or_insert(name);
        }
    }

    let crs_codes: BTreeSet<&String> = pairs.keys().chain(bare.keys()).collect();
    let mut out = Vec::new();
    for crs in crs_codes {
        match pairs.get(crs) {
            Some(tiplocs) => {
                let name = tiplocs.values().next().cloned().unwrap_or_default();
                for tiploc in tiplocs.keys() {
                    out.push((crs.clone(), tiploc.clone(), name.clone()));
                }
            }
            None => out.push((crs.clone(), String::new(), bare[crs].clone())),
        }
    }
    if out.is_empty() {
        bail!(
            "CORPUS extract yielded zero CRS codes -- not a plausible real extract (wrong file, \
             or the TIPLOCDATA/3ALPHA field names changed); refusing to write an empty file"
        );
    }
    Ok(out)
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
        let rows = crs_tiploc_from_corpus(json).unwrap();
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
        let rows = crs_tiploc_from_corpus(json).unwrap();
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
