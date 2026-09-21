//! Loading the "source of truth" this validator checks `lines/*.toml`
//! against, for both tiers described in `main.rs`'s module doc:
//! [`ReferenceData::from_vendored_csvs`] (fast, no-secrets tier, reads the
//! checked-in `reference-data/crs-tiploc.csv` / `reference-data/toc-codes.csv`)
//! and [`ReferenceData::fetch_live`] (thorough tier, hits the same
//! upstream site live over the network, plus the real RDM TOC feed when
//! credentials are available).
//!
//! Both tiers build the exact same [`ReferenceData`] shape, so
//! `checks::validate_lines`/`checks::coverage_report` have no idea which
//! tier produced the data they're checking against -- the only thing that
//! differs between "fast" and "thorough" is how this struct gets filled
//! in, matching this crate's whole reason for existing as a two-tier
//! design (see `main.rs`).

use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

/// Everything this validator needs to know about the outside world:
/// which CRS codes are real, which TIPLOCs each one is known to pair
/// with, a display name for each CRS (informational only -- see
/// `reference-data/line-catalogue-validation.md`'s "What this
/// deliberately does not check" section), and which ATOC operator codes
/// are currently valid.
#[derive(Debug, Default, Clone)]
pub struct ReferenceData {
    /// CRS -> every TIPLOC seen paired with it. An empty set means the CRS
    /// is known-real but no TIPLOC pairing could be confirmed for it (see
    /// the vendored CSV's provenance doc) -- callers must not treat an
    /// empty set as "this CRS has no valid TIPLOC", only as "this source
    /// can't confirm one either way".
    pub crs_to_tiploc: HashMap<String, HashSet<String>>,
    /// CRS -> a human-readable name, for error messages and the
    /// coverage-gap report only. Never compared against a station's own
    /// inline TOML comment (see the provenance doc).
    pub crs_to_name: HashMap<String, String>,
    /// Every currently-valid ATOC operator code -> operator name.
    pub toc_codes: HashMap<String, String>,
}

impl ReferenceData {
    pub fn known_crs(&self, crs: &str) -> bool {
        self.crs_to_tiploc.contains_key(crs)
    }

    pub fn known_operator(&self, code: &str) -> bool {
        self.toc_codes.contains_key(code)
    }

    /// `None` means this CRS isn't known at all (a separate, harder
    /// failure -- see `checks::validate_lines`). `Some(true)` means the
    /// given TIPLOC is a confirmed pairing for that CRS, or no TIPLOC
    /// pairing at all could be confirmed for the CRS (see the doc on
    /// `crs_to_tiploc` -- deliberately not treated as a mismatch).
    pub fn tiploc_matches(&self, crs: &str, tiploc: &str) -> Option<bool> {
        let known = self.crs_to_tiploc.get(crs)?;
        if known.is_empty() {
            return Some(true);
        }
        Some(known.contains(&tiploc.to_ascii_uppercase()))
    }

    /// Loads the fast, no-secrets tier's reference dataset from the two
    /// CSVs vendored in `reference-data/` (see
    /// `reference-data/line-catalogue-validation.md` for exactly how they
    /// were generated and their documented limitations).
    pub fn from_vendored_csvs(crs_tiploc_csv: &Path, toc_codes_csv: &Path) -> Result<Self> {
        let mut data = ReferenceData::default();

        #[derive(Deserialize)]
        struct CrsTiplocRow {
            crs: String,
            tiploc: String,
            name: String,
        }
        let mut rdr = csv::Reader::from_path(crs_tiploc_csv)
            .with_context(|| format!("reading {}", crs_tiploc_csv.display()))?;
        for row in rdr.deserialize::<CrsTiplocRow>() {
            let row = row.with_context(|| format!("parsing {}", crs_tiploc_csv.display()))?;
            let crs = row.crs.trim().to_ascii_uppercase();
            data.crs_to_name.entry(crs.clone()).or_insert(row.name);
            let entry = data.crs_to_tiploc.entry(crs).or_default();
            let tiploc = row.tiploc.trim().to_ascii_uppercase();
            if !tiploc.is_empty() {
                entry.insert(tiploc);
            }
        }

        #[derive(Deserialize)]
        struct TocRow {
            atoc_code: String,
            name: String,
        }
        let mut rdr = csv::Reader::from_path(toc_codes_csv)
            .with_context(|| format!("reading {}", toc_codes_csv.display()))?;
        for row in rdr.deserialize::<TocRow>() {
            let row = row.with_context(|| format!("parsing {}", toc_codes_csv.display()))?;
            data.toc_codes
                .insert(row.atoc_code.trim().to_ascii_uppercase(), row.name);
        }

        Ok(data)
    }

    /// Thorough, live tier: re-fetches the exact same
    /// railwaycodes.org.uk pages the vendored CSVs were generated from
    /// (see `reference-data/line-catalogue-validation.md`), applying the
    /// identical extraction rules documented there, plus -- when
    /// `rdm_api_key`/`rdm_tocs_base_url` are both given -- the real RDM
    /// Train Operating Company List feed (RSPS5050 P-03-00 Rev A §3, the
    /// same feed `crates/poller-tocs` polls in production) as a more
    /// authoritative operator-code source than the community site.
    ///
    /// This hits the network ~27 times (26 CRS pages + 1 TOC page, plus
    /// one more for the RDM feed if configured) and is deliberately not
    /// used by the fast/CI-blocking tier -- see `main.rs`.
    pub async fn fetch_live(
        client: &reqwest::Client,
        rdm_api_key: Option<&str>,
        rdm_tocs_base_url: Option<&str>,
    ) -> Result<Self> {
        let mut data = ReferenceData::default();

        for letter in b'a'..=b'z' {
            let letter = letter as char;
            let url = format!("https://www.railwaycodes.org.uk/crs/crs{letter}.shtm");
            let body = fetch_with_browser_ua(client, &url)
                .await
                .with_context(|| format!("fetching {url}"))?;
            parse_crs_tiploc_page(&body, &mut data);
        }

        let toc_url = "https://www.railwaycodes.org.uk/operators/toccodes.shtm";
        let toc_body = fetch_with_browser_ua(client, toc_url)
            .await
            .with_context(|| format!("fetching {toc_url}"))?;
        data.toc_codes = parse_current_toc_codes(&toc_body);

        if let (Some(api_key), Some(base_url)) = (rdm_api_key, rdm_tocs_base_url) {
            match crate::rdm_toc::fetch_rdm_tocs(client, base_url, api_key).await {
                Ok(rdm_tocs) => {
                    // The real RDM feed supersedes the community-site
                    // scrape above wherever it has data -- it's this
                    // app's own already-integrated authoritative source
                    // (see `crates/poller-tocs`).
                    data.toc_codes = rdm_tocs;
                }
                Err(err) => {
                    eprintln!(
                        "warning: RDM TOC feed fetch failed ({err:#}); falling back to the \
                         railwaycodes.org.uk scrape for operator codes"
                    );
                }
            }
        }

        Ok(data)
    }
}

async fn fetch_with_browser_ua(client: &reqwest::Client, url: &str) -> Result<String> {
    // railwaycodes.org.uk 403s a request with no `User-Agent` at all
    // (confirmed while building this validator) -- a plain browser-shaped
    // UA is enough, no other headers are needed.
    let resp = client
        .get(url)
        .header(
            "User-Agent",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        )
        .send()
        .await?
        .error_for_status()?;
    Ok(resp.text().await?)
}

/// Extraction rules mirrored exactly from
/// `reference-data/line-catalogue-validation.md`'s "Where the data comes
/// from" section -- keep the two in sync if either changes.
fn parse_crs_tiploc_page(html: &str, data: &mut ReferenceData) {
    let row_re = regex::Regex::new(r"(?s)<tr>(.*?)</tr>").unwrap();
    let cell_re = regex::Regex::new(r"(?s)<td[^>]*>(.*?)</td>").unwrap();
    let tag_re = regex::Regex::new(r"<[^>]+>").unwrap();
    let crs_token_re = regex::Regex::new(r"^[A-Z]{3}$").unwrap();
    let tiploc_token_re = regex::Regex::new(r"^[A-Z0-9]{2,7}$").unwrap();

    for row_caps in row_re.captures_iter(html) {
        let cells: Vec<&str> = cell_re
            .captures_iter(&row_caps[1])
            .map(|c| c.get(1).unwrap().as_str())
            .collect();
        if cells.len() != 6 {
            continue;
        }
        let name = clean_html_text(&tag_re, cells[0]);
        let crs_list: Vec<String> = tag_re
            .replace_all(cells[1], " ")
            .split_whitespace()
            .map(|t| t.to_ascii_uppercase())
            .filter(|t| crs_token_re.is_match(t))
            .collect();
        let tiploc_list: Vec<String> = tag_re
            .replace_all(cells[3], " ")
            .split_whitespace()
            .map(|t| t.to_ascii_uppercase())
            .filter(|t| tiploc_token_re.is_match(t))
            .collect();
        if crs_list.is_empty() {
            continue;
        }
        for crs in crs_list {
            data.crs_to_name
                .entry(crs.clone())
                .or_insert_with(|| name.clone());
            let entry = data.crs_to_tiploc.entry(crs).or_default();
            for tiploc in &tiploc_list {
                entry.insert(tiploc.clone());
            }
        }
    }
}

fn parse_current_toc_codes(html: &str) -> HashMap<String, String> {
    let row_re = regex::Regex::new(r"(?s)<tr>(.*?)</tr>").unwrap();
    let cell_re = regex::Regex::new(r"(?s)<td[^>]*>(.*?)</td>").unwrap();
    let tag_re = regex::Regex::new(r"<[^>]+>").unwrap();
    let comment_re = regex::Regex::new(r"(?s)<!--.*?-->").unwrap();

    let mut out = HashMap::new();
    for row_caps in row_re.captures_iter(html) {
        let cells: Vec<&str> = cell_re
            .captures_iter(&row_caps[1])
            .map(|c| c.get(1).unwrap().as_str())
            .collect();
        if cells.len() < 3 || !cells[2].contains("to date") {
            continue;
        }
        let code = clean_html_text(&tag_re, cells[0]);
        let without_comments = comment_re.replace_all(cells[1], "");
        let name = clean_html_text(&tag_re, &without_comments);
        out.insert(code.to_ascii_uppercase(), name);
    }
    out
}

fn clean_html_text(tag_re: &regex::Regex, s: &str) -> String {
    let stripped = tag_re.replace_all(s, "");
    let decoded = stripped
        .replace("&amp;", "&")
        .replace("&#x2716;", "")
        .replace("&nbsp;", " ");
    decoded.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_real_crs_tiploc_row_shape() {
        let html = r#"<table><tr><td>Euston</td><td>EUS</td><td>512900</td>
            <td>EUSTON</td><td>EUSTON</td><td>72410</td></tr></table>"#;
        let mut data = ReferenceData::default();
        parse_crs_tiploc_page(html, &mut data);
        assert!(data.known_crs("EUS"));
        assert_eq!(data.tiploc_matches("EUS", "EUSTON"), Some(true));
        assert_eq!(data.tiploc_matches("EUS", "BOGUS"), Some(false));
        assert_eq!(data.crs_to_name.get("EUS"), Some(&"Euston".to_string()));
    }

    #[test]
    fn a_crs_with_no_tiploc_recorded_is_still_known_and_never_mismatches() {
        let html = r#"<table><tr><td>Somewhere</td><td>SMW</td><td>1</td>
            <td></td><td></td><td></td></tr></table>"#;
        let mut data = ReferenceData::default();
        parse_crs_tiploc_page(html, &mut data);
        assert!(data.known_crs("SMW"));
        assert_eq!(data.tiploc_matches("SMW", "ANYTHING"), Some(true));
    }

    #[test]
    fn only_to_date_toc_rows_are_kept() {
        let html = r#"<table>
            <tr><td>AN</td><td>Arriva Trains Northern</td><td>2001 to 2004</td></tr>
            <tr><td>NT</td><td>Northern Trains <em>Northern</em></td><td>2016 to date</td></tr>
        </table>"#;
        let tocs = parse_current_toc_codes(html);
        assert!(!tocs.contains_key("AN"));
        assert_eq!(
            tocs.get("NT"),
            Some(&"Northern Trains Northern".to_string())
        );
    }
}
