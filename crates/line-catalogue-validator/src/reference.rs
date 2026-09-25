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

use anyhow::{Context, Result, bail};
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
            parse_crs_tiploc_page(&body, &mut data).with_context(|| format!("parsing {url}"))?;
        }

        let toc_url = "https://www.railwaycodes.org.uk/operators/toccodes.shtm";
        let toc_body = fetch_with_browser_ua(client, toc_url)
            .await
            .with_context(|| format!("fetching {toc_url}"))?;
        data.toc_codes =
            parse_current_toc_codes(&toc_body).with_context(|| format!("parsing {toc_url}"))?;

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

/// railwaycodes.org.uk annotates individual codes (and location names) with
/// a click-to-open footnote, marked up as a self-contained three-deep span:
///
/// ```text
/// <span class="popup" onclick="popup26()"><span class="popuptext" id="myPopup26"
///   ><span class="close">&#x2716;</span>Original code</span></span>
/// ```
///
/// **This markup MUST be removed before any code token is extracted from a
/// cell**, because the note is free English prose sitting *inside* the same
/// `<td>` as the real code. Merely stripping tags (which is all this parser
/// used to do) leaves the prose behind, and uppercasing it turns ordinary
/// words into things that pass the CRS/TIPLOC token filters --
/// `"Original code"` yields a TIPLOC `CODE`, `"See CRS explanation"` yields
/// CRS codes `SEE` *and* `CRS`, and `"Code not certain; conflicting raw
/// data"` yields a CRS `RAW` that has never been issued to anything. That
/// is not hypothetical: it is exactly how 340 junk rows (and one entirely
/// fabricated CRS code) got into `reference-data/crs-tiploc.csv`; see that
/// file's provenance doc, `reference-data/line-catalogue-validation.md`,
/// under "Where the data comes from".
///
/// This strips only *this* specific popup shape -- an HTML comment
/// (`<!--...-->`) sitting in the same cell is a different markup shape
/// carrying the exact same class of risk (unstripped prose surviving into
/// a code token), and is stripped separately by
/// [`extract_shape_valid_tokens`], the same way `parse_current_toc_codes`
/// already stripped comments (just not, until now, popups) before this fix.
///
/// The tail is `</span>\s*</span>`, not the stricter `</span></span>` this
/// function shipped with initially: that stricter form silently strips
/// nothing at all the moment the site ever emits so much as a newline
/// between the two closing tags, which is indistinguishable from "no
/// popups on this page" -- a silent regression back to exactly the bug
/// described above, with no signal it happened. The self-check below is
/// the other half of that guard: even with the tolerant tail, some future
/// markup shape (a differently-nested popup, or a single-`<span>` popup)
/// could still slip past unstripped, so this function refuses to return
/// success in that case -- see its `bail!` below -- rather than silently
/// handing prose-contaminated text back to a caller that has no way to
/// know stripping failed.
fn strip_popups(popup_re: &regex::Regex, cell: &str) -> Result<String> {
    let stripped = popup_re.replace_all(cell, " ").into_owned();
    if stripped.contains(r#"class="popup""#) {
        bail!(
            "popup markup survived stripping -- the site's markup shape has drifted from what \
             `strip_popups`'s regex expects, so this cell may still carry footnote prose that \
             would otherwise be silently scraped as a fabricated code; refusing to proceed \
             rather than risk that. Cell after the failed strip attempt: {stripped:?}"
        );
    }
    Ok(stripped)
}

/// Strips popup footnote markup ([`strip_popups`]) and HTML comments from
/// `cell`, strips remaining tags, then keeps only whitespace-separated
/// tokens that already match `shape_re` **in their original case** -- the
/// shape check runs *before* uppercasing, not after.
///
/// That ordering is the fix for the second instance of this morning's bug
/// class: a leftover word from unstripped prose (an HTML comment's "see
/// note", a popup's "now closed", ...) is lowercase in the source markup,
/// so checking it against an uppercase-only shape pattern (`^[A-Z]{3}$` for
/// a CRS, `^[A-Z]{2}$` for a TOC code) rejects it outright. Only *after* a
/// token has already proven itself shape-valid is it uppercased -- which is
/// a no-op for every real code, since railwaycodes.org.uk always renders
/// genuine codes in uppercase in its own markup already. Filtering after
/// uppercasing (the old order) is what let `<!-- see note -->` mint a
/// fabricated CRS `SEE`: uppercasing turned "see" into "SEE" *before* the
/// shape check ran, and "SEE" passes `^[A-Z]{3}$` same as a real code would.
fn extract_shape_valid_tokens(
    popup_re: &regex::Regex,
    comment_re: &regex::Regex,
    tag_re: &regex::Regex,
    shape_re: &regex::Regex,
    cell: &str,
) -> Result<Vec<String>> {
    let no_popups = strip_popups(popup_re, cell)?;
    let no_comments = comment_re.replace_all(&no_popups, " ");
    let no_tags = tag_re.replace_all(&no_comments, " ");
    Ok(no_tags
        .split_whitespace()
        .filter(|t| shape_re.is_match(t))
        .map(|t| t.to_ascii_uppercase())
        .collect())
}

/// Extraction rules mirrored exactly from
/// `reference-data/line-catalogue-validation.md`'s "Where the data comes
/// from" section -- keep the two in sync if either changes.
fn parse_crs_tiploc_page(html: &str, data: &mut ReferenceData) -> Result<()> {
    let row_re = regex::Regex::new(r"(?s)<tr>(.*?)</tr>").unwrap();
    let cell_re = regex::Regex::new(r"(?s)<td[^>]*>(.*?)</td>").unwrap();
    let tag_re = regex::Regex::new(r"<[^>]+>").unwrap();
    let comment_re = regex::Regex::new(r"(?s)<!--.*?-->").unwrap();
    // Non-greedy, so two footnotes in one cell (a location with an "Earlier
    // code"/"Later code" pair, e.g. Worcestershire Parkway High Level) are
    // stripped as two separate matches rather than one run swallowing the
    // real code between them. The `\s*` tolerates whitespace between the
    // two closing `</span>` tags -- see `strip_popups`'s doc.
    let popup_re = regex::Regex::new(r#"(?s)<span class="popup".*?</span>\s*</span>"#).unwrap();
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
        let name = clean_html_text(&tag_re, &strip_popups(&popup_re, cells[0])?);
        let crs_list =
            extract_shape_valid_tokens(&popup_re, &comment_re, &tag_re, &crs_token_re, cells[1])?;
        let tiploc_list = extract_shape_valid_tokens(
            &popup_re,
            &comment_re,
            &tag_re,
            &tiploc_token_re,
            cells[3],
        )?;
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
    Ok(())
}

/// Extracts the currently-valid ATOC operator code table. Active whenever
/// `RDM_API_KEY`/`RDM_TOCS_BASE_URL` aren't both configured (see
/// `ReferenceData::fetch_live`) -- i.e. the default state of this
/// validator's live tier, since the RDM feed's base URL has no known value
/// yet (`rdm_toc.rs`'s module doc). Applies the exact same popup-stripping
/// and shape-validated token extraction as `parse_crs_tiploc_page` (see
/// [`extract_shape_valid_tokens`]): a footnote on the code cell can no
/// longer mint a compound garbage key, and a footnote's stray "to date" in
/// a defunct operator's date cell can no longer resurrect it as currently
/// valid, because the "is this row current" check below runs against the
/// stripped cell text, not the raw HTML.
fn parse_current_toc_codes(html: &str) -> Result<HashMap<String, String>> {
    let row_re = regex::Regex::new(r"(?s)<tr>(.*?)</tr>").unwrap();
    let cell_re = regex::Regex::new(r"(?s)<td[^>]*>(.*?)</td>").unwrap();
    let tag_re = regex::Regex::new(r"<[^>]+>").unwrap();
    let comment_re = regex::Regex::new(r"(?s)<!--.*?-->").unwrap();
    let popup_re = regex::Regex::new(r#"(?s)<span class="popup".*?</span>\s*</span>"#).unwrap();
    let toc_token_re = regex::Regex::new(r"^[A-Z]{2}$").unwrap();

    let mut out = HashMap::new();
    for row_caps in row_re.captures_iter(html) {
        let cells: Vec<&str> = cell_re
            .captures_iter(&row_caps[1])
            .map(|c| c.get(1).unwrap().as_str())
            .collect();
        if cells.len() < 3 {
            continue;
        }

        // "Currently valid" must be decided from the STRIPPED date cell,
        // never raw HTML: a footnote (popup or HTML comment) on this cell
        // could otherwise carry the literal phrase "to date" -- e.g. "see
        // note, dates uncertain to date of writing" -- and wrongly
        // resurrect a long-defunct operator as currently valid.
        let no_popups = strip_popups(&popup_re, cells[2])?;
        let no_comments = comment_re.replace_all(&no_popups, " ");
        let valid_period = clean_html_text(&tag_re, &no_comments);
        if !valid_period.contains("to date") {
            continue;
        }

        // The code cell gets the same shape-validated extraction as CRS/
        // TIPLOC cells: a footnote here must not be able to turn "GW" plus
        // footnote prose into a compound garbage key, or a bare footnote
        // word into a fabricated 2-letter operator code.
        let code_tokens =
            extract_shape_valid_tokens(&popup_re, &comment_re, &tag_re, &toc_token_re, cells[0])?;
        let Some(code) = code_tokens.into_iter().next() else {
            continue;
        };

        let no_popups = strip_popups(&popup_re, cells[1])?;
        let without_comments = comment_re.replace_all(&no_popups, " ");
        let name = clean_html_text(&tag_re, &without_comments);
        out.insert(code, name);
    }
    Ok(out)
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
        parse_crs_tiploc_page(html, &mut data).unwrap();
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
        parse_crs_tiploc_page(html, &mut data).unwrap();
        assert!(data.known_crs("SMW"));
        assert_eq!(data.tiploc_matches("SMW", "ANYTHING"), Some(true));
    }

    /// Real markup, copied byte-for-byte from the live
    /// `https://www.railwaycodes.org.uk/crs/crs{a,g,m}.shtm` pages
    /// (2026-09-24), for the three footnote shapes that actually corrupted
    /// `reference-data/crs-tiploc.csv`: a footnote on the TIPLOC cell
    /// ("Original code" -> junk TIPLOC `CODE`, 242 rows), a footnote on the
    /// CRS cell ("See CRS explanation" -> junk CRS `SEE` and `CRS`, 54
    /// rows), and a footnote carrying a word that is itself a valid-looking
    /// CRS token ("conflicting raw data" -> the entirely fabricated CRS
    /// `RAW`). See [`strip_popups`].
    #[test]
    fn popup_footnotes_are_never_scraped_as_codes() {
        let html = r#"<table>
  <tr>
   <td>Abbey Wood</td>
   <td>ABW</td>
   <td>513100</td>
   <td>ABWD
ABBEYWD<span class="popup" onclick="popup26()"><span class="popuptext" id="myPopup26"><span class="close">&#x2716;</span>Original code</span></span>
</td>
   <td>ABBEYWOOD</td>
   <td>88601</td>
  </tr>
  <tr>
   <td>Glasgow Central High Level</td>
   <td>GLC<span class="popup" onclick="popup8()"><span class="popuptext" id="myPopup8"><span class="close">&#x2716;</span>See <a href="crs2.shtm">CRS explanation</a></span></span></td>
   <td>981300</td>
   <td>GLGC</td>
   <td>GLASGOW C</td>
   <td>07257</td>
  </tr>
  <tr>
   <td>Muck</td>
   <td>MUK MUC<span class="popup" onclick="popup2()"><span class="popuptext" id="myPopup2"><span class="close">&#x2716;</span>Code not certain; conflicting raw data</span></span></td>
   <td>906100</td>
   <td>MUCK</td>
   <td class="noshow"></td>
   <td>-</td>
  </tr>
</table>"#;
        let mut data = ReferenceData::default();
        parse_crs_tiploc_page(html, &mut data).unwrap();

        // The real codes on each row still parse, unchanged.
        assert_eq!(data.tiploc_matches("ABW", "ABWD"), Some(true));
        assert_eq!(data.tiploc_matches("ABW", "ABBEYWD"), Some(true));
        assert_eq!(data.tiploc_matches("GLC", "GLGC"), Some(true));
        assert_eq!(data.tiploc_matches("MUC", "MUCK"), Some(true));
        assert_eq!(data.tiploc_matches("MUK", "MUCK"), Some(true));

        // None of the footnote prose survives as a code.
        assert_eq!(data.tiploc_matches("ABW", "CODE"), Some(false));
        for fabricated in ["SEE", "CRS", "RAW", "NOT"] {
            assert!(
                !data.known_crs(fabricated),
                "{fabricated} is footnote prose, not a CRS code on any of these rows"
            );
        }

        // ...and the location name is the location, not the location plus
        // whatever its footnote says.
        assert_eq!(
            data.crs_to_name.get("GLC"),
            Some(&"Glasgow Central High Level".to_string())
        );
    }

    /// Regression guard on the committed snapshot itself, not just on the
    /// parser: the vendored CSV must not carry the footnote artifacts
    /// described in [`strip_popups`]. Reads the real
    /// `reference-data/crs-tiploc.csv` so a future hand-regeneration that
    /// forgets to strip footnotes fails here instead of silently shipping.
    #[test]
    fn vendored_crs_tiploc_snapshot_carries_no_footnote_artifacts() {
        let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .expect("crate lives at <repo>/crates/line-catalogue-validator");
        let data = ReferenceData::from_vendored_csvs(
            &repo_root.join("reference-data/crs-tiploc.csv"),
            &repo_root.join("reference-data/toc-codes.csv"),
        )
        .expect("the vendored reference CSVs must load");

        // `RAW` only ever existed as the word "raw" inside the note "Code
        // not certain; conflicting raw data" on railwaycodes.org.uk's Muck
        // row -- it is not a CRS code that has ever been issued.
        assert!(
            !data.known_crs("RAW"),
            "RAW is footnote prose, never an issued CRS code"
        );

        // No real TIPLOC is shared by a large number of unrelated CRS
        // codes; a token that is, is footnote prose (`CODE` reached 242).
        // Genuine multi-CRS TIPLOCs top out at 4 in this snapshot (e.g.
        // CANWHRF: CWF/CWX/ZCW/ZQC), so 6 is a comfortable ceiling that
        // still catches the artifact class by two orders of magnitude.
        let mut tiploc_to_crs: HashMap<&str, Vec<&str>> = HashMap::new();
        for (crs, tiplocs) in &data.crs_to_tiploc {
            for tiploc in tiplocs {
                tiploc_to_crs.entry(tiploc).or_default().push(crs);
            }
        }
        for (tiploc, crs_codes) in &tiploc_to_crs {
            assert!(
                crs_codes.len() <= 6,
                "TIPLOC {tiploc} is claimed by {} CRS codes ({crs_codes:?}) -- almost \
                 certainly a railwaycodes.org.uk footnote scraped as a code; see \
                 reference-data/line-catalogue-validation.md",
                crs_codes.len()
            );
        }

        // The `name` column must not carry a footnote's close-button glyph
        // or the note text that follows it.
        for (crs, name) in &data.crs_to_name {
            assert!(
                !name.contains('\u{2716}'),
                "name for {crs} carries a footnote close-button glyph: {name:?}"
            );
        }
    }

    #[test]
    fn only_to_date_toc_rows_are_kept() {
        let html = r#"<table>
            <tr><td>AN</td><td>Arriva Trains Northern</td><td>2001 to 2004</td></tr>
            <tr><td>NT</td><td>Northern Trains <em>Northern</em></td><td>2016 to date</td></tr>
        </table>"#;
        let tocs = parse_current_toc_codes(html).unwrap();
        assert!(!tocs.contains_key("AN"));
        assert_eq!(
            tocs.get("NT"),
            Some(&"Northern Trains Northern".to_string())
        );
    }

    /// Finding #2's repro: an HTML comment sitting in a code cell is a
    /// different markup shape from the popup spans
    /// `popup_footnotes_are_never_scraped_as_codes` covers, but the same
    /// bug class -- prose surviving into a code token.
    ///
    /// Two rows, because they exercise the fix's two separate halves:
    ///
    /// - Row 1 is finding #2's literal `<!-- see note -->` example. It
    ///   happens to *not* trip the pre-fix code, purely by accident of how
    ///   `tag_re`'s `<[^>]+>` is written: with no `>` anywhere inside the
    ///   comment, `<[^>]+>` greedily matches the *entire* `<!-- see note
    ///   -->` span (delimiters and text both) as a single "tag" and erases
    ///   it whole, incidentally masking the missing-comment-handling bug
    ///   for this one shape. It's kept here verbatim (per the review
    ///   finding) as a floor -- this exact string must keep working -- but
    ///   it does not by itself prove the fix does anything.
    /// - Row 2 is what actually reproduces the bug pre-fix, confirmed
    ///   empirically against the unpatched code before this fix landed: a
    ///   footnote comment containing so much as one inline `<a>` (the same
    ///   "See <a href=...>CRS explanation</a>" wording the real popup
    ///   fixtures above use, just inside a `<!--...-->` instead of a
    ///   `<span class="popup">`). The embedded tag's own `>` makes
    ///   `tag_re` stop matching partway through the comment, so
    ///   "CRS"/"explanation" survive as literal leftover text -- which the
    ///   pre-fix uppercase-then-filter order turns into a fabricated CRS
    ///   `CRS`, indistinguishable from the real Ely branch's genuine `CRS`
    ///   TIPLOC-turned-CRS-lookalike. `comment_re` stripping the whole
    ///   comment as one opaque unit (rather than relying on `tag_re`'s
    ///   incidental, shape-dependent behaviour) is what actually closes
    ///   this.
    #[test]
    fn html_comments_in_a_code_cell_are_never_scraped_as_codes() {
        let html = r#"<table>
  <tr>
   <td>Glasgow Central High Level</td>
   <td>GLC <!-- see note --></td>
   <td>981300</td>
   <td>GLGC</td>
   <td>GLASGOW C</td>
   <td>07257</td>
  </tr>
  <tr>
   <td>Muck</td>
   <td>MUC <!-- see <a href="crs2.shtm">CRS explanation</a> --></td>
   <td>906100</td>
   <td>MUCK</td>
   <td class="noshow"></td>
   <td>-</td>
  </tr>
</table>"#;
        let mut data = ReferenceData::default();
        parse_crs_tiploc_page(html, &mut data).unwrap();

        assert!(
            data.known_crs("GLC"),
            "the real code must still parse despite the trailing comment"
        );
        assert!(
            data.known_crs("MUC"),
            "the real code must still parse despite the trailing comment"
        );
        for fabricated in ["SEE", "NOTE", "CRS"] {
            assert!(
                !data.known_crs(fabricated),
                "{fabricated} is comment prose, not a CRS code on either of these rows -- it \
                 must be rejected by the shape check while still lowercase (or, for \"CRS\", \
                 stripped as part of the opaque comment span), not uppercased into a false \
                 match first"
            );
        }
    }

    /// Finding #3: `parse_current_toc_codes` runs whenever the RDM TOC feed
    /// isn't configured (`ReferenceData::fetch_live`, `rdm_toc.rs`'s module
    /// doc) -- i.e. this crate's actual default live-tier code path, not a
    /// hypothetical. Two failure modes in one fixture: a footnote on a
    /// *currently valid* operator's code cell must not corrupt its code
    /// into a compound garbage key, and a footnote's stray "to date" text
    /// on a *defunct* operator's date cell must not resurrect it as
    /// currently valid.
    #[test]
    fn toc_code_parsing_strips_popups_and_checks_validity_on_stripped_text() {
        let html = r#"<table>
  <tr>
   <td>GW<span class="popup" onclick="popup1()"><span class="popuptext" id="myPopup1"><span class="close">&#x2716;</span>Formerly First Great Western</span></span></td>
   <td>Great Western Railway</td>
   <td>2015 to date</td>
  </tr>
  <tr>
   <td>AN</td>
   <td>Arriva Trains Northern</td>
   <td>2001 to 2004<span class="popup" onclick="popup2()"><span class="popuptext" id="myPopup2"><span class="close">&#x2716;</span>Records patchy to date</span></span></td>
  </tr>
</table>"#;
        let tocs = parse_current_toc_codes(html).unwrap();

        // The currently-valid operator's real code still parses, with no
        // footnote prose appended to it.
        assert_eq!(tocs.get("GW"), Some(&"Great Western Railway".to_string()));

        // The defunct operator must NOT be resurrected by its footnote's
        // unrelated "to date" -- the validity check must run against the
        // stripped cell ("2001 to 2004 "), not the raw HTML that also
        // contains the footnote's "Records patchy to date".
        assert!(
            !tocs.contains_key("AN"),
            "a footnote's stray \"to date\" phrase must not resurrect a defunct operator"
        );
    }

    /// Finding #4(a): the two closing `</span>` tags this site's popup
    /// markup nests don't have to be perfectly adjacent -- whitespace (a
    /// newline, in this fixture, as real HTML is often pretty-printed)
    /// between them must still strip cleanly rather than leaving the whole
    /// popup, unstripped, sitting in the cell.
    #[test]
    fn strip_popups_tolerates_whitespace_between_the_two_closing_spans() {
        let popup_re = regex::Regex::new(r#"(?s)<span class="popup".*?</span>\s*</span>"#).unwrap();
        let cell = "ABW<span class=\"popup\" onclick=\"popup1()\"><span class=\"popuptext\" id=\"myPopup1\"><span class=\"close\">x</span>Original code</span>\n  </span>";
        let stripped = strip_popups(&popup_re, cell)
            .expect("whitespace between the two closing spans must still strip cleanly");
        assert!(!stripped.contains("Original code"));
        assert!(!stripped.contains("class=\"popup\""));
    }

    /// Finding #4(b): the self-check. A popup shape this regex genuinely
    /// cannot match at all (here: the site wraps a footnote in a single
    /// `<span class="popup">...</span>` instead of the expected two-deep
    /// nesting) must fail loudly, not silently leave the prose in place --
    /// which is exactly what would have kept shipping fabricated codes
    /// like `RAW` with nothing in the logs to say why.
    #[test]
    fn strip_popups_errors_loudly_instead_of_silently_leaving_unstripped_prose() {
        let popup_re = regex::Regex::new(r#"(?s)<span class="popup".*?</span>\s*</span>"#).unwrap();
        let cell = r#"MUC<span class="popup" onclick="popup1()">Code not certain</span>"#;
        let err = strip_popups(&popup_re, cell).expect_err(
            "a single-span popup shape can never match the two-close pattern, so this must \
             surface as a loud error rather than silently returning the cell unstripped",
        );
        assert!(err.to_string().contains("popup"));
    }
}
