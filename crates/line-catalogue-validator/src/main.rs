//! `line-catalogue-validator`: validates every `crs`, `tiploc` and
//! `operators` value across `lines/*.toml` against real, external ground
//! truth, and exits non-zero if anything doesn't check out.
//!
//! ```text
//!   cargo run -p line-catalogue-validator
//!   cargo run -p line-catalogue-validator -- --live   # thorough tier, see below
//! ```
//!
//! ## Why this exists
//!
//! A `lines/*.toml` `crs`/`tiploc` value being *syntactically* a 3-letter
//! code (or 7-char TIPLOC) says nothing about whether it's a *real* one,
//! or the *right* one -- a manual, file-by-file audit run earlier in this
//! effort found several real-world cases of exactly that (a valid-looking
//! CRS pointing at a completely unrelated station -- `WNE` used where
//! `WDM`/Windermere was meant, `WNE` actually being Wilnecote,
//! Staffordshire). This binary automates that class of check.
//!
//! ## Why a separate crate, not a `crates/api/src/bin/*.rs` binary
//!
//! `crates/api/src/bin/backfill_trains.rs`/`backfill_incident_lines.rs`
//! are the existing precedent for a one-off analysis binary in this repo,
//! but both genuinely need `api`'s own database layer (they run real SQL
//! against `tracked_trains`/`incidents`). This validator needs none of
//! that -- only `common::LineDefinition`'s existing TOML-parsing struct --
//! so putting it in `crates/api` would pull in `api`'s full dependency
//! tree (`sqlx`, `axum`, OAuth/SSO, ...) purely to link a binary that
//! never touches any of it, slowing every `cargo build --workspace` in CI
//! for no benefit. A new small crate depending only on `common` (plus
//! `csv`/`regex`/`reqwest` for its own two tiers) keeps this fast and
//! keeps its dependency footprint honest about what it actually does.
//!
//! ## Two tiers (see `reference.rs`'s module doc for the mechanics)
//!
//! - **Fast, no-secrets tier (default; what CI runs on every PR)**: checks
//!   against the CSV snapshots vendored in `reference-data/` (see
//!   `reference-data/line-catalogue-validation.md` for exactly where that
//!   data came from and its documented limitations). Deterministic, no
//!   network access, runs in well under a second against this repo's
//!   ~110 line files.
//! - **Thorough, live tier (`--live`; what the weekly cron in
//!   `.github/workflows/validate-line-catalogue.yml` runs, currently
//!   disabled)**: scrapes railwaycodes.org.uk's CRS pages live for
//!   CRS/TIPLOC (the one check nothing credential-free the project already
//!   has rights to covers), and takes operator codes from the real RDM
//!   Train Operating Company List feed when `RDM_API_KEY`/
//!   `RDM_TOCS_BASE_URL` are set, else from the vendored Knowledgebase
//!   `toc-codes.csv` -- see `reference.rs::ReferenceData::fetch_live`.
//!
//! Both tiers run the exact same `checks::validate_lines` against the
//! exact same `ReferenceData` shape -- there is only one set of
//! validation rules to keep correct, not two.
//!
//! ## Exit codes
//!
//! `0` if every `crs` and `operators` value is known-valid (TIPLOC
//! mismatches are printed but don't affect this -- see `checks.rs` for
//! why). Non-zero otherwise, with one line per finding giving the file,
//! best-effort line number, the bad value, and what's wrong.

mod checks;
mod corpus_db;
mod rdm_toc;
mod reference;
mod regenerate;

use std::path::PathBuf;

use clap::Parser;

use checks::Severity;
use reference::ReferenceData;
use regenerate::ReportDetail;

#[derive(Parser)]
struct Args {
    /// Directory of `lines/*.toml` files to validate.
    #[arg(long, default_value = "lines")]
    lines_dir: PathBuf,

    /// Directory holding the vendored `crs-tiploc.csv`/`toc-codes.csv`
    /// reference data. With `--live` only `toc-codes.csv` is read, as the
    /// operator-code source when the RDM TOC feed isn't configured.
    #[arg(long, default_value = "reference-data")]
    reference_dir: PathBuf,

    /// Run the thorough, live tier instead of the fast vendored-CSV tier
    /// -- see this binary's module doc.
    #[arg(long)]
    live: bool,

    /// Instead of validating, regenerate `<reference-dir>/crs-tiploc.csv`
    /// from this Network Rail CORPUS extract (`CORPUSExtract.json`,
    /// decompressed). See `regenerate.rs` and
    /// `reference-data/line-catalogue-validation.md`.
    #[arg(long, value_name = "CORPUS_JSON", group = "crs_tiploc_source")]
    regenerate_crs_tiploc_from_corpus: Option<PathBuf>,

    /// Instead of validating, regenerate `<reference-dir>/crs-tiploc.csv`
    /// from the CORPUS the app has loaded (`corpus_locations`, read from
    /// `DATABASE_URL`), with the same rules, report and `--compare-with` as
    /// `--regenerate-crs-tiploc-from-corpus`. Needs a build with
    /// `--features db`. See reference-data/line-catalogue-validation.md.
    #[arg(long, group = "crs_tiploc_source")]
    regenerate_crs_tiploc_from_db: bool,

    /// With a crs-tiploc regeneration: write the full inference report
    /// (per-rule counts, ambiguous groups, and every differing pair when
    /// `--compare-with` is given) to this file. A summary still goes to
    /// stderr. Without it the report (minus per-pair lists) goes to stderr.
    #[arg(long, value_name = "PATH", requires = "crs_tiploc_source")]
    report: Option<PathBuf>,

    /// With a crs-tiploc regeneration: compare the new output against this
    /// existing `crs-tiploc.csv` (read before anything is written, so it may
    /// be the file being replaced) and add agreement stats to the report.
    #[arg(long, value_name = "CRS_TIPLOC_CSV", requires = "crs_tiploc_source")]
    compare_with: Option<PathBuf>,

    /// Instead of validating, regenerate `<reference-dir>/toc-codes.csv`
    /// from this saved Knowledgebase Train Operating Company List XML
    /// response (the feed `poller-tocs` ingests).
    #[arg(long, value_name = "TOC_LIST_XML")]
    regenerate_toc_codes_from_rdm_xml: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    if args.regenerate_crs_tiploc_from_corpus.is_some()
        || args.regenerate_crs_tiploc_from_db
        || args.regenerate_toc_codes_from_rdm_xml.is_some()
    {
        let result = if let Some(corpus) = &args.regenerate_crs_tiploc_from_corpus {
            let json = std::fs::read(corpus)
                .map_err(|e| anyhow::anyhow!("reading {}: {e}", corpus.display()))?;
            Some(regenerate::crs_tiploc_from_corpus(&json)?)
        } else if args.regenerate_crs_tiploc_from_db {
            let rows = corpus_db::read_corpus_rows().await?;
            eprintln!(
                "read {} corpus_locations row(s) from DATABASE_URL",
                rows.len()
            );
            Some(regenerate::crs_tiploc_from_rows(&rows)?)
        } else {
            None
        };
        if let Some(result) = result {
            // Read the comparison file before writing: it is often the
            // very file about to be replaced.
            let comparison = match &args.compare_with {
                Some(path) => {
                    let file = std::fs::File::open(path)
                        .map_err(|e| anyhow::anyhow!("reading {}: {e}", path.display()))?;
                    let (old_pairs, old_crs) = regenerate::read_crs_tiploc_pairs(file)?;
                    Some((
                        path.as_path(),
                        regenerate::compare(&result, &old_pairs, &old_crs),
                    ))
                }
                None => None,
            };
            let comparison = comparison.as_ref().map(|(p, c)| (*p, c));
            match &args.report {
                Some(path) => {
                    let full = regenerate::render_report(&result, comparison, ReportDetail::Full);
                    std::fs::write(path, full)
                        .map_err(|e| anyhow::anyhow!("writing {}: {e}", path.display()))?;
                    eprint!(
                        "{}",
                        regenerate::render_report(&result, comparison, ReportDetail::Summary)
                    );
                    eprintln!("full report written to {}", path.display());
                }
                None => eprint!(
                    "{}",
                    regenerate::render_report(&result, comparison, ReportDetail::Standard)
                ),
            }
            let out = args.reference_dir.join("crs-tiploc.csv");
            regenerate::write_crs_tiploc_csv(&out, &result.rows)?;
            println!("wrote {} row(s) to {}", result.rows.len(), out.display());
        }
        if let Some(xml_path) = &args.regenerate_toc_codes_from_rdm_xml {
            let xml = std::fs::read_to_string(xml_path)
                .map_err(|e| anyhow::anyhow!("reading {}: {e}", xml_path.display()))?;
            let rows = regenerate::toc_codes_from_rdm_xml(&xml)?;
            let out = args.reference_dir.join("toc-codes.csv");
            regenerate::write_toc_codes_csv(&out, &rows)?;
            println!("wrote {} row(s) to {}", rows.len(), out.display());
        }
        return Ok(());
    }

    let reference = if args.live {
        eprintln!(
            "running the live tier -- fetching railwaycodes.org.uk CRS pages (and the RDM TOC feed, if configured)..."
        );
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;
        let rdm_api_key = std::env::var("RDM_API_KEY").ok();
        let rdm_tocs_base_url = std::env::var("RDM_TOCS_BASE_URL").ok();
        if rdm_api_key.is_none() || rdm_tocs_base_url.is_none() {
            eprintln!(
                "note: RDM_API_KEY/RDM_TOCS_BASE_URL not both set -- operator codes will come \
                 from the vendored Knowledgebase snapshot (toc-codes.csv) instead of the live \
                 RDM feed (see .github/workflows/validate-line-catalogue.yml for what's needed)"
            );
        }
        ReferenceData::fetch_live(
            &client,
            &args.reference_dir.join("toc-codes.csv"),
            rdm_api_key.as_deref(),
            rdm_tocs_base_url.as_deref(),
        )
        .await?
    } else {
        ReferenceData::from_vendored_csvs(
            &args.reference_dir.join("crs-tiploc.csv"),
            &args.reference_dir.join("toc-codes.csv"),
        )?
    };

    let lines = checks::load_all(&args.lines_dir)?;
    println!(
        "loaded {} line definitions from {}",
        lines.len(),
        args.lines_dir.display()
    );

    let findings = checks::validate_lines(&lines, &reference);
    let (errors, warnings): (Vec<_>, Vec<_>) =
        findings.iter().partition(|f| f.severity == Severity::Error);

    for finding in &findings {
        println!("{finding}");
    }

    println!();
    println!(
        "{} error(s), {} warning(s) across {} line file(s)",
        errors.len(),
        warnings.len(),
        lines.len()
    );

    // Stretch goal: informational-only coverage gaps. Printed always, on
    // both a clean and a failing run, and never affects the exit code --
    // see `checks::unused_operator_codes`'s doc for why this is scoped to
    // operators only, not stations.
    let gaps = checks::unused_operator_codes(&lines, &reference);
    println!();
    println!(
        "--- coverage report (informational, non-blocking): {} currently-valid ATOC code(s) \
         used by no lines/*.toml file ---",
        gaps.len()
    );
    for (code, name) in &gaps {
        println!("  {code}: {name}");
    }

    if !errors.is_empty() {
        std::process::exit(1);
    }
    Ok(())
}

#[cfg(test)]
mod args_tests {
    use clap::Parser;

    use super::Args;

    fn parse(args: &[&str]) -> Result<Args, clap::Error> {
        Args::try_parse_from(
            std::iter::once("line-catalogue-validator").chain(args.iter().copied()),
        )
    }

    #[test]
    fn crs_tiploc_regeneration_takes_one_source_and_the_report_options() {
        let db = parse(&[
            "--regenerate-crs-tiploc-from-db",
            "--compare-with",
            "reference-data/crs-tiploc.csv",
            "--report",
            "r.txt",
        ])
        .unwrap();
        assert!(db.regenerate_crs_tiploc_from_db);
        assert!(
            parse(&[
                "--regenerate-crs-tiploc-from-corpus",
                "c.json",
                "--report",
                "r.txt"
            ])
            .is_ok()
        );
        // Both sources at once, or report options with no source, are refused.
        assert!(
            parse(&[
                "--regenerate-crs-tiploc-from-db",
                "--regenerate-crs-tiploc-from-corpus",
                "c.json"
            ])
            .is_err()
        );
        assert!(parse(&["--report", "r.txt"]).is_err());
        assert!(parse(&["--compare-with", "x.csv"]).is_err());
    }
}
