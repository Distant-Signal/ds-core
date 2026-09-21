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
//!   disabled)**: re-fetches the same upstream site live, plus the real
//!   RDM Train Operating Company List feed for operator codes when
//!   `RDM_API_KEY`/`RDM_TOCS_BASE_URL` are set (falls back to the
//!   community-site scrape and prints a warning if they aren't -- see
//!   `reference.rs::ReferenceData::fetch_live`).
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
mod rdm_toc;
mod reference;

use std::path::PathBuf;

use clap::Parser;

use checks::Severity;
use reference::ReferenceData;

#[derive(Parser)]
struct Args {
    /// Directory of `lines/*.toml` files to validate.
    #[arg(long, default_value = "lines")]
    lines_dir: PathBuf,

    /// Directory holding the vendored `crs-tiploc.csv`/`toc-codes.csv`
    /// reference data (fast tier only -- ignored with `--live`).
    #[arg(long, default_value = "reference-data")]
    reference_dir: PathBuf,

    /// Run the thorough, live tier instead of the fast vendored-CSV tier
    /// -- see this binary's module doc.
    #[arg(long)]
    live: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let reference = if args.live {
        eprintln!(
            "running the live tier -- fetching railwaycodes.org.uk (and the RDM TOC feed, if configured)..."
        );
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;
        let rdm_api_key = std::env::var("RDM_API_KEY").ok();
        let rdm_tocs_base_url = std::env::var("RDM_TOCS_BASE_URL").ok();
        if rdm_api_key.is_none() || rdm_tocs_base_url.is_none() {
            eprintln!(
                "note: RDM_API_KEY/RDM_TOCS_BASE_URL not both set -- operator codes will use \
                 the railwaycodes.org.uk scrape instead of the authoritative RDM feed \
                 (see .github/workflows/validate-line-catalogue.yml for what's needed)"
            );
        }
        ReferenceData::fetch_live(
            &client,
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
