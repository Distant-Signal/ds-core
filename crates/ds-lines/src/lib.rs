//! The line catalogue (`lines/*.toml`), embedded in every binary that links
//! this crate (repo split spec §5.1 and §9, phase 3, item 5). Images no
//! longer copy `lines/` in: a service loads the catalogue it was built with.
//!
//! `--lines-dir`/`LINES_DIR` still overrides it, for local development
//! against an edited catalogue: every service's flag defaults to
//! [`EMBEDDED`] and parses through [`parse_lines`].
//!
//! `line-catalogue-validator` still reads `lines/` from disk: it checks the
//! files themselves.

use common::LineDefinition;
pub use common::config::LineCatalogue;

include!(concat!(env!("OUT_DIR"), "/catalogue.rs"));

/// The `--lines-dir` value (and every service's default) that selects the
/// embedded catalogue. A directory named `embedded` is `./embedded`.
pub const EMBEDDED: &str = "embedded";

/// The catalogue this binary was built with, checked as
/// `common::config::parse_lines` checks a directory.
pub fn embedded() -> anyhow::Result<LineCatalogue> {
    let lines = LineDefinition::from_files(FILES.iter().copied(), PASS_THROUGH)?;
    anyhow::ensure!(!lines.is_empty(), "the embedded line catalogue is empty");
    common::config::reject_reserved_ids(&lines, "the embedded line catalogue")?;
    Ok(LineCatalogue(lines))
}

/// The `--lines-dir` value parser: [`EMBEDDED`] is the embedded catalogue,
/// anything else a directory (`common::config::parse_lines`).
pub fn parse_lines(lines_dir: &str) -> anyhow::Result<LineCatalogue> {
    if lines_dir == EMBEDDED {
        embedded()
    } else {
        common::config::parse_lines(lines_dir)
    }
}

/// The core reference data in `reference-data/`, which
/// `line-catalogue-validator` checks the catalogue against, embedded for
/// other crates' tests: the directory stays in `ds-core` when they leave it
/// (repo split spec §9, phase 4). Only a crate that names a constant
/// carries its bytes.
pub mod reference_data {
    /// `reference-data/toc-codes.csv`: ATOC code, operator name.
    pub const TOC_CODES_CSV: &str = include_str!("../../../reference-data/toc-codes.csv");
    /// `reference-data/crs-tiploc.csv`: CRS, TIPLOC, name.
    pub const CRS_TIPLOC_CSV: &str = include_str!("../../../reference-data/crs-tiploc.csv");
}

/// The names of the embedded `lines/*.toml` files, sorted.
pub fn file_names() -> impl Iterator<Item = &'static str> {
    FILES.iter().map(|(name, _)| *name)
}

#[cfg(test)]
mod tests {
    use super::*;

    const LINES_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../lines");

    /// Every field of every line, in order, comparably: the serialised
    /// line (maps sorted: `severity_overrides` is a `HashMap`, so its
    /// `Debug` order varies) and its pass-through legs, which are never
    /// serialised.
    fn canonical(catalogue: &LineCatalogue) -> Vec<(serde_json::Value, String)> {
        catalogue
            .iter()
            .map(|line| {
                let value = serde_json::to_value(line).expect("serialises");
                (value, format!("{:?}", line.pass_through))
            })
            .collect()
    }

    /// The proof that embedding changes nothing: the embedded catalogue is
    /// the one `--lines-dir lines/` loads, every field of every line
    /// (pass-through stations included), in the same order.
    #[test]
    fn the_embedded_catalogue_is_the_lines_directory() {
        let from_dir = common::config::parse_lines(LINES_DIR).expect("lines/ loads");
        let embedded = embedded().expect("the embedded catalogue loads");
        assert_eq!(embedded.len(), from_dir.len());
        assert_eq!(canonical(&embedded), canonical(&from_dir));
        assert!(
            embedded.iter().any(|line| !line.pass_through.is_empty()),
            "the generated pass-through file is embedded"
        );
    }

    #[test]
    fn every_toml_file_is_embedded() {
        let mut on_disk: Vec<String> = std::fs::read_dir(LINES_DIR)
            .expect("lines/ exists")
            .map(|entry| entry.expect("entry").path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "toml"))
            .map(|path| {
                path.file_name()
                    .expect("name")
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        on_disk.sort();
        assert_eq!(file_names().collect::<Vec<_>>(), on_disk);
    }

    #[test]
    fn parse_lines_picks_the_embedded_catalogue_or_a_directory() {
        let embedded = parse_lines(EMBEDDED).expect("embedded");
        let from_dir = parse_lines(LINES_DIR).expect("directory");
        assert_eq!(canonical(&embedded), canonical(&from_dir));
        assert!(parse_lines("/nonexistent/lines").is_err());
    }
}
