//! Shared `--lines-dir` line-catalogue loader. Previously 4 byte-identical
//! (or near-identical) copies across `aggregator`, `api`,
//! `full-coverage-consumer`, and `schedule-reference` -- see
//! docs/superpowers/specs/2026-09-05-rust-service-deduplication-design.md
//! §3.4.

use std::path::PathBuf;

use crate::LineDefinition;

/// Newtype around the parsed line catalogue.
///
/// `clap_derive` infers the type it downcasts an `ArgMatches` entry to from
/// the field's *syntactic* shape, not from the `value_parser`'s `Value`
/// type: a bare `Vec<LineDefinition>` field is always treated as "one
/// `LineDefinition` per CLI occurrence, collected via `ArgAction::Append`"
/// -- this panics at runtime ("Mismatch between definition and access of
/// `lines`") the moment `--lines-dir`/`LINES_DIR`/`default_value` actually
/// supplies a value. `parse_lines` instead produces the *entire* vec from a
/// single `--lines-dir` occurrence, so the field type must not look like
/// `Vec<T>` to the derive macro. This newtype (plus `Deref`) sidesteps
/// that -- every existing call site that treated a local `LineCatalogue`
/// as `&[LineDefinition]` continues to work unchanged.
#[derive(Debug, Clone, Default)]
pub struct LineCatalogue(pub Vec<LineDefinition>);

impl std::ops::Deref for LineCatalogue {
    type Target = Vec<LineDefinition>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub fn parse_lines(path: &str) -> anyhow::Result<LineCatalogue> {
    LineDefinition::from_dir(&PathBuf::from(path)).map(LineCatalogue)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_lines_treats_a_nonexistent_directory_as_an_empty_catalogue() {
        // Mirrors the existing per-crate copies' own implicit contract:
        // LineDefinition::from_dir globs `{dir}/*.toml`, and `glob()` does
        // not error on a missing directory -- it simply yields zero
        // matches. This shared wrapper surfaces that unchanged (confirmed
        // by running this test against the pre-existing behavior; none of
        // the 4 per-crate copies this replaces had a test asserting the
        // opposite).
        let result = parse_lines("/nonexistent/path/that/should/not/exist");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[test]
    fn line_catalogue_derefs_to_the_inner_vec() {
        let catalogue = LineCatalogue(vec![]);
        assert_eq!(catalogue.len(), 0);
    }
}
