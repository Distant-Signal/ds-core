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

/// Rejects a catalogue containing a `custom-`-prefixed line id, loudly, at
/// startup. That prefix is the ONLY thing distinguishing a private,
/// user-owned line from a public one at every privacy gate in `crates/api`
/// (`data::custom_lines::CUSTOM_LINE_ID_PREFIX` and its callers), and those
/// gates all assume the converse too: that anything without the prefix is
/// safe to serve anonymously. A catalogue file that claimed such an id
/// would quietly invert one of those checks. Nothing in `lines/*.toml` does
/// this today -- this makes it an enforced invariant rather than an
/// observed one, since the gates themselves cannot detect the violation.
pub fn parse_lines(path: &str) -> anyhow::Result<LineCatalogue> {
    let catalogue = LineDefinition::from_dir(&PathBuf::from(path)).map(LineCatalogue)?;
    if let Some(offender) = catalogue.iter().find(|l| l.id.starts_with("custom-")) {
        anyhow::bail!(
            "catalogue line id {:?} uses the `custom-` prefix, which is reserved for private \
             user-created lines -- rename it in {path}",
            offender.id
        );
    }
    Ok(catalogue)
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
    fn parse_lines_rejects_a_catalogue_file_claiming_a_custom_prefixed_id() {
        let dir =
            std::env::temp_dir().join(format!("parse-lines-custom-prefix-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create fixture dir");
        let file = dir.join("impostor.toml");
        std::fs::write(
            &file,
            "id = \"custom-impostor\"\nname = \"Impostor\"\nmode = \"national-rail\"\n\
             category = \"main-line\"\noperators = [\"SW\"]\n\n\
             [[stations]]\ncrs = \"WOK\"\nrole = \"principal\"\n",
        )
        .expect("write fixture line file");

        let result = parse_lines(dir.to_str().expect("utf8 temp path"));

        std::fs::remove_file(&file).ok();
        std::fs::remove_dir(&dir).ok();

        let err = result
            .expect_err("a `custom-` catalogue id must not load")
            .to_string();
        assert!(
            err.contains("custom-impostor") && err.contains("reserved"),
            "the error must name the offending id and why it's rejected: {err}"
        );
    }

    #[test]
    fn line_catalogue_derefs_to_the_inner_vec() {
        let catalogue = LineCatalogue(vec![]);
        assert_eq!(catalogue.len(), 0);
    }
}
