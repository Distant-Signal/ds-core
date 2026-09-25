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
/// Signal Box Audit, common-crate Low finding "A missing line catalogue
/// directory yields an empty catalogue, not an error": `glob()` (used by
/// `LineDefinition::from_dir`) does not error on a missing directory, and
/// TOML-parsing zero files trivially succeeds -- so a typo'd or missing
/// `--lines-dir`/`LINES_DIR` used to load a silently EMPTY catalogue rather
/// than failing startup. Every real caller of this catalogue (the matcher,
/// LDBWS sampling, full-coverage gating) treats "zero lines" as a valid,
/// unremarkable state rather than a configuration error, so nothing further
/// downstream would ever notice -- the service would just run
/// indefinitely, reporting no incidents ever match anything.
/// `crates/api/src/bin/backfill_incident_lines.rs` already had to guard
/// this itself (`anyhow::ensure!(!lines.is_empty(), ...)`) precisely
/// because this function didn't; every OTHER real caller
/// (`aggregator`/`api`'s main service/`full-coverage-consumer`/
/// `schedule-reference`/`trust-backlog-consumer`, all via `value_parser =
/// parse_lines` on their `--lines-dir` clap arg) had no such guard at all.
/// Checking both cases here, once, closes it for every caller instead of
/// relying on each to remember its own post-hoc check.
pub fn parse_lines(path: &str) -> anyhow::Result<LineCatalogue> {
    let dir = PathBuf::from(path);
    anyhow::ensure!(
        dir.is_dir(),
        "line-catalogue directory {path:?} does not exist (or is not a directory) -- refusing \
         to start with a silently empty line catalogue. Check --lines-dir/LINES_DIR for a typo."
    );
    let catalogue = LineDefinition::from_dir(&dir).map(LineCatalogue)?;
    anyhow::ensure!(
        !catalogue.is_empty(),
        "line-catalogue directory {path:?} exists but contains no `*.toml` line definitions -- \
         refusing to start with a silently empty line catalogue. Check --lines-dir/LINES_DIR for \
         a typo."
    );
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
    fn parse_lines_rejects_a_nonexistent_directory() {
        // Was: "treats a nonexistent directory as an empty catalogue" --
        // `glob()` (inside `LineDefinition::from_dir`) does not error on a
        // missing directory, so this used to succeed with zero lines. See
        // this function's own doc comment (Signal Box Audit finding "A
        // missing line catalogue directory yields an empty catalogue, not
        // an error"): a typo'd `--lines-dir`/`LINES_DIR` must fail loudly
        // at startup instead.
        let err = parse_lines("/nonexistent/path/that/should/not/exist")
            .expect_err("a nonexistent catalogue directory must not load")
            .to_string();
        assert!(
            err.contains("does not exist"),
            "the error must explain why: {err}"
        );
    }

    #[test]
    fn parse_lines_rejects_an_existing_but_empty_directory() {
        let dir =
            std::env::temp_dir().join(format!("parse-lines-empty-dir-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create fixture dir");

        let result = parse_lines(dir.to_str().expect("utf8 temp path"));

        std::fs::remove_dir(&dir).ok();

        let err = result
            .expect_err("a catalogue directory with no *.toml files must not load")
            .to_string();
        assert!(
            err.contains("no `*.toml` line definitions"),
            "the error must explain why: {err}"
        );
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
