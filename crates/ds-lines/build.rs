//! Embeds the line catalogue: writes `catalogue.rs` into `OUT_DIR`, the
//! `include_str!` of every `lines/*.toml` file (sorted by name, the order
//! `LineDefinition::from_dir` reads them in) and of the generated
//! pass-through file when it exists.
//!
//! The `rerun-if-changed` on the directory makes cargo rerun this, and so
//! re-embed the catalogue, when any file under `lines/` changes.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::{env, fs};

/// The catalogue, relative to this crate.
const LINES: &str = "../../lines";
/// The generated pass-through file, relative to the catalogue
/// (`common::pass_through::PASS_THROUGH_FILE`).
const PASS_THROUGH: &str = "generated/pass-through.toml";

fn main() {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap_or_default());
    let out = PathBuf::from(env::var_os("OUT_DIR").unwrap_or_default());
    println!("cargo::rerun-if-changed=build.rs");

    let dir = manifest_dir.join(LINES);
    // A directory: cargo reruns this when any file under it changes.
    println!("cargo::rerun-if-changed={}", dir.display());
    let files = line_files(&dir)
        .unwrap_or_else(|err| panic!("reading the catalogue in {}: {err}", dir.display()));
    assert!(
        !files.is_empty(),
        "no *.toml line definitions in {}",
        dir.display()
    );

    let mut text = String::from("/// Every `lines/*.toml` file: its name and its text.\n");
    text.push_str("pub(crate) static FILES: &[(&str, &str)] = &[\n");
    for (name, path) in &files {
        writeln!(text, "    ({name:?}, include_str!({:?})),", path.display())
            .unwrap_or_else(|err| panic!("formatting: {err}"));
    }
    text.push_str("];\n\n/// The generated pass-through file, when there is one.\n");
    let pass_through = dir.join(PASS_THROUGH);
    if pass_through.is_file() {
        writeln!(
            text,
            "pub(crate) static PASS_THROUGH: Option<&str> = Some(include_str!({:?}));",
            pass_through.display()
        )
        .unwrap_or_else(|err| panic!("formatting: {err}"));
    } else {
        text.push_str("pub(crate) static PASS_THROUGH: Option<&str> = None;\n");
    }
    fs::write(out.join("catalogue.rs"), text)
        .unwrap_or_else(|err| panic!("writing catalogue.rs: {err}"));
}

/// The `*.toml` files directly in `dir` (not its subdirectories), as
/// (file name, absolute path), sorted by name.
fn line_files(dir: &Path) -> std::io::Result<Vec<(String, PathBuf)>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == "toml") && path.is_file() {
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_else(|| panic!("{} is not UTF-8", path.display()))
                .to_owned();
            files.push((name, fs::canonicalize(&path)?));
        }
    }
    files.sort();
    Ok(files)
}
