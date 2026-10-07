//! Generates `schema::REQUIRED_MIGRATION` (plan task 1A.12): the version
//! of the newest migration in `crates/api/migrations`, the directory the
//! api's `sqlx::migrate!()` embeds. Generated rather than hand-written so
//! adding a migration needs no second edit, and so it is, by construction,
//! the newest migration built into the same binary (spec §12.2).
//!
//! Plan task 1B.1 moves the directory to `crates/ds-store/migrations`;
//! [`MIGRATIONS`] then becomes `migrations`.

use std::path::{Path, PathBuf};
use std::{env, fs};

/// The migrations directory, relative to this crate.
const MIGRATIONS: &str = "../api/migrations";

fn main() {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap_or_default());
    let dir = manifest_dir.join(MIGRATIONS);
    // A directory: cargo reruns this when any file in it changes.
    println!("cargo::rerun-if-changed={}", dir.display());
    println!("cargo::rerun-if-changed=build.rs");

    let version = newest_version(&dir)
        .unwrap_or_else(|err| panic!("reading the migrations in {}: {err}", dir.display()));
    let out = PathBuf::from(env::var_os("OUT_DIR").unwrap_or_default());
    fs::write(out.join("required_migration.rs"), format!("{version}\n"))
        .unwrap_or_else(|err| panic!("writing required_migration.rs: {err}"));
}

/// The largest version among the migrations sqlx would apply: the leading
/// digits of each `<version>_<description>.sql`, skipping `.down.sql`
/// (reverts, which `migrate!` does not run forwards).
fn newest_version(dir: &Path) -> Result<i64, String> {
    let mut newest = None;
    for entry in fs::read_dir(dir).map_err(|err| err.to_string())? {
        let name = entry.map_err(|err| err.to_string())?.file_name();
        let Some(name) = name.to_str() else { continue };
        let is_sql = Path::new(name).extension().is_some_and(|ext| ext == "sql");
        if !is_sql || name.ends_with(".down.sql") {
            continue;
        }
        let Some((version, _)) = name.split_once('_') else {
            return Err(format!("{name} is not <version>_<description>.sql"));
        };
        let version: i64 = version
            .parse()
            .map_err(|err| format!("{name}: version {version:?}: {err}"))?;
        newest = newest.max(Some(version));
    }
    newest.ok_or_else(|| "no migrations".to_owned())
}
