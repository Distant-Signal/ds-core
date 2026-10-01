//! Locating a crate's own files from its tests (Train Register verification
//! 2026-10-01, N6).
//!
//! `env!("CARGO_MANIFEST_DIR")` is baked in when the test binary is
//! compiled. With `CARGO_TARGET_DIR` shared between git worktrees, cargo can
//! decide a binary another worktree built is still fresh (its fingerprints
//! are relative to the workspace root and mtime-based), so the test then
//! reads that OTHER worktree's `migrations/`, `charts/` or `lines/` -- or
//! panics on a missing file once that worktree is removed.
//!
//! `cargo test` (and `cargo run`, and nextest) also sets
//! `CARGO_MANIFEST_DIR` in the test process's environment, to the package
//! being tested in the worktree the command ran in. [`manifest_dir!`]
//! prefers that, and falls back to the compile-time value only when the
//! binary is run by hand.

/// Expands to the calling crate's manifest directory as a
/// [`std::path::PathBuf`], resolved at run time when cargo provides it (see
/// this module's docs) and at compile time otherwise.
///
/// A macro, not a function, so the compile-time fallback names the CALLING
/// crate rather than `common`.
#[macro_export]
macro_rules! manifest_dir {
    () => {
        ::std::env::var_os("CARGO_MANIFEST_DIR").map_or_else(
            || ::std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")),
            ::std::path::PathBuf::from,
        )
    };
}

#[cfg(test)]
mod tests {
    #[test]
    fn manifest_dir_is_this_crate() {
        let dir = crate::manifest_dir!();
        assert!(dir.join("src/test_paths.rs").is_file(), "{}", dir.display());
    }
}
