//! CORPUS locations: `corpus.rs` and `corpus_crosswalk.rs` (whole),
//! `corpus_comparison::log_after_load`, and the load checks
//! `corpus_load_problem` and `is_sha256_hex` from `routes/ingest.rs`.
//!
//! The CORPUS fallback flag (`CORPUS_FALLBACK_ENABLED`) moved first, in
//! wave 0; the rest moves in plan task 1A.6.

use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;

/// Env var turning the fallback on. Unset or `false`: off.
pub const FALLBACK_ENV: &str = "CORPUS_FALLBACK_ENABLED";

static FALLBACK_ENABLED: AtomicBool = AtomicBool::new(false);

/// Whether the lookups fall back to CORPUS. Always false unless
/// [`init_fallback_from_env`] read `true`.
pub fn fallback_enabled() -> bool {
    FALLBACK_ENABLED.load(Ordering::Relaxed)
}

/// Parses a [`FALLBACK_ENV`] value: unset or blank is off, anything other
/// than `true`/`false` is a startup error rather than a silent default.
pub fn parse_fallback_flag(value: Option<&str>) -> Result<bool> {
    match value.map(str::trim) {
        None | Some("") => Ok(false),
        Some(v) if v.eq_ignore_ascii_case("true") => Ok(true),
        Some(v) if v.eq_ignore_ascii_case("false") => Ok(false),
        Some(v) => anyhow::bail!("{FALLBACK_ENV} must be true or false, got {v:?}"),
    }
}

/// Reads [`FALLBACK_ENV`] once at startup and logs the outcome.
pub fn init_fallback_from_env() -> Result<()> {
    let enabled = parse_fallback_flag(std::env::var(FALLBACK_ENV).ok().as_deref())?;
    FALLBACK_ENABLED.store(enabled, Ordering::Relaxed);
    if enabled {
        tracing::info!(
            "CORPUS fallback ON: TIPLOC/STANOX lookups fall back to corpus_tiploc_crs/corpus_stanox_crs after the timetable crosswalk"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_flag_is_strict_and_off_by_default() {
        assert!(!parse_fallback_flag(None).unwrap());
        assert!(!parse_fallback_flag(Some(" ")).unwrap());
        assert!(!parse_fallback_flag(Some("false")).unwrap());
        assert!(parse_fallback_flag(Some("true")).unwrap());
        assert!(parse_fallback_flag(Some("TRUE")).unwrap());
        assert!(parse_fallback_flag(Some("1")).is_err());
        assert!(parse_fallback_flag(Some("yes")).is_err());
        assert!(!fallback_enabled());
    }
}
