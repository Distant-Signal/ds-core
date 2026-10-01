//! The on-disk handoff between `schedule-ingest` (which extracts each CIF
//! delivery zip into `storage_dir/<YYYYMMDDTHHMMSSZ>/`) and
//! `schedule-reference` (which reads the newest complete one). The two run
//! as separate containers on one shared volume, so "complete" has to be
//! something the writer states explicitly (PL-6).

/// Written by `schedule-ingest` as the very last file of a delivery
/// directory, after every entry has been extracted and fsynced, and before
/// the directory is atomically renamed into place. `schedule-reference`
/// ignores any delivery directory without it. Its contents list each
/// extracted file as `name<TAB>bytes<TAB>sha256` (no hash in markers written
/// before 2026-10-01), one per line, so a restarted
/// `schedule-ingest` can re-POST a delivery without extracting it again.
pub const COMPLETE_MARKER: &str = ".delivery-complete";

/// Prefix of the scratch directory a delivery is extracted into before the
/// rename (`storage_dir/.tmp-<dir_name>`). Anything starting with `.` in
/// `storage_dir` is never a delivery.
pub const TEMP_DIR_PREFIX: &str = ".tmp-";
