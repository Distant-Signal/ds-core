//! CORPUS (`corpus_locations`) compared with the timetable-derived
//! crosswalk (`tiploc_crs`, `stanox_crs`) and the Knowledgebase station
//! names (`stations`), before anything user-visible reads CORPUS.
//!
//! CORPUS is put through the same conservative inference
//! (`common::corpus_inference`) and narrowed to the same one-CRS-per-key
//! crosswalk the runtime fallback would use
//! ([`crate::data::corpus_crosswalk`]), and through the same stations
//! filter (`common::corpus_inference::restrict_to_stations`, against the
//! `stations` CRS codes read here), so "CORPUS only" below is exactly what
//! turning the fallback on would add, and "conflict" what it would NOT
//! change (the timetable wins). The fills the stations filter drops are
//! counted separately, by [`Exclusion`], so its effect stays visible.
//! Agreements and conflicts are counted before that filter: they compare
//! the two data sets, and the fallback never overrides the timetable
//! either way.
//!
//! Two ways to run it, both read-only:
//!
//! - after every CORPUS load, `POST /private/corpus-locations` logs the
//!   [`ReportDetail::Summary`] and sets the `distant_signal_api_corpus_comparison_*`
//!   gauges ([`record_metrics`]);
//! - on demand, `corpus_compare` (`crates/api/src/bin/corpus_compare.rs`,
//!   shipped in the api image) prints the full report against
//!   `DATABASE_URL`.
//!
//! With no CORPUS loaded neither does any work: the route never runs, and
//! the binary stops after one `COUNT(*)`.

// Moved to `ds_store::corpus::comparison` (ingest architecture plan 1A.6), with its
// tests.
pub use ds_store::corpus::comparison::{
    Conflict, CorpusComparison, Fill, NameDiff, ReportDetail, Timetable, compare, load_timetable,
    log_after_load, record_metrics, render,
};
