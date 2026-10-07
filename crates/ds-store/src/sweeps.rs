//! Background sweeps: `schedule_matching::run_schedule_match_sweep`,
//! `find_schedule_match` and `reconciliation::run_reconciliation_sweep`.
//!
//! Moved whole from the api's `data::schedule_matching` and
//! `data::reconciliation` (ingest architecture plan 1A.10); the api's
//! modules of the same names re-export them.

pub mod reconciliation;
pub mod schedule_matching;
