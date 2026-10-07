//! Moved to `ds_store::sweeps::reconciliation` (ingest architecture plan
//! 1A.10). The re-exports keep every `data::reconciliation::…` call site
//! unchanged.

// Moved to ds_store::sweeps::reconciliation (ingest architecture plan 1A.10).
pub use ds_store::sweeps::reconciliation::{
    ReconciliationSweepResult, reconcile_stuck_resolution_status,
    retry_schedule_enrichment_for_nr_primary_trains, run_reconciliation_sweep,
    true_origin_departure,
};

// Moved to ds_store::sweeps::reconciliation (ingest architecture plan 1A.10).
#[cfg(test)]
pub(crate) use ds_store::sweeps::reconciliation::enrichment_candidate_ids_for_tests;
