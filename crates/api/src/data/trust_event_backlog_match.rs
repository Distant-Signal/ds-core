// Moved to ds_store::backlog::matching (ingest architecture plan 1A.10),
// with its DB tests (ingest architecture plan 1A, unit F).
pub use ds_store::backlog::matching::{
    BacklogReplayOutcome, attempt_backlog_match, attempt_backlog_match_by_uid,
    find_train_id_by_uid, run_backlog_match_sweep,
};
