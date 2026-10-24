// Moved to ds_store::backlog::matching (ingest architecture plan 1A.10),
// with its DB tests (ingest architecture plan 1A, unit F).
// The api replays through the train-event outbox (`ReplayWrite::Outbox`):
// its role may only read the shared train state (DS-118).
pub use ds_store::backlog::matching::{
    BacklogReplayOutcome, ReplayWrite, attempt_backlog_match, attempt_backlog_match_by_uid,
    attempt_backlog_match_by_uid_via, attempt_backlog_match_via, find_train_id_by_uid,
    run_backlog_match_sweep,
};
