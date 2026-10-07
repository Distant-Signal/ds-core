//! The ingest half of `train_tracking.rs`: train movement and event
//! upserts, `reopen_subscriptions_after_reinstatement`,
//! `apply_schedule_match`, the pending-pin listers,
//! `list_active_tracked_trains`, `TrackedTrainState`; and
//! `notifier_forward_queue::insert_forward_signals`. The user half stays
//! in the api.
//!
//! Empty until plan task 1A.9 moves it in.
