// crates/api/src/data/notifier_forward_queue.rs
//! Write side of the notifier-forwarding queue (Task 17). Read/poll side
//! lives in `crates/notifier` (Task 18).

// Moved to ds_store::tracking::forward_queue (ingest architecture plan 1A.9)
pub use ds_store::tracking::forward_queue::insert_forward_signals;
