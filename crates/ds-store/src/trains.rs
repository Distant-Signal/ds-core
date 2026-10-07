//! Train identity and per-stop state: `trains.rs`'s shared functions
//! (`find_or_create_train*`, `mark_train*_resolved*`,
//! `destination_crs_for_train*`, `bind_subscription_unless_other_train`),
//! `stop_delay.rs`, `stop_live_status.rs`, `eta_blend::london_to_utc`,
//! and the `JourneyStop`/`StopStatus`/`StopTimetable` types (which
//! `api`'s `journey.rs` re-exports).
//!
//! Empty until plan task 1A.4 moves it in.
