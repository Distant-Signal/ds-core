//! The incident snapshot: `upsert_incident_snapshot` and its helpers,
//! `incident_removal.rs` (whole) and `parse_snapshot` (today's
//! `incident_snapshot_from_body`). Redis stays out: the upsert takes a
//! publish callback, and the api passes `publish_text_changed`.
//!
//! Empty until plan task 1A.8 moves it in.
