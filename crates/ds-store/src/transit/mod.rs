//! Transit networks (docs/superpowers/specs/2026-10-09-transit-networks-design.md):
//! the `transit_*` tables.
//!
//! - [`write`]: the ingest-writer's side of a network's versioned
//!   timetable: [`stage_part`] for each `transit-timetable/1` part and
//!   [`commit`] for the `transit-timetable-commit/1` that activates it
//!   (spec §9.2).
//!
//! The read queries (the api's routes) come in batch 3.

pub mod write;

pub use write::{CommitOutcome, PartIndex, StageOutcome, commit, stage_part};
