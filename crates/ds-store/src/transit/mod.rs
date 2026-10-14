//! Transit networks (docs/superpowers/specs/2026-10-09-transit-networks-design.md):
//! the `transit_*` tables.
//!
//! - [`write`]: the ingest-writer's side of a network's versioned
//!   timetable: [`stage_part`] for each `transit-timetable/1` part and
//!   [`commit`] for the `transit-timetable-commit/1` that activates it
//!   (spec §9.2).
//! - [`situations`]: the ingest-writer's side of a network's situations and
//!   the line statuses derived from them: [`apply_situations`] for each
//!   `transit-situations/1` snapshot (spec §9.3, §10.3).
//! - [`read`]: the api's queries.

pub mod read;
pub mod situations;
pub mod write;

pub use situations::{SituationsApplied, SituationsOutcome, apply_situations};
pub use write::{CommitOutcome, PartIndex, StageOutcome, commit, stage_part};
