//! A pure, offline CIF `SCHEDULE` (`BS`/`BX`/`LO`/`LI`/`CR`/`LT`) parsing and
//! STP-overlay resolution library -- Option B's first safe slice.
//!
//! Gated on
//! `docs/superpowers/specs/2026-09-03-option-b-consumer-scoping.md`'s split
//! verdict and built per
//! `docs/superpowers/plans/2026-09-03-option-b-consumer-first-slice-plan.md`,
//! this crate does exactly one thing: given the already-read text of a real
//! CIF `MCA` extract (the same `RJTTF*MCA.txt` file
//! `crates/schedule-reference` already reads for its own, separate `TI`/`A`
//! record parsing -- this crate reads the *other* record family from the
//! same file and does not depend on or duplicate that crate's logic),
//! resolve, for a given `(UID, date)`, the STP-overlay-correct booked
//! calling-point schedule, or, symmetrically, every schedule touching a
//! given set of TIPLOCs on a given date.
//!
//! # What this crate is not
//!
//! - **Not wired into any production data path.** As of this commit,
//!   nothing in `crates/trust-consumer`, `crates/aggregator`, or
//!   `crates/api` depends on this crate -- mirroring the already-merged
//!   full-coverage presentation scaffolding's own "kept honest about being
//!   inert" precedent (see `common::FullCoverageAvailability` /
//!   `LineDefinition.full_coverage_enabled`'s own doc comments: "nothing
//!   consumes this yet"). This crate is built and tested, and left unused,
//!   on purpose.
//! - **No I/O of any kind.** Every public function takes `&str`
//!   (already-read file content) or already-parsed structures in, and
//!   returns plain data out -- the same "parsing logic pure and testable
//!   separately from I/O" convention `crates/schedule-reference/src/parser.rs`'s
//!   own module doc establishes, applied here from the start rather than
//!   retrofitted.
//! - **No Kafka consumer, no HTTP route, no database table or migration,
//!   no dependency on `tokio`/`reqwest`/`sqlx`/`rdkafka`.**
//! - **No CIF `AA` (Association) record, no freight-specific field, no
//!   record type not already independently exercised against real
//!   production CIF bytes** in the four validation sessions behind
//!   `docs/superpowers/specs/2026-08-29-trust-schedule-delay-validation-findings.md`.
//!
//! # Why this exists anyway
//!
//! Independent of Option B's own still-open "does this add real value"
//! question, this closes an already-documented gap in an already-shipped
//! feature: `crates/trust-consumer/src/matching.rs`'s own module doc names
//! "this app has no CIF schedule lookup to bridge Activation's `train_uid`
//! to a departure time" as a real, current limitation of live pin
//! resolution. Nothing in this crate wires into that module -- see the
//! scoping doc's "Explicitly out of scope" -- but the gap it names is
//! exactly the one this crate's two queries (`ScheduleIndex::schedule_for_uid`,
//! `resolve::schedules_touching`) close, for a future pass to wire up
//! deliberately.
//!
//! # Layout
//!
//! - `records`: plain record/struct shapes (`BasicSchedule`, `CallingPoint`,
//!   `RawSchedule`), each documenting the exact real byte offsets it was
//!   decoded against.
//! - `parse`: `parse_schedule_records`, turning raw `MCA` text into
//!   `Vec<RawSchedule>`.
//! - `resolve`: STP-overlay resolution (`resolve_for_date`,
//!   `schedules_touching`) and the `ScheduleIndex` that makes repeated
//!   queries cheap.
//! - `tiploc`: `normalize_tiploc`, the fixed 7-character space-padding
//!   gotcha every schedule-body TIPLOC field carries.
//!
//! `tests/real_cif_fixtures.rs` and each module's own inline `#[cfg(test)]`
//! block exercise all of the above against real, byte-verbatim CIF lines
//! quoted in the findings/verification docs above, plus a handful of
//! lines clearly commented as synthetic where those docs only quote a
//! value in paraphrased form. `examples/inspect.rs` is a separate,
//! explicitly-labeled dev-only tool (not part of this crate's `cargo test`
//! gate) for a human to re-check this crate's byte offsets against the
//! real, full, untracked `timetable_full.zip` extract by hand.

pub mod parse;
pub mod records;
pub mod resolve;
pub mod tiploc;

pub use parse::parse_schedule_records;
pub use records::{
    BasicSchedule, CallingPoint, CallingPointKind, LinePopulationEntry, RawSchedule,
    ScheduleDeparture, StpIndicator,
};
pub use resolve::{
    ResolvedSchedule, ScheduleIndex, departures_by_crs, resolve_for_date, schedules_touching,
};
pub use tiploc::normalize_tiploc;
