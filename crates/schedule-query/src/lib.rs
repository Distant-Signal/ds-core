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
//! - **Status (2026-09-28): now wired in.** This crate started out
//!   deliberately unused by any production data path; that is no longer
//!   true. `crates/api` (trip planning among others),
//!   `crates/full-coverage-consumer`,
//!   `crates/schedule-reference` and `crates/trip-planner` all depend on it
//!   today. The crate itself still does no I/O (below).
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
//! # What it is used for
//!
//! Bridging a pin's origin and departure time to a CIF `train_uid`:
//! `crates/api/src/data/schedule_matching.rs` resolves pins schedule-first
//! through [`match_pin`], so most pins reach trust-consumer already knowing
//! their `train_uid` (trust-consumer itself still does no CIF lookup; see
//! `crates/trust-consumer/src/matching.rs`). `crates/schedule-reference`
//! publishes the per-line schedule population and network departures built
//! with [`resolve::schedules_touching`], which full-coverage-consumer
//! consumes, and `crates/trip-planner` builds its connections from the same
//! resolved schedules.
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

pub mod compact;
pub mod connections;
pub mod interchange;
pub mod line_membership;
pub mod parse;
pub mod records;
pub mod resolve;
pub mod tiploc;

pub use compact::SmallStr;
pub use connections::{
    CallingPointForConnections, Connection, PassIndex, build_connections,
    build_connections_with_passes,
};
pub use interchange::{
    ChangeTime, FixedLink, InterchangeData, ModalChangeBuffer, fixed_links_from,
    minimum_change_time, sibling_tiplocs,
};
pub use line_membership::{
    DayTrains, LineDue, LineMembers, LineScope, Membership, MembershipLine, RunDirection, classify,
    line_due,
};
pub use parse::{ScheduleRecordParser, parse_schedule_records};
pub use records::{
    Activity, BasicSchedule, CallingPoint, CallingPointKind, DestinationDeparture, HalfMinuteTime,
    LinePopulationEntry, Platform, RawSchedule, ScheduleDeparture, ServiceMode, StpIndicator,
    Tiploc, TrainCategory, is_bus_or_ship, service_mode,
};
pub use resolve::{
    ResolvedSchedule, ScheduleIndex, ScheduleIndexBuilder, departures_by_crs,
    departures_by_destination_crs, match_pin, match_pin_with_delta, resolve_for_date,
    schedules_touching, unresolved_booked_tiplocs,
};
pub use tiploc::normalize_tiploc;
