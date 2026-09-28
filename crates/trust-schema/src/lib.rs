//! Pure TRUST movement-feed message parsing, dedup-key derivation, and
//! journey-state derivation -- extracted from `crates/trust-consumer` so
//! `crates/full-coverage-consumer` (a second, independent consumer of the
//! same feed) doesn't duplicate ~300 lines of already-tested envelope
//! parsing. `movement-relay`, `trust-backlog-consumer` and `api` depend on
//! it too. Pure code motion, no behavior change -- see
//! docs/superpowers/plans/2026-09-04-option-b-live-consumer-plan.md Task 1
//! and docs/superpowers/specs/2026-09-04-option-b-live-consumer-design.md
//! Decision 1 for the extraction rationale. No I/O, no `tokio`, no
//! `rdkafka` dependency -- every caller owns its own Kafka/Redis
//! plumbing; this crate only understands the message bytes once they're
//! already `&str`.

pub mod dedup;
pub mod journey;
pub mod schema;
