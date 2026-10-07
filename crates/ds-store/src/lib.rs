//! Distant Signal's shared Postgres data access: the SQL for every table
//! more than one binary writes or reads, the publish protocols, the input
//! validation the api's `/private` handlers do today, the pool builder with
//! pool metrics and the schema gate.
//!
//! See docs/superpowers/specs/2026-10-06-ingest-architecture-design.md §5
//! (the boundary, what moves, who uses it) and
//! docs/superpowers/plans/2026-10-06-ingest-architecture-plan.md phase 1A.
//!
//! # Boundary (spec §5.1)
//!
//! - No web framework, HTTP client or Redis: the normal dependency closure
//!   must not contain `axum`, `tower`, `hyper`, `redis`, `reqwest`,
//!   `oauth2`, `openidconnect` or `api` (`scripts/check-crate-deps.py`,
//!   CI's scripts-lint job). A function that publishes to Redis today
//!   takes a callback or returns what to publish; the caller publishes.
//! - Runtime-checked `sqlx::query` only: no `query!` macros, no `.sqlx`
//!   cache, no `migrate!` (spec §5.4). Query correctness comes from the
//!   DB-gated `#[ignore]` tests, which move with their functions.
//!
//! # Phase 1A: behaviour-neutral moves
//!
//! Every module starts empty and is filled by one move task. A move keeps
//! the SQL text, metric names and routes identical, and leaves
//! `pub use ds_store::…` shims in `crates/api/src/data/*` so no call site
//! changes. `scripts/diff-api-surface.py` checks this against the pre-move
//! base commit.
//!
//! | Module | Task | Contents |
//! |---|---|---|
//! | [`validate`] | 1A.2 | `validate_short_text`, `validate_code_list`, `is_crs_code` |
//! | [`freshness`] | 1A.3 | `record_ingest`, the `last_*_fetch` readers, `data_freshness` |
//! | [`trains`] | 1A.4 | train identity, `stop_delay`, `stop_live_status`, journey stop types |
//! | [`samples`] | 1A.5 | station, full-coverage, `TfL` and island-of-Ireland sample writers |
//! | [`reference`] | 1A.6 | stations, TOCs, STANOX/TIPLOC crosswalks, fixed links |
//! | [`corpus`] | 1A.6 | CORPUS locations, the crosswalk, the load checks |
//! | [`schedule`] | 1A.7 | the schedule publish protocol, products and feed markers |
//! | [`incidents`] | 1A.8 | the incident snapshot upsert and removal inference |
//! | [`tracking`] | 1A.9 | the ingest half of `train_tracking`, forward signals |
//! | [`backlog`] | 1A.10 | the TRUST event backlog and its match sweep, train reasons |
//! | [`sweeps`] | 1A.10 | the schedule-match and reconciliation sweeps |
//! | [`pool`] | 1A.11 | `common::pg::PoolSettings` wrapped with `db_pool_*` metrics, the DB health probe |
//! | [`schema`] | 1A.12 | `REQUIRED_MIGRATION`; `wait_for_schema` in 1B.2 |
//!
//! Later phases add `migrate` (1B.1: `api::migrate`, with the migrations
//! directory moving into this crate) and `reads` (phase 4: the internal
//! readers behind narrow views).

pub mod backlog;
pub mod corpus;
pub mod freshness;
pub mod incidents;
pub mod pool;
pub mod reference;
pub mod samples;
pub mod schedule;
pub mod schema;
pub mod sweeps;
pub mod tracking;
pub mod trains;
pub mod validate;
