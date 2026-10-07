//! Each journey stop's LDBWS-style live status, served as `status` (and
//! `lateMinutes`) on every `JourneyStop`. This sits beside the existing
//! finer-grained fields (actual/estimated times, `stopStatus`,
//! `variationStatus`, `delayMinutes`), which stay unchanged. It exists so a
//! client moving off LDBWS `GetServiceDetails` has one field that reads
//! like LDBWS's `at`/`et`/`isCancelled`.
//!
//! Values, first match wins:
//!
//! | `status`    | When | LDBWS equivalent |
//! |-------------|------|------------------|
//! | `Cancelled` | Darwin or TRUST says this call is skipped (`stopStatus: Skipped`), or the train is cancelled and has not reached this stop | `isCancelled: true` |
//! | `Departed`  | A TRUST departure was reported here | `at`/`atd` = the actual time |
//! | `Arrived`   | A TRUST arrival, but no departure yet (or the terminus) | `ata` = the actual time |
//! | `NoReport`  | Not reported, but a LATER stop has been, so the train has passed it | `at: "No report"` |
//! | `Late`      | Not yet reached, and the live estimate is at least one minute after the PUBLIC time; `lateMinutes` is by how much | `et: "HH:MM"` (later than scheduled) or `"Delayed"` |
//! | `OnTime`    | Not yet reached, and the live estimate is not after the public time | `et: "On time"` |
//! | `Scheduled` | Not yet reached, with no live data for the train or no estimate for this stop | none (LDBWS shows `"On time"` here; DS does not claim it) |
//!
//! "The public time" is the stop's public (GBTT) departure, else its public
//! arrival, falling back to the working (`scheduled*`) time only for a side
//! with no public time (design doc §9 decision 2). Like Darwin, a train
//! running late into a terminus whose public arrival carries recovery
//! margin is forecast against that later public time.
//!
//! Differences from Darwin:
//! - The estimates are TRUST's current delay carried forward to every
//!   later stop's working time (`journey::apply_delay_estimates`), not
//!   Darwin's own forecasts, so recovery time between stops is not
//!   modelled.
//! - A train cancelled en route marks every stop it has not reached as
//!   `Cancelled`, including a stop it passed without TRUST reporting it
//!   (unless a later stop was reported, which gives `NoReport`).
//! - There is no `Delayed` (late, with no estimate): a stop with no
//!   estimate is `Scheduled`.

// Moved to ds_store::trains::stop_live_status (ingest architecture plan 1A.4)
pub use ds_store::trains::stop_live_status::{LiveStopStatus, apply, train_has_live_data};
