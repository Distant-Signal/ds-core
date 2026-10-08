//! Moved to `ds_store::migrate::legacy_backfill` (ingest phase 5 prep, Q2 of
//! docs/ingest-phase5-runbook.md), with its tests; re-exported so the
//! api's startup guard and the deprecated `backfill_trains` binary keep
//! working until step 5.4b deletes them. The new entry point is
//! `ds-migrate backfill-trains`.

pub use ds_store::migrate::legacy_backfill::*;
