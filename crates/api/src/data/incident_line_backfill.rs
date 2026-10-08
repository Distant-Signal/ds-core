//! Moved to `ds_store::incidents::line_backfill` (ingest phase 5 prep, Q2
//! of docs/ingest-phase5-runbook.md); re-exported so the deprecated
//! `backfill_incident_lines` binary and the api's tests keep working until
//! step 5.4b deletes them. The new entry point is
//! `writer-maintenance backfill-incident-lines` in the ingest-writer image.

pub use ds_store::incidents::line_backfill::*;
