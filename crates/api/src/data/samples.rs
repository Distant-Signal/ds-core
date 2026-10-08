//! Which stations `poller-ldbws` should sample: moved to
//! `ds_store::reads::sample_stations` (ingest architecture plan 4.2), so
//! the api's `GET /private/sample-stations` and poller-ldbws's direct read
//! (`SAMPLE_STATIONS_SOURCE=db`) share one implementation.

pub use ds_store::reads::sample_stations::{
    SampleSelection, dedup_sample_stations, select_sample_stations,
};
