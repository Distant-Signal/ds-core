//! The schedule publish protocol and products: `SchedulePublishPart`
//! (with `new` validating the chunk parameters), the publish-key SQL,
//! the destination-departure, calling-point, network-departure and
//! line-population upserts and readers, the feed-ingest and
//! reference-publish markers, and `schedule_feed_ingest_problem`
//! (spec §5.2).
//!
//! Filled by plan task 1A.7. The submodules live under `schedule/`; every
//! item is re-exported here, so callers name `ds_store::schedule::…`.
//!
//! - `markers`: the feed-ingest and reference-publish markers, and the
//!   feed-ingest record's validation.
//! - `population`: the per-line schedule population.
//! - `publish`: the network, destination and calling-point products and
//!   the chunked diff-publish protocol.
//! - [`services`]: the per-date `schedule_services` publish (plan 2a.2).
//! - [`summaries`]: `line_train_summaries`' derivation and the population
//!   upsert that writes them in its transaction (plan 2a.2).

mod markers;
mod population;
mod publish;
pub mod services;
pub mod summaries;

pub use markers::{
    ScheduleFeedFile, ScheduleFeedIngestRequest, ScheduleFeedSource, insert_schedule_feed_ingest,
    insert_schedule_reference_publish, last_completed_schedule_reference_publish,
    last_schedule_feed_fetch, schedule_feed_ingest_problem,
};
pub use population::{
    ConditionalPopulation, get_overnight_schedule_line_population_entries,
    get_schedule_line_population, get_schedule_line_population_conditional,
    get_schedule_line_population_entries, list_line_ids_with_uid_in_population,
    list_schedule_line_population_dates, upsert_schedule_line_population,
};
pub use publish::{
    MAX_PUBLISH_ID_LEN, SCHEDULE_PUBLISH_ROWS_METRIC, SCHEDULE_PUBLISH_STAGED_MISMATCH_METRIC,
    STORE_SCHEDULE_PUBLISH_ROWS_METRIC, STORE_SCHEDULE_PUBLISH_STAGED_MISMATCH_METRIC,
    ScheduleCallingPointsFullRow, ScheduleDestinationDeparturesRow, ScheduleNetworkDeparturesRow,
    SchedulePublishBusy, SchedulePublishPart,
    finish_schedule_calling_points_full_publish_without_rows,
    finish_schedule_destination_departures_publish_without_rows, is_statement_timeout,
    register_schedule_publish_metrics, upsert_schedule_calling_points_full,
    upsert_schedule_calling_points_full_publish_part, upsert_schedule_destination_departures,
    upsert_schedule_destination_departures_publish_part, upsert_schedule_network_departures,
    use_store_metric_names,
};
