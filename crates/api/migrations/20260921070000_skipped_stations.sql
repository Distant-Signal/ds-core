-- -------------------------------------------------------------------------
-- Threads Darwin/LDBWS's own explicit per-calling-point `isCancelled` flag
-- (`common::StationDeparture.skipped_stations`) through to the journey
-- timeline's new "Skipped" stop status
-- (`crates/api/src/data/journey.rs`'s `StopStatus`/`SkipSource`).
--
-- That signal is captured only as a one-time snapshot at pin/search time
-- (`GET /public/stations/{crs}/departures` -> `TrackTrainForm.tsx`'s
-- picker), long before a `trains` row -- keyed on the real `(train_uid,
-- service_date)` identity `journey::build_journey_stops` reads from -- may
-- even exist yet. So it has to be captured twice on the way through:
--
-- 1. `train_subscriptions.pin_skipped_stations` holds the PIN's own
--    snapshot from the moment the user tracked this service, surviving
--    between pin creation and whenever a schedule match (synchronous at
--    pin time, or the periodic sweep) actually resolves a `trains_id` --
--    exactly the same "pin-scoped fact captured now, applied to the shared
--    row once resolved" shape `pin_origin_crs`/`pin_scheduled_departure`
--    already have (`20260828120000_train_tracking.sql`).
-- 2. `trains.skipped_stations` is the SHARED row's own copy, merged in by
--    `data::trains::find_or_create_train_with_schedule_match` at the same
--    point it already merges `calling_points`/`destination_crs` -- this is
--    what `journey::build_journey_stops` actually reads.
--
-- Both are `TEXT[] NOT NULL DEFAULT '{}'`, never `NULL`, so callers can
-- always treat "no skip signal from Darwin" as a plain empty slice rather
-- than an extra `Option` layer -- CRS codes, matching
-- `common::StationDeparture.skipped_stations`'s own shape verbatim.
-- -------------------------------------------------------------------------

ALTER TABLE train_subscriptions
    ADD COLUMN pin_skipped_stations TEXT[] NOT NULL DEFAULT '{}';

ALTER TABLE trains
    ADD COLUMN skipped_stations TEXT[] NOT NULL DEFAULT '{}';
