-- -------------------------------------------------------------------------
-- Threads Darwin/LDBWS's own `platform` signal
-- (`common::StationDeparture.platform`/`planned_platform`) through to the
-- journey timeline's per-stop platform display, mirroring
-- `20260921070000_skipped_stations.sql`'s own shape and reasoning
-- verbatim -- see that migration's header comment for the full "captured
-- at pin time, twice on the way through" explanation.
--
-- Unlike `skipped_stations` (a `TEXT[]`, always safely defaultable to
-- `'{}'`), platform is a single nullable value: there is no "no signal"
-- sentinel to invent that isn't already `NULL`, so both new columns here
-- are plain nullable `TEXT`, `COALESCE`d rather than `CASE`-guarded on the
-- way into `trains` (see `data::trains::find_or_create_train_with_schedule_match`).
--
-- Darwin/RDM's live departure board only ever reports a CRS's OWN current
-- platform for a service actually departing that station -- never a
-- per-calling-point platform for the rest of the route (see
-- `poller-ldbws/src/schema.rs`'s `RdmCallingPoint`, which carries no
-- platform field at all). So these columns capture the ORIGIN calling
-- point's platform only, from the exact departure-board row the user
-- picked when tracking the train (`TrackTrainForm.tsx`'s `pickDeparture`) --
-- `journey::build_journey_stops` attaches it to that one stop; every other
-- calling point's `platform` stays genuinely unknown (`NULL`), which is
-- the honest answer given what this data source can see.
-- -------------------------------------------------------------------------

ALTER TABLE train_subscriptions
    ADD COLUMN pin_platform TEXT,
    ADD COLUMN pin_planned_platform TEXT;

ALTER TABLE trains
    ADD COLUMN platform TEXT,
    ADD COLUMN planned_platform TEXT;
