-- -------------------------------------------------------------------------
-- `schedule-reference`'s OWN per-delivery completion marker.
--
-- The bug this exists to fix: `schedule-reference` seeded its restart-durable
-- `last_processed_delivery` dedup marker from `MAX(delivered_at) FROM
-- schedule_feed_ingests` -- a row the SIBLING container (`schedule-ingest`)
-- writes the moment it has EXTRACTED and verified a delivery zip, long
-- before `schedule-reference` has read a single byte of it. The two facts
-- are not the same fact:
--
--   * `schedule_feed_ingests.delivered_at` means "a delivery landed on the
--     PVC and was verified stable" (`schedule-ingest`'s job, done).
--   * this table means "every product `schedule-reference` derives from that
--     delivery was published successfully" (`schedule-reference`'s job,
--     done).
--
-- Conflating them meant that if the `reference` container restarted after
-- ingest recorded a delivery but before reference finished processing it --
-- an OOM kill during the in-memory CIF parse, a rolling deploy, any crash --
-- the restarted process seeded `last_processed_delivery` to that delivery's
-- directory name anyway, `poll_once` short-circuited on "no new delivery
-- since last successful parse", and EVERY product for that delivery (both
-- CRS crosswalks, fixed links, per-line population, network departures, and
-- up to 8 days each of destination departures and full calling points)
-- silently never published until the next delivery landed roughly 24 hours
-- later. See `crates/schedule-reference/src/main.rs`'s
-- `seed_last_processed_delivery` and `poll_once`.
--
-- `delivery` is the delivery DIRECTORY NAME (`YYYYMMDDTHHMMSSZ`, see
-- `schedule-reference::discovery::CompleteDelivery::dir_name`), stored
-- verbatim rather than as a timestamp, because that string is exactly what
-- `poll_once` compares -- see
-- `common::ingest::ScheduleReferencePublishRequest`'s own doc comment.
--
-- One row per delivery, not a single mutable "latest" row: the history is
-- cheap (one row per day, ~17 bytes of key) and is the only durable evidence
-- available after the fact that a given delivery's publish cycle really did
-- complete. `completed_at` is when the LAST product of that delivery's cycle
-- finished publishing, which is what makes "most recently completed" an
-- honest `ORDER BY completed_at DESC LIMIT 1`.
-- -------------------------------------------------------------------------
CREATE TABLE schedule_reference_publishes (
    delivery     TEXT        PRIMARY KEY,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Backs the one read this table has: "which delivery did `schedule-reference`
-- most recently finish publishing?" (`queries::last_completed_schedule_reference_publish`).
-- At one row per day this is not about scan cost -- it is about the read
-- being an index-only top-1 regardless of how many years of markers
-- accumulate.
CREATE INDEX schedule_reference_publishes_completed_at_idx
    ON schedule_reference_publishes (completed_at DESC);
