-- ---------------------------------------------------------------------
-- ONE ROW PER DEPARTURE, not one row per destination bucket: every
-- CIF-SCHEDULE-derived, departure-bearing calling point of every
-- non-cancelled schedule TERMINATING at `destination_crs`, for one whole
-- rail day, UNCAPPED, published by schedule-reference once per CIF
-- delivery. Backs GET /public/trains/search.
--
-- The destination-keyed sibling of schedule_network_departures
-- (20260904110000_schedule_network_departures.sql), which is keyed by the
-- departure's OWN origin CRS. Both are produced from the SAME transient,
-- per-cycle ScheduleIndex in the same pass over the same delivery -- see
-- crates/schedule-reference/src/main.rs's publish_cif_derived_products --
-- so this table costs one extra grouping pass and one extra POST, not a
-- second parse and not a resident index. See
-- docs/superpowers/specs/2026-09-07-train-listing-page-design.md, Approach
-- B (the recommended one), and §0 point 5 for why an origin-keyed bucket
-- could not answer "which trains go to X".
--
-- WHY FLAT AND NOT A JSONB BUCKET, which is what this table was first
-- designed as. A destination bucket is enormous and its size is NOT
-- knowable in advance: London Waterloo buckets ~9,634 departure-bearing
-- calling points for one day, with the next several busiest destinations
-- within the same order of magnitude, so no per-destination cap is
-- defensible -- see
-- docs/superpowers/specs/2026-09-07-train-listing-destination-search-sizing-design.md
-- (§1 for the measurement, §3 Approach C for this shape). Worse, the
-- publish fires once per CIF DELIVERY (roughly daily), not once per
-- 30-minute cycle, so any earliest-first cap freezes at delivery time and
-- is entirely in the past by the evening. Storing the rows instead of a
-- capped bucket removes the cap, and moves both the `now`-forward filter
-- and pagination to REQUEST time, where the clock is actually correct.
--
-- THE PRIMARY KEY IS THE POINT. (service_date, destination_crs, scheduled,
-- train_uid, origin_crs) is also the covering index for the only query
-- shape the read route needs, in exactly the order it needs it: equality
-- on the first two columns, a range scan on `scheduled`, and
-- (scheduled, train_uid, origin_crs) as a total order for a keyset cursor.
-- Waterloo therefore costs the same as Bootle Oriel Road: LIMIT + 1 index
-- entries touched, never the whole day. Do not add a second index without
-- a measured reason; do not reorder these columns.
--
-- `destination_crs` IS stored on every row here, unlike the bucket design
-- it replaces (where it was the key and therefore implicit). It is a real
-- column because it is a real filter predicate.
--
-- No `updated_at`: an ingest wholesale-replaces a whole service_date in one
-- transaction (DELETE by service_date, then one UNNEST bulk INSERT -- see
-- queries::upsert_schedule_destination_departures), so per-row write
-- timestamps would all be identical and carry no information the
-- service_date does not already carry.
--
-- RETENTION: REQUIRED, and pruned by the aggregator -- see
-- crates/aggregator/src/queries.rs's prune_schedule_destination_departures
-- and Config::schedule_destination_departures_retention_days (default 2).
-- This deliberately DIVERGES from schedule_network_departures, which has
-- no pruning job anywhere in this repo. That divergence is not an
-- oversight: the sibling's wholesale replace is scoped per (crs,
-- service_date) over ~2,500 CRS codes, so its steady-state size is
-- trivial, whereas THIS table accrues roughly 377,000 rows for every
-- service date it has ever seen and nothing would ever delete yesterday's.
-- Every read is scoped to today, computed server-side, so nothing reads a
-- past date and a 2-day window is ample -- 2 rather than 1 for safety
-- around the rail-day/midnight boundary and around a CIF delivery that
-- lands late. Design doc §7 Open Question 4, RE-resolved by the addendum's
-- §3 "Retention becomes required" (it reverses the original plan's
-- "retention: none, by parity" answer, which was sound only for the bucket
-- shape).
--
-- Partitioning by service_date is the standard mitigation if the once-daily
-- DELETE + bulk INSERT turns out to cost too much WAL or leave too much
-- bloat. It is NAMED here and deliberately not built -- addendum §7 item 5.
-- ---------------------------------------------------------------------

CREATE TABLE schedule_destination_departures (
    service_date    DATE NOT NULL,
    destination_crs TEXT NOT NULL,
    scheduled       TIME NOT NULL,
    train_uid       TEXT NOT NULL,
    origin_crs      TEXT NOT NULL,
    PRIMARY KEY (service_date, destination_crs, scheduled, train_uid, origin_crs)
);
