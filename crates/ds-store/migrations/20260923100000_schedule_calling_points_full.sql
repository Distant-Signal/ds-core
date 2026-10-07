-- -------------------------------------------------------------------------
-- Dynamic Trip Planning Phase 2: the persisted, whole-network, STP-resolved
-- calling-point product this app's own §0.2 investigation already
-- identified as sitting in memory every ~30-minute cycle
-- (schedule_query::ScheduleIndex) but never previously written to
-- Postgres. See
-- docs/superpowers/specs/2026-09-22-dynamic-trip-planning-design.md §0.2
-- and this plan's own Judgment Call 1 for why this is the chosen answer
-- to that design spec's Open Question 1.
--
-- One row per calling point of one resolved (non-cancelled) schedule on one
-- service date -- the literal GTFS "ordered stop_times per trip" shape,
-- persisted un-bucketed for the first time. `api` reads this back, per
-- trip-planning query, to build a Connection array fresh (see
-- schedule_query::connections::build_connections) -- never held resident
-- between requests.
--
-- Wholesale-replaced per service_date on every publish cycle, same
-- "DELETE ... WHERE service_date = $1" convention
-- schedule_destination_departures already established -- see
-- crates/api/src/routes/ingest.rs's existing handler for that table.
-- -------------------------------------------------------------------------

CREATE TABLE schedule_calling_points_full (
    service_date      DATE NOT NULL,
    uid               TEXT NOT NULL,
    -- 0-based position within this schedule's own calling-point sequence
    -- (from the publisher's own `.enumerate()`, schedule-reference's
    -- publish_schedule_calling_points_full) -- the ORDER BY key that
    -- reconstructs stopping order; NOT a real CIF field, assigned at
    -- publish time. Only relative order matters for this column; there is
    -- no significance to 0 itself beyond "first."
    seq               SMALLINT NOT NULL,
    tiploc            TEXT NOT NULL,
    -- Persisted for Phase 3/4's future use (route-pattern grouping, real
    -- CSA/RAPTOR boarding-vs-alighting logic) -- not read by this phase's
    -- own code (`fetch_calling_points_for_date`'s SELECT omits it). Not
    -- dead: a future reader should not assume it is safe to drop.
    kind              TEXT NOT NULL CHECK (kind IN ('origin', 'intermediate', 'terminate')),
    booked_arrival    TIME,
    booked_departure  TIME,
    day_offset        SMALLINT NOT NULL,
    PRIMARY KEY (service_date, uid, seq)
);
