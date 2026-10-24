SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- TfL's Tube, DLR and tram route sequences (DS-151; docs/superpowers/specs/
-- 2026-10-23-trips-plan-tfl-stop-ends-design.md §3): each line's routes end
-- to end (`Uxbridge ↔ Aldgate`), their stops in running order. Written by
-- the ingest-writer from poller-tfl's daily `tfl-line-routes/1` snapshot on
-- ds:ingest:tfl-stops (ds_store::tfl_line_routes::replace); read by the
-- api's Tube access legs, which measure a ride along a route and charge a
-- change where no one route has both stops.
--
-- Additive only: one new table, nothing else changes, so no lock on any
-- existing table.
--
-- `stop_ids` are `tfl_stops.stop_id` values but carry no foreign key: the
-- two snapshots arrive separately, and a stop missing from `tfl_stops` only
-- means the api cannot place it (it falls back to its spanning-tree
-- estimate). A stop may appear twice in a route (the Circle passes Edgware
-- Road twice). `route_index` is the route's place in TfL's list, which
-- keeps the rows of one line and direction apart.
-- -------------------------------------------------------------------------
CREATE TABLE tfl_line_routes (
    line_id     TEXT NOT NULL CHECK (line_id ~ '^[a-z0-9-]{1,40}$'),
    direction   TEXT NOT NULL CHECK (direction IN ('inbound', 'outbound')),
    route_index SMALLINT NOT NULL CHECK (route_index >= 0),
    name        TEXT NOT NULL CHECK (char_length(name) <= 200),
    stop_ids    TEXT[] NOT NULL CHECK (cardinality(stop_ids) BETWEEN 2 AND 200),
    -- The snapshot's produced_at (decision D13).
    fetched_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (line_id, direction, route_index)
);
