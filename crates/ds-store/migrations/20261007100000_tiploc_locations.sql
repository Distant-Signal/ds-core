SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- `tiploc_locations`: one row per CIF `TI` record -- the TIPLOC's display
-- name, what kind of place it is, and, for a bus stop or ferry terminal,
-- the station it belongs to. Published by crates/schedule-reference
-- (`locations.rs`) through POST /private/tiploc-locations; every daily
-- delivery replaces the whole table (upsert by `tiploc`, then the TIPLOCs
-- the delivery no longer lists are deleted, in one transaction).
--
-- Deliberately separate from `tiploc_crs`, which feeds the planner's
-- interchange data and the CRS crosswalks: a bus stop must never become a
-- station there. `parent_crs` is a CRS as schedule-reference saw it; readers
-- join `stations` and ignore a parent with no row there.
--
-- See docs/superpowers/specs/2026-10-06-tiploc-locations-design.md.
-- -------------------------------------------------------------------------
CREATE TABLE tiploc_locations (
    tiploc TEXT PRIMARY KEY,
    location_type TEXT NOT NULL CHECK (location_type IN (
        'station', 'bus_stop', 'ferry_terminal', 'junction', 'siding',
        'passing_point', 'other'
    )),
    name TEXT NOT NULL,
    display_name TEXT NOT NULL,
    ti_name TEXT,
    ti_crs TEXT,
    stanox TEXT,
    msn_name TEXT,
    msn_code TEXT,
    msn_easting INTEGER,
    msn_northing INTEGER,
    msn_interchange SMALLINT,
    parent_crs TEXT,
    parent_source TEXT CHECK (parent_source IN ('same_tiploc', 'nearest', 'curated')),
    parent_distance_m INTEGER,
    rail_calls INTEGER NOT NULL DEFAULT 0,
    rail_passes INTEGER NOT NULL DEFAULT 0,
    bus_calls INTEGER NOT NULL DEFAULT 0,
    ship_calls INTEGER NOT NULL DEFAULT 0,
    source_sequence INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK ((parent_crs IS NULL) = (parent_source IS NULL))
);

-- The planner's endpoint list and the location search read only the bus
-- stops and ferry terminals (a few hundred of ~12k rows).
CREATE INDEX tiploc_locations_road_or_water_idx ON tiploc_locations (location_type)
    WHERE location_type IN ('bus_stop', 'ferry_terminal');
