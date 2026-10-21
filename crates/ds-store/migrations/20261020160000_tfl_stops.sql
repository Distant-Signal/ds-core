SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- TfL's Tube, DLR and tram stops (docs/superpowers/specs/
-- 2026-10-20-tfl-stop-lookup-design.md §2): the untracked-leg stop lookup
-- and the planner's Tube transfers. Written by the ingest-writer from
-- poller-tfl's daily `tfl-stops/1` snapshot on ds:ingest:tfl-stops
-- (ds_store::tfl_stops::replace); read by the api.
--
-- Additive only: two new tables, nothing else changes.
--
-- `search_name` is `common::tfl_stops::search_key(name)` (lower case,
-- punctuation dropped, `&` as `and`), which the api's search matches. No
-- index on it and no pg_trgm: about 400 rows, scanned like `stations`.
--
-- `tfl_stop_crs` links a stop to the National Rail station at the same TfL
-- interchange hub: the hub code's own CRS (`HUBEUS` -> EUS, `source =
-- 'hub'`) and the CRS of each of the hub's rail TIPLOCs in `tiploc_crs`
-- (`source = 'tiploc'`). Rebuilt with each snapshot. It cascades from
-- `tfl_stops`, so a stop leaving TfL's data takes its links with it.
-- -------------------------------------------------------------------------
CREATE TABLE tfl_stops (
    stop_id     TEXT PRIMARY KEY CHECK (stop_id ~ '^[0-9A-Z]{4,20}$'),
    name        TEXT NOT NULL CHECK (char_length(name) BETWEEN 1 AND 100),
    search_name TEXT NOT NULL,
    modes       TEXT[] NOT NULL CHECK (cardinality(modes) >= 1),
    -- [{"id": "victoria", "name": "Victoria"}, ...]
    lines       JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(lines) = 'array'),
    hub_code    TEXT,
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION,
    -- The snapshot's produced_at (decision D13).
    fetched_at  TIMESTAMPTZ NOT NULL
);

CREATE TABLE tfl_stop_crs (
    stop_id TEXT NOT NULL REFERENCES tfl_stops (stop_id) ON DELETE CASCADE,
    crs     CHAR(3) NOT NULL,
    source  TEXT NOT NULL CHECK (source IN ('hub', 'tiploc')),
    PRIMARY KEY (stop_id, crs)
);

CREATE INDEX tfl_stop_crs_crs_idx ON tfl_stop_crs (crs);
