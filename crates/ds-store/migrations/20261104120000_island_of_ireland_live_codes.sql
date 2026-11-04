SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Island of Ireland: join the catalogue to the live boards, and keep the
-- line and station display data the feeds already carry.
--
-- island_of_ireland_live_stations: Iarnród Éireann's live station list
-- (getAllStationsXML), sent by poller-irish-rail-live as
-- `ioi-live-stations/1`. Its `code` is the StationCode the departure boards
-- (island_of_ireland_station_samples.station_id) are keyed by. Upsert on
-- code, no history, like the other catalogue tables.
--
-- island_of_ireland_stations.live_code: that StationCode for a catalogue
-- station (a GTFS stop_id), matched by the ingest-writer on name and
-- position with explicit overrides (common::island_of_ireland::live_codes).
-- NULL when the station has no live board or no confident match.
-- raw_name: the source's own name when `name` is a tidied form of it
-- (NIR's uppercase OpenDataNI values).
--
-- island_of_ireland_lines: the GTFS route's short name, agency name and
-- colours (`#RRGGBB`).
--
-- Expand-only: nullable columns with no default (catalog-only changes)
-- and a new table.
-- -------------------------------------------------------------------------

ALTER TABLE island_of_ireland_stations
    ADD COLUMN IF NOT EXISTS live_code TEXT,
    ADD COLUMN IF NOT EXISTS raw_name TEXT;

ALTER TABLE island_of_ireland_lines
    ADD COLUMN IF NOT EXISTS short_name TEXT,
    ADD COLUMN IF NOT EXISTS agency TEXT,
    ADD COLUMN IF NOT EXISTS color TEXT,
    ADD COLUMN IF NOT EXISTS text_color TEXT;

CREATE TABLE IF NOT EXISTS island_of_ireland_live_stations (
    code        TEXT        PRIMARY KEY,
    name        TEXT        NOT NULL,
    alias       TEXT,
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION,
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
