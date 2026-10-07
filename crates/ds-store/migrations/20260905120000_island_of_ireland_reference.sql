-- Iarnród Éireann (and, once built, NIR) station/line reference data --
-- Tier A of docs/superpowers/specs/2026-09-05-ireland-rail-support-design.md.
-- Same upsert-on-id, no-history posture as `stations`/`tocs`
-- (20260706004003_reference_data.sql): reference data is a snapshot of
-- current facts, not an event stream worth auditing.
--
-- `id` is TEXT, not CHAR(3) like `stations.crs` -- GTFS stop/route ids are
-- not fixed-width (friction doc's own sample ids run longer than 3
-- characters). `network` is a TEXT enum tag
-- (common::island_of_ireland::IslandOfIrelandNetwork's kebab-case wire
-- values: 'republic-of-ireland' | 'northern-ireland'), not a Postgres
-- native enum -- matching this schema's existing preference for plain TEXT
-- columns over native enum types (no CREATE TYPE anywhere in this
-- migration set).

CREATE TABLE island_of_ireland_stations (
    id          TEXT        PRIMARY KEY,
    name        TEXT        NOT NULL,
    network     TEXT        NOT NULL,
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION,
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX island_of_ireland_stations_network_idx ON island_of_ireland_stations (network);

-- `stations` is an ordered JSONB array of `island_of_ireland_stations.id`
-- values (IslandOfIrelandLineDefinition.stations), not a join table --
-- same posture `schedule_line_population.population` and
-- `station_samples.departures` already take for a "always written and
-- read as a whole ordered unit" value (20260510023522_initial.sql's own
-- comment on `line_status.statuses`).
CREATE TABLE island_of_ireland_lines (
    id          TEXT        PRIMARY KEY,
    name        TEXT        NOT NULL,
    network     TEXT        NOT NULL,
    stations    JSONB       NOT NULL DEFAULT '[]',
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX island_of_ireland_lines_network_idx ON island_of_ireland_lines (network);
