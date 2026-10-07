-- Iarnrod Eireann live departure-board samples -- Tier B of
-- docs/superpowers/specs/2026-09-05-ireland-rail-support-design.md. Same
-- shape as `station_samples` (20260510023522_initial.sql): one row per
-- station, replaced wholesale each poll cycle, no history table.
--
-- Deliberately NO foreign key to `island_of_ireland_stations.id` --
-- `station_id` here is api.irishrail.ie's own StationCode, which this
-- plan has not confirmed matches the GTFS-derived id Tier A stores. See
-- docs/superpowers/plans/2026-09-05-ireland-rail-support-plan.md's
-- Judgment Call #1.

CREATE TABLE island_of_ireland_station_samples (
    station_id  TEXT        PRIMARY KEY,
    network     TEXT        NOT NULL,
    polled_at   TIMESTAMPTZ NOT NULL,
    departures  JSONB       NOT NULL DEFAULT '[]'
);
