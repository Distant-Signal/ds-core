-- -------------------------------------------------------------------------
-- Live STANOX->CRS reference table, replacing (for trust-consumer's
-- purposes -- the CSV stays as a fallback, see this plan's Global
-- Constraints) the static reference-data/stanox-crs.csv. Populated by
-- crates/schedule-reference from the CIF SCHEDULE feed's own TI/A records.
-- Replace-on-write: every daily delivery is a full refresh, so every
-- successful POST /private/stanox-crs upserts the complete current table
-- by `stanox` -- see
-- docs/superpowers/specs/2026-09-01-schedule-ingest-stanox-crs-table-design.md
-- Decision 2.
-- -------------------------------------------------------------------------

CREATE TABLE stanox_crs (
    stanox TEXT PRIMARY KEY,
    crs TEXT NOT NULL,
    tiploc TEXT NOT NULL,
    station_name TEXT NOT NULL,
    source_sequence INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
