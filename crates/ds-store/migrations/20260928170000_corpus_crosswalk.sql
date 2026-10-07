SET LOCAL lock_timeout = '5s';
-- -------------------------------------------------------------------------
-- The CORPUS-derived TIPLOC->CRS and STANOX->CRS crosswalk: the output of
-- the conservative inference (`common::corpus_inference`, the rule recorded
-- in reference-data/line-catalogue-validation.md, "Decision (2026-09-28)")
-- over the current `corpus_locations`, narrowed to keys with exactly one
-- CRS. Rebuilt by `api` in the same transaction as every CORPUS load, and
-- at startup when the stored build is older than the newest delivery or
-- than the running build's rules (`api::data::corpus_crosswalk`).
--
-- Read only by the timetable lookups' OFF-BY-DEFAULT fallback
-- (`CORPUS_FALLBACK_ENABLED`, chart `api.corpusFallback.enabled`), always
-- after the timetable-derived `tiploc_crs`/`stanox_crs`, which win on any
-- conflict. Empty until CORPUS is loaded, so costs nothing before that.
--
-- Indexes on these NEW tables are built in this same transaction: the
-- tables are empty here (see crates/api/tests/migration_index_locking.rs).
-- -------------------------------------------------------------------------
CREATE TABLE corpus_tiploc_crs (
    tiploc       TEXT PRIMARY KEY,
    crs          TEXT NOT NULL,
    -- NULL when the TIPLOC's CORPUS rows carry no single usable STANOX.
    stanox       TEXT,
    -- The station's CORPUS NLCDESC (a platform TIPLOC carries its
    -- station's name).
    station_name TEXT NOT NULL,
    rule         TEXT NOT NULL CHECK (rule IN ('direct', 'station_name', 'station_qualifier'))
);

CREATE INDEX corpus_tiploc_crs_crs_idx ON corpus_tiploc_crs (crs);

CREATE TABLE corpus_stanox_crs (
    stanox       TEXT PRIMARY KEY,
    crs          TEXT NOT NULL,
    tiploc       TEXT NOT NULL,
    station_name TEXT NOT NULL
);

-- One row: which delivery and which rules version the two tables above were
-- derived from.
CREATE TABLE corpus_crosswalk_build (
    singleton     BOOLEAN     PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    delivered_at  TIMESTAMPTZ NOT NULL,
    rules_version INTEGER     NOT NULL,
    tiploc_rows   INTEGER     NOT NULL,
    stanox_rows   INTEGER     NOT NULL,
    built_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
