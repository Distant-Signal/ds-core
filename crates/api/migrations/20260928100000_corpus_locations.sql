SET LOCAL lock_timeout = '5s';
-- -------------------------------------------------------------------------
-- Network Rail CORPUS location reference data, pushed by RDM over the same
-- SFTP account as the CIF timetable and loaded by `schedule-ingest`
-- (`CORPUS_INGEST_ENABLED`, off by default) through
-- `POST /private/corpus-locations`. See
-- docs/superpowers/specs/2026-09-28-corpus-sftp-ingest-design.md.
--
-- `corpus_locations` holds exactly ONE delivery: every load replaces the
-- whole set in one transaction (`data::corpus::replace_corpus_locations`),
-- so readers see either the old or the new extract, never a mixture.
--
-- There is no natural key. In a real 55,972-row extract 20 NLCs and 20
-- TIPLOCs repeat, most rows have no TIPLOC/STANOX/CRS at all, and many
-- STANOXes are shared. Hence a surrogate id and plain (partial) lookup
-- indexes rather than a unique constraint that a future extract could
-- violate and so block every load.
--
-- Values are stored as delivered, normalised only so that equal codes
-- compare equal: trimmed, a blank (CORPUS pads absent values with a single
-- space) stored as NULL, an all-digit NLC left-padded to 6 digits and an
-- all-digit STANOX to 5 (extracts have carried both as JSON numbers, which
-- lose their leading zeros). `crs` is CORPUS's `3ALPHA`.
--
-- Indexes on these NEW tables are built in this same transaction: both
-- tables are empty here, so nothing is locked for longer than an empty
-- build (see crates/api/tests/migration_index_locking.rs).
-- -------------------------------------------------------------------------
CREATE TABLE corpus_locations (
    id           BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nlc          TEXT        NOT NULL,
    stanox       TEXT,
    tiploc       TEXT,
    crs          TEXT,
    uic          TEXT,
    nlc_desc     TEXT,
    nlc_desc16   TEXT,
    delivered_at TIMESTAMPTZ NOT NULL,
    source_file  TEXT        NOT NULL,
    loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX corpus_locations_tiploc_idx ON corpus_locations (tiploc) WHERE tiploc IS NOT NULL;
CREATE INDEX corpus_locations_stanox_idx ON corpus_locations (stanox) WHERE stanox IS NOT NULL;
CREATE INDEX corpus_locations_crs_idx ON corpus_locations (crs) WHERE crs IS NOT NULL;
CREATE INDEX corpus_locations_nlc_idx ON corpus_locations (nlc);

-- -------------------------------------------------------------------------
-- The publish/freshness marker: one row per successfully loaded delivery,
-- written in the same transaction as the replace above. `delivered_at` is
-- the delivered file's own mtime (the identity of "which delivery"),
-- `loaded_at` when it was loaded. `MAX(delivered_at)` answers "how fresh is
-- CORPUS". CORPUS is published monthly, so this grows by about 12 tiny rows
-- a year and needs no pruning.
-- -------------------------------------------------------------------------
CREATE TABLE corpus_deliveries (
    delivered_at TIMESTAMPTZ PRIMARY KEY,
    source_file  TEXT        NOT NULL,
    row_count    INTEGER     NOT NULL,
    loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
