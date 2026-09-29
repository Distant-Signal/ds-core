SET LOCAL lock_timeout = '5s';
-- -------------------------------------------------------------------------
-- The CORPUS crosswalk (20260928170000_corpus_crosswalk.sql) now keeps a
-- TIPLOC or STANOX only when its inferred CRS is a Knowledgebase station
-- (`stations`): the first production CORPUS load would otherwise have
-- filled 579 TIPLOCs with bus-stop, tram, foreign, closed-station and
-- pseudo codes. See `api::data::corpus_crosswalk` and
-- `common::corpus_inference::restrict_to_stations` (RULES_VERSION 2).
--
-- `stations_fingerprint` is the md5 of the sorted `stations` CRS codes the
-- stored build was filtered against, so `api` rebuilds when a station is
-- added or removed; NULL (every build before this column) always rebuilds.
-- The two counts record how many unambiguous keys the filter left out.
--
-- `corpus_crosswalk_build` has at most one row; adding nullable columns
-- without a default is a catalogue-only change.
-- -------------------------------------------------------------------------
ALTER TABLE corpus_crosswalk_build
    ADD COLUMN stations_fingerprint TEXT,
    ADD COLUMN tiploc_rows_excluded INTEGER,
    ADD COLUMN stanox_rows_excluded INTEGER;
