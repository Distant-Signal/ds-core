SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- station_samples.tiplocs: the TIPLOCs a station's current LDBWS board
-- covers, decoded from each row's serviceID (`common::service_id_tiploc`)
-- by `queries::upsert_station_samples` on every ingest.
--
-- Why: a board is published under the station's main CRS, but CIF gives
-- some platforms of that station their own TIPLOC with its own CRS
-- (Paddington's Elizabeth line PADTLL -> PDX, St Pancras' Thameslink
-- STPXBOX -> SPL, Liverpool Lime Street low level -> LVL, Glasgow Central
-- low level -> GCL, Reading RDNG4AB -> RDZ; 18 such pairs measured on prod
-- 2026-09-27). A journey stop at PADTLL has crs = PDX, and there is no PDX
-- board, so the per-stop board overlay (`api::data::stop_board`) also reads
-- the board whose tiplocs contain the stop's TIPLOC.
--
-- NULL until a station's first poll after this migration; the ingest
-- rewrites it on the next poll even when the board is unchanged. No index:
-- the table is one row per sampled station (~560 rows), and the read
-- (`crs = ANY(...) OR tiplocs && ...`) scans those narrow rows and only
-- detoasts `departures` for the rows that match.
--
-- ADD COLUMN without a default is catalog-only (no table rewrite).
-- -------------------------------------------------------------------------
ALTER TABLE station_samples ADD COLUMN tiplocs TEXT[];
