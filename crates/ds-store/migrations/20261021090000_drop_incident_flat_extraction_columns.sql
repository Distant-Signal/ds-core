-- contract: the six flat incident extraction columns (code stopped using them in b944ddb0)
-- -------------------------------------------------------------------------
-- Step 2 of the two-step multi-period extraction migration
-- (docs/superpowers/specs/2026-08-21-multi-period-extraction-design.md §5;
-- step 1 is 20260822090000_incident_extraction_periods.sql). Drops the
-- deprecated flat single-fact columns that `extracted_periods` replaced:
--
--   extracted_resolution_status, extracted_schedule_window, extracted_eta,
--   extraction_confidence, extracted_severity, extracted_severity_confidence
--
-- Nothing has read or written them since b944ddb0 (2026-08-21): `enricher`
-- writes only `extracted_periods` and `aggregator` reads only that, and no
-- api route, export or grant names them (db-grants.yaml grants `incidents`
-- table-wide, and the enricher's column-level UPDATE list never included
-- them). The precondition §5 sets, that the sweep has re-populated
-- `extracted_periods` for the whole table, held in production on
-- 2026-10-21: not one of 2549 rows had a value in any of the six columns,
-- so nothing is lost.
--
-- Their CHECK constraints go with them. DROP COLUMN only marks the columns
-- dropped in the catalog (no rewrite, no scan), but it takes an ACCESS
-- EXCLUSIVE lock on `incidents` to do so, hence the timeout: a long reader
-- makes this fail fast and the startup retry converge, rather than queue
-- every incidents query behind it.
-- -------------------------------------------------------------------------
SET LOCAL lock_timeout = '5s';

ALTER TABLE incidents
    DROP COLUMN extracted_resolution_status,
    DROP COLUMN extracted_schedule_window,
    DROP COLUMN extracted_eta,
    DROP COLUMN extraction_confidence,
    DROP COLUMN extracted_severity,
    DROP COLUMN extracted_severity_confidence;
