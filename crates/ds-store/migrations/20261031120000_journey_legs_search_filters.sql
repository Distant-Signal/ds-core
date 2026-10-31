SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- A window leg's search filters (DS-215): the Trains section's Between
-- stations filters -- operator, and the Advanced options' Starts at,
-- Terminates at, Previously calls at, Previously stops at, Call at, Pass
-- through, Avoid completely and Don't stop at -- saved with the leg's
-- window by "Add a leg" and "Edit search", so the leg's candidates
-- (`GET /Journeys/{id}/legs/{legId}/candidates`) list only the trains
-- they allow. One JSON object with the api's camelCase names
-- (`crates/api/src/data/journeys.rs`, `LegSearchFilters`), each value the
-- same text `/public/trains/search` takes; NULL for no filters, and for
-- every leg before this column.
--
-- Additive only: one nullable column (a catalog-only ADD COLUMN, no
-- rewrite). No CHECK on the shape: the api validates the filters when it
-- writes them and again when it reads them.
-- -------------------------------------------------------------------------
ALTER TABLE journey_legs ADD COLUMN search_filters JSONB;
