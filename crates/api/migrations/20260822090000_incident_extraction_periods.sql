-- -------------------------------------------------------------------------
-- Incidents: multi-period NLP extraction storage. Replaces the flat
-- single-fact extraction shape (extracted_resolution_status /
-- extracted_schedule_window / extracted_eta / extraction_confidence /
-- extracted_severity / extracted_severity_confidence) with one JSONB array
-- column holding `Vec<ExtractionPeriod>` (each period self-contained,
-- including its own confidence fields). Written only by the `enricher`
-- crate and read only by `aggregator`, same as the six columns it replaces.
--
-- This is step 1 of a deliberate two-step migration (design §5): the six
-- deprecated flat columns are left in place, untouched, as a rollback
-- window -- `enricher` starts writing only `extracted_periods`;
-- `aggregator` starts reading only `extracted_periods`
-- (`extracted_periods IS NULL` behaves identically to "no extraction yet,"
-- same fail-safe default as the old columns' NULL state). The six old
-- columns are dropped in a follow-up housekeeping migration once the sweep
-- has re-populated `extracted_periods` for the whole table. See
-- docs/superpowers/specs/2026-08-21-multi-period-extraction-design.md, §3/§5.
-- -------------------------------------------------------------------------

ALTER TABLE incidents
    ADD COLUMN extracted_periods JSONB;
