-- -------------------------------------------------------------------------
-- Incidents: NLP-extracted structured fields, written only by the
-- `enricher` crate and read only by `aggregator`. All nullable and
-- additive -- a row with every column NULL behaves identically to today's
-- regex-only classifier. See
-- docs/superpowers/specs/2026-08-20-incident-nlp-extraction-design.md.
-- -------------------------------------------------------------------------

ALTER TABLE incidents
    ADD COLUMN source_text_hash TEXT,
    ADD COLUMN extracted_category TEXT,
    ADD COLUMN extracted_resolution_status TEXT
        CHECK (extracted_resolution_status IN ('ongoing', 'residual', 'resolved')),
    ADD COLUMN extracted_schedule_window JSONB,
    ADD COLUMN extracted_eta TIMESTAMPTZ,
    ADD COLUMN extraction_confidence TEXT
        CHECK (extraction_confidence IN ('high', 'low')),
    ADD COLUMN extraction_model_version TEXT,
    ADD COLUMN extracted_at TIMESTAMPTZ;
