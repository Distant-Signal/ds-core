-- -------------------------------------------------------------------------
-- Incidents: NLP-extracted "does the text sound more severe than the
-- regex classifier caught" signal, independently verified the same way
-- extracted_resolution_status is -- a primary pass plus an adversarial
-- pass arguing the disruption is milder than it sounds. Kept in its own
-- confidence column rather than reusing extraction_confidence: the two
-- signals answer different questions (is this over vs. how bad is this),
-- and conflating their confidence would let a disagreement on one wrongly
-- suppress a perfectly good read on the other. See
-- docs/superpowers/specs/2026-08-20-incident-nlp-extraction-design.md.
-- -------------------------------------------------------------------------

ALTER TABLE incidents
    ADD COLUMN extracted_severity TEXT
        CHECK (extracted_severity IN ('normal', 'moderate_disruption', 'severe_disruption', 'blocked_or_suspended')),
    ADD COLUMN extracted_severity_confidence TEXT
        CHECK (extracted_severity_confidence IN ('high', 'low'));
