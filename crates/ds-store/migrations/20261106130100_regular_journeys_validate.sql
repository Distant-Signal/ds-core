SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Validates the three constraints 20261106130000_regular_journeys.sql
-- added NOT VALID. VALIDATE CONSTRAINT takes SHARE UPDATE EXCLUSIVE on
-- journey_templates (reads and writes carry on; the foreign key also takes
-- ROW SHARE on the same table as its referenced side). Every existing row
-- has NULL in the new columns and 0 in target_day_offset, so none can fail.
-- -------------------------------------------------------------------------

ALTER TABLE journey_templates VALIDATE CONSTRAINT journey_templates_target_check;
ALTER TABLE journey_templates VALIDATE CONSTRAINT journey_templates_paired_fkey;
ALTER TABLE journey_templates VALIDATE CONSTRAINT journey_templates_paired_not_self;
