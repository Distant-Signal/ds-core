SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Validates the three CHECKs 20261106230000_recurrence_patterns.sql added
-- NOT VALID. VALIDATE CONSTRAINT takes SHARE UPDATE EXCLUSIVE (reads and
-- writes carry on). Every existing row is 'weekly', interval 1, with no
-- anchor, so none can fail.
-- -------------------------------------------------------------------------

ALTER TABLE journey_templates VALIDATE CONSTRAINT journey_templates_recurrence_kind_check;
ALTER TABLE journey_templates VALIDATE CONSTRAINT journey_templates_interval_weeks_check;
ALTER TABLE journey_templates VALIDATE CONSTRAINT journey_templates_anchor_week_check;
