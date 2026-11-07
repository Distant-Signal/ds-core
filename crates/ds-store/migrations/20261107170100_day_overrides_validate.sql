SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Validates the CHECK 20261107170000_day_overrides.sql added NOT VALID.
-- VALIDATE CONSTRAINT takes SHARE UPDATE EXCLUSIVE (reads and writes carry
-- on). Every existing leg has a NULL variant_weekday, so none can fail.
-- -------------------------------------------------------------------------

ALTER TABLE journey_template_legs
    VALIDATE CONSTRAINT journey_template_legs_variant_weekday_check;
