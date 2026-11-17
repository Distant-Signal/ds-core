SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Validates the CHECK 20261117100000_journeys_public_id.sql added NOT
-- VALID (SHARE UPDATE EXCLUSIVE: reads and writes carry on), then makes
-- the column NOT NULL. With a validated `public_id IS NOT NULL` CHECK in
-- place SET NOT NULL skips its own full scan. The CHECK stays: it also
-- pins the id's format.
-- -------------------------------------------------------------------------

ALTER TABLE journeys VALIDATE CONSTRAINT journeys_public_id_format;
ALTER TABLE journeys ALTER COLUMN public_id SET NOT NULL;
