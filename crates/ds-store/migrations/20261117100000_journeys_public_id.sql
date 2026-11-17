SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Opaque public ids for journeys (DS-534, stage 2 info finding). The api
-- exposes journeys.id, a global BIGSERIAL, in URLs and responses: it shows
-- how many journeys the platform holds and lets anyone probe neighbouring
-- ids. journeys.public_id is the id the api hands out instead; journeys.id
-- stays the primary key and every foreign key keeps pointing at it.
--
-- Format: 'j_' followed by the 32 hex digits of gen_random_uuid() (a
-- version 4 UUID: 122 random bits from the server's CSPRNG, not derived
-- from id). Lowercase, URL-safe, fixed length (34 characters). The letter
-- prefix guarantees an opaque id can never parse as an integer, so the
-- api can tell it from a legacy numeric id by shape alone.
--
-- Additive: a nullable column, a default for new rows (a volatile default
-- on ADD COLUMN would rewrite the table, so the default is set after the
-- column exists, which is metadata only), then the backfill. The
-- not-null and format CHECK goes on NOT VALID and is validated in
-- 20261117100200_journeys_public_id_validate.sql, once the unique index
-- (20261117100100) exists. A previous api (which never
-- names the column) keeps inserting: the default fills it.
--
-- The backfill runs here in one statement: production holds a handful of
-- journeys, so the row locks are held for milliseconds. gen_random_uuid()
-- is called per row.
-- -------------------------------------------------------------------------

ALTER TABLE journeys ADD COLUMN public_id TEXT;

ALTER TABLE journeys
    ALTER COLUMN public_id
    SET DEFAULT 'j_' || replace(gen_random_uuid()::text, '-', '');

UPDATE journeys
SET public_id = 'j_' || replace(gen_random_uuid()::text, '-', '')
WHERE public_id IS NULL;

ALTER TABLE journeys
    ADD CONSTRAINT journeys_public_id_format
    CHECK (public_id IS NOT NULL AND public_id ~ '^j_[0-9a-f]{32}$') NOT VALID;
