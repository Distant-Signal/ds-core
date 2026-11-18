SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Opaque public ids for regular journey templates (DS-534, wave 2 of the opaque ids that
-- journeys.public_id began). The api exposes journey_templates.id, a global BIGSERIAL, in regular-journey page
-- URLs, bookmarks and referrers: it shows how many rows the
-- platform holds and lets anyone probe neighbouring ids. journey_templates.public_id is the id the api hands out instead; the
-- numeric id stays the primary key and every foreign key keeps pointing at it.
--
-- Format: 'r_' followed by the 32 hex digits of gen_random_uuid() (a
-- version 4 UUID: 122 random bits from the server's CSPRNG, not derived
-- from id), as for journeys.public_id (20261117100000). Lowercase, URL-safe,
-- fixed length (34 characters). The letter prefix guarantees an opaque id
-- can never parse as an integer, and each table has its own prefix, so the
-- api can tell the forms apart by shape alone.
--
-- Additive: a nullable column, a default for new rows (a volatile default
-- on ADD COLUMN would rewrite the table, so the default is set after the
-- column exists, which is metadata only), then the backfill. The not-null
-- and format CHECK goes on NOT VALID and is validated in
-- 20261118150200_journey_templates_public_id_validate.sql, once the unique index
-- (20261118150100_journey_templates_public_id_index.sql) exists. Writers that never name the column keep
-- inserting: the default fills it.
--
-- The backfill runs here in one statement: the table is small, so the row
-- locks are held for milliseconds. gen_random_uuid() is called per row.
-- -------------------------------------------------------------------------

ALTER TABLE journey_templates ADD COLUMN public_id TEXT;

ALTER TABLE journey_templates
    ALTER COLUMN public_id
    SET DEFAULT 'r_' || replace(gen_random_uuid()::text, '-', '');

UPDATE journey_templates
SET public_id = 'r_' || replace(gen_random_uuid()::text, '-', '')
WHERE public_id IS NULL;

ALTER TABLE journey_templates
    ADD CONSTRAINT journey_templates_public_id_format
    CHECK (public_id IS NOT NULL AND public_id ~ '^r_[0-9a-f]{32}$') NOT VALID;
