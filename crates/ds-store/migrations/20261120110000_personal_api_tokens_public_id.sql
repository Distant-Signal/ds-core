SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Opaque public ids for personal_api_tokens (DS-534, wave 3 of the opaque ids that
-- journeys.public_id began). The api exposes personal_api_tokens.id, which the api serves on the personal tokens page (the token secret itself, dsp_..., is a different thing and is not touched): a global
-- counter that shows how many rows the platform holds. personal_api_tokens.public_id is the id the api hands out
-- instead; the numeric id stays the primary key and every foreign key keeps
-- pointing at it.
--
-- Format: 'k_' followed by the 32 hex digits of gen_random_uuid() (a
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
-- 20261120110100_personal_api_tokens_public_id_validate.sql, once the unique index
-- (20261120110100_personal_api_tokens_public_id_index.sql) exists. Writers that never name the column
-- keep inserting: the default fills it.
--
-- The backfill runs here in one statement: the table is tiny, so the row
-- locks are held for milliseconds. gen_random_uuid() is called per row.
-- -------------------------------------------------------------------------

ALTER TABLE personal_api_tokens ADD COLUMN public_id TEXT;

ALTER TABLE personal_api_tokens
    ALTER COLUMN public_id
    SET DEFAULT 'k_' || replace(gen_random_uuid()::text, '-', '');

UPDATE personal_api_tokens
SET public_id = 'k_' || replace(gen_random_uuid()::text, '-', '')
WHERE public_id IS NULL;

ALTER TABLE personal_api_tokens
    ADD CONSTRAINT personal_api_tokens_public_id_format
    CHECK (public_id IS NOT NULL AND public_id ~ '^k_[0-9a-f]{32}$') NOT VALID;
