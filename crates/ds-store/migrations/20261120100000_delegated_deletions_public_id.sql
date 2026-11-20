SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Opaque public ids for delegated_deletions (DS-534, wave 3 of the opaque ids that
-- journeys.public_id began). The api exposes delegated_deletions.id, which the api serves as the id of a recently deleted journey and of a connected app's deletion (one id serves both lists): a global
-- counter that shows how many rows the platform holds. delegated_deletions.public_id is the id the api hands out
-- instead; the numeric id stays the primary key and every foreign key keeps
-- pointing at it.
--
-- Format: 'x_' followed by the 32 hex digits of gen_random_uuid() (a
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
-- 20261120100100_delegated_deletions_public_id_validate.sql, once the unique index
-- (20261120100100_delegated_deletions_public_id_index.sql) exists. Writers that never name the column
-- keep inserting: the default fills it.
--
-- The backfill runs here in one statement: the table is tiny, so the row
-- locks are held for milliseconds. gen_random_uuid() is called per row.
-- -------------------------------------------------------------------------

ALTER TABLE delegated_deletions ADD COLUMN public_id TEXT;

ALTER TABLE delegated_deletions
    ALTER COLUMN public_id
    SET DEFAULT 'x_' || replace(gen_random_uuid()::text, '-', '');

UPDATE delegated_deletions
SET public_id = 'x_' || replace(gen_random_uuid()::text, '-', '')
WHERE public_id IS NULL;

ALTER TABLE delegated_deletions
    ADD CONSTRAINT delegated_deletions_public_id_format
    CHECK (public_id IS NOT NULL AND public_id ~ '^x_[0-9a-f]{32}$') NOT VALID;
