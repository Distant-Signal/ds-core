SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- group_members.public_id: an opaque, random, per-membership id that the
-- api shows to the other members of a group in place of users.id (DS-534
-- assessment L12: users.id is the OIDC sub, which account continuity
-- re-keys on, so it must not be handed to co-members). One id per
-- (group, user) membership, so the same person has a different id in each
-- group and the ids cannot be joined across groups. It never changes: a
-- re-key of users.id (ON UPDATE CASCADE) leaves it alone.
--
-- Expand-only: a nullable column, a default so every new row gets an id
-- from the first statement on (no writer needs to change), then a backfill
-- of the existing rows. The default is set BEFORE the backfill so a row
-- inserted in between cannot be missed. gen_random_uuid() is a built-in
-- (PostgreSQL 13+): 122 random bits, printed as 32 hex characters.
-- 20261117120100 adds the unique index; 20261117120200 and 20261117120300
-- add and validate a NOT NULL check.
-- -------------------------------------------------------------------------

ALTER TABLE group_members ADD COLUMN IF NOT EXISTS public_id TEXT;

ALTER TABLE group_members
    ALTER COLUMN public_id SET DEFAULT replace(gen_random_uuid()::text, '-', '');

UPDATE group_members
SET public_id = replace(gen_random_uuid()::text, '-', '')
WHERE public_id IS NULL;
