SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Validates the CHECK 20261120100000_delegated_deletions_public_id.sql added NOT
-- VALID (SHARE UPDATE EXCLUSIVE: reads and writes carry on). The CHECK is
-- `public_id IS NOT NULL AND public_id ~ '^x_[0-9a-f]{32}$'`, so once it is
-- valid the column cannot hold NULL or a malformed id, which is what NOT
-- NULL would say. The column attribute itself is left for a later
-- migration (`SET NOT NULL` counts as destructive DDL here and needs a
-- `-- contract:` header; with this CHECK valid it would not rescan).
-- -------------------------------------------------------------------------

ALTER TABLE delegated_deletions VALIDATE CONSTRAINT delegated_deletions_public_id_format;
