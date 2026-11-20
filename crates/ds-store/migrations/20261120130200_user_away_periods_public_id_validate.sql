SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Validates the CHECK 20261120130000_user_away_periods_public_id.sql added NOT
-- VALID (SHARE UPDATE EXCLUSIVE: reads and writes carry on). The CHECK is
-- `public_id IS NOT NULL AND public_id ~ '^a_[0-9a-f]{32}$'`, so once it is
-- valid the column cannot hold NULL or a malformed id, which is what NOT
-- NULL would say. The column attribute itself is left for a later
-- migration (`SET NOT NULL` counts as destructive DDL here and needs a
-- `-- contract:` header; with this CHECK valid it would not rescan).
-- -------------------------------------------------------------------------

ALTER TABLE user_away_periods VALIDATE CONSTRAINT user_away_periods_public_id_format;
