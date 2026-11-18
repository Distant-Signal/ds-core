SET LOCAL lock_timeout = '5s';

-- Validates group_members_public_id_format (20261118130100). VALIDATE
-- CONSTRAINT takes SHARE UPDATE EXCLUSIVE: reads and writes carry on.
-- Every row was prefixed by 20261118130000, so it passes.
ALTER TABLE group_members VALIDATE CONSTRAINT group_members_public_id_format;
