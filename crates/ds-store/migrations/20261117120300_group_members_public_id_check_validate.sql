SET LOCAL lock_timeout = '5s';

-- Validates group_members_public_id_not_null (20261117120200). VALIDATE
-- CONSTRAINT takes SHARE UPDATE EXCLUSIVE: reads and writes carry on.
ALTER TABLE group_members VALIDATE CONSTRAINT group_members_public_id_not_null;
