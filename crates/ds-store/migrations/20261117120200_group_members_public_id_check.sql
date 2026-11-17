SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- group_members.public_id is never NULL (20261117120000 backfilled it and
-- gave it a default). A CHECK on NOT VALID, validated by
-- 20261117120300, in place of SET NOT NULL, which scans the table under
-- ACCESS EXCLUSIVE.
-- -------------------------------------------------------------------------

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'group_members_public_id_not_null'
          AND conrelid = 'group_members'::regclass
    ) THEN
        ALTER TABLE group_members
            ADD CONSTRAINT group_members_public_id_not_null
            CHECK (public_id IS NOT NULL) NOT VALID;
    END IF;
END
$$;
