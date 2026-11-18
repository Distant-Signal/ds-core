SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- The format CHECK the other public_id columns have, on group_members
-- (see 20261118130000): `m_` and 32 lowercase hex digits, never NULL. Added
-- NOT VALID so it takes only a brief lock and is enforced on new and
-- updated rows at once; 20261118130200 validates it. The older
-- group_members_public_id_not_null CHECK stays (it says a subset of this
-- one). No SET NOT NULL: it counts as destructive DDL here, and a valid
-- CHECK says the same.
-- -------------------------------------------------------------------------

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'group_members_public_id_format'
          AND conrelid = 'group_members'::regclass
    ) THEN
        ALTER TABLE group_members
            ADD CONSTRAINT group_members_public_id_format
            CHECK (public_id IS NOT NULL AND public_id ~ '^m_[0-9a-f]{32}$') NOT VALID;
    END IF;
END
$$;
