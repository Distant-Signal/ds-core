SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Give group_members.public_id the `m_` prefix the other opaque ids have
-- (DS-534). 20261117120000 added it as 32 bare hex digits with no format
-- CHECK, while journeys, journey_legs, train_subscriptions and
-- push_subscriptions carry `j_`, `l_`, `t_` and `d_` and a CHECK on the
-- shape. A prefix per table means an id pasted into the wrong route is
-- rejected by shape and an opaque id can never parse as an integer.
--
-- The new default is set first, so a row inserted from here on is already
-- `m_` plus the 32 hex digits of gen_random_uuid() (34 characters). The
-- rewrite then prefixes every existing id that lacks it. Prefixing keeps
-- ids distinct (every row gets the same two characters), so the unique
-- index from 20261117120100 stays valid and a member's id stays stable
-- apart from the prefix. The api accepts the bare and the prefixed form
-- of the same id during the transition.
--
-- One statement: production holds a handful of memberships, so the row
-- locks are held for milliseconds. The format CHECK follows in
-- 20261118130100 (NOT VALID) and 20261118130200 (validated).
-- -------------------------------------------------------------------------

ALTER TABLE group_members
    ALTER COLUMN public_id
    SET DEFAULT 'm_' || replace(gen_random_uuid()::text, '-', '');

UPDATE group_members
SET public_id = 'm_' || public_id
WHERE public_id !~ '^m_';
