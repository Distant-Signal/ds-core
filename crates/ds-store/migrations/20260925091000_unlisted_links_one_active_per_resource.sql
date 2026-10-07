-- -------------------------------------------------------------------------
-- 19-pass security/bug review, journeys area, Medium finding 2:
-- `unlisted_links::rotate_link` (crates/api/src/data/unlisted_links.rs)
-- revokes any currently-active link then inserts a fresh one, both inside
-- one transaction -- but two CONCURRENT `rotate_link` calls for the same
-- `(resource_type, resource_id)` can each run their own `UPDATE ...
-- revoked_at IS NULL` against zero-or-one already-revoked rows (READ
-- COMMITTED never blocks that on its own), then both unconditionally
-- INSERT a brand-new row. Nothing before this migration stops both inserts
-- from committing, leaving TWO simultaneously active tokens for the same
-- resource until the next revoke -- breaking "regenerate invalidates the
-- old token".
--
-- This is generic table (see this table's own migration,
-- 20260923110000_unlisted_links.sql, and unlisted_links.rs's module doc)
-- with no single "owning resource" row of a known type to lock with
-- `SELECT ... FOR UPDATE` -- `resource_type`/`resource_id` name a row in a
-- DIFFERENT table per resource type, by design. A partial unique index
-- instead makes the invariant itself unrepresentable in the data: at most
-- one row per `(resource_type, resource_id)` may have `revoked_at IS
-- NULL` at any time. The loser of a concurrent rotation now gets a clean
-- constraint-violation error from its INSERT instead of silently
-- coexisting -- the same "a unique constraint turns a rare concurrent
-- double-click into a clean error on the losing request" posture this
-- codebase already accepts for `journey_legs (journey_id, leg_order)`
-- (see `journeys::owned_next_leg_order`'s own doc comment).
-- -------------------------------------------------------------------------

CREATE UNIQUE INDEX unlisted_links_one_active_per_resource
    ON unlisted_links (resource_type, resource_id)
    WHERE revoked_at IS NULL;
