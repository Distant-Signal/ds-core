-- no-transaction
-- -------------------------------------------------------------------------
-- A unique index on group_members.public_id (20261117120000): the api looks
-- a member up by it (promote, demote, remove) within a group, and a
-- collision would be a bug worth a loud failure.
--
-- CONCURRENTLY and alone in its file: see
-- crates/ds-store/tests/migration_index_locking.rs. If the build is
-- interrupted it leaves an INVALID index, which `ds_store::migrate` drops
-- before the retry (crates/ds-store/src/migrate.rs).
-- -------------------------------------------------------------------------

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS group_members_public_id
    ON group_members (public_id);
