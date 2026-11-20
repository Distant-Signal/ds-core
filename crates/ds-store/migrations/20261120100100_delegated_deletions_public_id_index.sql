-- no-transaction
-- -------------------------------------------------------------------------
-- The unique index behind delegated_deletions.public_id (20261120100000): the api looks a row up by it
-- on the routes that take the opaque id, and uniqueness backs the
-- (astronomically unlikely) collision of two random ids.
--
-- CONCURRENTLY and alone in its file: see
-- crates/ds-store/tests/migration_index_locking.rs. If the build is
-- interrupted it leaves an INVALID index, which `ds_store::migrate` drops
-- before the retry (crates/ds-store/src/migrate.rs).
-- -------------------------------------------------------------------------

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS delegated_deletions_public_id
    ON delegated_deletions (public_id);
