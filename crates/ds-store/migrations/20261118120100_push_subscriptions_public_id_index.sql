-- no-transaction
-- -------------------------------------------------------------------------
-- The unique index behind push_subscriptions.public_id (20261118120000): the api
-- looks a row up by it on the routes that take the opaque id, and uniqueness
-- backs the (astronomically unlikely) collision of two random ids.
--
-- CONCURRENTLY and alone in its file: see
-- crates/ds-store/tests/migration_index_locking.rs. If the build is
-- interrupted it leaves an INVALID index, which `ds_store::migrate` drops
-- before the retry (crates/ds-store/src/migrate.rs).
-- -------------------------------------------------------------------------

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS push_subscriptions_public_id
    ON push_subscriptions (public_id);
