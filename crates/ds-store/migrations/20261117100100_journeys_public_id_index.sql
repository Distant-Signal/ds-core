-- no-transaction
-- -------------------------------------------------------------------------
-- The unique index behind journeys.public_id (20261117100000): the api
-- looks a journey up by it on every journey route, and uniqueness backs
-- the (astronomically unlikely) collision of two random ids.
--
-- CONCURRENTLY and alone in its file: see
-- crates/ds-store/tests/migration_index_locking.rs. If the build is
-- interrupted it leaves an INVALID index, which `ds_store::migrate` drops
-- before the retry (crates/ds-store/src/migrate.rs).
-- -------------------------------------------------------------------------

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS journeys_public_id
    ON journeys (public_id);
