-- no-transaction
-- -------------------------------------------------------------------------
-- The unique index behind personal_api_tokens.public_id (20261120110000): the api looks a row up by it
-- on the routes that take the opaque id, and uniqueness backs the
-- (astronomically unlikely) collision of two random ids.
--
-- CONCURRENTLY and alone in its file: see
-- crates/ds-store/tests/migration_index_locking.rs. If the build is
-- interrupted it leaves an INVALID index, which `ds_store::migrate` drops
-- before the retry (crates/ds-store/src/migrate.rs).
-- -------------------------------------------------------------------------

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS personal_api_tokens_public_id
    ON personal_api_tokens (public_id);
