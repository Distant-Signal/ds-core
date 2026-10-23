-- no-transaction
-- -------------------------------------------------------------------------
-- `journey_follows` by journey (20261024100000): the `journey_id` cascade
-- when a journey is deleted, and the share-link routes' "drop the follows
-- through this journey's dead links". The primary key leads with
-- `user_id`, so it can't serve these.
--
-- CONCURRENTLY and alone in its file: see
-- crates/api/tests/migration_index_locking.rs. If the build is interrupted
-- it leaves an INVALID index, which `ds_store::migrate` drops before the
-- retry (crates/ds-store/src/migrate.rs).
-- -------------------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS journey_follows_journey_id
    ON journey_follows (journey_id);
