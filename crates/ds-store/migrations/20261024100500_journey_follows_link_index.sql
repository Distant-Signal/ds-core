-- no-transaction
-- -------------------------------------------------------------------------
-- `journey_follows` by share link (20261024100000): the cascade when a dead
-- link row is pruned (`unlisted_links::prune_dead_links`). Partial: group
-- follows have no link.
--
-- CONCURRENTLY and alone in its file: see
-- crates/api/tests/migration_index_locking.rs. If the build is interrupted
-- it leaves an INVALID index, which `ds_store::migrate` drops before the
-- retry (crates/ds-store/src/migrate.rs).
-- -------------------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS journey_follows_link
    ON journey_follows (link_token_hash)
    WHERE link_token_hash IS NOT NULL;
