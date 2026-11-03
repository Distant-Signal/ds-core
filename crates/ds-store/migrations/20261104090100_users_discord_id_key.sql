-- no-transaction
-- -------------------------------------------------------------------------
-- One DS account per Discord account (20261104090000): the re-key at login
-- matches on `users.discord_id`, so two rows with the same id would make
-- the match ambiguous. Partial: most rows have no Discord id.
--
-- CONCURRENTLY and alone in its file: see
-- crates/ds-store/tests/migration_index_locking.rs. If the build is
-- interrupted it leaves an INVALID index, which `ds_store::migrate` drops
-- before the retry (crates/ds-store/src/migrate.rs).
-- -------------------------------------------------------------------------
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS users_discord_id_key
    ON users (discord_id)
    WHERE discord_id IS NOT NULL;
