-- no-transaction
-- -------------------------------------------------------------------------
-- custom_line_group_grants.granted_by references users(id) with ON DELETE
-- and ON UPDATE actions, so deleting an account (or changing a user's id)
-- looks its rows up by granted_by, which no index led with: a scan of the
-- whole table. Review K3, 2026-11-16.
--
-- CONCURRENTLY and alone in its file: see
-- crates/ds-store/tests/migration_index_locking.rs. If the build is
-- interrupted it leaves an INVALID index, which `ds_store::migrate` drops
-- before the retry (crates/ds-store/src/migrate.rs).
-- -------------------------------------------------------------------------

CREATE INDEX CONCURRENTLY IF NOT EXISTS custom_line_group_grants_granted_by
    ON custom_line_group_grants (granted_by);
