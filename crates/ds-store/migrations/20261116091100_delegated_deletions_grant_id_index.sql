-- no-transaction
-- -------------------------------------------------------------------------
-- delegated_deletions.grant_id references delegated_grants(id) with ON
-- DELETE SET NULL, so deleting a grant (or the account that holds it)
-- looks its rows up by grant_id, which no index led with: a scan of the
-- whole table. Partial: the column is NULL once its grant is deleted, and
-- a lookup by value never matches NULL. Review K3, 2026-11-16.
--
-- CONCURRENTLY and alone in its file: see
-- crates/ds-store/tests/migration_index_locking.rs. If the build is
-- interrupted it leaves an INVALID index, which `ds_store::migrate` drops
-- before the retry (crates/ds-store/src/migrate.rs).
-- -------------------------------------------------------------------------

CREATE INDEX CONCURRENTLY IF NOT EXISTS delegated_deletions_grant_id
    ON delegated_deletions (grant_id)
    WHERE grant_id IS NOT NULL;
