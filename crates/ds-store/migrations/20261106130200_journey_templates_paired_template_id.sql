-- no-transaction
-- -------------------------------------------------------------------------
-- journey_templates_paired_fkey (20261106130000) is ON DELETE SET NULL:
-- deleting a template looks up the rows that point at it by
-- paired_template_id. Partial: most templates have no pair.
--
-- CONCURRENTLY and alone in its file: see
-- crates/ds-store/tests/migration_index_locking.rs. If the build is
-- interrupted it leaves an INVALID index, which `ds_store::migrate` drops
-- before the retry (crates/ds-store/src/migrate.rs).
-- -------------------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS journey_templates_paired_template_id
    ON journey_templates (paired_template_id)
    WHERE paired_template_id IS NOT NULL;
