-- no-transaction
-- -------------------------------------------------------------------------
-- The template legs' new unique key: one leg per position for the
-- template's own legs (variant_weekday NULL, read as 0) and for each
-- weekday's variant. Built CONCURRENTLY, so writes carry on while it
-- builds. A later release drops the old (template_id, leg_order) key,
-- which would refuse a variant leg at a position the template's own legs
-- already use.
--
-- CONCURRENTLY and alone in its file: see
-- crates/ds-store/tests/migration_index_locking.rs. If the build is
-- interrupted it leaves an INVALID index, which `ds_store::migrate` drops
-- before the retry (crates/ds-store/src/migrate.rs).
-- -------------------------------------------------------------------------

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS journey_template_legs_template_variant_order
    ON journey_template_legs (template_id, COALESCE(variant_weekday, 0), leg_order);
