-- no-transaction
-- -------------------------------------------------------------------------
-- `journey_follows` by group share (20261024100000): the cascade when a
-- journey is unshared from a group or the group is deleted, and
-- `groups::remove_member_in_tx` dropping a departing member's follows.
-- Partial: link follows have no group.
--
-- CONCURRENTLY and alone in its file: see
-- crates/api/tests/migration_index_locking.rs. If the build is interrupted
-- it leaves an INVALID index, which `ds_store::migrate` drops before the
-- retry (crates/ds-store/src/migrate.rs).
-- -------------------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS journey_follows_group
    ON journey_follows (group_id, journey_id)
    WHERE group_id IS NOT NULL;
