-- no-transaction
-- -------------------------------------------------------------------------
-- The notifier's per-user state for a tracked train is deleted by the ON
-- DELETE CASCADE from train_subscriptions, its only pruning. The table's
-- one index, the primary key (user_id, tracked_train_id), cannot serve a
-- lookup by tracked_train_id alone, so every train_subscriptions delete
-- scanned the whole table. Review K3, 2026-11-16.
--
-- CONCURRENTLY and alone in its file: see
-- crates/ds-store/tests/migration_index_locking.rs. If the build is
-- interrupted it leaves an INVALID index, which `ds_store::migrate` drops
-- before the retry (crates/ds-store/src/migrate.rs).
-- -------------------------------------------------------------------------

CREATE INDEX CONCURRENTLY IF NOT EXISTS train_notification_state_tracked_train_id
    ON train_notification_state (tracked_train_id);
