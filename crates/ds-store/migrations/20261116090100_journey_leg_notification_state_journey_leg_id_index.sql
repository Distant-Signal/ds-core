-- no-transaction
-- -------------------------------------------------------------------------
-- The notifier's per-user state for a journey leg is deleted by the ON
-- DELETE CASCADE from journey_legs, its only pruning. The table's one
-- index, the primary key (user_id, journey_leg_id), cannot serve a lookup
-- by journey_leg_id alone, so every journey_legs delete scanned the whole
-- table. Review K3, 2026-11-16.
--
-- CONCURRENTLY and alone in its file: see
-- crates/ds-store/tests/migration_index_locking.rs. If the build is
-- interrupted it leaves an INVALID index, which `ds_store::migrate` drops
-- before the retry (crates/ds-store/src/migrate.rs).
-- -------------------------------------------------------------------------

CREATE INDEX CONCURRENTLY IF NOT EXISTS journey_leg_notification_state_journey_leg_id
    ON journey_leg_notification_state (journey_leg_id);
