-- -------------------------------------------------------------------------
-- Per-table autovacuum thresholds for the tables that are pruned in big
-- date-shaped batches.
--
-- The global defaults (vacuum after 20% of a table is dead, analyze after
-- 10% changed, insert-driven vacuum after 20% new rows) assume churn is
-- spread thinly across a table. These tables instead lose one whole day's
-- worth of rows at a time to the aggregator's retention prunes, and each of
-- those batches is smaller than 20% of the table -- so dead tuples pile up
-- across several prunes before autovacuum ever looks. Measured in
-- production on 2026-09-26: `train_movement_events` had NEVER been
-- autovacuumed (autovacuum_count = 0) while carrying 619k dead rows against
-- 5.29M live (10.5%), under a default trigger of ~1.06M, and its
-- `(trains_id, dedup_key)` index had grown to ~1.7x its packed size.
--
-- Choices (each one is sized so a single daily prune/publish crosses it):
--
-- * train_movement_events -- 5.3M rows, cascade-deleted with `trains` a
--   service day at a time. vacuum 0.02 (~106k dead) makes each daily prune
--   trigger a vacuum; analyze 0.02 because an analyze samples a fixed
--   ~30k rows whatever the table size, so doing it more often is cheap.
--
-- * schedule_destination_departures, schedule_calling_points_full -- ~3.6M
--   rows each, 8-day retention, so one rail day (~10-11% of the table) is
--   inserted by the daily publish and one is deleted by the prune. 0.05 for
--   vacuum, insert-vacuum and analyze puts each of those daily events over
--   its threshold. The insert-driven vacuum matters here specifically:
--   `schedule_destination_departures`' primary key is documented as the
--   covering index for its hot query, and an index-only scan only skips the
--   heap for pages the visibility map marks all-visible -- which only a
--   vacuum sets. The analyze keeps the newly published service_date inside
--   the planner's statistics.
--
-- * trust_event_backlog -- ~560k rows, 24h retention pruned every aggregator
--   cycle, so the whole table turns over daily. 0.05 (~28k dead) keeps the
--   bloat of its unique dedup_key index bounded; analyze 0.05 keeps the
--   planned_timestamp histogram current for the backlog matcher's
--   recent-time range lookups, which otherwise always sit past the end of
--   stale stats.
--
-- `ALTER TABLE ... SET (storage_parameter)` is a catalog-only change taking
-- SHARE UPDATE EXCLUSIVE, which does not block reads or writes, so it is
-- safe inside this migration's ordinary transaction at api startup.
-- -------------------------------------------------------------------------

ALTER TABLE train_movement_events SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_analyze_scale_factor = 0.02
);

ALTER TABLE schedule_destination_departures SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_vacuum_insert_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.05
);

ALTER TABLE schedule_calling_points_full SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_vacuum_insert_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.05
);

ALTER TABLE trust_event_backlog SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.05
);
