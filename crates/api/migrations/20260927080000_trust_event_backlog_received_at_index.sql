-- no-transaction
-- -------------------------------------------------------------------------
-- Supports the aggregator's retention prune of `trust_event_backlog`
-- (`crates/aggregator/src/queries.rs::prune_trust_event_backlog`), which
-- deletes `WHERE received_at < NOW() - <retention>` every cycle (~1/min).
-- No index led by `received_at` existed, so every cycle was a sequential
-- scan of the whole backlog (prod 2026-09-27: 552k rows / 123 MB heap, 18
-- seq scans / 9.96M tuples in 20 minutes; DB review F10). With this index
-- the prune's "is anything old enough?" MIN probe and each batch's
-- `ORDER BY received_at LIMIT n` are index range scans.
--
-- CONCURRENTLY and alone in its file: see
-- crates/api/tests/migration_index_locking.rs. If the build is interrupted
-- it leaves an INVALID index that IF NOT EXISTS would then skip; recovery
-- is `DROP INDEX CONCURRENTLY trust_event_backlog_received_at;` and a
-- restart.
-- -------------------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS trust_event_backlog_received_at
    ON trust_event_backlog (received_at);
