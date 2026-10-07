-- -------------------------------------------------------------------------
-- Lets `trust_event_backlog` store TRUST `0005` (Train Reinstatement) rows.
--
-- The H4 fix (commit 556e53af) taught trust-backlog-consumer to forward
-- Reinstatements, and gave both backlog replay paths a matching `"0005"`
-- arm (`data::trust_event_backlog::ingest_shared_movements_batch` ->
-- `journey::apply_reinstatement`, and
-- `data::trust_event_backlog_match`'s replay), so a cancel -> reinstate
-- sequence un-sticks a late-tracked journey. It never widened this table's
-- `msg_type IN ('0001', '0002', '0003')` CHECK
-- (20260905160000_trust_event_backlog.sql), so every POST
-- /private/trust-event-backlog batch that contained a Reinstatement failed
-- the whole transaction. trust-backlog-consumer does not XACK a failed
-- batch, so those entries sat in its Redis pending-entries list and failed
-- again on every 30 s XAUTOCLAIM replay, taking every valid
-- Activation/Cancellation/Movement in the same batch down with them.
--
-- `0006`/`0007` (ChangeOfOrigin/ChangeOfIdentity) remain excluded: the
-- consumer still drops them and neither replay path has an arm for them.
--
-- NOT VALID here, VALIDATE in the next migration. sqlx wraps each file in
-- its own transaction, so this file holds ACCESS EXCLUSIVE only for a
-- catalog update (no table scan). The next file's VALIDATE CONSTRAINT scans
-- the table under SHARE UPDATE EXCLUSIVE, which does not block reads or
-- writes. Doing both in one file would keep ACCESS EXCLUSIVE for the whole
-- scan. The old constraint is dropped and re-added in one ALTER TABLE so
-- there is no moment with no CHECK at all. The lock_timeout makes a wait
-- behind a long-running transaction (e.g. the aggregator's retention
-- DELETE) fail fast; the migration then reruns on the next api start
-- instead of queueing every other query on this table behind it.
-- -------------------------------------------------------------------------
SET LOCAL lock_timeout = '10s';

ALTER TABLE trust_event_backlog
    DROP CONSTRAINT trust_event_backlog_msg_type_check,
    ADD CONSTRAINT trust_event_backlog_msg_type_check
        CHECK (msg_type IN ('0001', '0002', '0003', '0005')) NOT VALID;
