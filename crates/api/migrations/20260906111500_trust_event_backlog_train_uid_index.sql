-- Adds the train_uid read path named by
-- docs/superpowers/specs/2026-09-06-shared-train-identity-design.md §3.
-- Confirmed directly against 20260905160000_trust_event_backlog.sql: no
-- index on train_uid exists on this table today (only dedup_key,
-- (crs, planned_timestamp), and (train_id, service_date)).
CREATE INDEX trust_event_backlog_train_uid
    ON trust_event_backlog (train_uid, service_date)
    WHERE train_uid IS NOT NULL;
