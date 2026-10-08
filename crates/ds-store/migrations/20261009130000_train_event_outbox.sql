SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- train_event_outbox (ingest architecture plan 3b.3, decided 2026-10-08):
-- trust-consumer's direct DB sink (INGEST_SINK=db) connects as
-- distant_signal_trust_consumer, which has only SELECT on
-- train_subscriptions. A train event that has to change a subscription --
-- a resolution (resolved_train_id set: the status flip and the trains_id
-- link), a cancellation (status 'cancelled') or a reinstatement (msg_type
-- 0005) -- is written here instead, in the same transaction as the batch's
-- other events, together with every later event of the same subscription
-- while one of its rows is pending, so its order is kept. The
-- ingest-writer's train-event-outbox loop applies the rows in id order with
-- ds_store::tracking::upsert_train_event_on (exactly what the api's POST
-- /private/train-events runs) and deletes them; a row refused for a data
-- error stays, with rejected_at and rejection set.
--
-- UNIQUE (tracked_train_id, dedup_key): a redelivered movements entry
-- inserts nothing while its row is still here (ON CONFLICT DO NOTHING), and
-- after the row is applied a redelivery re-applies an idempotent event, as
-- a redelivered POST does today. No foreign key: an event for a
-- subscription deleted meanwhile is a no-op when applied, as on the api.
-- -------------------------------------------------------------------------
CREATE TABLE train_event_outbox (
    id               BIGSERIAL PRIMARY KEY,
    tracked_train_id BIGINT      NOT NULL,
    dedup_key        TEXT        NOT NULL,
    -- common::TrainMovementEventMessage, as the api route receives it.
    event            JSONB       NOT NULL,
    -- common::TrainForwardSignalMessage this event raised, queued once the
    -- event is applied; NULL when it raised none.
    forward_signal   JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    rejected_at      TIMESTAMPTZ,
    rejection        TEXT,
    CONSTRAINT train_event_outbox_event_key UNIQUE (tracked_train_id, dedup_key)
);
