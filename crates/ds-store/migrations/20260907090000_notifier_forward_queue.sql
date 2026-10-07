-- The lightweight notifier-forwarding queue named by
-- docs/superpowers/specs/2026-09-06-shared-train-identity-design.md §3 --
-- a forwarding SIGNAL, not a data store: notifier polls this on a second,
-- faster-cadence query (Task 18), still deciding whether to actually send
-- a push via its own unchanged cooldown/escalation logic. No consumed-row
-- bookkeeping column -- notifier tracks its own read position via a
-- second `notifier_cursor` row (Task 18), the same watermark-cursor
-- pattern `poll_train_candidates` already uses.
CREATE TABLE notifier_forward_queue (
    id           BIGSERIAL PRIMARY KEY,
    trains_id    BIGINT NOT NULL REFERENCES trains(id) ON DELETE CASCADE,
    event_summary TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX notifier_forward_queue_trains_id ON notifier_forward_queue (trains_id);
