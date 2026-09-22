-- -------------------------------------------------------------------------
-- Per-(user, journey leg) dedup state for station-skip push notifications
-- (§5.2, docs/superpowers/specs/2026-09-22-journey-tracking-design.md).
-- Same escalation-only "written only after a successful send" discipline
-- as train_notification_state (20260902100000_notifications.sql) and
-- line_notification_state -- crates/notifier's own decide_skip_notification
-- fires only on last_notified_skipped false -> true, never the reverse, and
-- this row is only ever upserted after send_to_all_subscriptions actually
-- succeeded (or the user has zero push_subscriptions -- "still counts as
-- handled", crates/notifier/src/main.rs's own send_to_all_subscriptions doc
-- comment), never before -- an unresolved send failure retries at the next
-- poll cycle rather than being queued.
-- -------------------------------------------------------------------------

CREATE TABLE journey_leg_notification_state (
    user_id                TEXT        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    journey_leg_id         BIGINT      NOT NULL REFERENCES journey_legs(id) ON DELETE CASCADE,
    last_notified_skipped  BOOLEAN     NOT NULL,
    last_notified_at       TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (user_id, journey_leg_id)
);
