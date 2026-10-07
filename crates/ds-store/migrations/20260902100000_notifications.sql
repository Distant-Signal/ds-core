-- -------------------------------------------------------------------------
-- Web Push notifications: per-device subscriptions, and per-(user, target)
-- last-notified state used for the notifier's escalate-now/cooldown
-- decision (Decision 5, docs/superpowers/specs/2026-09-02-line-status-notifications-design.md).
-- notifier_cursor is the watermark the crates/notifier poll loop advances
-- over line_status_history and train_movement_events (Decision 3).
-- -------------------------------------------------------------------------

CREATE TABLE push_subscriptions (
    id           BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id      TEXT        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    endpoint     TEXT        NOT NULL UNIQUE,
    p256dh       TEXT        NOT NULL,
    auth         TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX push_subscriptions_user_id ON push_subscriptions (user_id);

CREATE TABLE line_notification_state (
    user_id                      TEXT        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    line_id                      TEXT        NOT NULL,
    last_notified_severity_rank  SMALLINT    NOT NULL,
    last_notified_at             TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (user_id, line_id)
);

CREATE TABLE train_notification_state (
    user_id                       TEXT        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    tracked_train_id              BIGINT      NOT NULL REFERENCES tracked_trains(id) ON DELETE CASCADE,
    last_notified_status          TEXT        NOT NULL,
    last_notified_delay_minutes   INTEGER,
    last_notified_at              TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (user_id, tracked_train_id)
);

-- name is either 'line_status_history' or 'train_movement_events'. Rows
-- are upserted by the notifier itself on first use (see
-- crates/notifier/src/queries.rs's read_cursor) rather than seeded here,
-- so this migration only needs to declare the shape.
CREATE TABLE notifier_cursor (
    name               TEXT   PRIMARY KEY,
    last_processed_id  BIGINT NOT NULL DEFAULT 0
);
