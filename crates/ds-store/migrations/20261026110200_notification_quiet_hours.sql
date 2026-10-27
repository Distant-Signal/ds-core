SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Quiet hours (DS-153): a daily window, in the user's own time zone, in
-- which pushes are held and delivered when it ends. Cancellations still go
-- through, and the grant-created security notice is never held. All three
-- columns or none (no quiet hours), start <> end; the CHECK goes in NOT
-- VALID and 20261026110300 validates it. NULLs, and a missing row, mean
-- no quiet hours: today's behaviour.
--
-- `notification_holds` is what a held push leaves behind until quiet hours
-- end: one row per (user, push target), replaced by every newer decision
-- for that target, so the user gets the latest state, never a backlog.
-- Only line and train pushes are held: they come from cursor-polled
-- tables, which never decide the same change twice. The full-poll pushes
-- (skipped stop, recurring journey needs attention, journey break
-- suggestions) are simply not sent while quiet: their cycles decide them
-- again once quiet hours end, if they still apply. The notifier releases
-- a hold once the user's quiet hours are over, after checking it again
-- against what has been sent since, the user's switches and the train's
-- state (crates/notifier/src/hold.rs).
--
--   target         'line:<line_id>' or 'train:<train_subscriptions.id>'
--   line_*         the line push's state (its severity rank), or NULL
--   tracked_train_id, train_*   the train push's state, or NULL
--   title .. tag   the push itself, as decided
--   decided_at     when the newest decision for the target was made
--   expires_at     when the hold is dropped unsent (a day after deciding)
--
-- Expand-only. The ADD COLUMNs have no default (catalog only); their brief
-- ACCESS EXCLUSIVE on `notification_preferences` is bounded by the 5 s
-- lock_timeout. The new table is empty, so its index is built here; its
-- foreign keys take a brief SHARE ROW EXCLUSIVE on `users` and
-- `train_subscriptions`, under the same timeout.
-- -------------------------------------------------------------------------

ALTER TABLE notification_preferences
    ADD COLUMN quiet_hours_start TIME,
    ADD COLUMN quiet_hours_end TIME,
    ADD COLUMN quiet_hours_timezone TEXT;

ALTER TABLE notification_preferences
    ADD CONSTRAINT notification_preferences_quiet_hours_check
    CHECK (
        (quiet_hours_start IS NULL AND quiet_hours_end IS NULL AND quiet_hours_timezone IS NULL)
        OR (quiet_hours_start IS NOT NULL AND quiet_hours_end IS NOT NULL
            AND quiet_hours_timezone IS NOT NULL AND quiet_hours_start <> quiet_hours_end)
    )
    NOT VALID;

CREATE TABLE notification_holds (
    user_id             TEXT        NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    target              TEXT        NOT NULL,
    line_id             TEXT,
    line_rank           SMALLINT,
    tracked_train_id    BIGINT      REFERENCES train_subscriptions (id) ON DELETE CASCADE,
    train_status        TEXT,
    train_delay_minutes INTEGER,
    title               TEXT        NOT NULL,
    body                TEXT        NOT NULL,
    url                 TEXT        NOT NULL,
    tag                 TEXT        NOT NULL,
    decided_at          TIMESTAMPTZ NOT NULL,
    expires_at          TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (user_id, target),
    CONSTRAINT notification_holds_target_check CHECK (
        (line_id IS NOT NULL AND line_rank IS NOT NULL
            AND tracked_train_id IS NULL AND train_status IS NULL)
        OR (line_id IS NULL AND line_rank IS NULL
            AND tracked_train_id IS NOT NULL AND train_status IS NOT NULL)
    )
);

-- The cascade from a deleted tracked train.
CREATE INDEX notification_holds_tracked_train_id
    ON notification_holds (tracked_train_id) WHERE tracked_train_id IS NOT NULL;
