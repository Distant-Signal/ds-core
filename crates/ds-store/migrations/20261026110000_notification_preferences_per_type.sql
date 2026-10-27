SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- A switch per kind of push, and a per-user delay threshold (DS-153), on
-- top of `journey_suggestions` (20261023090200). Each switch is read as
-- TRUE when the user has no row, and `delay_threshold_minutes` NULL means
-- the notifier's TRAIN_DELAY_THRESHOLD_MINUTES default (15), so a missing
-- row is exactly today's behaviour and nothing needs backfilling.
--
--   line_status         a pinned line's status changes
--   train_delay         a tracked train is late, at or over the threshold
--   train_cancel        a tracked train is cancelled
--   predicted_start     a tracked train is expected to start late (DS-127)
--   station_skip        a train on a journey stops calling at its station
--   template_attention  a recurring journey found no train for today
--
-- The grant-created notice has no switch: it is a security notice.
--
-- Expand-only. Each ADD COLUMN has a constant default (or none), so it
-- changes the catalog only; its brief ACCESS EXCLUSIVE is bounded by the
-- 5 s lock_timeout. The threshold's CHECK is added NOT VALID (new writes
-- are checked at once) and validated by 20261026110100 under SHARE UPDATE
-- EXCLUSIVE. Code before this change never reads the columns.
-- -------------------------------------------------------------------------

ALTER TABLE notification_preferences
    ADD COLUMN line_status BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN train_delay BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN train_cancel BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN predicted_start BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN station_skip BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN template_attention BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN delay_threshold_minutes SMALLINT;

ALTER TABLE notification_preferences
    ADD CONSTRAINT notification_preferences_delay_threshold_check
    CHECK (delay_threshold_minutes IS NULL OR delay_threshold_minutes IN (5, 10, 15, 30))
    NOT VALID;
