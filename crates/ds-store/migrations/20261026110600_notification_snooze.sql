SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Snooze (DS-153): "pause notifications until" a time. Until then pushes
-- are held exactly as in quiet hours (crates/notifier/src/hold.rs): line
-- and train pushes wait in `notification_holds` and the latest state goes
-- when the snooze ends, the full-poll pushes are decided again then,
-- cancellations still go through and the grant-created notice is never
-- held. NULL, or a time already past, means not snoozed.
--
-- Expand-only: a nullable column with no default, so the ADD COLUMN
-- changes the catalog only; its brief ACCESS EXCLUSIVE on
-- `notification_preferences` is bounded by the 5 s lock_timeout.
-- -------------------------------------------------------------------------

ALTER TABLE notification_preferences
    ADD COLUMN snoozed_until TIMESTAMPTZ;
