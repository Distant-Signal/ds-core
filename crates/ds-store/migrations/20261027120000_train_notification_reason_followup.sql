SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Cancellation reason follow-up (DS-184). A cancellation push goes out at
-- once, never held; when TRUST's reason for it is not stored yet (it is
-- filed by a different consumer, often a little after the cancellation
-- itself), the delivered push records until when one follow-up may still
-- be sent. The notifier's reason cycle (crates/notifier/src/cancel_reason.rs)
-- sends that follow-up, with the same tag so it replaces the first push on
-- the device, as soon as the reason arrives inside the window, then clears
-- this. NULL: no follow-up due (the push carried its reason, or it was not
-- a cancellation, or the follow-up went).
--
-- Expand-only: a nullable column with no default, so the ADD COLUMN
-- changes the catalog only; its brief ACCESS EXCLUSIVE on
-- `train_notification_state` is bounded by the 5 s lock_timeout. No index:
-- the table holds one row per user and tracked train pushed, and the
-- cycle's scan is cheap at that size.
-- -------------------------------------------------------------------------

ALTER TABLE train_notification_state
    ADD COLUMN reason_followup_until TIMESTAMPTZ;
