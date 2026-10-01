SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Records which status a subscription had when a TRUST Cancellation moved
-- it to 'unresolved', so a later Reinstatement (0005) of the same train can
-- put it back (H4 residual, 2026-10-01 verification pass).
--
-- `mark_subscription_unresolved_on_cancellation` moves a 'pending' or
-- 'schedule_matched' subscription to 'unresolved', and
-- `list_active_tracked_trains` and both pending sweeps skip 'unresolved'.
-- Nothing reversed it on a reinstatement, so recovery depended on
-- trust-consumer's in-memory state surviving until the train's next
-- Movement. NULL means "not made unresolved by a cancellation" (every
-- existing row, and every other status).
--
-- A nullable column with no default is a catalog-only change: no rewrite,
-- no scan. The lock_timeout makes a wait behind a long transaction fail
-- fast; the migration reruns on the next api start.
-- -------------------------------------------------------------------------
ALTER TABLE train_subscriptions ADD COLUMN unresolved_from TEXT;
