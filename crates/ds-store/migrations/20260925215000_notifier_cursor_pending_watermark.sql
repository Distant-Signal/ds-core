-- -------------------------------------------------------------------------
-- A grace window for every `notifier_cursor` watermark, so an
-- out-of-order COMMIT can no longer silently skip a row.
--
-- Real bug this closes: every notifier cursor was a plain "MAX(id) observed"
-- watermark. Ids come from a sequence and are handed out at INSERT time, but
-- a row only becomes VISIBLE at COMMIT time -- so a transaction that took
-- id 100 and committed after a later transaction holding id 101 had already
-- committed AND been observed by a notifier cycle was skipped forever: the
-- cursor was already at 101, and nothing in this codebase ever re-checks a
-- passed-over row. Every writer of the three polled tables is transactional
-- and can do exactly that: the aggregator's own `write_line_status` /
-- `api`'s `upsert_tfl_line_status` for line_status_history, TRUST ingest for
-- train_movement_events, and trust-consumer's forwarding write for
-- notifier_forward_queue.
--
-- The fix is a two-phase watermark rather than a wider read: each cycle
-- PROPOSES the maximum id it observed (`pending_id`, stamped
-- `pending_observed_at`) and only promotes a proposal into
-- `last_processed_id` on a later cycle, once that proposal is older than the
-- notifier's own grace window (--cursor-grace-seconds). Because the
-- promoting cycle has itself just re-read everything above
-- `last_processed_id`, any lower-id row that committed late inside the grace
-- window is picked up by that read BEFORE the cursor moves past it. Rows are
-- therefore read at least twice; every notification path this feeds is
-- already idempotent against that (line sends are gated on
-- `line_notification_state`'s own last-notified rank, train sends on
-- `train_notification_state`'s -- see `decision::decide_user_notification`'s
-- "already notified this exact resulting state" guard).
--
-- Both columns are nullable with no default: NULL means "no proposal yet"
-- (a brand new cursor row, or one written by a pre-fix notifier build), and
-- the first cycle after this migration simply makes the first proposal.
-- -------------------------------------------------------------------------

ALTER TABLE notifier_cursor
    ADD COLUMN pending_id          BIGINT,
    ADD COLUMN pending_observed_at TIMESTAMPTZ;
