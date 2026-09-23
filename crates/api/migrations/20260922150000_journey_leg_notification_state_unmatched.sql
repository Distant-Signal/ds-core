-- -------------------------------------------------------------------------
-- Dedup state for the new "today's occurrence needs attention" push
-- notification (spec §4.2, narrowly scoped to the auto-commit-found-zero-
-- candidates case only -- see
-- docs/superpowers/plans/2026-09-22-reusable-journeys-phaseC-recurrence-plan.md
-- Judgment Call 4). Nullable, unlike journey_leg_notification_state's
-- existing last_notified_skipped/last_notified_at (NOT NULL): a row
-- written first by the skip-check path (Phase 3) has no opinion yet on
-- whether THIS leg was ever unmatched-notified, and vice versa -- NULL
-- means "never notified for this reason," matching skip_notification_state's
-- own Option<bool>-from-NULL read convention
-- (crates/notifier/src/queries.rs::skip_notification_state).
-- -------------------------------------------------------------------------

ALTER TABLE journey_leg_notification_state
    ADD COLUMN last_notified_unmatched    BOOLEAN,
    ADD COLUMN last_notified_unmatched_at TIMESTAMPTZ;
