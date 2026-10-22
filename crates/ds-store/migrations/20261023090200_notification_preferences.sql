SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- The opt-outs for suggestion pushes
-- (docs/superpowers/specs/2026-10-22-mid-journey-changes-design.md §6.5):
-- a per-user switch (`notification_preferences.journey_suggestions`, read
-- as TRUE when the user has no row) and a per-journey mute
-- (`journeys.suggestions_muted`). Either suppresses the push only; the
-- in-app banner still shows.
--
-- Expand-only. The column has a constant default, so ADD COLUMN changes the
-- catalog only (no rewrite); its brief ACCESS EXCLUSIVE on `journeys` is
-- bounded by the 5 s lock_timeout. Code before this change never reads it.
-- -------------------------------------------------------------------------

CREATE TABLE notification_preferences (
    user_id             TEXT PRIMARY KEY REFERENCES users (id) ON DELETE CASCADE,
    journey_suggestions BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE journeys
    ADD COLUMN suggestions_muted BOOLEAN NOT NULL DEFAULT FALSE;
