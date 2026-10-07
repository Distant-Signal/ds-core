-- -------------------------------------------------------------------------
-- chatbot_allowed_users: a beta/feature-flag gate for the /chat page's own
-- visibility -- NOT a spend-protection mechanism (it was originally built
-- as one, back when Option B held a DS-funded Anthropic key server-side;
-- see docs/superpowers/specs/2026-09-02-embedded-chatbot-option-b-client-side-tokens-design.md's
-- Decision 4 for why that framing stopped being accurate once each user
-- pays for their own Anthropic usage directly). See
-- docs/superpowers/plans/2026-09-02-embedded-chatbot-option-b.md's Task 2
-- for this table's original shape (unchanged) and
-- docs/superpowers/plans/2026-09-02-embedded-chatbot-option-b-client-side-tokens.md's
-- Task 6 for this re-framing.
--
-- A bare allowlist -- membership alone, no per-row metadata -- since
-- nothing today needs more than "may this user reach the chatbot at all."
-- `ON DELETE CASCADE`, matching this schema's established posture for a
-- row that only ever means something in relation to a `users` row (the
-- same reasoning custom_lines/tracked_trains' own ownership columns
-- already use): an allowlist entry for a deleted user is meaningless, not
-- an orphan worth preserving.
-- -------------------------------------------------------------------------

CREATE TABLE chatbot_allowed_users (
    user_id TEXT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE
);
