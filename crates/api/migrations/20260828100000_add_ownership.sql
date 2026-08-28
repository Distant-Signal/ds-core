-- -------------------------------------------------------------------------
-- Ownership retrofit for the three tables that predate any user model. See
-- docs/superpowers/specs/2026-08-28-user-accounts-sso-design.md's Data
-- model section for the full reasoning -- this comment restates the
-- conclusions only. Depends on the `users` table from
-- 20260828090000_user_accounts.sql, which must run first.
--
-- custom_lines: user_id is added NULLABLE. Existing rows keep
-- user_id = NULL -- there is no owner to attribute them to. NULL is
-- deliberately NOT "public, owned by nobody" for write access: a
-- NULL-owned row stays readable (GET /lines, GET /lines/{id} are
-- unauthenticated and unscoped either way -- nothing currently working
-- 404s) but is not editable or deletable by anyone
-- (crates/api/src/data/custom_lines.rs's update_custom_line/
-- delete_custom_line now filter `AND user_id = $n`, which a NULL user_id
-- can never match) until an operator manually assigns a real owner.
--
-- OPERATOR RUNBOOK (existing deployments with pre-existing custom_lines
-- rows only -- not needed on a fresh install): after this migration runs,
-- decide who should own any pre-existing custom lines and run, once, by
-- hand:
--   UPDATE custom_lines SET user_id = '<admin sub>' WHERE user_id IS NULL;
-- This migration deliberately does not do this automatically -- it has no
-- way to know which user *should* own pre-existing data, only a human
-- operator does. Leaving rows ownerless is safe (read-only until that
-- manual step happens), never destructive.
--
-- pinned_lines / pinned_stations: unlike custom_lines, these need more
-- than an added column -- today's schema has ONE GLOBAL ROW per pinned
-- line/station (line_id / crs as the sole PRIMARY KEY). Once ownership
-- exists, the same line must be independently pinnable by many users, so
-- the primary key itself changes to a composite (user_id, line_id) /
-- (user_id, crs). A NULL user_id can't carry existing rows forward
-- through that change (NULL <> NULL under a composite PK means every
-- unowned row would be a permanently-invisible group of its own, visible
-- to no real account, ever) -- so instead of a NULL-owner retrofit,
-- existing rows are intentionally NOT carried forward. They're pure UI
-- convenience state (unlike custom_lines' authored content, which IS
-- carried forward), so this TRUNCATEs both tables as part of adding the
-- composite PK. Every user starts with an empty pinned set post-migration
-- and re-pins -- a one-time, low-cost inconvenience for this app's
-- "single trusted personal instance"-sized deployments (DESIGN.md), not a
-- data-loss concern.
-- -------------------------------------------------------------------------

ALTER TABLE custom_lines
    ADD COLUMN user_id TEXT REFERENCES users(id) ON DELETE CASCADE;

CREATE INDEX custom_lines_user_id ON custom_lines (user_id) WHERE user_id IS NOT NULL;

-- See header comment: pre-existing rows are deliberately not preserved.
TRUNCATE TABLE pinned_lines;
ALTER TABLE pinned_lines
    ADD COLUMN user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    DROP CONSTRAINT pinned_lines_pkey,
    ADD PRIMARY KEY (user_id, line_id);

TRUNCATE TABLE pinned_stations;
ALTER TABLE pinned_stations
    ADD COLUMN user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    DROP CONSTRAINT pinned_stations_pkey,
    ADD PRIMARY KEY (user_id, crs);
