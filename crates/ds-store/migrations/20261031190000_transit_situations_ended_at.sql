SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- When a transit situation ended (DS-228; docs/api-changelog.md,
-- 2026-10-31): each situation now has its own page,
-- /transit/<network>/incidents/<id>, and a shared link to one that has
-- since ended should say so rather than not be found.
--
--   ended_at   when the situations snapshot first stopped listing it; NULL
--              while it is listed. The writer keeps an ended situation for
--              7 days, then deletes it, and clears ended_at if the feed
--              lists it again.
--
-- Expand-only: the column is nullable with no default, so the ADD COLUMN
-- changes the catalog only (the 5 s lock_timeout bounds its ACCESS
-- EXCLUSIVE). Code before this change never reads it; the writer before it
-- deletes ended situations outright, which leaves no row with it set.
-- -------------------------------------------------------------------------

ALTER TABLE transit_situations
    ADD COLUMN ended_at TIMESTAMPTZ;
