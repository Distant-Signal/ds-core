SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Pinning a line and being alerted about it, separated (DS-153).
-- `pinned_lines.notify` FALSE keeps the line on the home page but sends no
-- line-status push for it. Every existing pin keeps alerting: the column's
-- constant default is TRUE, which is the backfill. The web app asks
-- "Alert me about this line" when a line is pinned, on by default.
--
-- Expand-only. ADD COLUMN with a constant default changes the catalog
-- only (no rewrite); its brief ACCESS EXCLUSIVE on `pinned_lines` is
-- bounded by the 5 s lock_timeout. Code before this change never reads it,
-- and `PUT /preferences/pinned-lines` keeps the flag of every line it
-- keeps pinned.
-- -------------------------------------------------------------------------

ALTER TABLE pinned_lines
    ADD COLUMN notify BOOLEAN NOT NULL DEFAULT TRUE;
