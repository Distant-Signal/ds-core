-- -------------------------------------------------------------------------
-- Closes the NULL-owner state `20260828100000_add_ownership.sql` left open
-- on purpose. Per the repo owner's explicit instruction (see
-- docs/superpowers/specs/2026-08-31-private-custom-lines-and-tracked-trains-design.md
-- Decision 2, and the go/no-go recorded in
-- docs/superpowers/plans/2026-08-31-private-custom-lines-and-tracked-trains.md
-- Task 1 on 2026-09-01), a custom line with no real owner must become
-- genuinely impossible, not just application-layer-inaccessible.
--
-- DEVIATION FROM THE PLAN'S DEFAULT: the plan's own recommended default is
-- to reassign any surviving NULL-owner row to an unreachable placeholder
-- account rather than delete it. The repo owner was explicitly offered
-- both mechanics (reassign-and-migrate vs. delete-and-migrate) and chose
-- the destructive delete path instead, without running the plan's Step 1
-- fact-finding count first (no live database was reachable from the
-- implementing session either) -- so this migration does not carry
-- forward any legacy NULL-owner row's content. If this table in fact holds
-- rows worth keeping, they will not survive this migration.
-- -------------------------------------------------------------------------

DELETE FROM custom_lines WHERE user_id IS NULL;

ALTER TABLE custom_lines ALTER COLUMN user_id SET NOT NULL;
