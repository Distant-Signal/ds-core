SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- A journey's operator filter (docs/superpowers/specs/2026-10-31-planner-
-- operator-filtering-design.md §6.9): the filter the journey's plan was
-- made with on /plan, kept so its mid-journey replans and break suggestions
-- honour it.
--
--   operator_filter_mode     `only`, `avoid` or `prefer`; NULL: no filter.
--   operator_filter_codes    the operators as applied, groups expanded
--                            (`{TL,GN,SN,GX}`): what replans use, so a
--                            later edit to a group doesn't change a saved
--                            journey.
--   operator_filter_entries  as the traveller chose them (`{group:GTR}`):
--                            what the journey page shows.
--
-- On `journeys`, not on a revision: it is the traveller's intent for the
-- whole journey, like custom_name, and outlives the revisions replans add.
--
-- Expand-only: the nullable column and the two with constant defaults
-- change the catalog only, and both CHECKs go in NOT VALID (no scan under
-- the ACCESS EXCLUSIVE, which the 5 s lock_timeout bounds);
-- 20261031150100 validates them. Code before this change never reads or
-- writes the columns.
-- -------------------------------------------------------------------------

ALTER TABLE journeys
    ADD COLUMN operator_filter_mode TEXT,
    ADD COLUMN operator_filter_codes TEXT[] NOT NULL DEFAULT '{}',
    ADD COLUMN operator_filter_entries TEXT[] NOT NULL DEFAULT '{}',
    ADD CONSTRAINT journeys_operator_filter_mode_check CHECK (
        operator_filter_mode IN ('only', 'avoid', 'prefer')
    ) NOT VALID,
    ADD CONSTRAINT journeys_operator_filter_consistent CHECK (
        (operator_filter_mode IS NULL) = (cardinality(operator_filter_codes) = 0)
        AND cardinality(operator_filter_entries) <= 8
    ) NOT VALID;
