-- -------------------------------------------------------------------------
-- 19-pass security/bug review, journeys area, Medium finding 1: a
-- time-flexible template leg (crates/api/src/data/journey_templates.rs's
-- `validate_template_leg`, deliberately allowing all four window bounds to
-- stay unset -- see that function's own doc comment) materializes
-- (`materialize_template`) into a `journey_legs` row with
-- `depart_after`/`depart_before`/`arrive_after`/`arrive_before` all NULL.
-- That is byte-for-byte the same shape a `pin`/`knownTrain`-mode leg has
-- (no window was ever searched for THOSE either) -- so once the traveller
-- picks a train for the template-sourced leg, `JourneyLegCard.tsx`'s
-- `hasWindow` check (any of the four bounds non-null) can no longer tell
-- "this leg was intentionally an open any-train-any-time search, re-search
-- is still meaningful" apart from "this leg never had a window at all,
-- re-search makes no sense" -- and permanently hides "Change train" for the
-- former, with no recovery except deleting the leg outright.
--
-- `window_searched` records that distinction directly, at the only point
-- it's actually known: leg-creation time. `TRUE` for every leg created via
-- a window search (`create_journey_with_window_leg`/
-- `add_window_leg_to_journey`, and now `materialize_template` -- a template
-- leg is ALWAYS a search, even a fully-open one, never a direct pin/known-
-- train pick), `FALSE` for a `pin`/`knownTrain`-mode leg. The frontend's
-- `hasWindow` gate switches to reading this column instead of re-deriving
-- it from the four nullable bounds -- see JourneyLegCard.tsx's own updated
-- doc comment.
--
-- Backfill: best-effort for legs written before this column existed --
-- `TRUE` wherever any of the four bounds is already set (a real,
-- unambiguous window search), `FALSE` (the column's own default) otherwise.
-- This cannot retroactively distinguish a pre-migration fully-open
-- template leg from a pre-migration pin/knownTrain leg (both are all-NULL
-- today, and nothing else on the row records which path created it) -- an
-- accepted gap for data that already existed before this fix shipped; every
-- leg materialized FROM THIS POINT ON gets the correct value at creation
-- time.
-- -------------------------------------------------------------------------

ALTER TABLE journey_legs ADD COLUMN window_searched BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE journey_legs
SET window_searched = TRUE
WHERE depart_after IS NOT NULL
   OR depart_before IS NOT NULL
   OR arrive_after IS NOT NULL
   OR arrive_before IS NOT NULL;
