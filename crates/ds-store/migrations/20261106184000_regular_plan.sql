SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Regular journeys, phase 3 (distant-signal's
-- docs/superpowers/specs/2026-11-06-regular-journeys-design.md §6.3): the
-- api's plan sweep (`regular_plan_sweep`) chooses the day's train for an
-- occurrence of a template with a target and records what it chose here.
-- Expand-only: four nullable columns (no rewrite) and a CHECK added NOT
-- VALID, validated by 20261106184100_regular_plan_validate.sql. Old code
-- never reads these columns.
-- -------------------------------------------------------------------------

ALTER TABLE journeys
    -- The choice, up to two alternatives and the leg ids the sweep
    -- committed itself (the api's shape). NULL: never planned.
    ADD COLUMN regular_plan        JSONB,
    -- 'pending', 'suggested' (remind mode), 'committed' (auto mode) or
    -- 'no_service' (no itinerary on a published day). NULL: not an
    -- occurrence the sweep plans.
    ADD COLUMN regular_plan_status TEXT,
    -- The planner's GraphMarker the plan was made against: a new marker
    -- (a schedule publish) makes the occurrence due again.
    ADD COLUMN regular_plan_marker TEXT,
    -- When the sweep last planned it; the accept route echoes it back.
    ADD COLUMN regular_planned_at  TIMESTAMPTZ;

ALTER TABLE journeys
    ADD CONSTRAINT journeys_regular_plan_check CHECK (
        (regular_plan_status IS NULL
         OR regular_plan_status IN ('pending', 'suggested', 'committed', 'no_service'))
        AND (regular_plan IS NULL OR jsonb_typeof(regular_plan) = 'object')
    ) NOT VALID;
