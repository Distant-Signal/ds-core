SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Regular journeys, phase 1 (distant-signal's
-- docs/superpowers/specs/2026-11-06-regular-journeys-design.md §4.1).
-- Expand-only: a template may carry a target (origin, destination, a
-- depart-after or arrive-by time and a day offset), the plan options it was
-- planned with, the usual itinerary the user picked, and a link to its
-- paired return template. Every new column is nullable or has a constant
-- default (no rewrite); the three constraints are added NOT VALID and
-- validated by 20261106130100_regular_journeys_validate.sql. Old code
-- never reads these columns.
-- -------------------------------------------------------------------------

ALTER TABLE journey_templates
    ADD COLUMN origin_crs         TEXT,
    ADD COLUMN destination_crs    TEXT,
    -- 'depart_after' or 'arrive_by'. NULL: a template without a target
    -- (made before phase 1, or with the per-leg editor), which keeps the
    -- per-leg windows.
    ADD COLUMN target_kind        TEXT,
    -- A London clock time; the template has no date.
    ADD COLUMN target_time        TIME,
    -- 1: the target time is on the day after the service date (an
    -- arrive-by after midnight), as /Trips/plan's dayOffset.
    ADD COLUMN target_day_offset  SMALLINT NOT NULL DEFAULT 0,
    -- Vias, avoids, maximum changes, operator filter: the api's shape.
    ADD COLUMN plan_options       JSONB,
    -- The picked itinerary trimmed to what a later day needs: per leg the
    -- kind, places, train UID, operator and booked clock times.
    ADD COLUMN usual_itinerary    JSONB,
    -- The outbound and the return point at each other.
    ADD COLUMN paired_template_id BIGINT;

-- A CHECK passes on NULL, so the target branch tests every column it needs
-- with IS NOT NULL first: without that, a time with no target kind (or a
-- target with no origin) would evaluate to NULL and be accepted.
ALTER TABLE journey_templates
    ADD CONSTRAINT journey_templates_target_check CHECK (
        (target_kind IS NULL AND target_time IS NULL AND usual_itinerary IS NULL)
        OR (target_kind IS NOT NULL AND origin_crs IS NOT NULL AND destination_crs IS NOT NULL
            AND target_kind IN ('depart_after', 'arrive_by')
            AND target_time IS NOT NULL
            AND origin_crs ~ '^[A-Z]{3}$' AND destination_crs ~ '^[A-Z]{3}$'
            AND target_day_offset IN (0, 1)
            AND (usual_itinerary IS NULL OR jsonb_typeof(usual_itinerary) = 'object')
            AND (plan_options IS NULL OR jsonb_typeof(plan_options) = 'object'))
    ) NOT VALID,
    -- Deleting one template of a pair unlinks the other.
    ADD CONSTRAINT journey_templates_paired_fkey
        FOREIGN KEY (paired_template_id) REFERENCES journey_templates (id)
        ON DELETE SET NULL NOT VALID,
    ADD CONSTRAINT journey_templates_paired_not_self
        CHECK (paired_template_id IS DISTINCT FROM id) NOT VALID;
