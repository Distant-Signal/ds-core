SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Regular journeys: a different time on some days (distant-signal's
-- docs/superpowers/specs/2026-11-06-regular-journeys-design.md, the
-- scheduling options' R3).
--
-- - `journey_template_day_overrides`: for one ISO weekday (1 Monday to 7
--   Sunday) of a template with a target, that day's own target and usual
--   itinerary. Days without a row use the template's own. The api's plan
--   sweep plans an occurrence on that weekday with the override's target,
--   and a later reroute must read it too.
-- - `journey_template_legs.variant_weekday`: NULL for the template's own
--   legs; 1 to 7 for the legs the api derives from that weekday's
--   override. The mint takes the weekday's variant legs when it has any,
--   else the NULL ones. A notifier that doesn't know the column would mint
--   both, so the notifier that reads it ships before any api writes one.
--
-- Expand-only. The ADD COLUMN is nullable, so it changes the catalog only;
-- its brief ACCESS EXCLUSIVE is bounded by the 5 s lock_timeout. Its CHECK
-- is added NOT VALID and validated by 20261106233100. The new table's
-- foreign key takes a brief SHARE ROW EXCLUSIVE lock on
-- `journey_templates`. The unique key moves to include the weekday in
-- 20261106233200 (built concurrently); a later release drops the old key
-- with a contract header, and no api writes a variant leg before that.
-- -------------------------------------------------------------------------

CREATE TABLE journey_template_day_overrides (
    template_id       BIGINT   NOT NULL REFERENCES journey_templates (id) ON DELETE CASCADE,
    weekday           SMALLINT NOT NULL CHECK (weekday BETWEEN 1 AND 7),
    target_kind       TEXT     NOT NULL CHECK (target_kind IN ('depart_after', 'arrive_by')),
    target_time       TIME     NOT NULL,
    target_day_offset SMALLINT NOT NULL DEFAULT 0 CHECK (target_day_offset IN (0, 1)),
    usual_itinerary   JSONB    CHECK (usual_itinerary IS NULL OR jsonb_typeof(usual_itinerary) = 'object'),
    PRIMARY KEY (template_id, weekday)
);

ALTER TABLE journey_template_legs ADD COLUMN variant_weekday SMALLINT;
ALTER TABLE journey_template_legs
    ADD CONSTRAINT journey_template_legs_variant_weekday_check
    CHECK (variant_weekday IS NULL OR variant_weekday BETWEEN 1 AND 7) NOT VALID;
