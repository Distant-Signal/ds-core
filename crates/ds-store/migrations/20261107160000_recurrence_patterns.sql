SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Regular journeys: recurrence patterns (distant-signal's
-- docs/superpowers/specs/2026-11-06-regular-journeys-design.md, the
-- scheduling options' R2). Read by `ds_store::recurrence`.
--
-- - `recurrence_kind`: 'weekly' (the weekdays, every template so far),
--   'dates' (only the extra dates) or 'cycle' (kept for shift rotas; no
--   code writes it yet, and every reader treats it as never due). A reader
--   that doesn't know a kind never mints it.
-- - `interval_weeks` (1 to 4) and `anchor_week` (the Monday of a week that
--   runs): "every other week" is 2 with this or next week's Monday.
-- - `journey_template_extra_dates`: dates a template also runs on, or for
--   'dates' its only dates. The weekday pattern's start, end and bank
--   holidays don't apply to them; tombstones, time away and a pause do.
--
-- No backfill: the defaults make every existing row weekly, every week.
--
-- Expand-only. Each ADD COLUMN is nullable or has a constant default, so it
-- changes the catalog only; its brief ACCESS EXCLUSIVE is bounded by the
-- 5 s lock_timeout. The CHECKs are added NOT VALID and validated by
-- 20261107160100. The new table's foreign key takes a brief SHARE ROW
-- EXCLUSIVE lock on `journey_templates`. Old code never reads any of it.
-- -------------------------------------------------------------------------

ALTER TABLE journey_templates
    ADD COLUMN recurrence_kind TEXT     NOT NULL DEFAULT 'weekly',
    ADD COLUMN interval_weeks  SMALLINT NOT NULL DEFAULT 1,
    ADD COLUMN anchor_week     DATE;

ALTER TABLE journey_templates
    ADD CONSTRAINT journey_templates_recurrence_kind_check
        CHECK (recurrence_kind IN ('weekly', 'dates', 'cycle')) NOT VALID,
    ADD CONSTRAINT journey_templates_interval_weeks_check
        CHECK (interval_weeks BETWEEN 1 AND 4
               AND (interval_weeks = 1 OR anchor_week IS NOT NULL)) NOT VALID,
    ADD CONSTRAINT journey_templates_anchor_week_check
        CHECK (anchor_week IS NULL OR EXTRACT(ISODOW FROM anchor_week) = 1) NOT VALID;

CREATE TABLE journey_template_extra_dates (
    template_id  BIGINT NOT NULL REFERENCES journey_templates (id) ON DELETE CASCADE,
    service_date DATE   NOT NULL,
    PRIMARY KEY (template_id, service_date)
);
