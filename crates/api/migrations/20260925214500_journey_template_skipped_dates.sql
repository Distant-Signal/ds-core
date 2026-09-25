-- -------------------------------------------------------------------------
-- Tombstones for "this template's occurrence for this date was explicitly
-- discarded by its owner -- do not re-mint it."
--
-- Real bug this closes: the recurrence sweep's own mint idempotency guard
-- (crates/notifier/src/queries.rs::materialize_due_template_occurrence) is
-- purely "does a journey with this source_template_id already have a leg on
-- this date," so DELETING today's auto-minted occurrence (the user isn't
-- travelling today) made that guard false again and the very next sweep
-- tick -- within the hour -- re-minted it, re-auto-committed its legs and
-- resumed pushing notifications for a journey the user had explicitly
-- discarded. There was no other record anywhere that the deletion had ever
-- happened: `journeys` rows are the only trace of an occurrence, and
-- deleting one removes that trace by definition.
--
-- A tombstone row, not a "skipped" flag on `journeys`: the whole point is
-- to outlive the deleted `journeys` row, and a soft-delete column on
-- `journeys` would have to be excluded by every existing reader of that
-- table (`list_journeys_for_user`, `get_journey_summary`, the group-sharing
-- joins, ...) instead of by the ONE writer that needs to care.
--
-- Written by `api`'s own delete paths (data::journeys::delete_journey, and
-- data::journeys::delete_leg when removing the last leg takes the journey
-- with it) and cleared by an explicit re-materialize of the same date
-- (data::journey_templates::materialize_template -- a human pressing "Run
-- now" for a date they previously discarded is unambiguously asking for it
-- back). Read by the sweep's mint guard.
--
-- ON DELETE CASCADE on template_id: a deleted template has no occurrences
-- left to suppress, so its tombstones are dead weight -- same posture as
-- journey_template_legs' own FK (20260922140000_journey_templates.sql).
-- -------------------------------------------------------------------------

CREATE TABLE journey_template_skipped_dates (
    template_id  BIGINT      NOT NULL REFERENCES journey_templates(id) ON DELETE CASCADE,
    service_date DATE        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (template_id, service_date)
);
