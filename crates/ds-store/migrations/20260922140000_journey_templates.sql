-- -------------------------------------------------------------------------
-- Journey templates, Phase B (durable, on-demand only) --
-- docs/superpowers/specs/2026-09-22-reusable-repeating-journeys-design.md
-- §2.2, §8.
--
-- A template is a saved journey SHAPE a user can stamp a new `journeys` row
-- from, either on demand (this migration -- Phase B) or, eventually, on a
-- schedule (Phase C). Deliberately its OWN table pair, not folded onto
-- `journeys`/`journey_legs`: a template leg has no `service_date`
-- (`journey_legs.service_date` is NOT NULL and every existing reader
-- assumes a concrete date), no `train_subscription_id` (a template is
-- never itself bound to a real train), and needs fields that would be
-- meaningless NULLs on every ordinary journey.
--
-- Phase-C-only columns (days_of_week, active, starts_on, ends_on,
-- default_match_mode, auto_commit_rule) are created NOW, in this Phase-B
-- migration, deliberately unused by any Phase-B code path -- adding them
-- later would be a second migration for no reason; every Phase-B route
-- round-trips them (a client can set/read default_match_mode etc. today)
-- but nothing acts on them until Phase C's sweep exists. See the design
-- doc's §8 note making this explicit.
-- -------------------------------------------------------------------------

CREATE TABLE journey_templates (
    id                  BIGSERIAL PRIMARY KEY,
    user_id             TEXT NOT NULL REFERENCES users(id),
    custom_name         TEXT,
    -- Phase C: NULL = a one-shot template (Phase B's only shape). Non-NULL
    -- = which days this template auto-materializes on, Mon=1..Sun=64.
    -- Written/read by this plan's routes (round-tripped, always NULL in
    -- practice until Phase C's UI sets it) but never checked by
    -- materialize_template -- Phase B ignores this column entirely.
    days_of_week        SMALLINT,
    -- Phase C: pause without deleting. Always TRUE in practice through
    -- Phase B (no UI writes anything but the DEFAULT), but round-tripped
    -- by GET/PUT so a future Phase C toggle has somewhere to write.
    active              BOOLEAN NOT NULL DEFAULT TRUE,
    -- Phase C: recurrence window bounds. Unused by Phase B.
    starts_on           DATE,
    ends_on             DATE,
    -- Phase C: how a materialized leg's match_mode is seeded. Phase B's
    -- own materialize_template ignores this column completely -- every
    -- leg it mints is 'unmatched', regardless of what this says.
    default_match_mode  TEXT NOT NULL DEFAULT 'manual'
                         CHECK (default_match_mode IN ('manual', 'auto')),
    -- Phase C: NULL unless default_match_mode='auto'. See the design doc's
    -- §2.3/§3.2 for the 2026-09-22-resolved 'nearest_to_now' mechanics --
    -- none of that is implemented anywhere in this migration or plan.
    auto_commit_rule    TEXT
                         CHECK (auto_commit_rule IN ('earliest', 'nearest_to_now')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX journey_templates_user_id ON journey_templates (user_id);

CREATE TABLE journey_template_legs (
    id                  BIGSERIAL PRIMARY KEY,
    template_id         BIGINT NOT NULL REFERENCES journey_templates(id) ON DELETE CASCADE,
    leg_order           INT NOT NULL,
    -- Both nullable: a leg promoted from a pin/knownTrain-mode journey leg
    -- may legitimately have neither yet (the "no schedule data yet" gap
    -- journey_legs.origin_crs/destination_crs already accept -- see
    -- 20260922090000_journeys.sql's own header comment for the same
    -- reasoning one layer down). This plan's own route-level validation
    -- (Task 3) still requires BOTH to be present for a MANUALLY-created
    -- template leg -- only a promoted leg can legitimately arrive here
    -- with either NULL, and even then only transiently (see Task 2's
    -- promote-path handling).
    origin_crs          TEXT,
    destination_crs     TEXT,
    -- No service_date -- a template leg is date-less by definition; the
    -- materialized journey_legs row gets the target date at stamping time
    -- (Task 2's materialize_template).
    depart_after        TIME,
    depart_before       TIME,
    arrive_after        TIME,
    arrive_before       TIME,
    UNIQUE (template_id, leg_order)
);
CREATE INDEX journey_template_legs_template_id ON journey_template_legs (template_id);

-- Lineage: which template (if any) produced a given journey. Nullable and
-- ON DELETE SET NULL so deleting a template never cascades into deleting
-- journeys it already produced -- matches journey_legs.train_subscription_id's
-- own ON DELETE SET NULL precedent (20260922090000_journeys.sql): a
-- deleted parent orphans its children's foreign key, never deletes the
-- children themselves.
ALTER TABLE journeys ADD COLUMN source_template_id BIGINT
    REFERENCES journey_templates(id) ON DELETE SET NULL;
CREATE INDEX journeys_source_template_id ON journeys (source_template_id);
