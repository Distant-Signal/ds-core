-- -------------------------------------------------------------------------
-- Incidents: track when we first saw each incident_id, independent of
-- anything RDM reports. `upsert_incidents` (crates/api/src/data/queries.rs)
-- sets this once on INSERT and never touches it again on UPDATE -- it's
-- our own clock for incident age, immune to RDM leaving is_cleared/
-- validity_periods stale after an edit. See
-- docs/superpowers/specs/2026-07-16-stale-incident-handling-design.md.
-- -------------------------------------------------------------------------

ALTER TABLE incidents
    ADD COLUMN first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
