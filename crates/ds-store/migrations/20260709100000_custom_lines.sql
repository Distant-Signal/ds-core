-- -------------------------------------------------------------------------
-- Custom lines: user-defined lines (arbitrary station sets), stored
-- server-side so the aggregator can run the same incident-matching +
-- LDBWS-inference pipeline on them as the static lines/*.toml catalogue.
--
-- Deliberately simpler than the static catalogue's `LineDefinition`: no
-- per-station segment/tiploc/role, no match_keywords/excluded_keywords/
-- severity_overrides/exclusive_segments — those encode official-line route
-- topology and threshold tuning that doesn't apply to an arbitrary
-- user-picked station set. See
-- docs/superpowers/specs/2026-07-09-custom-lines-and-blended-stats-design.md
-- for the full rationale.
--
-- No owner/user column: unauthenticated for now, by design (see that
-- spec's Non-goals) — add ownership in the migration that actually adds
-- auth, not speculatively here.
--
-- `stations` is a plain `TEXT[]` of ordered CRS codes rather than the
-- spec's suggested jsonb `[{crs}, ...]`: since a custom-line station has no
-- other per-station data (no tiploc/role/segment, unlike catalogue lines),
-- a flat array achieves the same ordering with less structure. Matches how
-- `incidents.operators`/`affected_stations` already use `TEXT[]` elsewhere
-- in this schema.
-- -------------------------------------------------------------------------

CREATE TABLE custom_lines (
    id                      TEXT        PRIMARY KEY,
    name                    TEXT        NOT NULL,
    operators               TEXT[]      NOT NULL DEFAULT '{}',
    stations                TEXT[]      NOT NULL,
    headcode_prefixes       TEXT[]      NOT NULL DEFAULT '{}',
    destination_crs_filter  TEXT[]      NOT NULL DEFAULT '{}',
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
