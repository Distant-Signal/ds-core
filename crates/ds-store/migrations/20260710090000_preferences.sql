-- -------------------------------------------------------------------------
-- User preferences: which lines and stations are pinned to the home page.
-- No FK to custom_lines/stations: official line ids are compile-time TOML,
-- not DB rows, so no single constraint can cover both catalogue and
-- custom lines. The API filters out stale ids on read instead (see
-- `crates/api/src/routes/preferences.rs`). No owner column — unauthenticated
-- for now, same rationale as `custom_lines` (see
-- docs/superpowers/specs/2026-07-09-custom-lines-and-blended-stats-design.md's
-- Non-goals).
-- -------------------------------------------------------------------------

CREATE TABLE pinned_lines (
    line_id    TEXT        PRIMARY KEY,
    pinned_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE pinned_stations (
    crs        CHAR(3)     PRIMARY KEY,
    pinned_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
