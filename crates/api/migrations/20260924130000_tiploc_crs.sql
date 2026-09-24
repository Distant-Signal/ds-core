-- -------------------------------------------------------------------------
-- TIPLOC-primary sibling of stanox_crs (20260901150000_stanox_crs.sql):
-- one row per TIPLOC that resolves to a CRS, rather than one row per
-- STANOX. This is purely additive -- stanox_crs is untouched -- and
-- closes the gap where a real station whose STANOX is shared by more than
-- one genuine calling-point TIPLOC (e.g. Vauxhall's VAUXHLM/VAUXHLW, both
-- CRS VXH) only ever kept one of those TIPLOCs in stanox_crs's own
-- STANOX-keyed disambiguation. Populated by crates/schedule-reference from
-- the CIF SCHEDULE feed's own TI/A records via parser::resolve_tiploc_crs.
-- Replace-on-write, same as stanox_crs: every daily delivery is a full
-- refresh, so every successful POST /private/tiploc-crs upserts the
-- complete current table by `tiploc`. See
-- docs/superpowers/plans/2026-09-24-tiploc-crs-crosswalk-plan.md and
-- crates/api/src/data/journey.rs's `tiploc_key` doc comment ("What this
-- does NOT fix" item 1) for the gap this closes.
-- -------------------------------------------------------------------------

CREATE TABLE tiploc_crs (
    tiploc TEXT PRIMARY KEY,
    crs TEXT NOT NULL,
    station_name TEXT NOT NULL,
    stanox TEXT NOT NULL,
    source_sequence INTEGER NOT NULL,
    change_time_minutes INTEGER,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX tiploc_crs_crs_idx ON tiploc_crs (UPPER(crs));
