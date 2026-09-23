-- -------------------------------------------------------------------------
-- Dynamic Trip Planning Phase 1: real interchange data.
-- docs/superpowers/specs/2026-09-22-dynamic-trip-planning-design.md §0.2,
-- §0.4, §4, §8 Phase 1; this plan's own Task 4.
--
-- Same-station changes: a nullable `change_time_minutes` on the existing
-- `stanox_crs` table (a per-TIPLOC/STANOX attribute, so it belongs on the
-- table already keyed by that identity, not a new table). NULL means "no
-- MSN record matched this TIPLOC at all" -- a real, different fact from a
-- present-but-sentinel (98/99) value or the modal 5 -- see this plan's
-- Judgment Call 3. No default/sentinel interpretation happens here or in
-- `schedule-reference`; Phase 2's connections-array builder is where that
-- policy lives.
--
-- Cross-station walking/tube/bus/tram/ferry transfers: a new `fixed_links`
-- table, CRS-keyed (ALF's own identifier space -- NOT TIPLOCs, see
-- `alf.rs`'s own module doc). Wholesale-replaced every publish cycle
-- (DELETE + INSERT in one transaction, see `queries::upsert_fixed_links`)
-- rather than per-row upserted, because a real ALF row has no natural
-- stable per-row key -- the same physical link legitimately appears as
-- several rows differing only in mode/validity window (this plan's
-- Judgment Call 4).
-- -------------------------------------------------------------------------

ALTER TABLE stanox_crs ADD COLUMN change_time_minutes INTEGER;

CREATE TABLE fixed_links (
    id                BIGSERIAL PRIMARY KEY,
    mode              TEXT NOT NULL,
    from_crs          TEXT NOT NULL,
    to_crs            TEXT NOT NULL,
    minutes           INTEGER NOT NULL,
    -- Raw "HHMM" (4 ASCII digits), not a TIME column -- see alf.rs's own
    -- doc comment on ParsedFixedLink::valid_from/valid_to for why this
    -- stays a raw string, matching this app's "store raw CIF value,
    -- interpret at read time" convention.
    valid_from        TEXT NOT NULL,
    valid_to          TEXT NOT NULL,
    -- Raw 7-char '0'/'1' bitmask, Monday-first, matching
    -- schedule_query::records::BasicSchedule::days_of_week's convention.
    days_mask         TEXT NOT NULL,
    source_sequence   INTEGER NOT NULL,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX fixed_links_from_crs ON fixed_links (from_crs);
