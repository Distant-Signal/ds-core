-- Supports GET /public/incidents' default ordering, keyset cursor, and
-- from/to date-range filter (leading column) -- see
-- docs/superpowers/specs/2026-09-12-incident-archive-design.md Decision 5.
-- incidents_active (WHERE NOT is_cleared) carries no orderable column and
-- was built for a different, narrower check ("does this specific still-
-- active id exist"); this is the first index on first_seen_at at all.
-- incidents_operators_gin / incidents_affected_stations_gin already cover
-- this feature's operator/line filters unchanged -- no new index needed
-- for either.
CREATE INDEX incidents_first_seen_at_id
    ON incidents (first_seen_at DESC, incident_id DESC);
