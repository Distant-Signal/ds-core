-- nr-status-v2 database schema
-- PostgreSQL. Mirrors the three logical stores from the architecture doc:
-- active_incidents, station_samples, and line_status (aggregator output).

-- -------------------------------------------------------------------------
-- Incidents
-- Raw Knowledgebase incident messages, written by the KB poller.
-- Rows are upserted on incident_id; there is no delete behavior — an
-- incident that leaves the feed simply stops being refreshed. Whether an
-- incident is still active is read from `is_cleared` (see the later
-- reference-data migration, which replaces the columns below with
-- `priority`, `validity_periods`, and `is_cleared` to match the real
-- Knowledgebase schema).
-- -------------------------------------------------------------------------

CREATE TABLE incidents (
    incident_id       TEXT PRIMARY KEY,
    summary           TEXT        NOT NULL,
    description       TEXT        NOT NULL,
    operators         TEXT[]      NOT NULL,  -- ATOC codes
    affected_stations TEXT[]      NOT NULL,  -- CRS codes
    severity_hint     TEXT        CHECK (severity_hint IN ('major', 'minor')),
    valid_from        TIMESTAMPTZ,
    valid_to          TIMESTAMPTZ,
    is_planned        BOOLEAN     NOT NULL DEFAULT FALSE,
    fetched_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Fast lookup for /StopPoint/{crs}/Disruption and the matcher.
CREATE INDEX incidents_affected_stations_gin ON incidents USING GIN (affected_stations);
CREATE INDEX incidents_operators_gin         ON incidents USING GIN (operators);
-- Partial index over currently-active incidents for the common query path.
-- NOTE: the predicate here is intentionally just `valid_to IS NULL`, not
-- `valid_to IS NULL OR valid_to > NOW()` — Postgres requires partial-index
-- predicates to be IMMUTABLE, and NOW() is only STABLE, so the NOW()
-- variant is rejected outright at CREATE INDEX time ("functions in index
-- predicate must be marked IMMUTABLE"). This index is superseded by a
-- rebuilt `incidents_active` in a later migration once the table gains an
-- `is_cleared` column to key off instead.
CREATE INDEX incidents_active ON incidents (valid_from)
    WHERE valid_to IS NULL;


-- -------------------------------------------------------------------------
-- Station samples
-- Most-recent LDBWS departure board poll per station, written by the
-- LDBWS sampler. One row per CRS; upserted on each poll cycle.
-- Departures are kept as JSONB — the list structure maps directly to
-- StationDeparture and is never queried field-by-field in the DB.
-- -------------------------------------------------------------------------

CREATE TABLE station_samples (
    crs        CHAR(3)     PRIMARY KEY,
    polled_at  TIMESTAMPTZ NOT NULL,
    departures JSONB       NOT NULL DEFAULT '[]'
);


-- -------------------------------------------------------------------------
-- Line status
-- Aggregator output: one row per line, fully replaced each aggregation
-- cycle. `statuses` is a JSONB array of LineStatus objects (severity,
-- reason, validity, disruption, data_quality). Stored as JSONB because
-- the list is always written and read as a unit, and the nested
-- ValidityPeriod / Disruption / AffectedRoute shapes make column-per-field
-- awkward without benefit.
-- -------------------------------------------------------------------------

CREATE TABLE line_status (
    line_id     TEXT        PRIMARY KEY,
    name        TEXT        NOT NULL,
    mode_name   TEXT        NOT NULL,
    operators   TEXT[]      NOT NULL,
    statuses    JSONB       NOT NULL DEFAULT '[]',
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Supports /Line/Mode/{mode}/Status filtering.
CREATE INDEX line_status_mode ON line_status (mode_name);


-- -------------------------------------------------------------------------
-- Line status history
-- Append-only audit log written by the aggregator alongside the upsert
-- to line_status. Useful for debugging regressions and building a
-- "status over time" view. Pruned by a periodic job (e.g. keep 7 days).
-- -------------------------------------------------------------------------

CREATE TABLE line_status_history (
    id          BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    line_id     TEXT        NOT NULL,
    statuses    JSONB       NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX line_status_history_line_time ON line_status_history (line_id, computed_at DESC);


-- -------------------------------------------------------------------------
-- Incident history
-- Snapshot of an incident each time the poller sees it change (summary,
-- description, or validity_periods differ from the stored row — see the
-- reference-data migration, which also updates this table's columns to
-- match). Lets us reconstruct how an incident evolved.
-- -------------------------------------------------------------------------

CREATE TABLE incident_history (
    id          BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    incident_id TEXT        NOT NULL,
    summary     TEXT        NOT NULL,
    description TEXT        NOT NULL,
    operators   TEXT[]      NOT NULL,
    affected_stations TEXT[] NOT NULL,
    severity_hint TEXT,
    valid_from  TIMESTAMPTZ,
    valid_to    TIMESTAMPTZ,
    is_planned  BOOLEAN     NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX incident_history_id_time ON incident_history (incident_id, recorded_at DESC);
