-- Reference data (stations, TOCs) + incidents schema fixes.
--
-- -------------------------------------------------------------------------
-- Stations reference data
-- Published by the station-reference feed. Simple upsert-on-crs table,
-- no history — reference data is a snapshot of "current facts", not an
-- event stream worth auditing (Global Constraint 6).
-- -------------------------------------------------------------------------

CREATE TABLE stations (
    crs               CHAR(3)     PRIMARY KEY,
    name              TEXT        NOT NULL,
    latitude          DOUBLE PRECISION,
    longitude         DOUBLE PRECISION,
    station_operator  TEXT,
    accessibility     JSONB       NOT NULL DEFAULT '{}',
    fetched_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- -------------------------------------------------------------------------
-- TOC (Train Operating Company) reference data
-- Published by the TOC-reference feed. Same rationale as `stations`: no
-- history table (Global Constraint 6).
-- -------------------------------------------------------------------------

CREATE TABLE tocs (
    atoc_code        CHAR(2)     PRIMARY KEY,
    name             TEXT        NOT NULL,
    legal_name       TEXT        NOT NULL,
    atoc_member      BOOLEAN,
    station_operator BOOLEAN,
    fetched_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- -------------------------------------------------------------------------
-- Incidents: align columns with the Task-1 `IncidentMessage` shape.
--
-- The initial migration's `severity_hint`/`valid_from`/`valid_to` columns
-- never matched the real Knowledgebase schema modeled in `common`
-- (IncidentMessage has `priority: i32` and `validity: Vec<ValidityPeriod>`,
-- not a hint enum or a single from/to pair). The table holds no real data
-- yet, so this is a straight schema fix rather than a careful migration.
--
-- This also fixes a pre-existing bug: `incidents_active` used
-- `WHERE valid_to IS NULL OR valid_to > NOW()` as a partial index
-- predicate. Postgres requires partial-index predicates to be IMMUTABLE,
-- and `NOW()` is STABLE (changes within a transaction), not IMMUTABLE —
-- `CREATE INDEX` with that predicate is rejected outright. The replacement
-- predicate below (`WHERE NOT is_cleared`) is a plain boolean column
-- comparison, which is IMMUTABLE.
-- -------------------------------------------------------------------------

DROP INDEX incidents_active;

ALTER TABLE incidents
    DROP COLUMN severity_hint,
    DROP COLUMN valid_from,
    DROP COLUMN valid_to,
    ADD COLUMN priority          INTEGER     NOT NULL,
    ADD COLUMN validity_periods  JSONB       NOT NULL DEFAULT '[]',
    ADD COLUMN is_cleared        BOOLEAN     NOT NULL DEFAULT FALSE;

CREATE INDEX incidents_active ON incidents (incident_id) WHERE NOT is_cleared;


-- -------------------------------------------------------------------------
-- Incident history: mirror the same column changes so snapshots keep
-- storing a full copy of the row being superseded (see the initial
-- migration's comment on this table for the "snapshot on change" intent).
-- -------------------------------------------------------------------------

ALTER TABLE incident_history
    DROP COLUMN severity_hint,
    DROP COLUMN valid_from,
    DROP COLUMN valid_to,
    ADD COLUMN priority          INTEGER     NOT NULL,
    ADD COLUMN validity_periods  JSONB       NOT NULL DEFAULT '[]',
    ADD COLUMN is_cleared        BOOLEAN     NOT NULL DEFAULT FALSE;
