SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- The trip planner's timetable snapshot (DS-235; api data::planner_snapshot):
-- every published service date's planning inputs, built once per schedule
-- publish (or transit feed) by the api and loaded by every api process, so
-- a date's graph is a filter over it instead of a ~490k-row read.
--
--   version    the snapshot's key: a SHA-256 over its format, the transit
--              networks it covers and the graph cache's marker (the latest
--              publish and each network's active feed)
--   sha256     of `bytes`, checked on load
--   bytes      the snapshot, gzip-compressed JSON (tens of MB)
--   built_at   when it was built: one older than the graph cache's max age
--              is rebuilt
--
-- The api keeps the newest two rows (a process still on the previous
-- publish can load it until it moves on) and deletes the rest. A new table:
-- nothing reads it before this change.
-- -------------------------------------------------------------------------

CREATE TABLE planner_snapshots (
    version  TEXT PRIMARY KEY,
    sha256   TEXT NOT NULL,
    bytes    BYTEA NOT NULL,
    built_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE planner_snapshots IS
    'Trip planner timetable snapshots (DS-235): one per schedule publish and transit feed, built and read by the api.';
