SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Transit networks (Manchester Metrolink first): the generic `transit_*`
-- tables, keyed by network (docs/superpowers/specs/2026-10-09-transit-
-- networks-design.md §8, decision D3).
--
-- Only the ingest-writer writes them, from the `ds:ingest:transit-<network>`
-- streams (`ds_store::transit`); the api reads them (batch 3). Every table is
-- new, so this is expand-only and its indexes are built here, on empty
-- tables, inside the migration's transaction.
--
-- A network's timetable is a versioned feed. Each `transit-timetable/1`
-- part inserts rows under (network, feed_version) and records itself in
-- transit_feed_parts; the `transit-timetable-commit/1` entry activates the
-- version only if every part it lists arrived (spec §9.2), by pointing
-- transit_networks.active_feed_version at it. Readers join on the active
-- version, so they never see half a feed. At most `active` and `previous`
-- are kept: activation deletes the rest, and every versioned table
-- cascades from transit_feed_versions.
--
-- Ids: a network and a local line id are [a-z0-9-]{1,32}; a stop id is a
-- NaPTAN ATCO code, [A-Z0-9]{1,16}. Public data only (BODS, NaPTAN, OGL);
-- no personal data.
-- -------------------------------------------------------------------------

-- One row per network, upserted from the network part at each activation.
CREATE TABLE transit_networks (
    network TEXT PRIMARY KEY CHECK (network ~ '^[a-z0-9-]{1,32}$'),
    name TEXT NOT NULL,
    mode TEXT NOT NULL,
    timezone TEXT NOT NULL,
    attribution JSONB NOT NULL,
    active_feed_version TEXT,
    activated_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL
);

-- Each feed version: staged by its parts, then active, then previous.
-- network_row and rail_links are the staged network and rail-link parts
-- (applied to transit_networks and transit_rail_links, which are not
-- versioned, at activation); the commit's manifest fills the rest.
CREATE TABLE transit_feed_versions (
    network TEXT NOT NULL CHECK (network ~ '^[a-z0-9-]{1,32}$'),
    feed_version TEXT NOT NULL CHECK (length(feed_version) BETWEEN 1 AND 64),
    state TEXT NOT NULL CHECK (state IN ('staging', 'active', 'previous')),
    network_row JSONB,
    rail_links JSONB NOT NULL DEFAULT '[]',
    content_sha256 TEXT CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    counts JSONB,
    horizon_from DATE,
    horizon_to DATE,
    source JSONB,
    -- The envelope produced_at of the first part staged, and of the commit
    -- that activated it (ingest D13: "data as of").
    staged_at TIMESTAMPTZ NOT NULL,
    produced_at TIMESTAMPTZ,
    activated_at TIMESTAMPTZ,
    PRIMARY KEY (network, feed_version)
);

-- The parts of a version that have arrived: part `part` of `parts` of a
-- kind. The commit compares them with its manifest.
CREATE TABLE transit_feed_parts (
    network TEXT NOT NULL,
    feed_version TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (
        kind IN ('network', 'lines', 'stops', 'patterns', 'trips', 'service_days', 'rail_links')
    ),
    part INT NOT NULL,
    parts INT NOT NULL,
    CHECK (part BETWEEN 1 AND parts),
    PRIMARY KEY (network, feed_version, kind, part),
    FOREIGN KEY (network, feed_version)
        REFERENCES transit_feed_versions (network, feed_version) ON DELETE CASCADE
);

CREATE TABLE transit_lines (
    network TEXT NOT NULL,
    feed_version TEXT NOT NULL,
    line_id TEXT NOT NULL CHECK (line_id ~ '^[a-z0-9-]{1,32}$'),
    name TEXT NOT NULL,
    colour TEXT CHECK (colour ~ '^#[0-9a-fA-F]{6}$'),
    sort_order INT NOT NULL,
    -- [{direction, headsign, stations: [stop_id, ...]}] (spec §6.2 step 10).
    directions JSONB NOT NULL,
    PRIMARY KEY (network, feed_version, line_id),
    FOREIGN KEY (network, feed_version)
        REFERENCES transit_feed_versions (network, feed_version) ON DELETE CASCADE
);

CREATE TABLE transit_stops (
    network TEXT NOT NULL,
    feed_version TEXT NOT NULL,
    stop_id TEXT NOT NULL CHECK (stop_id ~ '^[A-Z0-9]{1,16}$'),
    kind TEXT NOT NULL CHECK (kind IN ('station', 'platform')),
    parent_stop_id TEXT,
    name TEXT NOT NULL,
    indicator TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    wheelchair BOOLEAN,
    PRIMARY KEY (network, feed_version, stop_id),
    FOREIGN KEY (network, feed_version)
        REFERENCES transit_feed_versions (network, feed_version) ON DELETE CASCADE
);

-- A stop pattern: a line (NULL for a replacement bus), a direction (GTFS
-- direction_id) and its calls (transit_pattern_stops).
CREATE TABLE transit_patterns (
    network TEXT NOT NULL,
    feed_version TEXT NOT NULL,
    pattern_id TEXT NOT NULL CHECK (length(pattern_id) BETWEEN 1 AND 64),
    line_id TEXT CHECK (line_id ~ '^[a-z0-9-]{1,32}$'),
    direction SMALLINT NOT NULL CHECK (direction IN (0, 1)),
    headsign TEXT,
    PRIMARY KEY (network, feed_version, pattern_id),
    FOREIGN KEY (network, feed_version)
        REFERENCES transit_feed_versions (network, feed_version) ON DELETE CASCADE
);

-- A pattern's calls, in order; offsets in seconds from the first
-- departure; pickup and drop_off are GTFS pickup_type/drop_off_type.
CREATE TABLE transit_pattern_stops (
    network TEXT NOT NULL,
    feed_version TEXT NOT NULL,
    pattern_id TEXT NOT NULL,
    seq INT NOT NULL CHECK (seq >= 0),
    stop_id TEXT NOT NULL CHECK (stop_id ~ '^[A-Z0-9]{1,16}$'),
    arr_offset_s INT NOT NULL CHECK (arr_offset_s >= 0),
    dep_offset_s INT NOT NULL CHECK (dep_offset_s >= arr_offset_s),
    pickup SMALLINT NOT NULL CHECK (pickup BETWEEN 0 AND 3),
    drop_off SMALLINT NOT NULL CHECK (drop_off BETWEEN 0 AND 3),
    PRIMARY KEY (network, feed_version, pattern_id, seq),
    FOREIGN KEY (network, feed_version, pattern_id)
        REFERENCES transit_patterns (network, feed_version, pattern_id) ON DELETE CASCADE
);

-- Boards: the calls at a station's platforms (spec §11).
CREATE INDEX transit_pattern_stops_stop ON transit_pattern_stops (network, feed_version, stop_id);

-- start_s: seconds after the service day's local midnight; GTFS times run
-- past 24:00, so up to two days.
CREATE TABLE transit_trips (
    network TEXT NOT NULL,
    feed_version TEXT NOT NULL,
    trip_id TEXT NOT NULL CHECK (length(trip_id) BETWEEN 1 AND 128),
    line_id TEXT CHECK (line_id ~ '^[a-z0-9-]{1,32}$'),
    pattern_id TEXT NOT NULL,
    service_id TEXT NOT NULL,
    start_s INT NOT NULL CHECK (start_s >= 0 AND start_s < 172800),
    headsign TEXT,
    replacement_bus BOOLEAN NOT NULL,
    -- A replacement bus has no line (Q3); every other trip has one.
    CHECK (replacement_bus OR line_id IS NOT NULL),
    PRIMARY KEY (network, feed_version, trip_id),
    FOREIGN KEY (network, feed_version)
        REFERENCES transit_feed_versions (network, feed_version) ON DELETE CASCADE
);

-- Boards and the line timetable: a pattern's trips.
CREATE INDEX transit_trips_pattern ON transit_trips (network, feed_version, pattern_id);

-- The dates each service runs, within the stored horizon (60 days, Q8).
CREATE TABLE transit_service_days (
    network TEXT NOT NULL,
    feed_version TEXT NOT NULL,
    service_id TEXT NOT NULL,
    service_date DATE NOT NULL,
    PRIMARY KEY (network, feed_version, service_id, service_date),
    FOREIGN KEY (network, feed_version)
        REFERENCES transit_feed_versions (network, feed_version) ON DELETE CASCADE
);

-- The network's current situations (SIRI-SX, spec §8.3, batch 4): the
-- whole set is replaced by each snapshot.
CREATE TABLE transit_situations (
    network TEXT NOT NULL CHECK (network ~ '^[a-z0-9-]{1,32}$'),
    situation_id TEXT NOT NULL CHECK (length(situation_id) BETWEEN 1 AND 128),
    version INT,
    progress TEXT NOT NULL,
    planned BOOLEAN NOT NULL,
    reason TEXT,
    summary TEXT NOT NULL,
    description TEXT,
    info_url TEXT,
    validity JSONB NOT NULL,
    publication JSONB NOT NULL,
    consequences JSONB NOT NULL,
    line_ids TEXT[] NOT NULL,
    stop_ids TEXT[] NOT NULL,
    scope TEXT NOT NULL CHECK (scope IN ('line', 'stop', 'network')),
    observed_at TIMESTAMPTZ NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (network, situation_id)
);

-- Links between a transit station and a National Rail station (spec §8.4),
-- not versioned: replaced whole at each activation.
CREATE TABLE transit_rail_links (
    network TEXT NOT NULL CHECK (network ~ '^[a-z0-9-]{1,32}$'),
    stop_id TEXT NOT NULL CHECK (stop_id ~ '^[A-Z0-9]{1,16}$'),
    crs TEXT NOT NULL CHECK (crs ~ '^[A-Z]{3}$'),
    tiploc TEXT NOT NULL CHECK (tiploc ~ '^[A-Z0-9]{1,7}$'),
    kind TEXT NOT NULL CHECK (kind IN ('interchange', 'nearby')),
    note TEXT,
    observed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (network, stop_id, crs)
);

-- National Rail station pages: the links of a CRS (spec §12.1).
CREATE INDEX transit_rail_links_crs ON transit_rail_links (crs);
