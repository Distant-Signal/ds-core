SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Iarnród Éireann board history (distant-signal's
-- docs/superpowers/specs/2026-11-05-irish-rail-train-tracking-design.md
-- §4.1). island_of_ireland_station_samples keeps only each station's latest
-- board; these two keep what the boards said about each train, by train
-- code and Traindate (train codes repeat daily), so a train has a page
-- after it leaves a board. Raw feed values: common's
-- island_of_ireland::train_state reads them.
--
-- Written by the ingest-writer from ioi-station-samples/1, in the samples'
-- transaction, and pruned there by train_date: 14 days, the window
-- untracked GB trains (and their train_movement_events) are kept (§4.3).
-- -------------------------------------------------------------------------

-- One row per train per service date: its newest observation on any board.
CREATE TABLE island_of_ireland_trains (
    network          TEXT NOT NULL,
    train_code       TEXT NOT NULL,
    train_date       DATE NOT NULL,
    origin           TEXT NOT NULL,
    destination      TEXT NOT NULL,
    -- HH:MM, Irish local time, as sent.
    origin_time      TEXT,
    destination_time TEXT,
    -- The feed's Status text, e.g. 'En Route'.
    status           TEXT NOT NULL,
    -- The feed's Late as sent (meaningless while status says no report).
    late_minutes     INTEGER,
    -- The feed's Lastlocation, e.g. 'Departed Dundalk'.
    last_location    TEXT,
    first_seen_at    TIMESTAMPTZ NOT NULL,
    -- The newest board it was on.
    last_seen_at     TIMESTAMPTZ NOT NULL,
    -- When the values above last changed.
    changed_at       TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (network, train_code, train_date),
    CHECK (train_code ~ '^[A-Z0-9]{1,8}$')
);
CREATE INDEX island_of_ireland_trains_train_date
    ON island_of_ireland_trains (train_date);

-- One row per train per station whose board showed it: that board's times.
-- Written only when they change, so it has no "last seen" of its own.
CREATE TABLE island_of_ireland_train_calls (
    network             TEXT NOT NULL,
    train_code          TEXT NOT NULL,
    train_date          DATE NOT NULL,
    -- The live StationCode (island_of_ireland_stations.live_code).
    station_code        TEXT NOT NULL,
    -- HH:MM, Irish local time; NULL where the feed sends 00:00 for none.
    scheduled_arrival   TEXT,
    scheduled_departure TEXT,
    expected_arrival    TEXT,
    expected_departure  TEXT,
    late_minutes        INTEGER,
    first_seen_at       TIMESTAMPTZ NOT NULL,
    -- When these values were seen.
    changed_at          TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (network, train_code, train_date, station_code),
    CHECK (train_code ~ '^[A-Z0-9]{1,8}$')
);
CREATE INDEX island_of_ireland_train_calls_train_date
    ON island_of_ireland_train_calls (train_date);

-- Both are pruned a day at a time and the trains table is updated every
-- poll: vacuum and analyze at 5% rather than the default 20%, as the other
-- batch-deleted tables do (20260926181000).
ALTER TABLE island_of_ireland_trains SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.05
);
ALTER TABLE island_of_ireland_train_calls SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.05
);
