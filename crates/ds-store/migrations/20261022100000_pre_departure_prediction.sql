SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Pre-departure prediction, phase 1 (DS-127,
-- docs/superpowers/specs/2026-10-21-pre-departure-prediction-design.md).
-- Additive: two new tables and one new view.
-- -------------------------------------------------------------------------

-- Each passenger train's previous working: the earlier service whose unit,
-- by the timetable, forms this one. Phase 1 infers it from the platform
-- sequence (`link = 'inferred'`, spec section 4 option A): the event just
-- before this train's origin departure on the same station and booked
-- platform is a terminating arrival by the same operator, 2-60 minutes
-- earlier. Phase 2's CIF association links write the same table with
-- `link = 'association'`.
--
-- Written by schedule-reference for each service date it publishes, right
-- after that date's calling points and services (one statement that
-- replaces the date's rows), so a read is a key lookup.
CREATE TABLE schedule_turnarounds (
    service_date              DATE        NOT NULL,
    uid                       TEXT        NOT NULL,
    prev_service_date         DATE        NOT NULL,
    prev_uid                  TEXT        NOT NULL,
    -- This train's working origin departure minus the previous working's
    -- working terminus arrival, rounded down to the minute.
    booked_turnaround_minutes SMALLINT    NOT NULL CHECK (booked_turnaround_minutes >= 0),
    link                      TEXT        NOT NULL CHECK (link IN ('inferred', 'association')),
    origin_tiploc             TEXT        NOT NULL,
    platform                  TEXT,
    -- This train's working origin departure, as an instant: the 3-hour
    -- horizon and the notifier's window read it.
    departs_at                TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (service_date, uid)
);

CREATE INDEX schedule_turnarounds_departs_at ON schedule_turnarounds (departs_at);

COMMENT ON TABLE schedule_turnarounds IS
    'Per (service_date, uid) previous working (inferred from the platform sequence, or a timetable association), precomputed at schedule publish.';

-- Targeted departure boards for tracked trains' own stops (spec section
-- 6.2 item 3): poller-ldbws's second lane asks LDBWS for the board of each
-- tracked train's boarding, alighting and change stations, narrowed with
-- timeOffset/timeWindow to that stop's booked time, and ingest-writer
-- applies it here (schema `tracked-stop-boards/1`). The api's per-stop board
-- match reads these rows before the station's general sample.
--
-- Latest board only, like station_samples. ingest-writer deletes rows a day
-- past their poll as it writes, so the table holds about one day of tracked
-- stops.
CREATE TABLE tracked_stop_boards (
    train_uid    TEXT        NOT NULL,
    service_date DATE        NOT NULL,
    crs          TEXT        NOT NULL,
    polled_at    TIMESTAMPTZ NOT NULL,
    departures   JSONB       NOT NULL,
    PRIMARY KEY (train_uid, service_date, crs)
);

CREATE INDEX tracked_stop_boards_polled_at ON tracked_stop_boards (polled_at);

-- What poller-ldbws's targeted lane polls: every station a user boards,
-- alights or changes at on a tracked train (a journey leg's ends, else the
-- subscription's pin), where that train has a booked departure (boards
-- list departures only), with the booked public departure as an instant.
-- No user id: like the other ingest_* views, the poller's role
-- (ldbws_ro) gets SELECT on this view only, never on the personal tables.
CREATE VIEW ingest_tracked_stop_targets AS
WITH stops AS (
    SELECT DISTINCT tr.id AS trains_id, tr.train_uid, tr.service_date, UPPER(TRIM(u.crs)) AS crs
    FROM train_subscriptions ts
    JOIN trains tr ON tr.id = ts.trains_id
    LEFT JOIN journey_legs jl ON jl.train_subscription_id = ts.id
    CROSS JOIN LATERAL (
        VALUES (ts.pin_origin_crs), (ts.pin_destination_crs),
               (jl.origin_crs), (jl.destination_crs)
    ) AS u(crs)
    WHERE u.crs IS NOT NULL
      AND tr.service_date >= CURRENT_DATE - 1
      AND tr.service_date <= CURRENT_DATE + 1
),
crosswalk AS (
    SELECT DISTINCT ON (tiploc) tiploc, UPPER(crs) AS crs FROM (
        SELECT tiploc, crs, 1 AS priority FROM tiploc_crs
        UNION ALL
        SELECT tiploc, crs, 2 AS priority FROM stanox_crs
    ) merged
    ORDER BY tiploc, priority
)
SELECT s.train_uid, s.service_date, s.crs,
       ((c.service_date + c.day_offset
         + CASE WHEN c.booked_arrival IS NOT NULL
                     AND c.booked_departure < c.booked_arrival THEN 1 ELSE 0 END
         + COALESCE(c.public_departure, c.booked_departure))
        AT TIME ZONE 'Europe/London') AS departs_at
FROM stops s
JOIN schedule_calling_points_full c
  ON c.service_date = s.service_date AND c.uid = s.train_uid
JOIN crosswalk x ON x.tiploc = UPPER(TRIM(c.tiploc)) AND x.crs = s.crs
LEFT JOIN train_current_state cs ON cs.trains_id = s.trains_id
WHERE c.booked_departure IS NOT NULL
  AND (cs.status IS NULL OR cs.status NOT IN ('completed', 'cancelled'));
