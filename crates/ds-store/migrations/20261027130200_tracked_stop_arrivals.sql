SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Arrival boards for tracked trains' alighting stops, pre-departure
-- prediction phase 3 (DS-152,
-- docs/superpowers/specs/2026-10-21-pre-departure-prediction-design.md
-- section 6.2 item 3, "Arrival boards"). Additive: one table, one view.
--
-- Departure boards list departures only, so a stop where the train
-- terminates or only sets down (usually the user's destination) never gets
-- a live platform from them. poller-ldbws's tracked-stop lane asks the
-- LDBWS arrivals product for those stops' boards too, narrowed to the
-- train's booked arrival, and ingest-writer applies them here (schema
-- tracked-stop-arrival-boards/1, on ds:ingest:station-samples).
-- -------------------------------------------------------------------------

-- The latest arrival board per tracked train and alighting station, like
-- tracked_stop_boards. The aggregator deletes rows a day past their poll.
CREATE TABLE tracked_stop_arrival_boards (
    train_uid    TEXT        NOT NULL,
    service_date DATE        NOT NULL,
    crs          TEXT        NOT NULL,
    polled_at    TIMESTAMPTZ NOT NULL,
    arrivals     JSONB       NOT NULL,
    PRIMARY KEY (train_uid, service_date, crs)
);

CREATE INDEX tracked_stop_arrival_boards_polled_at ON tracked_stop_arrival_boards (polled_at);

COMMENT ON TABLE tracked_stop_arrival_boards IS
    'Latest LDBWS arrival board per tracked train and alighting stop (DS-152).';

-- What the arrivals lane polls: every station a user alights at on a
-- tracked train (a journey leg's destination, else the subscription's pin
-- destination) where that train has a booked arrival, with the booked
-- public arrival (else the working one) as an instant. No user id: like
-- the other ingest_* views, poller-ldbws's role (ldbws_ro) gets SELECT on
-- this view only, never on the personal tables.
CREATE VIEW ingest_tracked_stop_arrival_targets AS
WITH stops AS (
    SELECT DISTINCT tr.id AS trains_id, tr.train_uid, tr.service_date, UPPER(TRIM(u.crs)) AS crs
    FROM train_subscriptions ts
    JOIN trains tr ON tr.id = ts.trains_id
    LEFT JOIN journey_legs jl ON jl.train_subscription_id = ts.id
    CROSS JOIN LATERAL (
        VALUES (ts.pin_destination_crs), (jl.destination_crs)
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
       ((c.service_date + c.day_offset + COALESCE(c.public_arrival, c.booked_arrival))
        AT TIME ZONE 'Europe/London') AS arrives_at
FROM stops s
JOIN schedule_calling_points_full c
  ON c.service_date = s.service_date AND c.uid = s.train_uid
JOIN crosswalk x ON x.tiploc = UPPER(TRIM(c.tiploc)) AND x.crs = s.crs
LEFT JOIN train_current_state cs ON cs.trains_id = s.trains_id
WHERE c.booked_arrival IS NOT NULL
  AND (cs.status IS NULL OR cs.status NOT IN ('completed', 'cancelled'));
