SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Revision 1 again, as a data-only catch-up
-- (docs/superpowers/specs/2026-10-22-mid-journey-changes-design.md §10.2,
-- Phase 2): a journey an older api pod created during the phase 1 rollout
-- has no revision, and the break sweep turned on with this release reads
-- them. Exactly the statement of 20261023090500 (it skips every journey
-- that already has a revision), so it only fills the gap the rollout left.
-- Row locks on the new tables only.
-- -------------------------------------------------------------------------

DO $$
DECLARE
    written INT;
BEGIN
    LOOP
        WITH batch AS (
            SELECT nextval(pg_get_serial_sequence('journey_revisions', 'id')) AS revision_id,
                   j.id AS journey_id
            FROM journeys j
            WHERE NOT EXISTS (SELECT 1 FROM journey_revisions r WHERE r.journey_id = j.id)
              AND EXISTS (
                  SELECT 1 FROM journey_legs jl
                  WHERE jl.journey_id = j.id
                    AND jl.service_date >= (now() AT TIME ZONE 'Europe/London')::date - 1
              )
            ORDER BY j.id
            LIMIT 500
        ), revisions AS (
            INSERT INTO journey_revisions
                (id, journey_id, revision_no, reason, actor, is_compensation_baseline)
            SELECT revision_id, journey_id, 1, 'booked', 'system', TRUE FROM batch
            RETURNING id, journey_id
        )
        INSERT INTO journey_revision_legs
            (revision_id, position, source_leg_id, kind, train_uid, service_date, trains_id,
             atoc_code, origin_crs, destination_crs, booked_departure, booked_arrival,
             transit_mode, origin_place, destination_place, line_name,
             planned_departure, planned_arrival, ticket_ids)
        SELECT r.id,
               ROW_NUMBER() OVER (PARTITION BY r.id ORDER BY leg.leg_order)::INT,
               leg.leg_id, leg.kind, leg.train_uid, leg.service_date, leg.trains_id,
               leg.atoc_code, leg.origin_crs, leg.destination_crs,
               leg.booked_departure, leg.booked_arrival,
               leg.transit_mode, leg.origin_place, leg.destination_place, leg.line_name,
               leg.planned_departure, leg.planned_arrival, leg.ticket_ids
        FROM revisions r
        CROSS JOIN LATERAL (
            SELECT jl.id AS leg_id, jl.leg_order, jl.kind, tr.train_uid, jl.service_date,
                   ts.trains_id,
                   (SELECT MIN(sdd.operator_atoc) FROM schedule_destination_departures sdd
                     WHERE sdd.service_date = tr.service_date AND sdd.train_uid = tr.train_uid
                   ) AS atoc_code,
                   ends.origin_crs, ends.destination_crs,
                   COALESCE(
                       (SELECT ((cp.service_date + cp.day_offset::INT)
                                + COALESCE(cp.public_departure, cp.booked_departure))
                               AT TIME ZONE 'Europe/London'
                          FROM schedule_calling_points_full cp
                         WHERE cp.service_date = tr.service_date AND cp.uid = tr.train_uid
                           AND COALESCE(cp.public_departure, cp.booked_departure) IS NOT NULL
                           AND UPPER(TRIM(cp.tiploc)) IN (
                               SELECT UPPER(TRIM(tc.tiploc)) FROM tiploc_crs tc
                                WHERE UPPER(tc.crs) = ends.origin_crs
                               UNION
                               SELECT UPPER(TRIM(sc.tiploc)) FROM stanox_crs sc
                                WHERE UPPER(sc.crs) = ends.origin_crs)
                         ORDER BY cp.seq LIMIT 1),
                       ts.pin_scheduled_departure
                   ) AS booked_departure,
                   (SELECT ((cp.service_date + cp.day_offset::INT)
                            + COALESCE(cp.public_arrival, cp.booked_arrival))
                           AT TIME ZONE 'Europe/London'
                      FROM schedule_calling_points_full cp
                     WHERE cp.service_date = tr.service_date AND cp.uid = tr.train_uid
                       AND COALESCE(cp.public_arrival, cp.booked_arrival) IS NOT NULL
                       AND UPPER(TRIM(cp.tiploc)) IN (
                           SELECT UPPER(TRIM(tc.tiploc)) FROM tiploc_crs tc
                            WHERE UPPER(tc.crs) = ends.destination_crs
                           UNION
                           SELECT UPPER(TRIM(sc.tiploc)) FROM stanox_crs sc
                            WHERE UPPER(sc.crs) = ends.destination_crs)
                     ORDER BY cp.seq DESC LIMIT 1
                   ) AS booked_arrival,
                   jl.transit_mode, jl.origin_place, jl.destination_place, jl.line_name,
                   jl.planned_departure, jl.planned_arrival,
                   COALESCE(
                       (SELECT array_agg(t.id ORDER BY t.id) FROM tracked_train_tickets t
                         WHERE t.tracked_train_id = jl.train_subscription_id),
                       '{}'
                   ) AS ticket_ids
            FROM journey_legs jl
            LEFT JOIN train_subscriptions ts ON ts.id = jl.train_subscription_id
            LEFT JOIN trains tr ON tr.id = ts.trains_id
            CROSS JOIN LATERAL (
                SELECT UPPER(COALESCE(jl.origin_crs, ts.pin_origin_crs, tr.origin_crs)) AS origin_crs,
                       UPPER(COALESCE(jl.destination_crs, ts.pin_destination_crs,
                                      tr.destination_crs)) AS destination_crs
            ) ends
            WHERE jl.journey_id = r.journey_id
        ) leg;
        GET DIAGNOSTICS written = ROW_COUNT;
        -- Every journey in a batch has a leg, so an empty batch writes none.
        EXIT WHEN written = 0;
    END LOOP;
END $$;
