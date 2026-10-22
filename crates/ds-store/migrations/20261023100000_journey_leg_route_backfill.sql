SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Copy each train's route onto the legs and subscriptions saved without
-- one (DS-144).
--
-- A subscription made for a train whose `trains` row the route had only
-- just minted by UID copied that bare row's NULL route into its pins, and
-- the journey leg copied the pins. Nothing rewrote either once the
-- timetable reached the row, so the notifier's push text and leg checks
-- saw no route and the web app titled such a journey "Unknown station".
-- From this release on, every writer of a train's route fills them on the
-- same transaction (`ds_store::trains::fill_route_into_subscribers`); this
-- fills the rows written before it, the same way:
--
-- * a train leg's NULL `origin_crs`/`destination_crs` from its
--   subscription's pin, else its train's route;
-- * a subscription's NULL `pin_origin_crs`/`pin_destination_crs`/
--   `pin_scheduled_departure` from its train's route.
--
-- Never overwrites a value already set. Idempotent: a row is touched only
-- while it has a NULL that its train can fill, and filling it ends that.
-- Legs first, so each reads its pin as it was. Batched 500 rows a
-- statement; row locks on the rows changed only. On 2026-10-22 production
-- had 17 train legs with a NULL end, 1 of them with a route to copy (the
-- rest have no train, or a train with no route yet), and 8 subscriptions
-- to fill, so this is one batch each.
-- -------------------------------------------------------------------------

DO $$
DECLARE
    written INT;
BEGIN
    LOOP
        WITH batch AS (
            SELECT jl.id,
                   COALESCE(jl.origin_crs, ts.pin_origin_crs, tr.origin_crs) AS origin_crs,
                   COALESCE(jl.destination_crs, ts.pin_destination_crs, tr.destination_crs)
                       AS destination_crs
            FROM journey_legs jl
            JOIN train_subscriptions ts ON ts.id = jl.train_subscription_id
            JOIN trains tr ON tr.id = ts.trains_id
            WHERE jl.kind = 'train'
              AND ((jl.origin_crs IS NULL
                    AND COALESCE(ts.pin_origin_crs, tr.origin_crs) IS NOT NULL)
                OR (jl.destination_crs IS NULL
                    AND COALESCE(ts.pin_destination_crs, tr.destination_crs) IS NOT NULL))
            ORDER BY jl.id
            LIMIT 500
        )
        UPDATE journey_legs jl
        SET origin_crs = batch.origin_crs,
            destination_crs = batch.destination_crs
        FROM batch
        WHERE jl.id = batch.id;
        GET DIAGNOSTICS written = ROW_COUNT;
        EXIT WHEN written = 0;
    END LOOP;

    LOOP
        WITH batch AS (
            SELECT ts.id, tr.origin_crs, tr.destination_crs, tr.scheduled_departure
            FROM train_subscriptions ts
            JOIN trains tr ON tr.id = ts.trains_id
            WHERE (ts.pin_origin_crs IS NULL AND tr.origin_crs IS NOT NULL)
               OR (ts.pin_destination_crs IS NULL AND tr.destination_crs IS NOT NULL)
               OR (ts.pin_scheduled_departure IS NULL AND tr.scheduled_departure IS NOT NULL)
            ORDER BY ts.id
            LIMIT 500
        )
        UPDATE train_subscriptions ts
        SET pin_origin_crs = COALESCE(ts.pin_origin_crs, batch.origin_crs),
            pin_destination_crs = COALESCE(ts.pin_destination_crs, batch.destination_crs),
            pin_scheduled_departure = COALESCE(ts.pin_scheduled_departure,
                                               batch.scheduled_departure)
        FROM batch
        WHERE ts.id = batch.id;
        GET DIAGNOSTICS written = ROW_COUNT;
        EXIT WHEN written = 0;
    END LOOP;
END
$$;
