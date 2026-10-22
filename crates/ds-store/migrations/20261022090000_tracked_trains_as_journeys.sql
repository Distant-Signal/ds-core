SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Tracked trains as one-leg journeys, phase 1 (expand): DS-124,
-- docs/superpowers/specs/2026-10-21-tracked-trains-as-journeys-design.md §5.
--
-- 1. Every train_subscriptions row no journey leg uses (a train tracked on
--    its own: DS-93 set tracked_standalone on exactly those) becomes a
--    journey with one train leg bound to it, as "Track this train" makes
--    one. The subscription keeps its id, so /Train/{id} links, tickets,
--    shares and notification state stay as they are.
-- 2. Every group_trains share of a subscription that has a one-leg
--    journey of its own moves to group_journeys, onto that journey (the
--    oldest, if there are several). A share of a train only a multi-leg
--    journey uses stays in group_trains, which the api still reads.
--
-- Both are recorded (standalone_track_conversions, group_train_share_moves)
-- so scripts/rollback-tracked-trains-as-journeys.sql can undo them until
-- phase 3 drops the bookkeeping with tracked_standalone. Rows cascade with
-- their journey, subscription or group: a rollback only restores what
-- still exists.
--
-- Idempotent: step 1 only picks subscriptions with no leg, and a converted
-- one has one; step 2 only picks group_trains rows, and a moved one is
-- deleted; the tables are CREATE ... IF NOT EXISTS. So the whole file
-- reruns cleanly: phase 2 reruns it to catch rows an old binary wrote,
-- and the api's DB test runs it twice.
-- tracked_standalone is left as it was, for a code rollback; nothing reads
-- it from this release on, and phase 3 drops it.
-- Batched: 500 subscriptions per statement. Production on 2026-10-21 had
-- one subscription to convert and two shares to move.
-- -------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS standalone_track_conversions (
    journey_id            BIGINT PRIMARY KEY REFERENCES journeys(id) ON DELETE CASCADE,
    train_subscription_id BIGINT NOT NULL UNIQUE
                          REFERENCES train_subscriptions(id) ON DELETE CASCADE,
    converted_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS group_train_share_moves (
    group_id              TEXT NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    train_subscription_id BIGINT NOT NULL
                          REFERENCES train_subscriptions(id) ON DELETE CASCADE,
    journey_id            BIGINT NOT NULL REFERENCES journeys(id) ON DELETE CASCADE,
    -- The group_trains row's own values, restored by the rollback.
    added_by              TEXT NOT NULL,
    added_at              TIMESTAMPTZ NOT NULL,
    -- Whether this move made the group_journeys row (FALSE: the journey
    -- was already shared into the group), so the rollback removes only
    -- the shares it made.
    created_share         BOOLEAN NOT NULL,
    moved_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (group_id, train_subscription_id)
);
CREATE INDEX IF NOT EXISTS group_train_share_moves_journey_id ON group_train_share_moves (journey_id);
CREATE INDEX IF NOT EXISTS group_train_share_moves_train_subscription_id
    ON group_train_share_moves (train_subscription_id);

DO $$
DECLARE
    converted INT;
BEGIN
    LOOP
        -- The journey id is drawn before the insert, so the journey, its
        -- leg and the bookkeeping row agree by construction (RETURNING on
        -- an INSERT ... SELECT cannot echo the source row's id).
        WITH batch AS (
            SELECT nextval(pg_get_serial_sequence('journeys', 'id')) AS journey_id,
                   ts.id, ts.user_id, ts.custom_name, ts.tracked_at, ts.service_date,
                   COALESCE(ts.pin_origin_crs, tr.origin_crs) AS origin_crs,
                   COALESCE(ts.pin_destination_crs, tr.destination_crs) AS destination_crs
            FROM train_subscriptions ts
            LEFT JOIN trains tr ON tr.id = ts.trains_id
            WHERE NOT EXISTS (
                SELECT 1 FROM journey_legs jl WHERE jl.train_subscription_id = ts.id
            )
            ORDER BY ts.id
            LIMIT 500
        ), new_journeys AS (
            INSERT INTO journeys (id, user_id, custom_name, created_at, updated_at)
            SELECT journey_id, user_id, custom_name, tracked_at, tracked_at FROM batch
            RETURNING id
        ), new_legs AS (
            INSERT INTO journey_legs (journey_id, leg_order, origin_crs, destination_crs,
                                      service_date, train_subscription_id, match_mode)
            SELECT b.journey_id, 1, b.origin_crs, b.destination_crs, b.service_date, b.id,
                   'manual'
            FROM batch b JOIN new_journeys j ON j.id = b.journey_id
            RETURNING journey_id, train_subscription_id
        )
        INSERT INTO standalone_track_conversions (journey_id, train_subscription_id)
        SELECT journey_id, train_subscription_id FROM new_legs;
        GET DIAGNOSTICS converted = ROW_COUNT;
        EXIT WHEN converted = 0;
    END LOOP;
END $$;

-- Each share's journey: the oldest one-leg journey of the subscription's
-- owner whose one leg is bound to it.
WITH own AS (
    SELECT DISTINCT ON (jl.train_subscription_id)
           jl.train_subscription_id, jl.journey_id
    FROM journey_legs jl
    JOIN journeys j ON j.id = jl.journey_id
    JOIN train_subscriptions ts ON ts.id = jl.train_subscription_id AND ts.user_id = j.user_id
    WHERE NOT EXISTS (
        SELECT 1 FROM journey_legs other
        WHERE other.journey_id = jl.journey_id AND other.id <> jl.id
    )
    ORDER BY jl.train_subscription_id, jl.journey_id
), moving AS (
    SELECT gt.group_id, gt.train_subscription_id, own.journey_id, gt.added_by, gt.added_at,
           NOT EXISTS (
               SELECT 1 FROM group_journeys gj
               WHERE gj.group_id = gt.group_id AND gj.journey_id = own.journey_id
           ) AS created_share
    FROM group_trains gt
    JOIN own ON own.train_subscription_id = gt.train_subscription_id
), shared AS (
    INSERT INTO group_journeys (group_id, journey_id, added_by, added_at)
    SELECT group_id, journey_id, added_by, added_at FROM moving WHERE created_share
    ON CONFLICT (group_id, journey_id) DO NOTHING
), recorded AS (
    INSERT INTO group_train_share_moves
        (group_id, train_subscription_id, journey_id, added_by, added_at, created_share)
    SELECT group_id, train_subscription_id, journey_id, added_by, added_at, created_share
    FROM moving
    ON CONFLICT (group_id, train_subscription_id) DO NOTHING
)
DELETE FROM group_trains gt
USING moving m
WHERE gt.group_id = m.group_id AND gt.train_subscription_id = m.train_subscription_id;
