-- -------------------------------------------------------------------------
-- Journey tracking, Phase 1 schema.
-- docs/superpowers/specs/2026-09-22-journey-tracking-design.md §1.1, with
-- one correction the spec's own §7.1 makes to its §1.1 sketch: origin_crs/
-- destination_crs are NULLABLE here, not NOT NULL. An NR-primary-created
-- leg (a `knownTrain`-mode POST /Journeys, crates/api/src/routes/journeys.rs)
-- whose underlying `trains` row has no schedule data yet can legitimately
-- have neither -- the same accepted gap `train_subscriptions.pin_origin_crs`/
-- `pin_destination_crs` already model (see
-- 20260906130000_nullable_pin_columns.sql for that same correction, made
-- for the same underlying reason, on the table this one wraps).
--
-- Additive only -- train_subscriptions itself is UNCHANGED by this
-- migration or anything in this plan: every existing route, the notifier,
-- tickets, and group_trains sharing keep working byte-for-byte regardless
-- of this feature.
-- -------------------------------------------------------------------------

CREATE TABLE journeys (
    id          BIGSERIAL PRIMARY KEY,
    user_id     TEXT NOT NULL REFERENCES users(id),
    custom_name TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX journeys_user_id ON journeys (user_id);

CREATE TABLE journey_legs (
    id                     BIGSERIAL PRIMARY KEY,
    journey_id             BIGINT NOT NULL REFERENCES journeys(id) ON DELETE CASCADE,
    -- 1-based sequence within the journey. Phase 1 never writes anything
    -- but 1 -- see crates/api/src/data/journeys.rs's own module doc
    -- comment -- but the column exists now so Phase 2's "add a leg" work
    -- (design doc §3) needs no schema change, only a new writer.
    leg_order              INT NOT NULL,
    -- Nullable: see this migration's own header comment above. This is the
    -- leg's OWN travel intent, kept even once matched -- not merely
    -- derived from whatever train ends up bound to it (design doc §1.1).
    origin_crs              TEXT,
    destination_crs         TEXT,
    service_date             DATE NOT NULL,
    -- Time-window search intent (design doc §1.1, §2.3's 2026-09-22
    -- addendum). NULL for a leg created by picking a specific known train
    -- directly (a `pin` or `knownTrain` mode leg -- no window was ever
    -- searched). Once set, kept FOREVER, even after train_subscription_id
    -- is populated -- see that addendum: a matched leg's window stays live
    -- so "Change train" can re-open the exact same candidate search
    -- without the user re-entering criteria.
    depart_after             TIME,
    depart_before             TIME,
    arrive_after              TIME,
    arrive_before              TIME,
    -- Binding to a real train working, reusing 100% of existing
    -- train_subscriptions/trains/notifier/journey.rs machinery. ON DELETE
    -- SET NULL: deleting the underlying tracked train (existing, unchanged
    -- DELETE /Train/{trackingId}) orphans the leg rather than cascading
    -- into deleting the journey itself.
    train_subscription_id      BIGINT REFERENCES train_subscriptions(id) ON DELETE SET NULL,
    match_mode                 TEXT NOT NULL DEFAULT 'unmatched'
                                CHECK (match_mode IN ('unmatched', 'manual', 'auto')),
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (journey_id, leg_order)
);
CREATE INDEX journey_legs_journey_id ON journey_legs (journey_id);
CREATE INDEX journey_legs_train_subscription_id ON journey_legs (train_subscription_id);
