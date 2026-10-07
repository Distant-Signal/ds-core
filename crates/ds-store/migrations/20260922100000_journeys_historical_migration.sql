-- -------------------------------------------------------------------------
-- One-time backfill: every EXISTING train_subscriptions row becomes its
-- own single-leg journey (design doc §7.1) -- strictly 1:1, never merged,
-- even for a user with many tracked trains (requirement #6b is explicit:
-- EACH pre-existing tracked train becomes its OWN journey).
--
-- Correlating each new `journeys` row back to the `train_subscriptions`
-- row it came from cannot use RETURNING alone: a RETURNING clause on an
-- INSERT ... SELECT only ever exposes columns of the row just inserted
-- into `journeys` itself -- there is no way to echo an arbitrary source
-- column (train_subscriptions.id) back out of it. Joining back afterwards
-- on (user_id, tracked_at) is unsafe too -- not unique (two subscriptions
-- for the same user created in the same instant are a real, if rare,
-- case) -- and the design doc's own §7.1 explicitly warns this off.
--
-- This uses a temporary scratch column instead: add it, populate it
-- alongside the real columns in the SAME INSERT (so `journeys.id` and
-- `train_subscriptions.id` are correlated by construction -- a real column
-- value copied verbatim, not by insertion-order assumption or a second,
-- fallible lookup), use it to drive the journey_legs INSERT, then drop it
-- -- all inside this one migration, so `journeys`' final, permanent shape
-- carries no trace of this bookkeeping.
-- -------------------------------------------------------------------------

ALTER TABLE journeys ADD COLUMN _migration_source_subscription_id BIGINT;

INSERT INTO journeys (user_id, custom_name, created_at, updated_at, _migration_source_subscription_id)
SELECT user_id, custom_name, tracked_at, tracked_at, id
FROM train_subscriptions;

-- Every migrated leg is already bound to a real train_subscriptions row --
-- there is no window to re-open, so match_mode = 'manual' and every
-- depart_*/arrive_* column stays NULL (design doc §7.1's own explicit
-- statement of this).
INSERT INTO journey_legs (journey_id, leg_order, origin_crs, destination_crs, service_date, train_subscription_id, match_mode)
SELECT j.id, 1, ts.pin_origin_crs, ts.pin_destination_crs, ts.service_date, ts.id, 'manual'
FROM journeys j
JOIN train_subscriptions ts ON ts.id = j._migration_source_subscription_id;

ALTER TABLE journeys DROP COLUMN _migration_source_subscription_id;
