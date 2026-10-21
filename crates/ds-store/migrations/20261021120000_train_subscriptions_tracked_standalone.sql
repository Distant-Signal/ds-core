SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Whether the user tracked a subscription on its own (DS-93).
--
-- One `train_subscriptions` row can serve both a journey leg and a train
-- the user tracked on its own: `create_subscription_for_train` is
-- idempotent per (user_id, trains_id), so "Track this train" on a leg's
-- train hands back the leg's row, and vice versa. Nothing recorded which,
-- so "Change train" could not tell a leg-only subscription (to drop with
-- the leg's old pick) from one the user also tracks (to keep): it disabled
-- every unreferenced old row, which left a leg-only one in `GET
-- /Train/mine` as a silent standalone train and silenced a really
-- standalone one.
--
-- TRUE once the standalone track routes (`POST /Train`, `POST
-- /Train/known`) create or reuse the row; never cleared by a journey path.
-- An ADD COLUMN with a constant default is catalog-only: no rewrite.
--
-- Backfill: a row no journey leg references is, today, a train tracked on
-- its own, except one "Change train" disabled (the only writer of
-- notifications_enabled = FALSE): that one is the leftover this fixes.
-- Those are deleted when they carry nothing the user attached (no ticket,
-- no group share); any with a ticket or share is left as it was. Both
-- statements only touch rows they have not already handled, so a rerun is
-- a no-op. Production on 2026-10-21 had 24 subscriptions, none disabled,
-- one unreferenced.
-- -------------------------------------------------------------------------
ALTER TABLE train_subscriptions
    ADD COLUMN tracked_standalone BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE train_subscriptions ts
SET tracked_standalone = TRUE
WHERE NOT ts.tracked_standalone
  AND ts.notifications_enabled
  AND NOT EXISTS (
      SELECT 1 FROM journey_legs jl WHERE jl.train_subscription_id = ts.id
  );

DELETE FROM train_subscriptions ts
WHERE NOT ts.notifications_enabled
  AND NOT ts.tracked_standalone
  AND NOT EXISTS (
      SELECT 1 FROM journey_legs jl WHERE jl.train_subscription_id = ts.id
  )
  AND NOT EXISTS (
      SELECT 1 FROM tracked_train_tickets t WHERE t.tracked_train_id = ts.id
  )
  AND NOT EXISTS (
      SELECT 1 FROM group_trains g WHERE g.train_subscription_id = ts.id
  );
