-- -------------------------------------------------------------------------
-- Data cleanup that must run immediately BEFORE
-- `20260925222000_trains_train_id_service_date_unique.sql`'s
-- `CREATE UNIQUE INDEX CONCURRENTLY trains_train_id_service_date` (this
-- file's version number is deliberately chosen to sort between that
-- migration and the one before it -- sqlx applies pending migrations in
-- ascending version order, so this always runs first). See that file's own
-- header comment for WHY this is a separate migration rather than folded
-- into it: `CREATE INDEX CONCURRENTLY` must be the only statement in its
-- migration file (Postgres refuses to run it at all otherwise, verified
-- empirically -- not merely "inside an explicit transaction"), so an
-- ordinary data cleanup that has to run first cannot live in that same
-- file.
--
-- WHAT THIS MIGRATION DOES AND WHY
-- ---------------------------------
-- If any two (or more) `trains` rows currently share one
-- `(train_id, service_date)` pair -- TRUST's own daily identifier plus the
-- service's date -- the next migration's unique index build fails at
-- deploy time with a duplicate-key violation, leaving an INVALID index
-- behind and the API unable to start until a human intervenes (see that
-- file's header for the full mechanics of that failure mode, and why it is
-- at least always a clean, retriable one rather than a stuck lock).
--
-- Rather than leaving that as a live landmine for the next deploy to step
-- on -- exactly what happened on 2026-09-25, whose corrupted row is
-- `scripts/remediate-2026-09-25-cross-matched-trains-row.sql`'s whole
-- reason to exist -- this migration finds and deletes EVERY row involved
-- in ANY such collision, unconditionally, as an ordinary part of the
-- deploy. This is a deliberate, accepted trade-off (already decided, not
-- re-litigated here): the data destroyed this way (live-TRUST movement
-- history, "where is it right now" snapshots, any in-flight notifier
-- signal, and -- unlike the narrower standalone remediation script's own
-- approach -- any schedule-match snapshot the doomed rows themselves
-- carried) is exactly the short-lived/historical class of data this app's
-- early stage already tolerates losing, and it is far simpler and more
-- robust than trying to determine, per collision, which of N rows is the
-- "real" one worth keeping. What this migration will NOT do, no matter how
-- a collision arose, is destroy anything a user would consider their own
-- data -- see "Preserving subscriptions" below.
--
-- Detecting the collision set (both statements below use the identical
-- `EXISTS` clause on purpose, so they always agree on exactly which rows
-- are affected -- see "Statement ordering and idempotency" below): a
-- `trains` row is affected if its own `(train_id, service_date)` pair
-- (with `train_id IS NOT NULL`) is shared by at least one OTHER `trains`
-- row. This is the direct, complete check -- NOT the narrower "TRUST
-- Activation contradicts this row's own claimed identity" signature the
-- standalone remediation script uses. That narrower signature is a good
-- fit for a human-reviewed, one-incident-at-a-time script (it explains
-- *why* a row is wrong, which a dry-run operator wants to see), but it
-- depends on `trust_event_backlog` still holding the relevant Activation
-- (which `20260905160000_trust_event_backlog.sql` retains for only 1 day)
-- and would miss any collision whose corrupting Activation has already
-- aged out by the time this migration runs. The direct pairwise check has
-- no such dependency and is exactly equivalent to what the next
-- migration's unique index itself will enforce, so nothing it would have
-- blocked can be missed -- and, using `EXISTS` rather than a fixed
-- "exactly 2 rows" shape, it correctly handles a collision group of ANY
-- size (3+ rows sharing one pair), deleting every member, not just a
-- pair's worth.
--
-- Preserving subscriptions: every `train_subscriptions` row pointing at a
-- `trains` row about to be deleted is detached FIRST (`trains_id` set to
-- `NULL`, `resolution_status` recomputed -- see below), by the `UPDATE`
-- below, before the `DELETE` runs. The subscription row itself is NEVER
-- deleted, so everything hanging off it survives untouched:
-- `tracked_train_tickets`, `custom_name`, and `group_trains` shares all key
-- off `train_subscriptions.id`, never `trains.id` (confirmed by grepping
-- every migration in this directory for `REFERENCES trains` -- see the "FK
-- fan-out" section below). The subscription is left exactly where a
-- brand-new, never-yet-matched pin starts out, so it is naturally picked
-- up again by the already-patched schedule-match / backlog-match sweeps
-- and can re-resolve on its own.
--
-- `resolution_status` recomputation: a detached subscription's
-- `resolution_status` must never regress to something stale (a bare
-- `trains_id = NULL` with `resolution_status` still reading
-- `'schedule_matched'`/`'resolved'` would be a lie -- there is no longer
-- any `trains` row backing that claim). The standalone remediation script
-- handles this differently, by checking the row's own `schedule_matched_at`
-- and setting `'schedule_matched'` when it's non-NULL -- because THAT
-- script never deletes the row, so the schedule-match snapshot is still
-- sitting right there afterwards and `'schedule_matched'` stays an honest
-- status. This migration cannot do that: the row (and any schedule-match
-- snapshot on it) is being deleted outright, so by the time the
-- subscription is next read there is nothing left to justify anything past
-- `'pending'` -- checking `schedule_matched_at` before deleting would not
-- change that conclusion, so this migration doesn't bother reading it. So
-- every detached subscription's `resolution_status` is recomputed as:
--   * `'unresolved'` stays `'unresolved'` -- this codebase's own
--     established convention (see the standalone script's Step 3, and
--     `mark_subscription_unresolved_on_cancellation`'s doc comment in
--     `crates/api/src/data/train_tracking.rs`) is that `'unresolved'` is a
--     considered terminal state ("nothing further any sweep can do for it
--     either way") that nothing here has a basis to move away from.
--   * every other status (`'pending'`, `'schedule_matched'`, `'resolved'`)
--     resets to `'pending'` -- the same starting waypoint `create_pin`/a
--     brand-new subscription begins in, with no `trains_id` and no
--     schedule-match snapshot, which is an honest description of what's
--     left once the doomed row is gone. This puts the subscription back in
--     front of the periodic schedule-match / backlog-match sweeps
--     (`list_pending_pins_for_schedule_match`, `list_pending_pins_for_backlog_match`,
--     both keyed on `resolution_status = 'pending' AND trains_id IS NULL`),
--     so it can re-resolve on its own -- the same "left exactly where the
--     already-fixed matching code can correctly re-resolve it" intent the
--     standalone script's own header describes, just one waypoint further
--     back, since there is no surviving snapshot to fast-forward it to.
--
-- FK fan-out from the `DELETE FROM trains` below, confirmed against every
-- migration in this directory that adds a `REFERENCES trains(id)` column:
--   * `train_subscriptions.trains_id` -- `ON DELETE SET NULL`
--     (`20260906100000_trains.sql`). Explicitly reset by the `UPDATE`
--     below anyway (so `resolution_status` can be recomputed alongside it
--     in the same statement) -- this FK's own cascade is now a redundant,
--     harmless no-op safety net for that column, not the primary
--     mechanism.
--   * `train_movement_events.trains_id` / `train_current_state.trains_id`
--     -- both `ON DELETE CASCADE` (`20260906110000_train_movement_trains_id.sql`).
--     Exactly the movement-history / "where is it now" data this
--     migration accepts losing for a doomed row -- no explicit handling
--     needed.
--   * `notifier_forward_queue.trains_id` -- `NOT NULL ... ON DELETE
--     CASCADE` (`20260907090000_notifier_forward_queue.sql`). Per that
--     table's own doc comment it is "a forwarding SIGNAL, not a data
--     store" -- losing an in-flight signal for a doomed row is the same
--     acceptable loss the standalone script's own header already argues
--     for the identical table.
--   * No other table's migration adds a `REFERENCES trains(id)` column --
--     `group_trains` and `tracked_train_tickets` both key off
--     `train_subscriptions.id` (formerly `tracked_trains.id`), never
--     `trains.id`, so they are untouched by this delete regardless of
--     which `train_subscriptions` rows it detaches.
--
-- Statement ordering and idempotency: this is an ordinary transactional
-- migration (no `-- no-transaction` marker), so sqlx wraps both statements
-- below in ONE transaction together with this migration's own
-- `_sqlx_migrations` bookkeeping row -- either both the `UPDATE` and the
-- `DELETE` take effect and this migration is marked applied, or (on any
-- error, including a connection loss mid-way) neither does and nothing is
-- recorded, so a retry starts from the same, unmodified pre-migration
-- state. The `UPDATE` still runs first regardless, for readability and
-- because it must observe the doomed rows' `trains_id` values (and every
-- subscription pointing at one) before the `DELETE` removes them.
-- Re-running this migration's own statements a second time against a
-- database that already converged (whether via a hypothetical repeat
-- application, or because there was nothing to collide with in the first
-- place -- e.g. a fresh, empty database migrated straight through from
-- nothing) is also a clean no-op: the `WHERE`/`EXISTS` clauses in both
-- statements require an actual collision to match anything, and once the
-- first pass has deleted every colliding row there is nothing left for
-- either statement to touch.
-- -------------------------------------------------------------------------

-- Detach every `train_subscriptions` row pointing at a `trains` row that is
-- about to be deleted below, BEFORE it is deleted -- see the
-- "resolution_status recomputation" section above for exactly why the
-- CASE expression is shaped this way.
UPDATE train_subscriptions ts
SET trains_id = NULL,
    resolution_status = CASE
        WHEN ts.resolution_status = 'unresolved' THEN ts.resolution_status
        ELSE 'pending'
    END
WHERE ts.trains_id IN (
    SELECT t.id
    FROM trains t
    WHERE t.train_id IS NOT NULL
      AND EXISTS (
          SELECT 1
          FROM trains t2
          WHERE t2.train_id = t.train_id
            AND t2.service_date = t.service_date
            AND t2.id <> t.id
      )
);

-- Delete every `trains` row involved in ANY `(train_id, service_date)`
-- collision -- both/all sides, not merely one survivor per group. Every
-- `train_subscriptions` row that pointed at one has already been detached
-- by the `UPDATE` above; every other FK referencing `trains.id`
-- (`train_movement_events`, `train_current_state`,
-- `notifier_forward_queue`) cascades per its own `ON DELETE CASCADE`,
-- discarding only the short-lived movement/current-state/signal data this
-- migration's header comment already accepts losing.
DELETE FROM trains t
WHERE t.train_id IS NOT NULL
  AND EXISTS (
      SELECT 1
      FROM trains t2
      WHERE t2.train_id = t.train_id
        AND t2.service_date = t.service_date
        AND t2.id <> t.id
  );
