-- -------------------------------------------------------------------------
-- Shared Train Identity, Step D (re-point), part 1 -- schema only. See
-- docs/superpowers/specs/2026-09-06-shared-train-identity-design.md §2
-- Step D.
-- -------------------------------------------------------------------------

-- train_movement_events: purely additive. The existing NOT NULL
-- tracked_train_id column and its UNIQUE (tracked_train_id, dedup_key)
-- constraint are untouched -- both the old and the new dedup constraint
-- coexist until Task 22's separately-gated final drop removes
-- tracked_train_id entirely.
ALTER TABLE train_movement_events
    ADD COLUMN trains_id BIGINT REFERENCES trains(id) ON DELETE CASCADE;

CREATE UNIQUE INDEX train_movement_events_trains_id_dedup
    ON train_movement_events (trains_id, dedup_key)
    WHERE trains_id IS NOT NULL;

CREATE INDEX train_movement_events_trains_id
    ON train_movement_events (trains_id, received_at)
    WHERE trains_id IS NOT NULL;

-- train_current_state: a genuine PK restructuring, not merely an additive
-- column. Today's PK, tracked_train_id, is NOT NULL by construction (every
-- PRIMARY KEY is) -- but this design's whole point is a row that can exist
-- for a trains_id with ZERO subscribers (the design spec's own "today,
-- train_current_state literally cannot answer 'where is this train' for a
-- train nobody has pinned" framing), which requires inserting a row with
-- no tracked_train_id value at all. A NOT NULL PK can never accommodate
-- that, so tracked_train_id stops being this table's PK here; a new
-- surrogate `id` column takes over, and tracked_train_id becomes a plain
-- nullable column, still unique when present (via its own partial index),
-- so any code not yet migrated to the trains_id path keeps working
-- unchanged against it.
--
-- Deliberate, reasoned deviation from this design spec's own §1 text
-- ("trains_id BIGINT PRIMARY KEY"), reconciled in favor of this plan's own
-- binding Global Constraint ("Step D's final schema keeps ...
-- train_current_state.trains_id nullable ... must not force a NOT
-- NULL/hard failure"): a PRIMARY KEY is always NOT NULL, and Step B's own
-- named edge case (a row with no natural train_uid key to backfill by,
-- confirmed and sized by Task 6) means trains_id can never be guaranteed
-- non-null for every row this table will ever hold. A partial UNIQUE index
-- (WHERE trains_id IS NOT NULL) delivers the same "one row per physical
-- train" guarantee for every row that DOES have one, without requiring the
-- column to be total.
--
-- `IF EXISTS`/explicit-name rather than assumed -- same defensive posture
-- 20260905150000_schedule_matched_resolution.sql already took for an
-- auto-generated constraint name: Postgres names an inline
-- `PRIMARY KEY` constraint `{table}_pkey` by default, confirmed against
-- this table's own original, un-named `tracked_train_id BIGINT PRIMARY KEY`
-- declaration (20260828120000_train_tracking.sql).
ALTER TABLE train_current_state ADD COLUMN id BIGSERIAL;
ALTER TABLE train_current_state DROP CONSTRAINT IF EXISTS train_current_state_pkey;
ALTER TABLE train_current_state ADD PRIMARY KEY (id);
ALTER TABLE train_current_state ALTER COLUMN tracked_train_id DROP NOT NULL;
CREATE UNIQUE INDEX train_current_state_tracked_train_id
    ON train_current_state (tracked_train_id)
    WHERE tracked_train_id IS NOT NULL;

ALTER TABLE train_current_state
    ADD COLUMN trains_id BIGINT REFERENCES trains(id) ON DELETE CASCADE;

CREATE UNIQUE INDEX train_current_state_trains_id
    ON train_current_state (trains_id)
    WHERE trains_id IS NOT NULL;
