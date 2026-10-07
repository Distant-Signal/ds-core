-- -------------------------------------------------------------------------
-- Shared Train Identity, Step A (expand): a train becomes a shared, public
-- entity keyed by real-world identity (train_uid, service_date) rather than
-- by whichever user pinned it first. See
-- docs/superpowers/specs/2026-09-06-shared-train-identity-design.md §1-2.
--
-- This migration is purely additive: it creates the new `trains` table and
-- adds a nullable FK column to the existing `tracked_trains` table. No
-- existing column is touched, no existing read path changes behavior.
-- -------------------------------------------------------------------------

CREATE TABLE trains (
    id                  BIGSERIAL PRIMARY KEY,
    train_uid           TEXT NOT NULL,
    service_date        DATE NOT NULL,

    -- Schedule-derived (mirrors tracked_trains' own schedule-match columns,
    -- 20260905150000_schedule_matched_resolution.sql).
    origin_crs          TEXT,
    scheduled_departure TIMESTAMPTZ,
    destination_crs     TEXT,
    calling_points      JSONB,
    matched_line_id     TEXT,
    schedule_matched_at TIMESTAMPTZ,

    -- Live-TRUST-derived. train_id is TRUST's own daily identifier string --
    -- NOT the same concept as this table's own surrogate `id` column (see
    -- this plan's Global Constraints on the trains_id naming convention).
    train_id            TEXT,
    resolved_at         TIMESTAMPTZ,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (train_uid, service_date)
);

ALTER TABLE tracked_trains
    ADD COLUMN trains_id BIGINT REFERENCES trains(id) ON DELETE SET NULL;

CREATE INDEX tracked_trains_trains_id ON tracked_trains (trains_id);

-- New product surface, schema-only in this migration: no per-subscription
-- mute/opt-in flag exists anywhere in this schema today. Wiring this into
-- notifier's send decision needs explicit product-owner sign-off before it
-- ships (see the design spec's §1) -- this column is added now only so the
-- Step D rename (a later task) doesn't need its own separate migration for
-- it. DEFAULT TRUE preserves today's implicit "every subscription notifies"
-- behavior for every existing and new row.
ALTER TABLE tracked_trains
    ADD COLUMN notifications_enabled BOOLEAN NOT NULL DEFAULT TRUE;
