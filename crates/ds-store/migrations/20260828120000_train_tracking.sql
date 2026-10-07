-- -------------------------------------------------------------------------
-- Individual train tracking: user-pinned (train_uid, date) journeys,
-- sourced from Network Rail's TRUST movement feed via trust-consumer.
-- See docs/superpowers/specs/2026-08-28-train-tracking-design.md and
-- docs/superpowers/plans/2026-08-28-train-tracking.md.
--
-- IMPORTANT: this migration depends on the `users` table from
-- docs/superpowers/plans/2026-08-28-user-accounts-sso.md's Task 1
-- (crates/api/migrations/20260828090000_user_accounts.sql), which must
-- apply first -- see the note at the top of
-- docs/superpowers/plans/2026-08-28-train-tracking.md. Unlike
-- custom_lines/pinned_lines/pinned_stations (which predate any user model
-- and needed a nullable-or-truncate ownership retrofit once accounts were
-- added), tracked_trains has no migration applied anywhere as of the
-- account-system design's writing, so it ships with a NOT NULL owner from
-- birth -- see that design doc's Data model section.
--
-- Three tables, mirroring the design doc's "What gets stored":
--   tracked_trains       -- one row per pin, owned by the user who created
--                           it (see user_id below). Starts 'pending'
--                           (train_uid unknown -- all we have is what the
--                           user was looking at on a departure board) and
--                           moves to 'resolved' once trust-consumer binds
--                           it to a TRUST Activation, or 'unresolved' if no
--                           Activation is ever matched to it.
--   train_movement_events -- immutable, append-only event log. One row per
--                           TRUST message matched to a resolved tracked
--                           train. dedup_key + the UNIQUE constraint below
--                           is what makes at-least-once Kafka delivery safe
--                           to write blindly (INSERT ... ON CONFLICT DO
--                           NOTHING).
--   train_current_state   -- denormalized "where is it right now" row,
--                           mirroring line_status being a materialized
--                           table the aggregator writes rather than
--                           something recomputed per request (DESIGN.md
--                           §4). One row per tracked_trains row, upserted
--                           on every event.
-- -------------------------------------------------------------------------

CREATE TABLE tracked_trains (
    id BIGSERIAL PRIMARY KEY,

    -- The user who created this pin (see Task 3). NOT NULL from birth --
    -- unlike custom_lines/pinned_lines/pinned_stations, this table has no
    -- pre-existing unowned rows to accommodate. Reads stay public (Task
    -- 5's GET routes are unscoped, matching a shareable-tracking-link
    -- posture -- see that task's note); only creation is owned.
    -- TEXT, not BIGINT/UUID: docs/superpowers/plans/2026-08-28-user-accounts-sso.md's
    -- Task 1 defines users.id as the bare OIDC `sub` claim stored verbatim
    -- (a TEXT primary key, matching this schema's existing natural-key
    -- convention -- incidents.incident_id, custom_lines.id, stations.crs).
    -- Keep this column's type in sync with that table if it ever changes.
    user_id TEXT NOT NULL REFERENCES users(id),

    -- Pin-time criteria: what the user was actually looking at. origin_crs
    -- + scheduled_departure + service_date is the best-effort key
    -- trust-consumer resolves against incoming Activation/Movement
    -- messages (see Task 10) -- there is no CIF lookup available to do
    -- this exactly, per this plan's Global Constraints.
    service_date        DATE NOT NULL,
    pin_origin_crs       TEXT NOT NULL,
    pin_scheduled_departure TIMESTAMPTZ NOT NULL,
    pin_destination_crs  TEXT,
    pin_operator         TEXT,

    -- Populated once resolved.
    train_uid  TEXT,
    train_id   TEXT,  -- TRUST's own daily identifier, the join key for
                       -- every subsequent message on this train.

    resolution_status TEXT NOT NULL DEFAULT 'pending'
        CHECK (resolution_status IN ('pending', 'resolved', 'unresolved')),

    tracked_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ
);

CREATE INDEX tracked_trains_user_id ON tracked_trains (user_id);

-- Only meaningful once resolved -- a resolved (train_uid, service_date)
-- pair must be unique (re-pinning an already-tracked train should not
-- create a second parallel event log for it). Multiple *pending* rows with
-- NULL train_uid are fine and expected; Postgres treats NULLs as distinct
-- for uniqueness purposes, so this constraint doesn't block them.
CREATE UNIQUE INDEX tracked_trains_resolved_identity
    ON tracked_trains (train_uid, service_date)
    WHERE train_uid IS NOT NULL;

-- trust-consumer's reference-reload query (Task 4) filters on this.
CREATE INDEX tracked_trains_resolution_status ON tracked_trains (resolution_status);

CREATE TABLE train_movement_events (
    id BIGSERIAL PRIMARY KEY,
    tracked_train_id BIGINT NOT NULL REFERENCES tracked_trains(id) ON DELETE CASCADE,

    dedup_key TEXT NOT NULL,  -- see Task 13 -- stable across redelivery of
                              -- the same real-world TRUST message.
    msg_type   TEXT NOT NULL, -- '0001'..'0007' (never '0005'/'0008' --
                              -- unconfirmed types are dropped before this
                              -- table, see Task 8).
    event_type TEXT,          -- ARRIVAL / DEPARTURE / PASS, Movement only.
    loc_stanox TEXT,
    loc_crs    TEXT,          -- best-effort STANOX->CRS translation; NULL
                              -- if untranslatable.
    planned_timestamp TIMESTAMPTZ,
    actual_timestamp  TIMESTAMPTZ,
    variation_status  TEXT,
    raw_body   JSONB NOT NULL, -- full message body, verbatim, for anything
                              -- this schema doesn't model explicitly.

    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (tracked_train_id, dedup_key)
);

CREATE INDEX train_movement_events_tracked_train
    ON train_movement_events (tracked_train_id, received_at);

CREATE TABLE train_current_state (
    tracked_train_id BIGINT PRIMARY KEY REFERENCES tracked_trains(id) ON DELETE CASCADE,

    status TEXT NOT NULL DEFAULT 'awaiting_activation'
        CHECK (status IN ('awaiting_activation', 'en_route', 'cancelled', 'completed')),

    last_reported_location TEXT,
    last_event_type        TEXT,
    delay_minutes           INTEGER,
    next_calling_point      TEXT,

    -- trust-propagated (naive forward delay propagation, always available
    -- once en route) vs darwin-estimated (blended in at read time by
    -- crates/api -- see Task 6). Never both at once: this column reflects
    -- what trust-consumer itself last computed, which Task 6 may override
    -- transiently in its response without writing back here.
    eta_next   TIMESTAMPTZ,
    eta_source TEXT CHECK (eta_source IN ('trust-propagated', 'darwin-estimated')),

    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
