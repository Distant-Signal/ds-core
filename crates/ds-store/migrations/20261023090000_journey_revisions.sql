SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Journey revisions: append-only snapshots of a journey's whole plan
-- (docs/superpowers/specs/2026-10-22-mid-journey-changes-design.md §4.1,
-- DS-128 phase 1).
--
-- `journey_legs` stays the current plan, unchanged in meaning (D-1). A
-- revision is the whole plan at one moment: revision 1 is the booked plan,
-- refreshed by planning edits until the journey starts or breaks (§4.3);
-- every later change (a replacement, a revert) appends the next one and
-- never edits an older one. The only update ever made to a snapshot leg is
-- its `fate`, once, from 'planned' when its revision is superseded, and the
-- only update to a revision is moving `is_compensation_baseline` (§9.2).
--
-- Legs are denormalised (train UID, date, stations, booked public times,
-- ticket ids) so they outlive the subscription a replacement releases
-- (DS-93) and the `trains` row retention deletes. `source_leg_id`,
-- `trains_id` and `ticket_ids` are therefore plain values, not foreign keys.
--
-- Expand-only: two new tables. The one foreign key to an existing table
-- (`journeys`) takes a brief SHARE ROW EXCLUSIVE on it, under the 5 s
-- lock_timeout. The enum CHECKs are added NOT VALID by 20261023090300 and
-- validated by 20261023090400. `break_id`'s foreign key is added with
-- `journey_breaks` (20261023090100).
-- -------------------------------------------------------------------------

CREATE TABLE journey_revisions (
    id                       BIGSERIAL PRIMARY KEY,
    journey_id               BIGINT NOT NULL REFERENCES journeys (id) ON DELETE CASCADE,
    -- 1 = booked.
    revision_no              INT NOT NULL,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- booked | replace | auto_shift | revert
    reason                   TEXT NOT NULL,
    -- cancelled | part_cancelled | connection_missed | user_missed |
    -- user_choice | skipped_stop; NULL for the booked revision.
    cause                    TEXT,
    -- operator | user | none | unknown; NULL for the booked revision.
    fault                    TEXT,
    break_id                 BIGINT,
    -- owner | system
    actor                    TEXT NOT NULL,
    -- The plan Delay Repay is measured against (§9.2): revision 1 until a
    -- passenger-fault or chosen change moves it.
    is_compensation_baseline BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (journey_id, revision_no)
);
CREATE INDEX journey_revisions_break_id ON journey_revisions (break_id)
    WHERE break_id IS NOT NULL;

CREATE TABLE journey_revision_legs (
    revision_id       BIGINT NOT NULL REFERENCES journey_revisions (id) ON DELETE CASCADE,
    -- 1-based, in the plan's order.
    position          INT NOT NULL,
    -- journey_legs.id when the snapshot was taken; no foreign key.
    source_leg_id     BIGINT,
    -- train | untracked
    kind              TEXT NOT NULL,
    train_uid         TEXT,
    service_date      DATE NOT NULL,
    -- No foreign key: `trains` has its own retention.
    trains_id         BIGINT,
    atoc_code         TEXT,
    origin_crs        TEXT,
    destination_crs   TEXT,
    -- Public (GBTT) times at origin_crs and destination_crs when the
    -- snapshot was first taken, else the working times.
    booked_departure  TIMESTAMPTZ,
    booked_arrival    TIMESTAMPTZ,
    -- Untracked legs (Tube, bus, walk).
    transit_mode      TEXT,
    origin_place      TEXT,
    destination_place TEXT,
    line_name         TEXT,
    planned_departure TIMESTAMPTZ,
    planned_arrival   TIMESTAMPTZ,
    -- tracked_train_tickets ids on the leg's subscription at the time.
    ticket_ids        BIGINT[] NOT NULL DEFAULT '{}',
    -- planned | travelled | missed | cancelled | part_cancelled | not_used
    fate              TEXT NOT NULL DEFAULT 'planned',
    PRIMARY KEY (revision_id, position)
);
