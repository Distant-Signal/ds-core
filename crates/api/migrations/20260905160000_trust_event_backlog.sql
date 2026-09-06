-- ---------------------------------------------------------------------
-- A windowed backlog of real TRUST calling-point events, for
-- late-tracking pins (docs/superpowers/specs/2026-09-05-trust-event-backlog-design.md,
-- docs/superpowers/plans/2026-09-05-trust-event-backlog-plan.md).
--
-- RETENTION SAFETY, STATED HERE BECAUSE THIS IS WHERE A FUTURE READER
-- WILL LOOK: this table's default retention is 1 DAY
-- (crates/aggregator's own `trust_event_backlog_retention_days` config
-- field, Task 6). A human must confirm TRUST's real Train Movements
-- licence terms directly with RDM before that value is ever configured
-- above 1 in a real production deployment -- see the design spec's
-- Decision 5 and this plan's own "Scope decision: retention tier and
-- the licensing safeguard" section. Do not bump the default here, or in
-- any Helm values.yaml default, without that confirmation happening
-- first.
--
-- Deliberately narrower message-type/event-type coverage than
-- train_movement_events (see the plan's own "What counts as a key
-- journey point" section): only Activation/Cancellation/Movement,
-- and Movement is further restricted to ARRIVAL/DEPARTURE (never PASS)
-- -- real calling points only, not every TRUST reporting point.
--
-- No raw_body column, unlike train_movement_events -- a deliberate,
-- named scope-narrowing tradeoff (see the design spec's Decision 2 and
-- this plan's Global Constraints), not an oversight. Do not "fix" this
-- by adding one back without re-running this plan's own storage-cost
-- math against the much bigger row size that would produce.
-- ---------------------------------------------------------------------

CREATE TABLE trust_event_backlog (
    id                 BIGSERIAL PRIMARY KEY,

    -- Best-effort STANOX->CRS translation, same posture as
    -- train_movement_events.loc_crs. NULL for Activation/Cancellation
    -- rows (neither carries a location at all) and for any Movement row
    -- whose STANOX didn't translate (dropped before insert by the
    -- consumer in that case -- see Task 9 -- since an untranslated
    -- Movement is useless for the CRS+time lookup this column exists
    -- to serve).
    crs                TEXT,

    -- NULL until an Activation for this train_id has been observed by
    -- the consumer (may never arrive within the retention window -- see
    -- Task 9's own note on this being an accepted, pre-existing category
    -- of gap, not a new one).
    train_uid          TEXT,

    -- TRUST's own daily identifier -- present on all three kept message
    -- types.
    train_id           TEXT NOT NULL,

    -- Best-effort service date. Sourced from an Activation's own
    -- schedule_start_date when this consumer observed one in-process for
    -- this train_id; falls back to the current Europe/London rail day
    -- otherwise (see Task 9's own note -- an accepted approximation
    -- identical in kind to trust-consumer's own existing Activation-
    -- binding gap, not a new one).
    service_date       DATE NOT NULL,

    -- Activation / Cancellation / Movement only. ChangeOfOrigin/
    -- ChangeOfIdentity carry no location or timing data at all
    -- (trust_schema::schema's own ChangeOfOrigin/ChangeOfIdentity
    -- structs are bare {train_id}), so they are not "key journey point"
    -- data by this plan's own resolution of that question and are never
    -- written here.
    msg_type           TEXT NOT NULL
        CHECK (msg_type IN ('0001', '0002', '0003')),

    -- Movement only. PASS is excluded at the database level, not just
    -- application level: a PASS event is TRUST reporting a train
    -- running through a location with no booked calling point at all
    -- (CIF's own LO/LI/LT records only ever carry an arrival and/or a
    -- departure, never a bare pass) -- see the plan's own "What counts
    -- as a key journey point" section for the full reasoning.
    event_type         TEXT
        CHECK (event_type IS NULL OR event_type IN ('ARRIVAL', 'DEPARTURE')),

    planned_timestamp  TIMESTAMPTZ,
    actual_timestamp   TIMESTAMPTZ,

    -- Raw TRUST field, needed to recompute delay_minutes identically to
    -- trust_schema::journey's own "LATE" gate when a backfilled row is
    -- replayed through upsert_train_event (Task 5).
    variation_status   TEXT,

    -- Denormalized convenience for a direct query of this table itself
    -- (e.g. future debugging/analytics) -- NOT load-bearing for the
    -- replay path in Task 5, which recomputes this itself from
    -- planned_timestamp/actual_timestamp/variation_status via the same
    -- trust_schema::journey logic a live event already uses, so this
    -- column and the replayed value are expected to agree but are never
    -- cross-checked against each other.
    delay_minutes      INTEGER,

    -- trust_schema::dedup::dedup_key(train_id, msg_type, event_type,
    -- loc_stanox, planned_timestamp) -- identical shape to
    -- train_movement_events.dedup_key, making a blind, at-least-once-safe
    -- INSERT ... ON CONFLICT DO NOTHING correct here too.
    dedup_key          TEXT NOT NULL,

    received_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX trust_event_backlog_dedup_key
    ON trust_event_backlog (dedup_key);

-- Decision 3 step 2's own query: "does any backlog row at this CRS,
-- around this scheduled time, exist" -- the CRS+time lookup a
-- late-tracking pin uses to discover a train_uid.
CREATE INDEX trust_event_backlog_crs_time
    ON trust_event_backlog (crs, planned_timestamp)
    WHERE crs IS NOT NULL;

-- Decision 3 step 3's own query: "the entire observed history for this
-- train" -- the full backfill. Keyed on train_id, NOT train_uid: Task 9's
-- own consumer never writes a train_uid onto a Movement/Cancellation row
-- (only an Activation row ever carries one -- see that task's own "this
-- consumer doesn't correlate Activation->Movement in-process" comment).
-- train_id, by contrast, is NOT NULL on every one of the three kept
-- message types, and it's the only column that actually ties a train's
-- Activation/Movement/Cancellation rows together in this table -- so it,
-- not train_uid, is the real backfill key. (An earlier draft of this
-- migration indexed (train_uid, service_date) here; that would have made
-- the Task 5 backfill query only ever retrieve the Activation row itself,
-- never the Movement/Cancellation history the whole feature exists to
-- replay -- caught and fixed during this plan's second review pass.)
CREATE INDEX trust_event_backlog_train
    ON trust_event_backlog (train_id, service_date);
