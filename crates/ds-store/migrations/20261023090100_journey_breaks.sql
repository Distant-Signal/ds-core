SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Journey breaks (docs/superpowers/specs/2026-10-22-mid-journey-changes-design.md
-- §4.1, §6): one row per time a journey stopped working -- a cancelled or
-- cut-short train, a connection missed on live times and minimum change
-- times, a skipped stop, or the traveller marking a train missed. At most
-- one is open per journey. The api writes them (the break sweep and the
-- "I missed this train" route); the notifier reads open ones and stamps
-- `notified_at`/`notified_severity` (§6.3).
--
-- Expand-only: one new table. Its foreign keys take brief SHARE ROW
-- EXCLUSIVE locks on `journeys`, `users`, `journey_legs` and
-- `journey_revisions` (new, empty), under the 5 s lock_timeout. Then
-- `journey_revisions.break_id` gets its foreign key, NOT VALID (a table an
-- earlier migration made is never scanned under its lock), validated with
-- the CHECKs in 20261023090400. The enum CHECKs come NOT VALID in
-- 20261023090300.
-- -------------------------------------------------------------------------

CREATE TABLE journey_breaks (
    id                   BIGSERIAL PRIMARY KEY,
    journey_id           BIGINT NOT NULL REFERENCES journeys (id) ON DELETE CASCADE,
    user_id              TEXT NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    detected_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- cancelled | part_cancelled | connection_missed | user_missed | skipped_stop
    cause                TEXT NOT NULL,
    -- operator | user | unknown
    fault                TEXT NOT NULL,
    -- 1 to 4 (§6.2).
    severity             SMALLINT NOT NULL,
    trigger_leg_id       BIGINT REFERENCES journey_legs (id) ON DELETE SET NULL,
    trigger_trains_id    BIGINT,
    -- 1-based position of the first leg that no longer works.
    break_position       INT NOT NULL,
    -- The inputs the suggestions were planned from (§5.4).
    fingerprint          TEXT NOT NULL,
    -- open | accepted | dismissed | recovered | expired
    status               TEXT NOT NULL DEFAULT 'open',
    -- The ranked alternatives (§17), as served.
    suggestions          JSONB,
    suggestions_at       TIMESTAMPTZ,
    -- How many sweeps in a row have seen it (a break is pushed only once
    -- seen twice, unless a cancellation; §11).
    confirmations        SMALLINT NOT NULL DEFAULT 1,
    notified_at          TIMESTAMPTZ,
    notified_severity    SMALLINT,
    opened_at            TIMESTAMPTZ,
    -- push | in_app
    opened_via           TEXT,
    resolved_at          TIMESTAMPTZ,
    -- willMakeIt | notTravelling | other, for a dismissed break.
    dismiss_reason       TEXT,
    accepted_revision_id BIGINT REFERENCES journey_revisions (id) ON DELETE SET NULL
);
CREATE UNIQUE INDEX journey_breaks_one_open ON journey_breaks (journey_id)
    WHERE status = 'open';
CREATE INDEX journey_breaks_to_notify ON journey_breaks (detected_at)
    WHERE status = 'open';
CREATE INDEX journey_breaks_journey_id ON journey_breaks (journey_id);
CREATE INDEX journey_breaks_user_id ON journey_breaks (user_id);
CREATE INDEX journey_breaks_trigger_leg_id ON journey_breaks (trigger_leg_id)
    WHERE trigger_leg_id IS NOT NULL;
CREATE INDEX journey_breaks_accepted_revision_id ON journey_breaks (accepted_revision_id)
    WHERE accepted_revision_id IS NOT NULL;

ALTER TABLE journey_revisions
    ADD CONSTRAINT journey_revisions_break_id_fkey
    FOREIGN KEY (break_id) REFERENCES journey_breaks (id) ON DELETE SET NULL
    NOT VALID;
