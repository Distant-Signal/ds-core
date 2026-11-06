SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Regular journeys, phase 2: leave by (distant-signal's
-- docs/superpowers/specs/2026-11-06-regular-journeys-design.md §5.1).
--
-- - `station_buffer_minutes`: the user's own minutes to get to the
--   station, margin included. A template carries it; each occurrence
--   copies it at mint, so a change to the template doesn't move a day
--   already under way, and one day can be changed on its own. NULL: no
--   leave-by.
-- - Four push preferences, each read as its default when the user has no
--   `notification_preferences` row: `leave_by` (on), `leave_by_quiet_hours`
--   (on: a leave-by still comes in quiet hours), `leave_by_lead_minutes`
--   (0: at the leave-by time) and `morning_summary` (off).
-- - `regular_journey_push_state`: the notifier's leave-by bookkeeping per
--   occurrence (the leg it was for, the leave-by it named, when, and how
--   many regular-journey pushes the occurrence has had, for the cap).
-- - `regular_journey_summaries`: one morning summary per user and London
--   day, pruned by the notifier after 7 days.
--
-- Expand-only. Each ADD COLUMN is nullable or has a constant default, so it
-- changes the catalog only; its brief ACCESS EXCLUSIVE is bounded by the
-- 5 s lock_timeout. The range CHECKs on the existing tables are added NOT
-- VALID and validated by 20261106190100. The new tables' foreign keys take
-- brief SHARE ROW EXCLUSIVE locks on `journeys` and `users`. Old code never
-- reads any of it.
-- -------------------------------------------------------------------------

ALTER TABLE journey_templates ADD COLUMN station_buffer_minutes SMALLINT;
ALTER TABLE journeys ADD COLUMN station_buffer_minutes SMALLINT;
ALTER TABLE notification_preferences
    ADD COLUMN leave_by              BOOLEAN  NOT NULL DEFAULT TRUE,
    ADD COLUMN leave_by_quiet_hours  BOOLEAN  NOT NULL DEFAULT TRUE,
    ADD COLUMN leave_by_lead_minutes SMALLINT NOT NULL DEFAULT 0,
    ADD COLUMN morning_summary       BOOLEAN  NOT NULL DEFAULT FALSE;

ALTER TABLE journey_templates
    ADD CONSTRAINT journey_templates_station_buffer_minutes_check
    CHECK (station_buffer_minutes IS NULL OR station_buffer_minutes BETWEEN 0 AND 120)
    NOT VALID;
ALTER TABLE journeys
    ADD CONSTRAINT journeys_station_buffer_minutes_check
    CHECK (station_buffer_minutes IS NULL OR station_buffer_minutes BETWEEN 0 AND 120)
    NOT VALID;
ALTER TABLE notification_preferences
    ADD CONSTRAINT notification_preferences_leave_by_lead_minutes_check
    CHECK (leave_by_lead_minutes BETWEEN 0 AND 5)
    NOT VALID;

CREATE TABLE regular_journey_push_state (
    journey_id       BIGINT PRIMARY KEY REFERENCES journeys (id) ON DELETE CASCADE,
    user_id          TEXT NOT NULL
                     REFERENCES users (id) ON UPDATE CASCADE ON DELETE CASCADE,
    -- The journey_legs id the last leave-by push was for (its tag). No
    -- foreign key: a replan deletes the leg, and the row must outlive it.
    leave_leg_id     BIGINT,
    -- The leave-by time the last leave-by push named.
    leave_by_sent    TIMESTAMPTZ,
    -- When that push was delivered.
    leave_by_sent_at TIMESTAMPTZ,
    -- Regular-journey pushes delivered for this occurrence (leave-by and
    -- its moves), for notifier.leaveReminders.maxPushesPerOccurrence.
    pushes           SMALLINT NOT NULL DEFAULT 0 CHECK (pushes >= 0)
);
CREATE INDEX regular_journey_push_state_user_id ON regular_journey_push_state (user_id);

CREATE TABLE regular_journey_summaries (
    user_id      TEXT NOT NULL REFERENCES users (id) ON UPDATE CASCADE ON DELETE CASCADE,
    -- The London day the summary was for.
    service_date DATE NOT NULL,
    sent_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (user_id, service_date)
);
