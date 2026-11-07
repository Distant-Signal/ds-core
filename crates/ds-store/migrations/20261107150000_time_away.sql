SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Regular journeys: time away and pause until (distant-signal's
-- docs/superpowers/specs/2026-11-06-regular-journeys-design.md, the
-- scheduling options' R1).
--
-- - `journey_templates.paused_until`: the template makes no journey for a
--   date before it, and runs again on it, with no tap needed. NULL: not
--   paused that way (`active` is still the pause with no end). Read by
--   `ds_store::recurrence` (reason `paused`).
-- - `user_away_periods`: the user's time away, inclusive dates. It applies
--   to every regular journey they have (reason `away`). Personal data: the
--   api caps it at 50 rows a user, prunes a period once it has ended, and
--   puts it in the account export; it goes with the account.
--
-- Expand-only. The ADD COLUMN is nullable, so it changes the catalog only;
-- its brief ACCESS EXCLUSIVE is bounded by the 5 s lock_timeout. The new
-- table's foreign key takes a brief SHARE ROW EXCLUSIVE lock on `users`.
-- Old code never reads any of it.
-- -------------------------------------------------------------------------

ALTER TABLE journey_templates ADD COLUMN paused_until DATE;

CREATE TABLE user_away_periods (
    id         BIGSERIAL PRIMARY KEY,
    user_id    TEXT NOT NULL REFERENCES users (id) ON UPDATE CASCADE ON DELETE CASCADE,
    starts_on  DATE NOT NULL,
    ends_on    DATE NOT NULL,
    -- The user's own name for it ("Holiday"), shown only to them.
    label      TEXT CHECK (label IS NULL OR char_length(label) BETWEEN 1 AND 60),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (ends_on >= starts_on),
    -- A year and a day: the longest the page offers.
    CHECK (ends_on - starts_on <= 366)
);
CREATE INDEX user_away_periods_user_id ON user_away_periods (user_id, ends_on);
