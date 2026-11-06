SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Regular journeys, phase 5: bank holidays (distant-signal's
-- docs/superpowers/specs/2026-11-06-regular-journeys-design.md §8.1).
--
-- - `bank_holidays`: the UK bank holidays per division, one copy for the
--   whole service. The api's `bank_holidays_sync` loop fills it from
--   GOV.UK's bank holidays JSON (Open Government Licence v3.0): each good
--   response upserts every event and deletes the rows inside the
--   response's date range that it no longer lists (a moved holiday). A
--   failed or implausible response changes nothing.
-- - `bank_holidays_sync`: one row, the loop's last attempt and last
--   success, so every api replica (and a restart) knows when the next
--   fetch is due, and the staleness gauge survives a restart.
-- - `users.bank_holiday_division`: the division whose holidays a user's
--   regular journeys skip. NULL: not chosen; readers use
--   'england-and-wales' (ds_store::bank_holidays::DEFAULT_DIVISION).
-- - `journey_templates.skip_bank_holidays`: whether a template skips them
--   (decision 8: on by default, existing templates included).
--
-- Expand-only. Each ADD COLUMN is nullable or has a constant default, so it
-- changes the catalog only; its brief ACCESS EXCLUSIVE is bounded by the
-- 5 s lock_timeout. The division CHECK on `users` is added NOT VALID and
-- validated by 20261106210100. Old code never reads any of it.
-- -------------------------------------------------------------------------

CREATE TABLE bank_holidays (
    division     TEXT NOT NULL CHECK (division IN
                   ('england-and-wales', 'scotland', 'northern-ireland')),
    holiday_date DATE NOT NULL,
    title        TEXT NOT NULL,
    -- The response that last listed it.
    fetched_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (division, holiday_date)
);

CREATE TABLE bank_holidays_sync (
    -- One row only.
    id              BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
    last_attempt_at TIMESTAMPTZ NOT NULL,
    -- 'ok', 'fetch_failed' (transport, status or body) or 'invalid' (it
    -- parsed but failed the plausibility checks).
    last_result     TEXT NOT NULL CHECK (last_result IN ('ok', 'fetch_failed', 'invalid')),
    -- NULL until the first good response.
    last_success_at TIMESTAMPTZ
);

ALTER TABLE users ADD COLUMN bank_holiday_division TEXT;
ALTER TABLE users
    ADD CONSTRAINT users_bank_holiday_division_check CHECK (
        bank_holiday_division IS NULL
        OR bank_holiday_division IN ('england-and-wales', 'scotland', 'northern-ireland')
    ) NOT VALID;

ALTER TABLE journey_templates
    ADD COLUMN skip_bank_holidays BOOLEAN NOT NULL DEFAULT TRUE;
