SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Opt-in usage analytics, phase 2 (DS-190). Additive: one column, one
-- table.
--
-- `users.analytics_opt_in`: a signed-in user's "Help improve Distant
-- Signal" choice, off unless they turn it on (Account, Preferences). In
-- the account export with the rest of the users row. ADD COLUMN with a
-- constant default changes the catalog only (no rewrite); its brief
-- ACCESS EXCLUSIVE on `users` is bounded by the 5 s lock_timeout. Code
-- before this change never reads it.
--
-- `usage_daily`: what `POST /public/events` counts, as daily totals only.
-- No user id, IP address, user agent or session id is stored, so a row
-- can never be tied back to a person. `dim` is the page's route template
-- (`/lines/[id]`, never a real line) plus the event's enumerated
-- properties, all from the api's allowlist (crates/api/src/usage_events.rs).
-- The api's hourly retention sweep deletes days older than 13 months
-- (docs/personal-data-retention.md).
-- -------------------------------------------------------------------------

ALTER TABLE users
    ADD COLUMN analytics_opt_in BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE usage_daily (
    day       DATE    NOT NULL,
    event     TEXT    NOT NULL,
    dim       TEXT    NOT NULL,
    signed_in BOOLEAN NOT NULL,
    count     BIGINT  NOT NULL DEFAULT 0 CHECK (count >= 0),
    PRIMARY KEY (day, event, dim, signed_in)
);

COMMENT ON TABLE usage_daily IS
    'Opt-in usage analytics: daily counts per event and dimension. No identifiers. Kept 13 months.';
COMMENT ON COLUMN users.analytics_opt_in IS
    'The user agreed to send anonymous usage counts (off by default).';
