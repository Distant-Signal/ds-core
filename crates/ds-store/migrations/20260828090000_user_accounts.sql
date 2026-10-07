-- -------------------------------------------------------------------------
-- User accounts via OIDC SSO: `users` and `sessions`, plus a short-lived
-- `oidc_login_state` table bridging GET /auth/login -> GET /auth/callback.
-- See docs/superpowers/specs/2026-08-28-user-accounts-sso-design.md's Data
-- model / Session architecture sections and
-- docs/superpowers/plans/2026-08-28-user-accounts-sso.md's Task 1/4/5.
--
-- IMPORTANT: this migration must apply before
-- crates/api/migrations/20260828120000_train_tracking.sql
-- (docs/superpowers/plans/2026-08-28-train-tracking.md's Task 1) --
-- tracked_trains.user_id references users(id). This file's timestamp
-- prefix (20260828090000) already sorts earlier; preserve that ordering if
-- either file is ever renamed. See the note at the top of both plan
-- documents.
--
-- users.id is the OIDC `sub` claim, stored verbatim -- a natural-key TEXT
-- primary key, matching this schema's existing convention
-- (incidents.incident_id, custom_lines.id, stations.crs) rather than
-- adding a uuid dependency for a value that's already a stable unique
-- string. Safe only under this design's single-issuer assumption (design
-- doc Open Question 1).
--
-- email is only ever written by crates/api/src/data/users.rs's
-- upsert_user when the ID token also asserted email_verified: true (design
-- doc Open Question 2) -- not enforced by this schema, enforced at the
-- application layer. No separate email_verified column: a dropped
-- (never-written) email already carries that signal for this app's
-- current needs.
-- -------------------------------------------------------------------------

CREATE TABLE users (
    id             TEXT        PRIMARY KEY,
    email          TEXT,
    name           TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_login_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- sessions.id stores a SHA-256 hex digest of the opaque cookie value, not
-- the raw token -- see crates/api/src/auth.rs's `hash_session_token` doc
-- comment (Task 6): a DB dump/leak alone then can't be replayed as a live
-- session cookie, the same property a password hash gives you.
CREATE TABLE sessions (
    id             TEXT        PRIMARY KEY,
    user_id        TEXT        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    refresh_token  TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at     TIMESTAMPTZ NOT NULL
);

CREATE INDEX sessions_user_id ON sessions (user_id);

-- Short-lived, single-use rows bridging /auth/login -> /auth/callback:
-- the PKCE verifier, nonce, and CSRF state token generated at login time
-- must survive the round trip to the SSO server and back, without relying
-- on the not-yet-issued real session cookie. `id` is set as a separate
-- short-lived `nr_login` cookie (Task 7). Rows older than 15 minutes are
-- swept opportunistically by crates/api/src/data/users.rs's
-- `insert_login_state` -- no cron needed for a table this small and
-- self-limiting.
CREATE TABLE oidc_login_state (
    id             TEXT        PRIMARY KEY,
    pkce_verifier  TEXT        NOT NULL,
    nonce          TEXT        NOT NULL,
    csrf_state     TEXT        NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
