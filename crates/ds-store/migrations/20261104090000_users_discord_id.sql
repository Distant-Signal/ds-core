SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- users.discord_id: the user's Discord account id (snowflake), for account
-- continuity across the move of public login to the dedicated `ds-sso`
-- identity provider (Ranma-Config docs/specs/ds-public-auth-idp.md,
-- "Account continuity").
--
-- `users.id` is the raw OIDC `sub`, and `ds-sso` mints new subs. On a
-- user's first `ds-sso` login the api finds their existing row by this
-- column (or, for staff, by the forwarded primary sub) and re-keys it in
-- place: `UPDATE users SET id = <new sub>`, which the ON UPDATE CASCADE
-- foreign keys (20261104090200) carry to every table that references it.
--
-- Filled from two places: the one-off export of the primary identity
-- provider's Discord source connections, loaded before the cut-over, and
-- the api's login callback, which stores the Discord id claim on every
-- login while the continuity feature is on. Never from an editable
-- profile attribute: only the provider's source-connection identifier.
--
-- Nullable with no default: a catalog-only change. Unique among non-NULL
-- values via the partial index in the next migration (20261104090100).
-- -------------------------------------------------------------------------

ALTER TABLE users ADD COLUMN discord_id TEXT;
