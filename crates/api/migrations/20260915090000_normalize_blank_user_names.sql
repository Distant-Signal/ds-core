-- -------------------------------------------------------------------------
-- Normalize blank users.name/users.email to NULL.
--
-- An identity provider with no name on file for a user does not omit the
-- `name` claim -- Authentik's stock `profile` scope mapping (the one this
-- deployment attaches) returns its `User.name` attribute verbatim, and
-- that attribute defaults to the empty string. Until now
-- data::users::upsert_user wrote that claim through unchanged, so
-- `users.name` holds `''` for every such user.
--
-- Everything that reads these columns treats "no name" as NULL and falls
-- back accordingly -- `''` is not NULL, so it won the fallback and was
-- rendered as the label itself: a group member row that was just a role
-- badge, a "Shared by " with nothing after it, and an empty nav-bar label.
--
-- `upsert_user` now normalizes on write, but that only takes effect on a
-- user's NEXT login; this backfills the rows already stored. Trimming as
-- well as NULLIF -- a name of "  Ada  " should never be rendered with its
-- padding intact either.
--
-- The trim set is given explicitly (ASCII space/tab/CR/LF/vertical
-- tab/form feed) rather than relying on one-argument btrim, which strips
-- SPACES ONLY and would leave a name of E'\t' looking non-blank. That is
-- still narrower than Rust's `str::trim` (all Unicode whitespace, NBSP
-- included), which is why `data::users::non_blank` keeps normalizing on
-- read as well: this migration is a cleanup, not the guarantee.
--
-- Idempotent, and a no-op on a database where neither has ever happened.
-- -------------------------------------------------------------------------

UPDATE users
   SET name = NULLIF(btrim(name, E' \t\r\n\v\f'), '')
 WHERE name IS NOT NULL
   AND name IS DISTINCT FROM NULLIF(btrim(name, E' \t\r\n\v\f'), '');

UPDATE users
   SET email = NULLIF(btrim(email, E' \t\r\n\v\f'), '')
 WHERE email IS NOT NULL
   AND email IS DISTINCT FROM NULLIF(btrim(email, E' \t\r\n\v\f'), '');
