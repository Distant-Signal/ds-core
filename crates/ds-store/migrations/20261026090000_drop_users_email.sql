-- contract: users.email (code stopped using it in e44f98f3)
-- -------------------------------------------------------------------------
-- Drops users.email. 9c1a53a0 (2026-09-30) stopped requesting the `email`
-- scope and reading the claim; upsert_user kept clearing the column
-- (`email = NULL`) at each sign-in until e44f98f3, which also took it
-- out of every fixture. Production held no email on 2026-10-23 (0 of 6
-- users), with every running api at a build that includes e44f98f3.
--
-- Ship only once no api older than e44f98f3 runs: an older api's
-- sign-in (upsert_user's ON CONFLICT ... SET email = NULL) fails with 42703
-- once the column is gone, so every returning user's login would fail.
--
-- The account export (`SELECT to_jsonb(u) FROM users u`) loses its
-- always-null `account.email` key; no other reader exists. db-grants.yaml
-- grants `users` table-wide, so no grant changes. DROP COLUMN is a
-- catalog change (no rewrite) under a brief ACCESS EXCLUSIVE lock, hence
-- the timeout.
-- -------------------------------------------------------------------------
SET LOCAL lock_timeout = '5s';

ALTER TABLE users DROP COLUMN IF EXISTS email;
