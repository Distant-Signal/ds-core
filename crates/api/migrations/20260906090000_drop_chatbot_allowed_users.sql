-- -------------------------------------------------------------------------
-- Drops chatbot_allowed_users (added by
-- 20260902110000_chatbot_allowed_users.sql): the embedded chatbot's
-- `/public/chatbot/access` gate (crates/api/src/routes/chatbot.rs,
-- crate::auth::ChatbotAuthorizedUser) no longer consults this per-user DB
-- allowlist -- it now checks the resolved user's own `groups` (the
-- already-decoded OIDC `groups` claim, `users.groups`, added by
-- 20260902160000_user_access_groups.sql) against a single configured
-- Authentik/SSO group name (`ServiceArguments::chatbot_access_group`,
-- crates/api/src/data/config.rs). An SSO group is a strictly better fit for
-- "which real people get this" than a hand-maintained per-user table: an
-- operator manages membership directly in Authentik, the same place every
-- other access group in this app already lives, instead of a bespoke admin
-- path onto this one table.
--
-- `data::users::is_chatbot_allowed`, the only reader of this table, is
-- removed alongside this migration -- confirmed no other caller.
-- -------------------------------------------------------------------------

DROP TABLE chatbot_allowed_users;
