SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- The "a personal API token was created" push
-- (docs/specs/personal-api-tokens.md §15 decision 3): the notifier sends
-- one notice per new token to each of the user's devices and records it
-- here, as it does for delegated grants (delegated_grants.push_notified_at).
--
-- Expand-only: a nullable column with no default (a catalog-only change).
-- Tokens created before this column existed are marked notified, so the
-- notice never goes out about old tokens. Production has none (the
-- feature is off), so the UPDATE touches no rows there.
-- -------------------------------------------------------------------------

ALTER TABLE personal_api_tokens ADD COLUMN IF NOT EXISTS push_notified_at TIMESTAMPTZ;

UPDATE personal_api_tokens SET push_notified_at = created_at WHERE push_notified_at IS NULL;
