SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Read-only personal API tokens (docs/specs/personal-api-tokens.md §2,
-- §11.1). A user creates a token on the Connected apps page and uses it on
-- the separate token listener (the API hostname) to read their own data.
--
-- Expand-only: one new table. No code reads it until the api ships with
-- api.personalTokens.enabled, which is off by default. The only lock on an
-- existing table is the brief SHARE ROW EXCLUSIVE the foreign key takes on
-- users; every other constraint is on this new, empty table, so nothing
-- needs NOT VALID and a later VALIDATE.
-- -------------------------------------------------------------------------

CREATE TABLE personal_api_tokens (
    id BIGSERIAL PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    name TEXT NOT NULL CHECK (char_length(name) BETWEEN 1 AND 60),
    -- SHA-256 of the whole token string (`dsp_...`). The token itself is
    -- never stored.
    token_hash BYTEA NOT NULL UNIQUE CHECK (octet_length(token_hash) = 32),
    -- The token's last 4 characters (checksum only), shown as `dsp_...hint`.
    hint TEXT NOT NULL CHECK (hint ~ '^[0-9A-Za-z]{4}$'),
    scopes TEXT[] NOT NULL CHECK (
        cardinality(scopes) BETWEEN 1 AND 4
        AND scopes <@ ARRAY['journeys:read', 'lines:read', 'trains:read', 'preferences:read']::TEXT[]
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    last_used_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    revoked_reason TEXT CHECK (revoked_reason IN ('user', 'admin', 'inactive', 'expired')),
    CHECK ((revoked_at IS NULL) = (revoked_reason IS NULL)),
    CHECK (expires_at > created_at)
);

-- The Connected apps list and the 10-active-tokens cap.
CREATE INDEX personal_api_tokens_user_active
    ON personal_api_tokens (user_id) WHERE revoked_at IS NULL;
-- The hourly sweep: expire and idle-revoke active tokens.
CREATE INDEX personal_api_tokens_active_expires_at
    ON personal_api_tokens (expires_at) WHERE revoked_at IS NULL;
-- The hourly sweep: delete rows a year after they ended.
CREATE INDEX personal_api_tokens_revoked_at
    ON personal_api_tokens (revoked_at) WHERE revoked_at IS NOT NULL;
