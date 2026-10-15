SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Delegated access, Phase 1b (docs/superpowers/specs/
-- 2026-10-14-ds-mcp-delegated-access-design.md §5.3, §5.5a, §5.8, §5.10).
--
-- A registered client (ds-mcp, from DELEGATED_CLIENTS) may act for a user
-- only while that user holds an active grant for it. Nothing here names MCP
-- or Claude: `client` is a configured client name (R6).
--
-- Expand-only: four new tables, and one column with a constant default on
-- journeys (a catalog-only change, no rewrite). Code before this change
-- never reads any of them.
-- -------------------------------------------------------------------------

-- One row per consent. At most one active (unrevoked) row per user and
-- client; a re-grant revokes the old row and inserts a new one, whose
-- `not_before` refuses tokens minted before the new consent.
CREATE TABLE delegated_grants (
    id BIGSERIAL PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    client TEXT NOT NULL CHECK (client ~ '^[a-z0-9-]{1,32}$'),
    -- The client's display name when the grant was made, for the push
    -- notice (the notifier has no DELEGATED_CLIENTS) and the account export.
    client_display_name TEXT NOT NULL CHECK (char_length(client_display_name) BETWEEN 1 AND 100),
    scopes TEXT[] NOT NULL CHECK (cardinality(scopes) BETWEEN 1 AND 8),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    not_before TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at TIMESTAMPTZ,
    -- Set by the notifier once the grant_created web push went out (or the
    -- user had no subscriptions).
    push_notified_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    revoked_reason TEXT CHECK (
        revoked_reason IN (
            'user', 'admin', 'backchannel', 'client_logout', 'inactive',
            'client_disabled', 'replaced', 'account_deleted'
        )
    ),
    CHECK ((revoked_at IS NULL) = (revoked_reason IS NULL))
);

CREATE UNIQUE INDEX delegated_grants_one_active
    ON delegated_grants (user_id, client) WHERE revoked_at IS NULL;
-- The hourly idle sweep and the notifier's push poll read active grants only.
CREATE INDEX delegated_grants_active_created_at
    ON delegated_grants (created_at) WHERE revoked_at IS NULL;

-- Single-use confirmation tokens issued by the consent page (§5.3,
-- compensating control 1). Stored hashed, bound to the session that
-- rendered the page and to the client, valid for 10 minutes. Deleting the
-- session (logout, revocation) deletes its tokens.
CREATE TABLE delegated_grant_confirmations (
    token_hash TEXT PRIMARY KEY CHECK (token_hash ~ '^[0-9a-f]{64}$'),
    session_id TEXT NOT NULL REFERENCES sessions (id) ON DELETE CASCADE,
    user_id TEXT NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    client TEXT NOT NULL CHECK (client ~ '^[a-z0-9-]{1,32}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX delegated_grant_confirmations_session
    ON delegated_grant_confirmations (session_id, client);
CREATE INDEX delegated_grant_confirmations_expires_at
    ON delegated_grant_confirmations (expires_at);

-- What a delegated delete removed (§5.5a), kept 30 days and shown on the
-- Connected apps page as "Recently deleted by the assistant". The journey
-- itself is gone, so `journey_id` is not a foreign key.
CREATE TABLE delegated_deletions (
    id BIGSERIAL PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    client TEXT NOT NULL CHECK (client ~ '^[a-z0-9-]{1,32}$'),
    grant_id BIGINT REFERENCES delegated_grants (id) ON DELETE SET NULL,
    journey_id BIGINT NOT NULL,
    deleted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- {customName, legs: [{serviceDate, originCrs, destinationCrs, trainUid}]}
    summary JSONB NOT NULL
);

CREATE INDEX delegated_deletions_user_deleted_at
    ON delegated_deletions (user_id, deleted_at);
CREATE INDEX delegated_deletions_deleted_at
    ON delegated_deletions (deleted_at);

-- `Idempotency-Key`s of POST /Journeys/itinerary, remembered per user for
-- 24 hours (§5.8): a repeat with the same body returns the stored
-- response; the same key with a different body is a 409.
CREATE TABLE journey_idempotency_keys (
    user_id TEXT NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    idempotency_key TEXT NOT NULL CHECK (char_length(idempotency_key) BETWEEN 1 AND 200),
    request_hash TEXT NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    response JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, idempotency_key)
);

CREATE INDEX journey_idempotency_keys_created_at
    ON journey_idempotency_keys (created_at);

-- Who created each journey: 'web' (the default, every existing row and
-- every cookie-authenticated create) or a delegated client's name. The
-- delegated delete rule (§5.5a) and the 50-a-day cap (§5.10) key on it.
ALTER TABLE journeys
    ADD COLUMN IF NOT EXISTS created_via TEXT NOT NULL DEFAULT 'web';
