SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Outbound webhooks (DS-415): a user's own destinations for the same
-- notifications Web Push sends. The notifier delivers to every enabled
-- destination of the user a push job is for, so quiet hours, pauses and the
-- per-train switches apply exactly as they do to push.
--
-- - `kind`: the preset that shapes the request. `generic` posts signed JSON,
--   `ntfy` and `gotify` post what those servers expect. More presets add a
--   value here in a later migration.
-- - `secret_ciphertext`, `secret_nonce`, `secret_key_id`: the destination's
--   secret, encrypted at rest (AES-256-GCM, ds_runtime::secret_box): `generic`
--   signs each body with it (HMAC-SHA256); `ntfy` sends it as a bearer token
--   when set; `gotify` sends it as the app token. The api encrypts on write
--   and never returns it; the notifier decrypts on send. The key id says
--   which key sealed it, so keys can rotate. All three are set, or none.
-- - `consecutive_failures`, `disabled_reason`, `last_*`: delivery health,
--   written by the notifier. A destination that fails repeatedly is
--   disabled (`enabled` FALSE) so a dead one is not retried forever.
--
-- New table, so no lock on existing ones beyond the foreign key's brief
-- SHARE ROW EXCLUSIVE on `users`, bounded by the 5 s lock_timeout.
-- -------------------------------------------------------------------------

CREATE TABLE webhook_destinations (
    id                   BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id              TEXT        NOT NULL REFERENCES users (id) ON UPDATE CASCADE ON DELETE CASCADE,
    kind                 TEXT        NOT NULL CHECK (kind IN ('generic', 'ntfy', 'gotify')),
    name                 TEXT        NOT NULL CHECK (char_length(name) BETWEEN 1 AND 60),
    url                  TEXT        NOT NULL CHECK (char_length(url) BETWEEN 1 AND 2048),
    secret_ciphertext    BYTEA       CHECK (secret_ciphertext IS NULL OR octet_length(secret_ciphertext) BETWEEN 17 AND 1024),
    secret_nonce         BYTEA       CHECK (secret_nonce IS NULL OR octet_length(secret_nonce) = 12),
    secret_key_id        TEXT        CHECK (secret_key_id IS NULL OR char_length(secret_key_id) BETWEEN 1 AND 32),
    enabled              BOOLEAN     NOT NULL DEFAULT TRUE,
    consecutive_failures INTEGER     NOT NULL DEFAULT 0 CHECK (consecutive_failures >= 0),
    disabled_reason      TEXT        CHECK (disabled_reason IS NULL OR char_length(disabled_reason) <= 200),
    last_success_at      TIMESTAMPTZ,
    last_failure_at      TIMESTAMPTZ,
    last_error           TEXT        CHECK (last_error IS NULL OR char_length(last_error) <= 200),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK ((secret_ciphertext IS NULL) = (secret_nonce IS NULL)
       AND (secret_ciphertext IS NULL) = (secret_key_id IS NULL)),
    UNIQUE (user_id, url)
);

CREATE INDEX webhook_destinations_user_id ON webhook_destinations (user_id);
