SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Stage 2 of the secret box AAD rollout (DS-615, docs/secret-box-aad.md):
-- let `webhook_destinations.secret_nonce` hold a v2 value. A v2 sealed value
-- stores `0x02` then the 12-byte nonce (13 bytes); a v1 value stays 12 bytes.
-- The nonce length is the format version, so the CHECK allows exactly those
-- two lengths.
--
-- Relaxing only: every row that satisfied `= 12` satisfies `IN (12, 13)`, and
-- code that only writes 12-byte nonces (every build until the write format
-- flips) is unaffected. Nothing writes 13 bytes until a later ds-core release
-- flips `DEFAULT_WRITE_FORMAT`; that release must be deployed after this
-- migration and after every reader accepts v2.
--
-- The constraint name is the Postgres default for the inline column check
-- created by 20261109120000_webhook_destinations.sql; the DROP has no
-- IF EXISTS on purpose, so a database where it differs fails loudly here
-- instead of keeping the old check beside the new one. The replacement is
-- added NOT VALID (no scan under the ACCESS EXCLUSIVE lock) and validated by
-- 20261125100100_webhook_secret_nonce_v2_validate.sql. A NOT VALID check is
-- still enforced for every new write.
--
-- No grants change: the table's grants are unchanged and no object is added.
-- -------------------------------------------------------------------------

ALTER TABLE webhook_destinations
    DROP CONSTRAINT webhook_destinations_secret_nonce_check,
    ADD CONSTRAINT webhook_destinations_secret_nonce_check
        CHECK (secret_nonce IS NULL OR octet_length(secret_nonce) IN (12, 13)) NOT VALID;
