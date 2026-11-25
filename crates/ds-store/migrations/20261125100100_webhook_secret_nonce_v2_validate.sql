SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Validates the CHECK 20261125100000_webhook_secret_nonce_v2.sql added NOT
-- VALID. VALIDATE CONSTRAINT takes SHARE UPDATE EXCLUSIVE on
-- webhook_destinations (reads and writes carry on). Every existing row has a
-- 12-byte nonce or NULL, so none can fail.
-- -------------------------------------------------------------------------

ALTER TABLE webhook_destinations VALIDATE CONSTRAINT webhook_destinations_secret_nonce_check;
