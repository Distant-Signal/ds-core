SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Validates the two checks 20261106120000_ireland_train_tracking.sql added
-- NOT VALID. VALIDATE CONSTRAINT takes SHARE UPDATE EXCLUSIVE (reads and
-- writes carry on). Every existing row has NULL in all four columns, which
-- were added in that migration, so neither check can fail.
-- -------------------------------------------------------------------------

ALTER TABLE trains VALIDATE CONSTRAINT trains_endpoint_names_check;
ALTER TABLE train_subscriptions VALIDATE CONSTRAINT train_subscriptions_pin_ref_check;
