SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Validates the delay threshold CHECK 20261026110000 added NOT VALID.
-- VALIDATE CONSTRAINT takes only SHARE UPDATE EXCLUSIVE, so the api keeps
-- writing while the (few) rows are scanned. The column is new and every
-- write since was checked, so this cannot fail.
-- -------------------------------------------------------------------------

ALTER TABLE notification_preferences
    VALIDATE CONSTRAINT notification_preferences_delay_threshold_check;
