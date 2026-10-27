SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Validates the quiet hours CHECK 20261026110200 added NOT VALID, under
-- SHARE UPDATE EXCLUSIVE, so the api keeps writing while the (few) rows
-- are scanned. The columns are new and every write since was checked, so
-- this cannot fail.
-- -------------------------------------------------------------------------

ALTER TABLE notification_preferences
    VALIDATE CONSTRAINT notification_preferences_quiet_hours_check;
