SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Validates the three range CHECKs 20261106190000_leave_by.sql added NOT
-- VALID. VALIDATE CONSTRAINT takes SHARE UPDATE EXCLUSIVE (reads and
-- writes carry on). Every existing row has NULL buffers and a 0 lead, so
-- none can fail.
-- -------------------------------------------------------------------------

ALTER TABLE journey_templates
    VALIDATE CONSTRAINT journey_templates_station_buffer_minutes_check;
ALTER TABLE journeys
    VALIDATE CONSTRAINT journeys_station_buffer_minutes_check;
ALTER TABLE notification_preferences
    VALIDATE CONSTRAINT notification_preferences_leave_by_lead_minutes_check;
