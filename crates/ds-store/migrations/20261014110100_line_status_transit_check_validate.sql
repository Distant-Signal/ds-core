SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Validates `line_status_transit_line_id_check`, added NOT VALID by the
-- previous migration (transit spec §8.5, plan 4.1). VALIDATE CONSTRAINT
-- takes only SHARE UPDATE EXCLUSIVE, so writers carry on while the rows
-- are scanned. No row had `source = 'transit'` before the check existed,
-- so this cannot fail.
-- -------------------------------------------------------------------------
ALTER TABLE line_status
    VALIDATE CONSTRAINT line_status_transit_line_id_check;
