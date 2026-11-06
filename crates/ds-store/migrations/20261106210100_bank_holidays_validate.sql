SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Validates the CHECK 20261106210000_bank_holidays.sql added NOT VALID.
-- VALIDATE CONSTRAINT takes SHARE UPDATE EXCLUSIVE on users (reads and
-- writes carry on). Every existing row has NULL in the new column, so none
-- can fail.
-- -------------------------------------------------------------------------

ALTER TABLE users VALIDATE CONSTRAINT users_bank_holiday_division_check;
