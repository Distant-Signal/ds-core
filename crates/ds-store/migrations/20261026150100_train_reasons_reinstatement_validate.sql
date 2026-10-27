SET LOCAL lock_timeout = '5s';

-- Validates the CHECK the previous migration added NOT VALID (DS-181).
-- VALIDATE takes SHARE UPDATE EXCLUSIVE, so writes carry on while it scans.
ALTER TABLE train_reasons VALIDATE CONSTRAINT train_reasons_msg_type_check;
