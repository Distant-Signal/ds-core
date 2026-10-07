-- Drop the legacy uid column.
--
-- contract: drop trains.legacy_uid (code stopped using it in 0123abc)
SET LOCAL lock_timeout = '5s';

ALTER TABLE trains DROP COLUMN legacy_uid;
DROP VIEW legacy_view;
