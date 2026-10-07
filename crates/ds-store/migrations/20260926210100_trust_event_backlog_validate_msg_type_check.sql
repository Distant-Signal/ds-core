-- -------------------------------------------------------------------------
-- Validates the widened `trust_event_backlog_msg_type_check` added NOT
-- VALID by the previous migration. VALIDATE CONSTRAINT takes only SHARE
-- UPDATE EXCLUSIVE, so the live backlog ingest keeps writing while the
-- existing rows are scanned. Every existing row already satisfied the
-- narrower ('0001', '0002', '0003') check, so this cannot fail.
-- -------------------------------------------------------------------------
ALTER TABLE trust_event_backlog
    VALIDATE CONSTRAINT trust_event_backlog_msg_type_check;
