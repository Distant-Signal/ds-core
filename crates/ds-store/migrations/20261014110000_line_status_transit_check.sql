SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Transit line ids on `line_status` (transit spec §8.5, plan 4.1).
--
-- The ingest-writer writes each transit network's line statuses with
-- `source = 'transit'` and `line_id = '<network>:<line>'`
-- (`common::transit::QualifiedLineId`). One `source` serves every network,
-- so the network lives in the id; this CHECK keeps every transit row
-- qualified, and so the handler's per-network prune
-- (`line_id LIKE '<network>:%'`) and the readers' filters exact.
--
-- Added NOT VALID: only the catalog changes here (a brief ACCESS EXCLUSIVE
-- lock, hence the timeout), and new writes are checked at once. No
-- existing row has `source = 'transit'`, so the next migration's VALIDATE
-- cannot fail; it scans under SHARE UPDATE EXCLUSIVE, so the aggregator
-- and the TfL handler keep writing meanwhile.
--
-- Expand-only: every existing writer's rows already satisfy it.
-- -------------------------------------------------------------------------
ALTER TABLE line_status
    ADD CONSTRAINT line_status_transit_line_id_check
    CHECK (source <> 'transit' OR line_id ~ '^[a-z0-9-]+:[a-z0-9-]+$')
    NOT VALID;
