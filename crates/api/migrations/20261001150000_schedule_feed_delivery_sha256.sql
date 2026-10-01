SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Provenance of each delivered file (docs/schedule-feed-sftp.md, Ranma's
-- sftp-audit-observability spec): schedule-ingest now hashes every file the
-- SFTP push delivers, so "which bytes did we accept" can be answered later
-- and joined to SFTPGo's Upload log line by name, size and time.
--
-- schedule_feed_ingests: the delivered zip itself. The extracted entries'
-- hashes go into each element of the existing `files` JSONB array
-- ({name, bytes, sha256}), which needs no DDL.
-- corpus_deliveries: the CORPUS file (its name is already source_file).
--
-- All nullable: rows from before this migration have no hash, and an older
-- schedule-ingest still posts without one. The CHECKs keep whatever is
-- written a lowercase hex SHA-256 and a non-negative size. They are added
-- NOT VALID, so adding them scans nothing under this migration's lock
-- (crates/api/tests/migration_index_locking.rs); they are enforced on every
-- new and updated row all the same. The only existing rows are NULL in the
-- new columns, which a CHECK accepts, so there is nothing to validate.
-- -------------------------------------------------------------------------

ALTER TABLE schedule_feed_ingests
    ADD COLUMN source_file TEXT,
    ADD COLUMN source_bytes BIGINT,
    ADD COLUMN source_sha256 TEXT;

ALTER TABLE schedule_feed_ingests
    ADD CONSTRAINT schedule_feed_ingests_source_bytes_check
        CHECK (source_bytes >= 0) NOT VALID,
    ADD CONSTRAINT schedule_feed_ingests_source_sha256_check
        CHECK (source_sha256 ~ '^[0-9a-f]{64}$') NOT VALID;

ALTER TABLE corpus_deliveries
    ADD COLUMN source_bytes BIGINT,
    ADD COLUMN sha256 TEXT;

ALTER TABLE corpus_deliveries
    ADD CONSTRAINT corpus_deliveries_source_bytes_check
        CHECK (source_bytes >= 0) NOT VALID,
    ADD CONSTRAINT corpus_deliveries_sha256_check
        CHECK (sha256 ~ '^[0-9a-f]{64}$') NOT VALID;
