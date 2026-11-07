SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- CORPUS redeliveries of identical content (DS-390).
--
-- The SFTP pusher can re-upload the same CORPUS file: from 2026-11-07 it
-- re-sent NLC20261106.xml.gz every hour, each copy with a new mtime and a
-- different file SHA-256 (a new gzip header) but the same rows. Each copy
-- was a new delivery (`delivered_at` is the file's mtime), so every hour
-- rewrote corpus_locations and the crosswalk, and the ingest-writer's
-- CORPUS loop re-ran the timetable comparison.
--
-- `content_sha256` is the SHA-256 of what a load applies: the normalised
-- rows, the file name and the kind of load
-- (`ds_store::corpus::content_sha256`). A load whose content matches the
-- last applied delivery's (the newest `loaded_at`) is a no-op: it writes
-- no rows and no new delivery, and only bumps that delivery's
-- `redelivered_at` (the newest identical copy's mtime) and
-- `redelivery_count`. Freshness reads the newer of `delivered_at` and
-- `redelivered_at`, so an unchanged CORPUS still reads as received.
--
-- Nullable: rows from before this migration, and loads by an older
-- schedule-ingest, have no content hash; such a delivery is never matched,
-- so the next load always applies. The CHECKs go on NOT VALID (validated in
-- 20261107140100); every existing row passes them.
-- -------------------------------------------------------------------------

ALTER TABLE corpus_deliveries
    ADD COLUMN content_sha256 TEXT,
    ADD COLUMN redelivered_at TIMESTAMPTZ,
    ADD COLUMN redelivery_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE corpus_deliveries
    ADD CONSTRAINT corpus_deliveries_content_sha256_check
        CHECK (content_sha256 ~ '^[0-9a-f]{64}$') NOT VALID,
    ADD CONSTRAINT corpus_deliveries_redelivery_count_check
        CHECK (redelivery_count >= 0) NOT VALID;
