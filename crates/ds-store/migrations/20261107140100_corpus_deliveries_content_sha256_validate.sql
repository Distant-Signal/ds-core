SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Validates the CHECKs 20261107140000_corpus_deliveries_content_sha256.sql
-- added NOT VALID. VALIDATE CONSTRAINT takes SHARE UPDATE EXCLUSIVE on
-- corpus_deliveries (a few dozen rows; reads and writes carry on). Every
-- existing row has a NULL content_sha256 and a redelivery_count of 0, so
-- none can fail.
-- -------------------------------------------------------------------------

ALTER TABLE corpus_deliveries VALIDATE CONSTRAINT corpus_deliveries_content_sha256_check;
ALTER TABLE corpus_deliveries VALIDATE CONSTRAINT corpus_deliveries_redelivery_count_check;
