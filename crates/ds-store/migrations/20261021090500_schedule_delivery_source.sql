SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Which delivery source the recorded copy came from (the GCS landing spec,
-- docs/superpowers/specs/2026-10-02-schedule-feed-gcs-landing-design.md
-- §9 "Records"; DS-89): `sftp` (the SFTP push) or `bucket` (the GCS bucket
-- source). With both sources on, each delivery arrives twice and is
-- recorded once, from whichever copy was accepted, so this says which.
--
-- Additive only: both columns are nullable, so ADD COLUMN changes only the
-- catalog. Rows from before this migration, and from an older
-- schedule-ingest, are NULL (all of them came over SFTP, the only source
-- there was). The CHECKs are added NOT VALID, as in
-- 20261001150000_schedule_feed_delivery_sha256.sql: a brief ACCESS
-- EXCLUSIVE for the catalog only (hence the timeout), enforced on every new
-- and updated row at once. Every existing row is NULL in the new column,
-- which a CHECK accepts, so there is nothing to validate.
-- -------------------------------------------------------------------------

ALTER TABLE schedule_feed_ingests
    ADD COLUMN delivery_source TEXT;

ALTER TABLE schedule_feed_ingests
    ADD CONSTRAINT schedule_feed_ingests_delivery_source_check
        CHECK (delivery_source IN ('sftp', 'bucket')) NOT VALID;

ALTER TABLE corpus_deliveries
    ADD COLUMN delivery_source TEXT;

ALTER TABLE corpus_deliveries
    ADD CONSTRAINT corpus_deliveries_delivery_source_check
        CHECK (delivery_source IN ('sftp', 'bucket')) NOT VALID;
