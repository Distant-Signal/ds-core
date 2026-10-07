-- -------------------------------------------------------------------------
-- Replaces schedule_feed_ingests' `sequence INTEGER PRIMARY KEY` with
-- `delivered_at TIMESTAMPTZ PRIMARY KEY`.
--
-- Correction (see
-- docs/superpowers/specs/2026-09-03-schedule-feed-zip-delivery-correction.md,
-- confirmed directly by the repo owner, 2026-09-03): the real CIF SCHEDULE
-- feed delivery is a single zip file, overwritten in place, with no
-- manifest and no sequence number -- the `sequence` this table's original
-- migration (20260901130000_schedule_feed_ingests.sql) assumed never
-- existed for a real delivery. `delivered_at` is the delivery zip's own
-- mtime (not `Utc::now()` at process time, which is what `ingested_at`
-- below now means) -- the one real, stable identifier a plain-overwrite
-- delivery has.
--
-- Clean drop-and-recreate, not a data migration: this table has never
-- successfully received a real row (nothing has ever matched the old
-- manifest format), so there is no production data to preserve.
-- -------------------------------------------------------------------------

DROP TABLE schedule_feed_ingests;

CREATE TABLE schedule_feed_ingests (
    delivered_at TIMESTAMPTZ PRIMARY KEY,
    ingested_at TIMESTAMPTZ NOT NULL,
    files JSONB NOT NULL
);
