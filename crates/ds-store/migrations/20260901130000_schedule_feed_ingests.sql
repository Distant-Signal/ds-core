-- -------------------------------------------------------------------------
-- Records each successfully-verified schedule-feed delivery, per
-- docs/superpowers/specs/2026-09-01-schedule-feed-push-design.md's Database
-- bookkeeping section (reused directly from the pull design doc, unchanged
-- -- this table's shape doesn't depend on how the files arrived).
-- `files` is a JSONB array of {name, bytes} -- the per-file sizes
-- schedule-ingest itself observed on disk once stable, NOT a manifest-
-- declared size (the real manifest has no such field -- see this plan's own
-- Task 3 research note).
-- -------------------------------------------------------------------------

CREATE TABLE schedule_feed_ingests (
    sequence INTEGER PRIMARY KEY,
    ingested_at TIMESTAMPTZ NOT NULL,
    files JSONB NOT NULL
);
