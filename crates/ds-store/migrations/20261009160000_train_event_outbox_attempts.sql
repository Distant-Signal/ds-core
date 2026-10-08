SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- train_event_outbox.attempts (security review L1, 2026-10-08): how many
-- of the ingest-writer's outbox ticks failed on this row with an error that
-- is not a data error (a lock or statement timeout, a serialization
-- failure, ...). Such an error used to roll the whole tick back, so one row
-- that always failed held up every row behind it forever. The writer now
-- counts the failure on the row and, at INGEST_WRITER_OUTBOX_MAX_ATTEMPTS
-- (5), marks it rejected like a data error.
--
-- Expand-only: a NOT NULL column with a constant default is a catalog-only
-- change (no table rewrite); the writer before this change never reads or
-- writes it.
-- -------------------------------------------------------------------------
ALTER TABLE train_event_outbox
    ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0;
