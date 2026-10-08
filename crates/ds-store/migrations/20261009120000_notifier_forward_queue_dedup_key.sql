SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- notifier_forward_queue.dedup_key (ingest architecture plan 3b.3, D1):
-- trust-consumer's direct DB sink writes its train events and their
-- forward signals in one transaction and XACKs movement-events only after
-- the commit. A crash or a failed XACK after the commit redelivers the
-- entry; the train events are idempotent by their own dedup_key, and this
-- key makes the forward signal idempotent too: `<trains_id>:<dedup_key of
-- the movement that raised it>`, unique where set
-- (20261009120100_notifier_forward_queue_dedup_key_index.sql), inserted
-- with ON CONFLICT DO NOTHING.
--
-- NULL for every row written before this column existed and for a signal
-- whose sender sends no key (an older trust-consumer). Nullable, no
-- default, catalog-only.
-- -------------------------------------------------------------------------
ALTER TABLE notifier_forward_queue ADD COLUMN dedup_key TEXT;
