SET LOCAL lock_timeout = '5s';
-- -------------------------------------------------------------------------
-- `ingest_dedup`: the ingest-writer's idempotency keys (ingest architecture
-- spec §7.4, plan 3a.3). Every stream entry the writer applies inserts its
-- envelope `key` here in the same transaction as its write; a key already
-- present (`ON CONFLICT DO NOTHING` returning nothing) means the entry was
-- already applied, so the writer skips and acks it. That makes a
-- redelivery (a lost XACK, an XAUTOCLAIM after a crash) apply once even for
-- the schemas that are not naturally idempotent.
--
-- The writer prunes rows older than 48 hours hourly (applied_at, indexed in
-- the next migration). Expand-only: nothing reads or writes it until a
-- writer stream is set to `apply` (INGEST_WRITER_STREAMS, default off).
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ingest_dedup (
    key        TEXT        PRIMARY KEY,
    stream     TEXT        NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
