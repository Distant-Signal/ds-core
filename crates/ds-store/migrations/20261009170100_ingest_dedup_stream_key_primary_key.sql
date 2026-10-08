-- contract: ingest_dedup's primary key on (key) alone (code stopped using it in 9cb89d89)
-- -------------------------------------------------------------------------
-- ingest_dedup keyed by (stream, key): the contract step of security review
-- L2 (2026-10-08). 20261009160100_ingest_dedup_stream_key_index.sql built
-- the unique index ingest_dedup_stream_key (stream, key) concurrently, and
-- the writer's claim has been `ON CONFLICT (stream, key) DO NOTHING` since
-- 9cb89d89. The old primary key on `key` alone still made a key claimed on
-- one stream a unique violation on another; this drops it and promotes the
-- existing index to the primary key, so each stream's keys are independent.
--
-- Ship only once no writer older than 9cb89d89 runs: those claim with
-- `ON CONFLICT (key)`, which needs a unique index on `key` alone and fails
-- (42P10, retried as transient: their streams stall) once it is gone.
--
-- One ALTER TABLE, so both happen atomically and a unique index on
-- (stream, key) exists throughout: the running writers' ON CONFLICT
-- (stream, key) infers the old index before and the primary key after.
-- PRIMARY KEY USING INDEX renames the index to the constraint's name and
-- builds nothing. It takes an ACCESS EXCLUSIVE lock on ingest_dedup for
-- the catalog change (claims wait for it, briefly), plus, where dropping the
-- old key cleared `key`'s NOT NULL, a scan to set it again: the table holds
-- at most 48 hours of keys (the writer's hourly prune). If the index is
-- missing or INVALID (an interrupted concurrent build) this fails and
-- changes nothing; rebuild it (see that migration) and retry.
-- -------------------------------------------------------------------------
SET LOCAL lock_timeout = '5s';

ALTER TABLE ingest_dedup
    DROP CONSTRAINT ingest_dedup_pkey,
    ADD CONSTRAINT ingest_dedup_pkey PRIMARY KEY USING INDEX ingest_dedup_stream_key;
