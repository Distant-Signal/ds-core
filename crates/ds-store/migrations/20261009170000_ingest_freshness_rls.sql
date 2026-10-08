SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Row-level security on `ingest_freshness` (security review L4,
-- 2026-10-08).
--
-- Every producer with a write grant here (the ingest-writer, poller-stations'
-- and poller-incidents' narrow roles) could upsert ANY source's row, and
-- `record_ingest` keeps GREATEST(stored, new): a compromised poller could
-- push another feed's `fetched_at` into the future and hide that feed's
-- staleness from DistantSignalIngestSourceStale and /public/freshness.
-- The chart's grants script (charts/distant-signal/files/db-grants.yaml
-- `row_policies`, rendered into postgres-grants.sql) gives each of those
-- roles RESTRICTIVE INSERT/UPDATE/DELETE policies pinning it to its own
-- source(s); its reads stay unrestricted.
--
-- This migration is the half every role shares, and it is
-- behaviour-neutral, as 20261009131300_line_status_rls.sql: it enables RLS
-- and adds ONE permissive policy for PUBLIC that allows every row for every
-- command, so every role (the api, the aggregator, the app role, and the
-- producers before their restrictive policies exist) sees and writes
-- exactly what it did before. Both statements run in this one transaction,
-- so no session ever sees RLS enabled without the permissive policy (which
-- would hide every row).
--
-- Not affected: the table owner (the migrations) and superusers bypass RLS.
--
-- ENABLE ROW LEVEL SECURITY and CREATE POLICY change only the catalog; they
-- take an ACCESS EXCLUSIVE lock on this one-row-per-feed table for that,
-- hence the lock timeout above.
-- -------------------------------------------------------------------------
ALTER TABLE ingest_freshness ENABLE ROW LEVEL SECURITY;

CREATE POLICY ingest_freshness_every_row ON ingest_freshness
    AS PERMISSIVE FOR ALL TO PUBLIC
    USING (true) WITH CHECK (true);
