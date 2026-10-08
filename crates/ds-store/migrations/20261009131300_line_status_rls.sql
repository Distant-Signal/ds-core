SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Row-level security on `line_status` (ingest plan 3c.3, spec §6.4,
-- decision D10).
--
-- The ingest-writer (TfL, `source = 'tfl'`) and the aggregator (every other
-- line) both write this table. Once the writer applies the `tfl` stream,
-- Postgres itself must stop it touching a row it does not own. That is a
-- RESTRICTIVE policy for the writer's role, `USING` and `WITH CHECK`
-- `source = 'tfl'`, which the chart's grants script creates
-- (charts/distant-signal/files/db-grants.yaml `row_policies`, rendered into
-- postgres-grants.sql), because only that script knows the role's name.
--
-- This migration is the half every role shares, and it is
-- behaviour-neutral: it enables RLS and adds ONE permissive policy for
-- PUBLIC that allows every row for every command, so every role (the api,
-- the aggregator, the notifier, the app role, the writer before its
-- restrictive policy exists) sees and writes exactly what it did before.
-- Both statements run in this one transaction, so no session ever sees RLS
-- enabled without the permissive policy (which would hide every row).
--
-- Why a restrictive policy for the writer rather than permissive per-role
-- policies: until phase 5 the writer is a member of the shared app role, so
-- any permissive policy that covers the app role (or PUBLIC) also covers
-- the writer, and permissive policies are OR'd. A restrictive policy is
-- AND'd with them, so it narrows the writer whatever else applies.
--
-- Not affected: the table owner (the migrations) and superusers bypass RLS.
-- A role subject to RLS that runs `pg_dump` needs `--enable-row-security`
-- (pg_dump otherwise refuses a table with RLS enabled); with the policy
-- above that dump still holds every row.
--
-- ENABLE ROW LEVEL SECURITY and CREATE POLICY change only the catalog;
-- they take an ACCESS EXCLUSIVE lock on the table for that, hence the lock
-- timeout above.
-- -------------------------------------------------------------------------
ALTER TABLE line_status ENABLE ROW LEVEL SECURITY;

CREATE POLICY line_status_every_row ON line_status
    AS PERMISSIVE FOR ALL TO PUBLIC
    USING (true) WITH CHECK (true);
