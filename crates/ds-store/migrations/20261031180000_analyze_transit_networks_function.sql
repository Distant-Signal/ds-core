SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- analyze_transit_networks(): ANALYZE transit_networks, with the rights of
-- this function's owner.
--
-- transit_networks holds one row per network (metrolink, tfl, net, ...),
-- written once when a network's first feed version activates and then only
-- updated. It never crosses autovacuum's analyze threshold (50 rows plus
-- 10% by default), so Postgres 16 never analysed it: the planner assumed
-- its default guess of 340 rows against 2 real ones, which fed the plans
-- that timed /transit/tfl out (DS-223). The ingest-writer now calls this
-- right after its activation inserts a NEW network row
-- (ds_store::transit::commit), so the row count the planner sees follows
-- the network set, and a re-import of a known network analyzes nothing.
--
-- Why a function rather than a plain ANALYZE from the writer: on Postgres
-- 16 only the table's owner (or a superuser) may ANALYZE it -- the MAINTAIN
-- privilege (and pg_maintain) came in 17 -- and the writer connects as its
-- own narrowed role (distant_signal_writer). A non-owner's ANALYZE does not
-- even fail: it logs "WARNING: permission denied to analyze ..., skipping
-- it" and succeeds, so the statistics would silently never move. Same
-- pattern as analyze_publish_keys (20261001140000). Once every database is
-- on 17+, `GRANT MAINTAIN ON transit_networks TO distant_signal_writer`
-- could replace it.
--
-- SECURITY DEFINER, so it runs as its owner (the schema owner role, which
-- runs migrations). It only ever analyzes public.transit_networks, takes no
-- argument, and has a fixed search_path, so it hands the caller nothing
-- else. EXECUTE is revoked from PUBLIC; the writer gets it from
-- charts/distant-signal/files/db-grants.yaml. A superuser needs no grant.
--
-- It runs inside the caller's transaction (ANALYZE may; VACUUM may not),
-- holding SHARE UPDATE EXCLUSIVE on transit_networks to COMMIT: that blocks
-- no read or write, only another ANALYZE, VACUUM or DDL of the table. A
-- row the transaction itself inserted counts as live in the sample.
-- -------------------------------------------------------------------------
CREATE FUNCTION analyze_transit_networks()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
    ANALYZE public.transit_networks;
END
$$;

REVOKE ALL ON FUNCTION analyze_transit_networks() FROM PUBLIC;

-- And once now, for a database whose networks were all inserted before
-- this (prod had a manual ANALYZE on 2026-10-31; others may have none).
-- Two-row table: instant, and SHARE UPDATE EXCLUSIVE blocks no reads or
-- writes.
ANALYZE transit_networks;
