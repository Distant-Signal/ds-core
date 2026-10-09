SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Drop the legacy, shared island-of-Ireland `ingest_freshness` rows.
--
-- Before the per-network split (2026-10-08) both Irish catalogue pollers'
-- streams recorded `island_of_ireland_stations` and `island_of_ireland_lines`.
-- Since then each network records its own `_gtfs`/`_nir` source, and the
-- shared names were kept one release only so a writer still on the old
-- release kept applying (the writer's RESTRICTIVE ingest_freshness policy in
-- charts/distant-signal/files/db-grants.yaml) and the readers
-- (`ds_store::samples::island_of_ireland::last_*_fetch`) took the newest of
-- the three. That release is live; this release stops allowing and reading
-- them, so their rows would only age, and the writer's freshness gauge would
-- keep exporting them.
--
-- Idempotent: deleting rows that are already gone deletes nothing. Runs as
-- the table owner, which RLS does not restrict. Two rows at most, under a
-- ROW EXCLUSIVE lock that blocks no reader or other writer.
-- -------------------------------------------------------------------------
DELETE FROM ingest_freshness
WHERE source IN ('island_of_ireland_stations', 'island_of_ireland_lines');
