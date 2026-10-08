SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- The incidents write-amplification fix (ingest architecture plan 2c.5;
-- spec §9.4). Every incidents poll used to bump `incidents.fetched_at` on
-- every listed row (about 600k row updates a day for a ~2k-row table). With
-- INCIDENTS_ROW_HEARTBEAT=false the snapshot instead stamps the feed's time
-- once, here, and readers derive a listed incident's display time from it
-- (`ds_store::incidents::FETCHED_AT_SQL`):
--
-- * `last_snapshot_at`: when the latest non-empty snapshot was applied (all
--   of its chunks committed). The display time of every uncleared incident
--   that snapshot listed.
-- * `previous_snapshot_at`: the snapshot before that one. The removal
--   inference stamps it as `fetched_at` on an incident's first miss: the
--   last snapshot that listed it, which is what `fetched_at` meant before.
--
-- Both stay NULL while INCIDENTS_ROW_HEARTBEAT is true (today's behaviour,
-- the default), and `GREATEST` ignores a NULL, so the readers return
-- exactly the per-row time until the switch is turned off.
--
-- Expand-only: two nullable columns with no default on a one-row table, a
-- catalog-only change (crates/api/tests/migration_index_locking.rs).
-- -------------------------------------------------------------------------
ALTER TABLE incident_feed_state ADD COLUMN last_snapshot_at TIMESTAMPTZ;

ALTER TABLE incident_feed_state ADD COLUMN previous_snapshot_at TIMESTAMPTZ;
