SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- "Ended (no longer listed)": an incident that left RDM's Knowledgebase feed
-- without RDM ever setting ClearedIncident (user decision, 2026-10-06; see
-- docs/superpowers/specs/2026-10-06-incident-source-removal-design.md).
--
-- The KB feed purges incidents nightly (~22:57-23:00 UTC) whatever their
-- state, and never clears planned ones, so `NOT is_cleared` alone left 407
-- rows "active" forever on 2026-10-06 (9 unplanned, 398 planned).
-- `is_cleared` stays RDM's own fact; these columns record OUR observation
-- that the source stopped listing the row:
--
-- * `source_missing_polls`: consecutive COMPLETE poller snapshots this
--   incident was missing from. Reset to 0 whenever it is listed again
--   (complete snapshot or not); only advanced by a snapshot that passes
--   `upsert_incidents`' guard (complete, non-empty, not more than 50% smaller
--   than the previous complete one).
-- * `source_removed_at`: set once the counter reaches 2, to the row's
--   `fetched_at` (the last time the feed listed it). NULL means still listed
--   (or not yet confirmed gone). NULLed again if the incident reappears.
--
-- Both are catalog-only: a nullable column with no default, and a NOT NULL
-- column with a constant default, rewrite and scan nothing
-- (crates/api/tests/migration_index_locking.rs). No index: the inference
-- UPDATE and the archive's `state` filter both run alongside predicates the
-- existing indexes already serve, over a table of a few thousand rows.
-- -------------------------------------------------------------------------
ALTER TABLE incidents ADD COLUMN source_removed_at TIMESTAMPTZ;

ALTER TABLE incidents ADD COLUMN source_missing_polls SMALLINT NOT NULL DEFAULT 0;

-- -------------------------------------------------------------------------
-- The previous complete snapshot, for the shrink guard: inference is skipped
-- when a complete snapshot is more than 50% smaller than this one. One row,
-- upserted by every complete snapshot (inference applied or not, so a real
-- large purge only delays inference by one poll rather than blocking it for
-- good). `last_complete_at` is also what
-- scripts/backfill-2026-10-06-incident-source-removed.sql measures
-- "missing" against. A new, empty table: nothing to lock.
-- -------------------------------------------------------------------------
CREATE TABLE incident_feed_state (
    singleton          BOOLEAN     PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    last_complete_at   TIMESTAMPTZ NOT NULL,
    last_complete_size INTEGER     NOT NULL CHECK (last_complete_size >= 0)
);
