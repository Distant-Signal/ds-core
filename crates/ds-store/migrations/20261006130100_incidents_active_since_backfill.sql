SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Backfill `incidents.active_since` (previous migration) for the rows whose
-- cutoff it can change: live (`NOT is_cleared AND source_removed_at IS
-- NULL`), unplanned incidents. Planned rows are exempt from the cutoff, and
-- cleared or ended ones are not loaded at all, so they keep NULL and read as
-- `first_seen_at`.
--
-- The value is the latest "arming" event in `incident_history` (one row per
-- change of the feed's content, ordered by `recorded_at`): an uncleared
-- snapshot that is the first one, or follows a cleared one (a reopen), or
-- whose summary/description differs from the previous snapshot (a text
-- change). Never earlier than `first_seen_at`. A row with no such history
-- keeps NULL.
--
-- Bounded: production had 17 uncleared unplanned rows on 2026-10-06 (8 of
-- them still listed) out of 2,107, and 7,991 `incident_history` rows in
-- all; only the live rows' history is read, through
-- `incident_history_id_time`. Row locks only, in its own short
-- transaction; an api upsert that races it waits at most a moment.
-- -------------------------------------------------------------------------
WITH live AS (
    SELECT incident_id
      FROM incidents
     WHERE NOT is_cleared
       AND source_removed_at IS NULL
       AND NOT is_planned
),
snapshots AS (
    SELECT h.incident_id,
           h.recorded_at,
           h.is_cleared,
           LAG(h.is_cleared) OVER w AS prev_cleared,
           (h.summary, h.description)
               IS DISTINCT FROM (LAG(h.summary) OVER w, LAG(h.description) OVER w) AS text_changed,
           ROW_NUMBER() OVER w AS n
      FROM incident_history h
      JOIN live USING (incident_id)
    WINDOW w AS (PARTITION BY h.incident_id ORDER BY h.recorded_at, h.id)
),
armed AS (
    SELECT incident_id, MAX(recorded_at) AS armed_at
      FROM snapshots
     WHERE NOT is_cleared
       AND (n = 1 OR prev_cleared OR text_changed)
     GROUP BY incident_id
)
UPDATE incidents i
   SET active_since = GREATEST(i.first_seen_at, a.armed_at)
  FROM armed a
 WHERE i.incident_id = a.incident_id
   AND NOT i.is_cleared
   AND i.source_removed_at IS NULL
   AND NOT i.is_planned;
