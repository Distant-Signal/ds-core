-- Step D's final, irreversible act
-- (docs/superpowers/specs/2026-09-06-shared-train-identity-design.md §2) --
-- gated on Task 22's own Step 1 dry-run row-count comparison, run manually
-- BEFORE this migration is ever applied to a real database. Bundles both
-- drops the design spec names together in its own closing paragraph: the
-- movement-table tracked_train_id columns, and tracked_trains' own
-- fully-retired legacy schedule/identity columns (Tasks 11/21 already
-- stopped writing every one of them; Task 8 already stopped reading them).

-- train_movement_events: drop the OLD dedup constraint before the column
-- it references, then the column itself. The new trains_id-keyed dedup
-- index (Task 9) is untouched -- it becomes this table's ONLY dedup
-- constraint from here on.
ALTER TABLE train_movement_events
    DROP CONSTRAINT IF EXISTS train_movement_events_tracked_train_id_dedup_key_key;
ALTER TABLE train_movement_events DROP COLUMN tracked_train_id;

-- train_current_state: drop the OLD partial-unique index (Task 9) before
-- the column it references, then the column itself. trains_id's own
-- partial unique index (Task 9) is untouched.
DROP INDEX IF EXISTS train_current_state_tracked_train_id;
ALTER TABLE train_current_state DROP COLUMN tracked_train_id;

-- tracked_trains: drop the OLD resolved-identity index before train_uid
-- (the column it references), then every retired legacy column.
DROP INDEX IF EXISTS tracked_trains_resolved_identity;
ALTER TABLE tracked_trains
    DROP COLUMN train_uid,
    DROP COLUMN train_id,
    DROP COLUMN matched_line_id,
    DROP COLUMN schedule_calling_points,
    DROP COLUMN schedule_destination_crs,
    DROP COLUMN schedule_matched_at,
    DROP COLUMN resolved_at;
