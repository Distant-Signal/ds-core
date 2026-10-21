SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Stop ids on untracked legs (docs/superpowers/specs/
-- 2026-10-20-tfl-stop-lookup-design.md §4): the TfL stop (`tfl_stops.
-- stop_id`, a NaPTAN id such as 940GZZLUEUS) an untracked leg starts or
-- ends at, when its place was picked from the stop lookup. NULL for a
-- free-text place and for every train leg.
--
-- Additive only: two nullable columns (a catalog-only ADD COLUMN, no
-- rewrite) and a CHECK added NOT VALID (new writes are checked at once),
-- validated by the next migration under SHARE UPDATE EXCLUSIVE. Every
-- existing row has both columns NULL, so it cannot fail. The untracked
-- shape CHECK (journey_legs_untracked_shape_check) says nothing about these
-- columns and is left alone. No foreign key: a stop can leave TfL's data
-- while a saved leg keeps its id.
-- -------------------------------------------------------------------------
ALTER TABLE journey_legs
    ADD COLUMN origin_stop_id TEXT,
    ADD COLUMN destination_stop_id TEXT;

ALTER TABLE journey_legs
    ADD CONSTRAINT journey_legs_stop_ids_check
    CHECK (
        (kind = 'untracked'
            OR (origin_stop_id IS NULL AND destination_stop_id IS NULL))
        AND (origin_stop_id IS NULL OR origin_stop_id ~ '^[0-9A-Z]{4,20}$')
        AND (destination_stop_id IS NULL OR destination_stop_id ~ '^[0-9A-Z]{4,20}$')
    )
    NOT VALID;
