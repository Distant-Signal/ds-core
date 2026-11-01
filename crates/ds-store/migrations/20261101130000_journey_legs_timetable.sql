SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Timetabled untracked legs (DS-237): a Tube, DLR, London tram or
-- Metrolink ride saved from a plan keeps which network and line it rides,
-- so the journey page can find it in the network's active timetable
-- (`transit_*`) by its content: the date, the line, the two stops and the
-- planned times. Trip ids are not a key: a feed version lasts about two
-- days (the horizon slides daily and only `active` and `previous` are
-- kept), and pattern and service ids are renumbered at every import.
--
--   transit_network   a `transit_networks.network` (`tfl`, `metrolink`).
--   transit_line_id   the network's local line id (`victoria`, `blue`).
--   headsign          where the train or tram was going, as planned.
--   timetable_hint    `{tripId, feedVersion}` of the trip matched when the
--                     leg was saved: a tie-break, never the key.
--
-- The same on journey_revision_legs, with the stop ids the revision
-- snapshot never kept, so going back to an earlier plan restores a leg's
-- stops and line (it dropped them before).
--
-- Still `kind = 'untracked'`: no subscription, no live data. Expand-only:
-- nullable columns change the catalog only, and the CHECKs go in NOT VALID
-- (no scan under the ACCESS EXCLUSIVE the 5 s lock_timeout bounds);
-- 20261101130100 validates them. Code before this change never reads or
-- writes the columns.
-- -------------------------------------------------------------------------

ALTER TABLE journey_legs
    ADD COLUMN transit_network TEXT,
    ADD COLUMN transit_line_id TEXT,
    ADD COLUMN headsign TEXT,
    ADD COLUMN timetable_hint JSONB,
    ADD CONSTRAINT journey_legs_timetable_check CHECK (
        (transit_network IS NULL
            AND transit_line_id IS NULL
            AND headsign IS NULL
            AND timetable_hint IS NULL)
        OR
        (kind = 'untracked'
            AND transit_network ~ '^[a-z0-9-]{1,32}$'
            AND transit_line_id ~ '^[a-z0-9-]{1,32}$'
            AND origin_stop_id IS NOT NULL
            AND destination_stop_id IS NOT NULL
            AND (headsign IS NULL OR char_length(headsign) BETWEEN 1 AND 100)
            AND (timetable_hint IS NULL OR jsonb_typeof(timetable_hint) = 'object'))
    ) NOT VALID;

ALTER TABLE journey_revision_legs
    ADD COLUMN origin_stop_id TEXT,
    ADD COLUMN destination_stop_id TEXT,
    ADD COLUMN transit_network TEXT,
    ADD COLUMN transit_line_id TEXT,
    ADD COLUMN headsign TEXT,
    ADD CONSTRAINT journey_revision_legs_timetable_check CHECK (
        (origin_stop_id IS NULL OR origin_stop_id ~ '^[0-9A-Z]{4,20}$')
        AND (destination_stop_id IS NULL OR destination_stop_id ~ '^[0-9A-Z]{4,20}$')
        AND (transit_network IS NULL OR transit_network ~ '^[a-z0-9-]{1,32}$')
        AND (transit_line_id IS NULL OR transit_line_id ~ '^[a-z0-9-]{1,32}$')
        AND (headsign IS NULL OR char_length(headsign) BETWEEN 1 AND 100)
    ) NOT VALID;
