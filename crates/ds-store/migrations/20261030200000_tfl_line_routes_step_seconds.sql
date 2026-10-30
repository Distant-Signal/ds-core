SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Timetabled running times along TfL's routes (DS-206): poller-tfl's
-- routes job now also reads each route's `GET /Line/{id}/Timetable/{from}/
-- to/{to}`, and the api's Tube access legs time a ride by them instead of
-- by distance alone.
--
--   step_seconds   step_seconds[i] is the running time in seconds from
--                  stop_ids[i] to stop_ids[i + 1] (the wait at stop_ids[i]
--                  included), so one fewer than the stops; an element is
--                  NULL where TfL's timetable has no train calling at both
--                  in turn, and the column is NULL when the route's
--                  timetable could not be fetched (and for every row an
--                  older poller wrote). The api falls back to its distance
--                  estimate for each step without a time.
--
-- Expand-only: the column is nullable with no default, so the ADD COLUMN
-- changes the catalog only, and the CHECK goes in NOT VALID (no scan under
-- the ACCESS EXCLUSIVE, which the 5 s lock_timeout bounds);
-- 20261030200100 validates it. Code before this change never reads the
-- column, and the writer before it never writes it.
-- -------------------------------------------------------------------------

ALTER TABLE tfl_line_routes
    ADD COLUMN step_seconds INTEGER[],
    ADD CONSTRAINT tfl_line_routes_step_seconds_check CHECK (
        step_seconds IS NULL
        OR (
            cardinality(step_seconds) = cardinality(stop_ids) - 1
            AND 0 <= ALL (step_seconds)
            AND 3600 >= ALL (step_seconds)
        )
    ) NOT VALID;
