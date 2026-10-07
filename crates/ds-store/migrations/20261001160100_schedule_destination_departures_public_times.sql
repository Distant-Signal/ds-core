SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- schedule_destination_departures: the public (GBTT) counterparts of the
-- three working-timetable times each row already carries. See
-- docs/superpowers/specs/2026-10-01-working-vs-public-times-design.md (P3).
--
-- * public_departure: the public departure of this row's own call
--   (`scheduled` is the WTT one, and stays the key).
-- * public_calling_point_arrival: the public arrival at this row's own call
--   (counterpart of calling_point_arrival).
-- * public_destination_arrival: the public arrival at the schedule's
--   terminus (counterpart of destination_arrival; same day offset).
--
-- Nullable, no default, catalog-only. NULL until the next schedule publish
-- rewrites each service date.
-- -------------------------------------------------------------------------
ALTER TABLE schedule_destination_departures
    ADD COLUMN public_departure TIME,
    ADD COLUMN public_calling_point_arrival TIME,
    ADD COLUMN public_destination_arrival TIME;
