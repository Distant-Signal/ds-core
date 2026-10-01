SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- schedule_destination_departures: passenger direction per row. See
-- docs/superpowers/specs/2026-10-01-working-vs-public-times-design.md §10
-- ("Journeys leg search") and §11.
--
-- The product used to hold only BOARDABLE calls, so a journey leg could not
-- END at a set-down-only (`D`) stop: the leg search finds its destination as
-- a later row of the same train. Set-down-only calls are now published too,
-- flagged:
--
-- * can_board: a passenger may board here. FALSE on a set-down-only row;
--   every reader that offers a row as a train to catch FROM origin_crs
--   (`/public/trains/search`, the journey leg search, the notifier's
--   auto-commit search) filters on it.
-- * can_alight: a passenger may get off here. FALSE on a pick-up-only (`U`)
--   row and the schedule's origin; the leg searches' destination side
--   filters on it.
--
-- NULL on a row published before this migration: read as TRUE (the
-- behaviour before), until the next schedule publish rewrites each service
-- date. Nullable, no default, catalog-only.
-- -------------------------------------------------------------------------
ALTER TABLE schedule_destination_departures
    ADD COLUMN can_board BOOLEAN,
    ADD COLUMN can_alight BOOLEAN;
