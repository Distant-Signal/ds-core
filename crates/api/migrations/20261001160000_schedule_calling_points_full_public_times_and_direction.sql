SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- schedule_calling_points_full: public times, exact working times and
-- direction per calling point. See
-- docs/superpowers/specs/2026-10-01-working-vs-public-times-design.md
-- (P2, P3 and the detailed-view decision).
--
-- * public_arrival / public_departure: the CIF public (GBTT) times, what a
--   passenger timetable shows. NULL in a direction with no public call
--   (CIF `0000`), e.g. the departure of a set-down-only stop.
-- * working_arrival / working_departure / working_pass: the exact working
--   (WTT) times, with `:30` seconds for a CIF half-minute. booked_arrival /
--   booked_departure stay the WTT time truncated to the minute, unchanged.
--   working_pass is set only on a passing point (no arrival or departure).
-- * can_board / can_alight / request_stop: from the CIF Activity field.
--   Set-down-only (`D`) stops are now published (can_board = false);
--   pick-up-only (`U`) stops carry can_alight = false.
--
-- All nullable, no default: ADD COLUMN is catalog-only. Rows published
-- before this migration read NULL until the next schedule-reference publish
-- replaces each service date (it republishes today + 7 days every cycle);
-- readers treat a NULL flag as "not known" and fall back to the old
-- behaviour (boardable and alightable except at the origin/terminus).
-- -------------------------------------------------------------------------
ALTER TABLE schedule_calling_points_full
    ADD COLUMN public_arrival TIME,
    ADD COLUMN public_departure TIME,
    ADD COLUMN working_arrival TIME,
    ADD COLUMN working_departure TIME,
    ADD COLUMN working_pass TIME,
    ADD COLUMN can_board BOOLEAN,
    ADD COLUMN can_alight BOOLEAN,
    ADD COLUMN request_stop BOOLEAN;
