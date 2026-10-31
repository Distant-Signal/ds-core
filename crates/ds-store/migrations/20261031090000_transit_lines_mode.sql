SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- A transit line's own mode (docs/superpowers/specs/2026-10-30-tfl-transit-
-- design.md §6): the tfl network mixes the Underground, the DLR and London
-- Trams, so /transit/tfl groups its lines by mode.
--
--   mode   `tube`, `dlr`, `tram`, ...; NULL for a line whose network has
--          one mode (Metrolink, NET), which reads as the network's own.
--
-- Expand-only: the column is nullable with no default, so the ADD COLUMN
-- changes the catalog only, and the CHECK goes in NOT VALID (no scan under
-- the ACCESS EXCLUSIVE, which the 5 s lock_timeout bounds);
-- 20261031090100 validates it. Code before this change never reads the
-- column, and the writer before it never writes it.
-- -------------------------------------------------------------------------

ALTER TABLE transit_lines
    ADD COLUMN mode TEXT,
    ADD CONSTRAINT transit_lines_mode_check CHECK (
        mode IS NULL OR mode ~ '^[a-z-]{1,16}$'
    ) NOT VALID;
