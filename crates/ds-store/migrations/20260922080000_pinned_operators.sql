-- -------------------------------------------------------------------------
-- pinned_operators: per-user operator pins backing the homepage's "Your
-- Operators" section and /operators' PinToggle. See
-- docs/superpowers/specs/2026-09-22-operator-overview-design.md §C/§E and
-- docs/superpowers/plans/2026-09-22-operator-overview-phase3-operators-list-and-pinning-plan.md.
--
-- Exact mirror of pinned_lines/pinned_stations AS THEY EXIST TODAY -- i.e.
-- already carrying the user_id ownership column and composite primary key
-- 20260828100000_add_ownership.sql retrofitted onto those two tables. This
-- table has no "pre-ownership" era to retrofit: operator pinning is new as
-- of this migration, so it is created directly in its final shape.
--
-- operator_code is TEXT, not CHAR(2) (unlike pinned_stations.crs, always
-- exactly 3 characters): a pinned code is either a real ATOC code
-- (tocs.atoc_code, 2 characters) or the literal synthetic string "TfL" (3
-- characters, common::TFL_OPERATOR) -- the column's two valid shapes are
-- different lengths, so a fixed-width CHAR column would not fit both.
-- -------------------------------------------------------------------------

CREATE TABLE pinned_operators (
    user_id        TEXT        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    operator_code  TEXT        NOT NULL,
    pinned_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, operator_code)
);
