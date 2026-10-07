-- -------------------------------------------------------------------------
-- The closed-day audit row gains the windowed-stats breakdown (design
-- section 7.2). Constant defaults, so Postgres records them in the catalog
-- and does not rewrite the table; the ALTER only needs its ACCESS
-- EXCLUSIVE lock for the catalog change, and lock_timeout makes a wait
-- behind a long transaction fail fast (the migration reruns on the next
-- api start).
--
-- `stats_version = 1` marks every existing row as the legacy
-- whole-population method (every unseen train counted as cancelled), so
-- the comparison tool never compares v1 and v2 rows as like with like.
-- full-coverage-consumer writes version 2 only with
-- FULL_COVERAGE_WINDOWED_STATS=true (off by default).
--
-- VERSION SKEW: an older api never names these columns, so its upserts
-- keep working (new rows get the defaults). An older consumer posts no
-- breakdown; this api then writes the defaults.
-- -------------------------------------------------------------------------
SET LOCAL lock_timeout = '5s';

ALTER TABLE full_coverage_line_stats
    ADD COLUMN IF NOT EXISTS cancelled_explicit INT      NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cancelled_presumed INT      NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS pending            INT      NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS unobserved         INT      NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS stats_version      SMALLINT NOT NULL DEFAULT 1;
