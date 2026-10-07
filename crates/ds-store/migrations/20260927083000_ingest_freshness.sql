SET LOCAL lock_timeout = '5s';
-- -------------------------------------------------------------------------
-- One row per poller-fed source recording when its last non-empty batch
-- landed (`queries::record_ingest`). Read by the `last_*_fetch` freshness
-- functions (the pollers' startup checks) and by `/public/freshness`.
--
-- Why: the upserts for `stations`, `tocs`, `incidents` and TfL
-- `line_status` gained no-op guards (DB review 2026-09-27 F3), so an
-- unchanged `stations`/`tocs` row is no longer rewritten and its
-- `fetched_at` now means "last changed" -- `MAX(fetched_at)` can no longer
-- answer "when did this feed last land". One row per source also makes
-- `/public/freshness` a single query of primary-key lookups instead of five
-- `MAX()` scans on five pooled connections per request (F10/F11).
--
-- Seeded from the per-row columns so freshness carries over the deploy.
-- -------------------------------------------------------------------------
CREATE TABLE ingest_freshness (
    source     TEXT        PRIMARY KEY,
    fetched_at TIMESTAMPTZ NOT NULL
);

INSERT INTO ingest_freshness (source, fetched_at)
SELECT 'stations', MAX(fetched_at) FROM stations HAVING MAX(fetched_at) IS NOT NULL
UNION ALL
SELECT 'tocs', MAX(fetched_at) FROM tocs HAVING MAX(fetched_at) IS NOT NULL
UNION ALL
SELECT 'incidents', MAX(fetched_at) FROM incidents HAVING MAX(fetched_at) IS NOT NULL
UNION ALL
SELECT 'tfl', MAX(computed_at) FROM line_status WHERE source = 'tfl'
    HAVING MAX(computed_at) IS NOT NULL;
