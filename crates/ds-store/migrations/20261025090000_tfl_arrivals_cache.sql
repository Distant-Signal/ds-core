SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- The shared `TfL` arrivals cache (DS-136): one row per stop, written and
-- read by the api's `data::tfl_arrivals` so its replicas share one
-- upstream call per stop per cache period instead of one each. Each pod
-- keeps a short in-memory copy in front of it.
--
-- * `fetched_at`, `predictions`, `error`: the last answer (`TfL`'s
--   predictions, or the error that came back instead) and when it came.
--   All NULL on a row that so far only holds a lease.
-- * `lease_until`: a replica claimed the refresh until then (an
--   INSERT ... ON CONFLICT DO UPDATE ... WHERE the lease is free and the
--   answer stale); the others serve the stale answer or wait for the new
--   one. A replica that dies mid-fetch just lets the lease run out.
--
-- UNLOGGED: a cache. A crash (or a failover to a standby) empties it and
-- the next request refetches. fillfactor 50: every row is rewritten every
-- 30 s or so; the free space keeps those updates HOT.
--
-- Bounded by `tfl_stops` (the route checks the stop exists before asking).
-- The api deletes rows untouched for a day (a stop dropped from
-- `tfl_stops`), at most hourly per replica.
--
-- Expand-only: a new, empty table with no foreign keys.
-- -------------------------------------------------------------------------

CREATE UNLOGGED TABLE tfl_arrivals_cache (
    stop_id     TEXT PRIMARY KEY,
    fetched_at  TIMESTAMPTZ,
    predictions JSONB,
    error       JSONB,
    lease_until TIMESTAMPTZ,
    CONSTRAINT tfl_arrivals_cache_one_answer
        CHECK (predictions IS NULL OR error IS NULL),
    CONSTRAINT tfl_arrivals_cache_answer_has_time
        CHECK ((predictions IS NULL AND error IS NULL) OR fetched_at IS NOT NULL)
) WITH (fillfactor = 50);
