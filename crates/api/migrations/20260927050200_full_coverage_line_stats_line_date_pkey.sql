-- -------------------------------------------------------------------------
-- Step 3 of 3: swap the primary key from (line_id) to
-- (line_id, service_date), adopting the index step 2 built concurrently.
--
-- One ALTER TABLE, so there is no moment without a primary key. It takes
-- ACCESS EXCLUSIVE, but only for catalog changes: dropping the old pkey
-- index, and renaming step 2's index to `full_coverage_line_stats_pkey`
-- (Postgres announces the rename with a NOTICE). Both columns are already
-- NOT NULL, so no scan is needed. The lock_timeout makes a wait behind a
-- long-running transaction fail fast; the migration then reruns on the
-- next api start.
--
-- VERSION SKEW. api runs this at startup and its Deployment is Recreate,
-- so no api binary expecting the old key is serving once this runs. The
-- new api upserts ON CONFLICT (line_id, service_date), which works whether
-- the consumer posting to it is old or new. Rolling the api BACK past this
-- change is not supported: the old binary's ON CONFLICT (line_id) has no
-- matching constraint any more, so its stats POSTs fail (the consumer
-- retries them every minute; nothing else writes this table).
-- -------------------------------------------------------------------------
SET LOCAL lock_timeout = '5s';

ALTER TABLE full_coverage_line_stats
    DROP CONSTRAINT full_coverage_line_stats_pkey,
    ADD CONSTRAINT full_coverage_line_stats_pkey
        PRIMARY KEY USING INDEX full_coverage_line_stats_line_id_service_date_key;
