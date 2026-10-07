-- Destructive drops, without a contract header.
SET LOCAL lock_timeout = '5s';

DROP TABLE IF EXISTS public.old_things;
DROP VIEW legacy_view;
DROP MATERIALIZED VIEW IF EXISTS legacy_stats;
DROP FUNCTION legacy_fn(integer);
ALTER TABLE trains
    DROP CONSTRAINT trains_legacy_check,
    DROP COLUMN legacy_uid,
    DROP IF EXISTS legacy_headcode;
ALTER TABLE IF EXISTS ONLY "Quoted" DROP COLUMN "Gone";
