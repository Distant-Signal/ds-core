SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- analyze_publish_keys(table): ANALYZE one schedule publish staging table,
-- with the rights of this function's owner.
--
-- The final chunk of a schedule publish (`finish_publish_part` in
-- crates/api/src/data/queries.rs) refreshes the staging table's statistics
-- right before its anti-join delete; without that the planner can pick the
-- nested loop that ran for 6+ minutes on 2026-09-27. On Postgres 16 only
-- the table's owner (or a superuser) may ANALYZE it: there is no MAINTAIN
-- privilege before 17. And a plain `ANALYZE` that lacks the right does not
-- fail, it only logs "WARNING: permission denied to analyze ..., skipping
-- it" and succeeds -- so once the services connect as the non-superuser
-- app role (docs/postgres-app-role.md) the statistics would silently stop
-- being refreshed. The api calls this instead.
--
-- SECURITY DEFINER, so it runs as its owner: the role that ran this
-- migration (today the superuser; with the role split, the schema owner,
-- after the role setup moves it there). It only ever analyzes one of the
-- two named tables, schema-qualified, with a fixed search_path, so it hands
-- the caller nothing else. EXECUTE is revoked from PUBLIC: the app role
-- gets it from the role setup (charts/distant-signal/files/postgres-roles.sql),
-- directly or through the owner's default privileges. A superuser needs no
-- grant, so a deployment without the role split behaves exactly as before.
--
-- Runs inside the caller's transaction like the plain ANALYZE did (same
-- SHARE UPDATE EXCLUSIVE lock, held to COMMIT).
-- -------------------------------------------------------------------------
CREATE FUNCTION analyze_publish_keys(target text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
    IF target = 'schedule_destination_departures_publish_keys' THEN
        ANALYZE public.schedule_destination_departures_publish_keys;
    ELSIF target = 'schedule_calling_points_full_publish_keys' THEN
        ANALYZE public.schedule_calling_points_full_publish_keys;
    ELSE
        RAISE EXCEPTION 'analyze_publish_keys: % is not a publish staging table', target
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
END
$$;

REVOKE ALL ON FUNCTION analyze_publish_keys(text) FROM PUBLIC;
