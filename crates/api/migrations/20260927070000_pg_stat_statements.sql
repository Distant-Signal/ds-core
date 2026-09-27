SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- pg_stat_statements: per-query execution statistics (DB review 2026-09-27,
-- F6 "slow queries are invisible").
--
-- The chart now preloads the library (`postgresql.config`
-- shared_preload_libraries = pg_stat_statements, which needs a Postgres
-- restart to take effect). This creates the extension's view and functions
-- in this database so `SELECT ... FROM pg_stat_statements` works once the
-- library is loaded. Creating it before the restart is harmless: the view
-- exists but errors until the library is preloaded.
--
-- Checked against production (2026-09-27): postgres:16 (16.15) ships
-- pg_stat_statements 1.10 in contrib, and the chart's `distant_signal` role
-- is the image's bootstrap superuser, so it may create the extension
-- (pg_stat_statements is not a "trusted" extension).
--
-- Best effort. On an external/managed database where the app role cannot
-- create it (insufficient privilege, or the contrib files are missing) this
-- logs a WARNING and succeeds, rather than failing the migration and
-- crash-looping api for an observability extra. Create it there by hand as
-- a privileged role:
--   CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
-- -------------------------------------------------------------------------
DO $$
BEGIN
    CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
EXCEPTION
    WHEN insufficient_privilege OR undefined_file OR feature_not_supported THEN
        RAISE WARNING 'pg_stat_statements was not created (%: %); create it by hand as a '
            'privileged role: CREATE EXTENSION IF NOT EXISTS pg_stat_statements', SQLSTATE, SQLERRM;
END
$$;
