-- A DO block runs at migration time, so its body is checked.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'old_things') THEN
        DROP TABLE old_things;
    END IF;
END
$$;
