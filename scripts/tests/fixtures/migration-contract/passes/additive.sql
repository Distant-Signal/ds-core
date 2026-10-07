-- Additive DDL only. Words in comments do not count: DROP TABLE trains;
SET LOCAL lock_timeout = '5s';

/* Nor in block comments /* even nested */ ALTER TABLE trains RENAME TO x; */
CREATE TABLE IF NOT EXISTS new_things (
    id bigint PRIMARY KEY,
    note text NOT NULL DEFAULT 'DROP TABLE trains; ALTER TABLE t RENAME TO u'
);
ALTER TABLE trains
    ADD COLUMN IF NOT EXISTS headcode text,
    ADD CONSTRAINT trains_headcode_check CHECK (headcode <> ''),
    DROP CONSTRAINT IF EXISTS trains_old_check,
    ALTER COLUMN platform SET DEFAULT 'TBC',
    ALTER COLUMN platform DROP NOT NULL,
    ALTER COLUMN platform DROP DEFAULT;
CREATE OR REPLACE VIEW train_heads AS SELECT id, headcode FROM trains;
DROP INDEX IF EXISTS trains_old_idx;
DROP TRIGGER IF EXISTS trains_touch ON trains;
-- A function body does not run at migration time.
CREATE OR REPLACE FUNCTION purge_scratch() RETURNS void
LANGUAGE plpgsql AS $fn$
BEGIN
    DROP TABLE IF EXISTS scratch;
    ALTER TABLE trains RENAME COLUMN a TO b;
END
$fn$;
ALTER TABLE trains ADD COLUMN "rename" integer;
