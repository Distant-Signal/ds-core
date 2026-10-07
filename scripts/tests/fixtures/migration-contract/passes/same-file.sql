-- Changing what this same migration created is not a contract change.
SET LOCAL lock_timeout = '5s';

CREATE TABLE fresh (id bigint, label text);
ALTER TABLE fresh ALTER COLUMN label SET NOT NULL;
ALTER TABLE fresh ALTER COLUMN id TYPE integer;
ALTER TABLE public.fresh DROP COLUMN label;

ALTER TABLE trains ADD COLUMN service_day date;
UPDATE trains SET service_day = CURRENT_DATE;
ALTER TABLE trains ALTER COLUMN service_day SET NOT NULL;

CREATE TEMP TABLE scratch AS SELECT id FROM trains;
DROP TABLE scratch;
