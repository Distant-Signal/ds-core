SET LOCAL lock_timeout = '5s';

-- Normalise custom-line station codes already stored (M3 residual,
-- 2026-10-01 review). `data::custom_lines` now trims, uppercases and
-- dedupes `stations` and `destination_crs_filter` on every write; rows
-- written before that could hold " wok" next to "WOK" (two LDBWS polls of
-- one station) or a lowercase destination that never matched LDBWS's
-- uppercase codes. Same rule here: each code trimmed and uppercased, later
-- duplicates dropped, first-occurrence order kept (a line's station order
-- is meaningful).
--
-- Idempotent: only rows whose arrays would change are touched, so a
-- second run updates nothing. Row locks only; `custom_lines` is tiny (2
-- rows in production on 2026-10-01, none needing a change).

-- Shape check first: both columns must still be the NOT NULL text[]
-- columns this rewrite assumes, or it stops here rather than guessing.
DO $$
BEGIN
    IF (
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'custom_lines'
          AND column_name IN ('stations', 'destination_crs_filter')
          AND data_type = 'ARRAY'
          AND udt_name = '_text'
          AND is_nullable = 'NO'
    ) <> 2 THEN
        RAISE EXCEPTION 'custom_lines.stations/destination_crs_filter are not NOT NULL text[] columns';
    END IF;
END
$$;

WITH normalised AS (
    SELECT
        cl.id,
        ARRAY(
            SELECT d.code
            FROM (
                SELECT UPPER(BTRIM(t.code)) AS code, MIN(t.ord) AS first_ord
                FROM unnest(cl.stations) WITH ORDINALITY AS t(code, ord)
                GROUP BY 1
            ) AS d
            ORDER BY d.first_ord
        ) AS stations,
        ARRAY(
            SELECT d.code
            FROM (
                SELECT UPPER(BTRIM(t.code)) AS code, MIN(t.ord) AS first_ord
                FROM unnest(cl.destination_crs_filter) WITH ORDINALITY AS t(code, ord)
                GROUP BY 1
            ) AS d
            ORDER BY d.first_ord
        ) AS destination_crs_filter
    FROM custom_lines AS cl
)
UPDATE custom_lines AS cl
SET stations = n.stations,
    destination_crs_filter = n.destination_crs_filter
FROM normalised AS n
WHERE n.id = cl.id
  AND (cl.stations <> n.stations OR cl.destination_crs_filter <> n.destination_crs_filter);

-- Codes that still aren't three letters after normalising are left in
-- place, not dropped: removing them could take a line below its 2-station
-- minimum, and `poller-ldbws` already skips (and warns about) any sample
-- station that isn't exactly three ASCII letters. Reported here so a
-- deploy log shows whether any exist.
DO $$
DECLARE
    malformed integer;
BEGIN
    SELECT COUNT(*) INTO malformed
    FROM custom_lines AS cl, unnest(cl.stations) AS s(code)
    WHERE s.code !~ '^[A-Z]{3}$';
    IF malformed > 0 THEN
        RAISE NOTICE 'custom_lines: % stored station code(s) are not 3-letter CRS codes', malformed;
    END IF;
END
$$;
