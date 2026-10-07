-- Renames, type changes and SET NOT NULL on existing objects.
SET LOCAL lock_timeout = '5s';

ALTER TABLE stations RENAME TO stops;
ALTER TABLE stops RENAME COLUMN crs TO crs_code;
ALTER INDEX stations_crs RENAME TO stops_crs;
ALTER TABLE stops ALTER COLUMN tiploc TYPE varchar(7);
ALTER TABLE stops
    ALTER COLUMN name SET DEFAULT '',
    ALTER name SET NOT NULL,
    ALTER lat SET DATA TYPE double precision USING lat::double precision;
