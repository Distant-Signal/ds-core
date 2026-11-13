SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Train service labels (DS-513): what kind of working a TRUST train is, for
-- internal reporting on delay and cancellation context (freight, ECS and
-- engineering movements against passenger services). A side table, not
-- columns on `trains`:
--
-- - `trains` is granted whole to the api (SIUD) and read by every
--   `read_shared` role, and a table-level SELECT covers every column added
--   later. `toc_id` (the operator of a freight movement) must never reach
--   the api or the frontend, so it lives in a table of class `internal`
--   that only the writer of the labels can reach.
-- - It keeps `trains` rows as small as they are (Postgres memory is
--   constrained, DS-435): this row is about 40 bytes plus its key, one per
--   Activation (about 32k a day), and goes with its train (ON DELETE
--   CASCADE, as the aggregator's archive deletes `trains`).
--
-- - `service_class`: derived from the first digit of the TRUST train id's
--   headcode (the third character of the 10-character id) by
--   ds-ingest's `trust-backlog-consumer`. A convention, not a fact: `6`
--   and `7` mix freight and engineering trains, `3` mixes parcels and
--   empty coaching stock, `9` is special workings and is mostly passenger
--   in the data (DS-513). A reader wanting passenger services must also
--   look at whether the train has a schedule. A train whose train id is not
--   in the usual shape is `unknown`.
-- - `toc_id`: the Activation's `toc_id`, TRUST's two-character operator
--   code. Internal only: never served, never shown, no operator names.
--
-- New table, so no lock on existing ones beyond the foreign key's brief
-- SHARE ROW EXCLUSIVE on `trains`, bounded by the 5 s lock_timeout.
-- -------------------------------------------------------------------------

CREATE TABLE train_service_labels (
    trains_id     BIGINT   PRIMARY KEY REFERENCES trains (id) ON DELETE CASCADE,
    service_class TEXT     NOT NULL CHECK (service_class IN (
        'passenger_express',
        'passenger_ordinary',
        'ecs',
        'ecs_or_parcels',
        'freight',
        'freight_or_engineering',
        'light_loco',
        'special',
        'unknown'
    )),
    toc_id        TEXT     CHECK (toc_id IS NULL OR char_length(toc_id) BETWEEN 1 AND 4)
);
