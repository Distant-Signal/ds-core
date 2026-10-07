-- -------------------------------------------------------------------------
-- Per-publish key staging for the diff-based schedule publishes
-- (`schedule_destination_departures`, `schedule_calling_points_full`).
--
-- WHY. Both products used to be published by clearing every service date a
-- publish touched and re-inserting every row. Measured live on 2026-09-26,
-- `schedule_destination_departures` had 3.64M live rows against 11.9M
-- inserted / 10.3M deleted, and its three B-tree indexes were 52-66% bloated
-- (354/317/310 MB vs ~170/108/139 MB packed; the calling-points pkey 256 MB
-- vs ~109 MB) -- heap space is reused after VACUUM, index pages are not
-- returned. Most re-published rows were byte-identical to what was already
-- there.
--
-- The publish is now a diff: incoming rows are upserted with
-- `ON CONFLICT ... DO UPDATE ... WHERE (...) IS DISTINCT FROM (...)`, so an
-- unchanged row is never rewritten, and only rows of the touched dates that
-- are ABSENT from the new publish are deleted. A day arrives in several
-- HTTP chunks (`schedule-reference`'s `post_date_scoped_rows_in_chunks`), so
-- "absent from the new publish" is only knowable once the LAST chunk has
-- arrived. These two tables remember the primary keys every chunk of one
-- publish (`publish_id`, chosen by the publisher) carried until then; the
-- final chunk deletes every row of the publish's dates whose key is not
-- staged here, then drops the publish's staged keys. See
-- `crates/api/src/data/queries.rs::upsert_schedule_destination_departures_publish_part`.
--
-- UNLOGGED, no indexes, no primary key -- deliberately:
-- * The contents are transient working state for at most one in-flight
--   publish per date (a new publish's first chunk discards any older
--   publish's keys for the same dates, and anything older than a day is
--   discarded too). Losing them (crash recovery truncates UNLOGGED tables;
--   a promoted standby has them empty) is SAFE: the final chunk verifies
--   that exactly `total_rows` keys are staged for its publish before it
--   deletes anything, so an incomplete staging set only means "leave stale
--   rows in place until the next publish", never "delete live rows".
-- * No primary key, because that count check relies on one staged row per
--   incoming row (a replayed chunk must over-count and so fail closed).
-- * No index, because the only reads are one count and one hash anti-join
--   per publish, both whole-publish scans -- an index here would just be a
--   new source of exactly the churn this change removes from the real
--   tables.
-- -------------------------------------------------------------------------

CREATE UNLOGGED TABLE schedule_destination_departures_publish_keys (
    publish_id      TEXT        NOT NULL,
    service_date    DATE        NOT NULL,
    destination_crs TEXT        NOT NULL,
    scheduled       TIME        NOT NULL,
    train_uid       TEXT        NOT NULL,
    origin_crs      TEXT        NOT NULL,
    staged_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNLOGGED TABLE schedule_calling_points_full_publish_keys (
    publish_id   TEXT        NOT NULL,
    service_date DATE        NOT NULL,
    uid          TEXT        NOT NULL,
    seq          SMALLINT    NOT NULL,
    staged_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
