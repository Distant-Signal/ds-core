SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- schedule-reference's run signals (CronJob phase 4, DS-241;
-- docs/scheduled-jobs.md, "schedule-reference"): the facts its alerts read,
-- kept in Postgres so they outlive a run. As a CronJob a run exits before
-- Prometheus could scrape it, so the always-on ingest-writer exports these
-- rows (and schedule_reference_publishes) as gauges instead.
--
-- One row per (kind, product), overwritten each time, so the table stays
-- a few dozen rows and needs no pruning:
--
--   kind         seeded           a run (or the Deployment, at start) read
--                                 its completion marker; product ''
--                rejected         a product's publish was refused as bad
--                                 data (SQLSTATE class 22/23)
--                staged_mismatch  a final chunk's staged key count did not
--                                 match the publisher's total (SCHED-2)
--                settled          a CronJob run finished a delivery with a
--                                 failure the same delivery can't fix; the
--                                 next runs skip it until a newer delivery
--                                 lands; product ''
--   product      the product's kind (its metric label), or ''
--   recorded_at  when it last happened
--   delivery     the delivery directory it happened for, when known
--
-- A new table: nothing reads it before this change.
-- -------------------------------------------------------------------------

CREATE TABLE schedule_reference_signals (
    kind        TEXT NOT NULL
        CHECK (kind IN ('seeded', 'rejected', 'staged_mismatch', 'settled')),
    product     TEXT NOT NULL DEFAULT '',
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivery    TEXT,
    PRIMARY KEY (kind, product)
);

COMMENT ON TABLE schedule_reference_signals IS
    'schedule-reference run signals (DS-241): written by schedule-reference, exported as metrics by the ingest-writer.';
