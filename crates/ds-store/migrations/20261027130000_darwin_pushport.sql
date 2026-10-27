SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Darwin Push Port, pre-departure prediction phase 3 (DS-152,
-- docs/superpowers/specs/2026-10-21-pre-departure-prediction-design.md
-- sections 2, 4 option C and 6.2 item 4). Additive: three new tables and
-- one view.
--
-- darwin-relay reads the Push Port JSON topic, keeps the parts below and
-- produces them on ds:ingest:darwin (schema darwin-pushport/1);
-- ingest-writer applies them here. Darwin names a service by its RID; a
-- schedule or train status message also gives the CIF UID and the
-- scheduled start date (SSD), which is how a row joins DS's own trains
-- (trains.train_uid, trains.service_date).
--
-- Every write is guarded by the Darwin message's own timestamp (`ts`), so
-- a replayed or reordered message never overwrites newer data. The
-- aggregator prunes all three tables a few days after their service date.
-- -------------------------------------------------------------------------

-- One row per Darwin service (RID), from schedule and train status
-- messages, plus deactivations.
CREATE TABLE darwin_services (
    rid           TEXT        PRIMARY KEY,
    uid           TEXT        NOT NULL,
    ssd           DATE        NOT NULL,
    toc           TEXT,
    -- The headcode (Darwin's trainId) and retail service id.
    train_id      TEXT,
    rsid          TEXT,
    is_passenger  BOOLEAN,
    -- The schedule was deleted (Darwin `deleted`): it no longer runs.
    is_deleted    BOOLEAN     NOT NULL DEFAULT FALSE,
    -- Darwin's cancellation reason code for the service, if any.
    cancel_reason TEXT,
    -- Darwin stopped tracking the service (`deactivated`): it has
    -- finished, or will not run.
    deactivated   BOOLEAN     NOT NULL DEFAULT FALSE,
    -- The newest schedule message applied (its `ts`); NULL until one is.
    schedule_ts   TIMESTAMPTZ,
    -- The newest message of any kind that touched the row.
    message_ts    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX darwin_services_uid_ssd ON darwin_services (uid, ssd);
CREATE INDEX darwin_services_ssd ON darwin_services (ssd);

COMMENT ON TABLE darwin_services IS
    'Darwin Push Port services by RID, with their CIF UID and scheduled start date (DS-152).';

-- One row per calling or passing location of a Darwin service. Darwin
-- identifies a location within its schedule by the TIPLOC plus its working
-- times (a circular train visits a TIPLOC twice), so the key is all four;
-- an absent time is ''. The schedule columns come from schedule messages,
-- the rest from train status (TS) messages, each guarded by its own `ts`.
-- A TS location replaces only the groups it carries (platform, arrival,
-- departure, pass); the others keep their last value.
CREATE TABLE darwin_calls (
    rid                     TEXT        NOT NULL,
    tiploc                  TEXT        NOT NULL,
    wta                     TEXT        NOT NULL DEFAULT '',
    wtd                     TEXT        NOT NULL DEFAULT '',
    wtp                     TEXT        NOT NULL DEFAULT '',
    -- The service's scheduled start date, for joins and pruning.
    ssd                     DATE        NOT NULL,
    -- Public times, "HH:MM" (Europe/London), when the location is a
    -- public call.
    pta                     TEXT,
    ptd                     TEXT,
    -- From schedule messages: Darwin's planned platform, and whether the
    -- call is cancelled.
    planned_platform        TEXT,
    is_cancelled            BOOLEAN     NOT NULL DEFAULT FALSE,
    schedule_ts             TIMESTAMPTZ,
    -- From TS messages: the current platform and its flags. `platsup`:
    -- suppressed from public display; `cisPlatsup`: suppressed by a
    -- station's CIS (customer information system); `conf`: confirmed by
    -- the operator; `platsrc`: P(lanned), A(utomatic, from train
    -- describers) or M(anual).
    platform                TEXT,
    platform_confirmed      BOOLEAN     NOT NULL DEFAULT FALSE,
    platform_suppressed     BOOLEAN     NOT NULL DEFAULT FALSE,
    platform_cis_suppressed BOOLEAN     NOT NULL DEFAULT FALSE,
    platform_source         TEXT        CHECK (platform_source IN ('P', 'A', 'M')),
    -- Forecasts (`et`) and actuals (`at`), "HH:MM" Europe/London, per
    -- event; `*_delayed` is Darwin's "delayed, time unknown".
    arrival_et              TEXT,
    arrival_at              TEXT,
    arrival_delayed         BOOLEAN     NOT NULL DEFAULT FALSE,
    departure_et            TEXT,
    departure_at            TEXT,
    departure_delayed       BOOLEAN     NOT NULL DEFAULT FALSE,
    pass_et                 TEXT,
    pass_at                 TEXT,
    -- The location is suppressed from public display (`suppr`).
    is_suppressed           BOOLEAN     NOT NULL DEFAULT FALSE,
    ts_ts                   TIMESTAMPTZ,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- What the platform rests on: `suppressed` (not shown to the public),
    -- `confirmed`, `altered` (not Darwin's planned platform), else
    -- `planned`; NULL without a platform.
    platform_basis          TEXT GENERATED ALWAYS AS (
        CASE
            WHEN platform IS NULL THEN NULL
            WHEN platform_suppressed OR platform_cis_suppressed THEN 'suppressed'
            WHEN platform_confirmed THEN 'confirmed'
            WHEN planned_platform IS NOT NULL
                 AND UPPER(platform) <> UPPER(planned_platform) THEN 'altered'
            ELSE 'planned'
        END
    ) STORED,
    PRIMARY KEY (rid, tiploc, wta, wtd, wtp)
);

CREATE INDEX darwin_calls_ssd ON darwin_calls (ssd);

COMMENT ON TABLE darwin_calls IS
    'Darwin Push Port per-location platform (with confirmed/suppressed flags), forecasts and cancellations (DS-152).';

-- Live associations between two Darwin services at a location: NP (next
-- working: `main` forms `assoc`), JJ (join), VV (divide), LK (linked).
-- `main_*`/`assoc_*` times are each service's working times at the
-- location, which identify the call in its schedule.
CREATE TABLE darwin_associations (
    main_rid     TEXT        NOT NULL,
    assoc_rid    TEXT        NOT NULL,
    tiploc       TEXT        NOT NULL,
    category     TEXT        NOT NULL CHECK (category IN ('NP', 'JJ', 'VV', 'LK')),
    is_cancelled BOOLEAN     NOT NULL DEFAULT FALSE,
    is_deleted   BOOLEAN     NOT NULL DEFAULT FALSE,
    main_wta     TIME,
    main_wtd     TIME,
    main_wtp     TIME,
    assoc_wta    TIME,
    assoc_wtd    TIME,
    assoc_wtp    TIME,
    -- The main service's scheduled start date (a RID starts with it), for
    -- pruning.
    main_ssd     DATE,
    message_ts   TIMESTAMPTZ NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (main_rid, assoc_rid, tiploc, category)
);

CREATE INDEX darwin_associations_assoc_rid ON darwin_associations (assoc_rid);
CREATE INDEX darwin_associations_main_ssd ON darwin_associations (main_ssd);

COMMENT ON TABLE darwin_associations IS
    'Darwin Push Port live associations (NP next working, JJ join, VV divide), by RID (DS-152).';

-- Each service's previous working by Darwin's live next-working (NP)
-- associations, keyed like schedule_turnarounds: the train formed at the
-- association's location (`assoc`) and the train whose unit forms it
-- (`main`). The booked turnaround is the next working's working departure
-- minus the previous working's working arrival there, across midnight.
-- When a train has several live NP links, the newest wins.
CREATE VIEW darwin_previous_workings AS
SELECT DISTINCT ON (nxt.uid, nxt.ssd)
       nxt.ssd AS service_date,
       nxt.uid,
       prv.ssd AS prev_service_date,
       prv.uid AS prev_uid,
       a.tiploc AS origin_tiploc,
       (MOD((EXTRACT(EPOCH FROM (a.assoc_wtd - a.main_wta)) / 60)::int + 1440, 1440))::smallint
           AS booked_turnaround_minutes
FROM darwin_associations a
JOIN darwin_services nxt ON nxt.rid = a.assoc_rid
JOIN darwin_services prv ON prv.rid = a.main_rid
WHERE a.category = 'NP'
  AND NOT a.is_cancelled
  AND NOT a.is_deleted
  AND a.main_wta IS NOT NULL
  AND a.assoc_wtd IS NOT NULL
ORDER BY nxt.uid, nxt.ssd, a.message_ts DESC;
