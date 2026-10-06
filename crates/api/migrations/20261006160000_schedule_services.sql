SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- `schedule_services`: one row per CIF schedule running on a service date
-- -- the STP-resolved winner for that (service_date, uid) -- carrying what
-- kind of vehicle it is. Published by `schedule-reference`
-- (`publish_schedule_services`) for the same eight-day forward window as
-- `schedule_calling_points_full`, and pruned by the aggregator with it.
--
-- Why it exists (user decision 2026-10-06): buses and ferries are 9% of
-- weekday schedules, 19% on Saturday and 25% on Sunday, and TRUST never
-- reports any of them. Every surface used to show them as trains that were
-- forever "waiting for a movement report". `mode` lets every reader label
-- them, and lets tracking treat them as timetable-only.
--
-- Classification is per (service_date, uid), not per uid: an STP overlay
-- can turn a permanent train into a replacement bus for some dates only
-- (P69679..P69695 in production are trains on some dates).
--
-- A MISSING row means "train": a reader LEFT JOINs and treats NULL as
-- `train`, so deploying the readers before the first publish (or reading a
-- date the publisher has not reached) degrades to the old behaviour rather
-- than hiding anything.
--
-- `mode` is derived from `train_status`/`train_category`
-- (schedule_query::service_mode); both are kept so a reader can tell a
-- permanent rail-link bus from a replacement one without a republish.
-- -------------------------------------------------------------------------
CREATE TABLE schedule_services (
    service_date   DATE NOT NULL,
    uid            TEXT NOT NULL,
    mode           TEXT NOT NULL
                   CHECK (mode IN ('train', 'replacement_bus', 'bus', 'ferry')),
    -- CIF BS Train Status (one character, e.g. 'P', 'B', '5', 'S'); NULL
    -- when blank.
    train_status   TEXT CHECK (char_length(train_status) = 1),
    -- CIF BS Train Category (two characters, e.g. 'OO', 'BS', 'BR'); NULL
    -- when blank.
    train_category TEXT CHECK (char_length(train_category) = 2),
    -- CIF BS Train Identity, the signalling headcode (e.g. '0B00').
    headcode       TEXT,
    -- CIF BX Retail Service ID.
    rsid           TEXT,
    -- CIF BX ATOC code.
    operator_atoc  TEXT,
    -- STP indicator of the winning record: P permanent, O overlay, N new
    -- (a cancelled winner is never published).
    stp            TEXT NOT NULL CHECK (stp IN ('P', 'O', 'N')),
    PRIMARY KEY (service_date, uid)
);

COMMENT ON TABLE schedule_services IS
    'Per (service_date, uid) CIF schedule facts incl. service mode (train/replacement_bus/bus/ferry); a missing row means train.';
