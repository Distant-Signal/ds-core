SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- TRUST reason codes per shared train: the latest `canx_reason_code` of a
-- `0002` Cancellation and the latest `reason_code` of a `0006` Change of
-- Origin. trust-backlog-consumer posts them to `POST /private/train-reasons`
-- (`api::data::train_reasons`). The public and tracked train-detail
-- responses serve them as `cancelReasonCode`/`cancelReason` and
-- `changeOfOriginReasonCode`/`changeOfOriginReason`, with the text looked
-- up in reference-data/delay-attribution-reasons.tsv at read time, so only
-- the code is stored here.
--
-- One row per (train, message type), not an event log: a later message of
-- the same type replaces the earlier one (see `event_at`), which is all the
-- read side needs. A cancel -> reinstate -> cancel-again sequence therefore
-- shows the second cancellation's reason.
--
-- `ON DELETE CASCADE` ties retention to `trains` (the aggregator's 30-day
-- prune), so this table needs no pruning of its own. The primary key leads
-- with `trains_id`, which is also the index the cascade uses.
-- -------------------------------------------------------------------------
CREATE TABLE train_reasons (
    trains_id   BIGINT      NOT NULL REFERENCES trains(id) ON DELETE CASCADE,
    msg_type    TEXT        NOT NULL CHECK (msg_type IN ('0002', '0006')),
    -- The TRUST delay attribution code, verbatim (e.g. 'TG'). Two
    -- characters in practice; not constrained, so a new or odd code is
    -- stored rather than rejected.
    reason_code TEXT        NOT NULL,
    -- '0002' only: 'AT ORIGIN' | 'EN ROUTE' | 'ON CALL' | 'OUT OF PLAN'.
    canx_type   TEXT,
    -- The STANOX the message applies at: where the train is cancelled
    -- from ('0002'), or its new origin ('0006').
    loc_stanox  TEXT,
    -- The message's own timestamp (canx_timestamp for '0002',
    -- dep_timestamp for '0006'), corrected like every TRUST timestamp.
    -- Orders two messages of the same type; NULL sorts as oldest.
    event_at    TIMESTAMPTZ,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trains_id, msg_type)
);
