SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- `train_reasons` also keeps a train's latest TRUST `0005` Reinstatement
-- (DS-181), so the train-detail `cancellation` model can tell a train that
-- was reinstated from where it was cancelled (no longer cancelled) from one
-- reinstated further on ("cancelled between A and B"). A `0005` row has no
-- reason code: it is stored as '' (the column stays NOT NULL), as is an
-- uncoded `0002` or `0006`, which trust-backlog-consumer used to drop and
-- now keeps for its location. `loc_stanox` is where the train runs again
-- from, `event_at` its `reinstatement_timestamp`.
--
-- Only the CHECK widens. It is added NOT VALID, so this file takes the
-- ACCESS EXCLUSIVE lock without scanning the table, and the next migration
-- validates it. Every existing row ('0002' or '0006') already satisfies it.
-- -------------------------------------------------------------------------
ALTER TABLE train_reasons DROP CONSTRAINT train_reasons_msg_type_check;
ALTER TABLE train_reasons
    ADD CONSTRAINT train_reasons_msg_type_check
    CHECK (msg_type IN ('0002', '0005', '0006')) NOT VALID;
