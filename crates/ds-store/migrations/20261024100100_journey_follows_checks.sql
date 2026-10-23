SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- A follow comes through exactly one share: a group or a link
-- (20261024100000). Added NOT VALID (a brief ACCESS EXCLUSIVE for the
-- catalog only, hence the timeout; new rows are checked at once) and
-- validated by 20261024100200 under SHARE UPDATE EXCLUSIVE, the two-file
-- pattern of 20261023090300/20261023090400. The table is new, so nothing
-- can fail.
-- -------------------------------------------------------------------------

ALTER TABLE journey_follows
    ADD CONSTRAINT journey_follows_one_share_check
    CHECK (num_nonnulls(group_id, link_token_hash) = 1)
    NOT VALID;
