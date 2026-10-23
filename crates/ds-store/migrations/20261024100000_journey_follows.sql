SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Journey follows (DS-146): someone a journey is shared with -- through a
-- group (`group_journeys`) or a public share link (`unlisted_links`) --
-- "follows" it, so it shows among their own current and upcoming
-- journeys, read-only. One row per (follower, journey), recording which
-- share let them in: `group_id` for a group, `link_token_hash` for a link,
-- never both (the CHECK added NOT VALID by 20261024100100 and validated by
-- 20261024100200).
--
-- A follow lasts only as long as the share it came through:
-- * the journey is deleted: `journey_id` cascades;
-- * the follower's account is deleted: `user_id` cascades;
-- * the journey is unshared from the group (or the group deleted): the
--   composite key onto `group_journeys` cascades;
-- * the follower leaves the group: `groups::remove_member_in_tx` deletes
--   their follows through it;
-- * the link is revoked or regenerated: the share-link routes delete the
--   follows through it; a dead link row pruned later cascades;
-- * the link expires: the hourly retention sweep deletes the follow.
-- Every read re-checks the share as well (`journey_follows::VALID_FOLLOW`),
-- so a follow is never honoured in the moment before its cleanup runs.
--
-- Expand-only: a new, empty table. Its foreign keys take a brief SHARE ROW
-- EXCLUSIVE on `users`, `journeys`, `group_journeys` and `unlisted_links`,
-- under the 5 s lock_timeout. Its secondary indexes are built CONCURRENTLY,
-- each alone in its own file (20261024100300 to 20261024100500).
--
-- No notifier grant: followers get no pushes for now (the group-sharing
-- decision, 2026-09-22). An opt-in would add a column here and a grant.
-- -------------------------------------------------------------------------

CREATE TABLE journey_follows (
    user_id         TEXT NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    journey_id      BIGINT NOT NULL REFERENCES journeys (id) ON DELETE CASCADE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- The group whose share the follow came through.
    group_id        TEXT,
    -- The share link it came through (`unlisted_links.token_hash`).
    link_token_hash TEXT REFERENCES unlisted_links (token_hash) ON DELETE CASCADE,
    PRIMARY KEY (user_id, journey_id),
    CONSTRAINT journey_follows_group_share_fkey
        FOREIGN KEY (group_id, journey_id)
        REFERENCES group_journeys (group_id, journey_id) ON DELETE CASCADE
);
