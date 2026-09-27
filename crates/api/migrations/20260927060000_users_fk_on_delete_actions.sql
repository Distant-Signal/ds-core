SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Account deletion (UK legal audit LEG-4, 2026-09-27): give every foreign
-- key to `users(id)` an explicit ON DELETE action, so `DELETE FROM users`
-- (the new `DELETE /public/account` route, and the optional inactive-account
-- retention sweep) succeeds instead of failing on the first of these ten
-- constraints. Every other FK to `users(id)` was already ON DELETE CASCADE.
--
-- Per-column decisions (see docs/personal-data-retention.md):
--
--   train_subscriptions.user_id        CASCADE  the user's own tracked trains
--   tracked_train_tickets.user_id      CASCADE  the user's own ticket records
--   journeys.user_id                   CASCADE  the user's own journeys
--   journey_templates.user_id          CASCADE  the user's own templates
--   unlisted_links.created_by          CASCADE  share links the user made
--   group_invite_links.created_by      CASCADE  invite links the user made
--   group_trains.added_by              CASCADE  trains the user shared into
--                                               a group (the same cleanup
--                                               `groups::remove_member`
--                                               already does on leave)
--   group_journeys.added_by            CASCADE  likewise for journeys
--   custom_line_group_grants.granted_by CASCADE a grant of the user's own
--                                               custom line; the line itself
--                                               already cascades with the
--                                               user, taking its grants
--   groups.created_by                  SET NULL the group itself belongs to
--                                               all its members. It is only
--                                               an attribution column (never
--                                               read by the api); ownership
--                                               lives in `group_members.role`
--                                               and is handed over by
--                                               `data::account::delete_account`
--                                               before the user row goes.
--
-- NOT VALID here, VALIDATE in the next migration: sqlx wraps each file in
-- its own transaction, so this one holds its locks only for catalog
-- updates (no table scans); the next file's VALIDATE CONSTRAINT scans under
-- SHARE UPDATE EXCLUSIVE, which does not block reads or writes. Each old
-- constraint is dropped and re-added in one ALTER TABLE, so there is no
-- moment without the FK. The constraint names are kept (including
-- `train_subscriptions`' pre-rename `tracked_trains_*` names).
-- `DROP NOT NULL` is a catalog-only change.
-- -------------------------------------------------------------------------

ALTER TABLE train_subscriptions
    DROP CONSTRAINT tracked_trains_user_id_fkey,
    ADD CONSTRAINT tracked_trains_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE NOT VALID;

ALTER TABLE tracked_train_tickets
    DROP CONSTRAINT tracked_train_tickets_user_id_fkey,
    ADD CONSTRAINT tracked_train_tickets_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE NOT VALID;

ALTER TABLE journeys
    DROP CONSTRAINT journeys_user_id_fkey,
    ADD CONSTRAINT journeys_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE NOT VALID;

ALTER TABLE journey_templates
    DROP CONSTRAINT journey_templates_user_id_fkey,
    ADD CONSTRAINT journey_templates_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE NOT VALID;

ALTER TABLE unlisted_links
    DROP CONSTRAINT unlisted_links_created_by_fkey,
    ADD CONSTRAINT unlisted_links_created_by_fkey
        FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE CASCADE NOT VALID;

ALTER TABLE group_invite_links
    DROP CONSTRAINT group_invite_links_created_by_fkey,
    ADD CONSTRAINT group_invite_links_created_by_fkey
        FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE CASCADE NOT VALID;

ALTER TABLE group_trains
    DROP CONSTRAINT group_trains_added_by_fkey,
    ADD CONSTRAINT group_trains_added_by_fkey
        FOREIGN KEY (added_by) REFERENCES users(id) ON DELETE CASCADE NOT VALID;

ALTER TABLE group_journeys
    DROP CONSTRAINT group_journeys_added_by_fkey,
    ADD CONSTRAINT group_journeys_added_by_fkey
        FOREIGN KEY (added_by) REFERENCES users(id) ON DELETE CASCADE NOT VALID;

ALTER TABLE custom_line_group_grants
    DROP CONSTRAINT custom_line_group_grants_granted_by_fkey,
    ADD CONSTRAINT custom_line_group_grants_granted_by_fkey
        FOREIGN KEY (granted_by) REFERENCES users(id) ON DELETE CASCADE NOT VALID;

ALTER TABLE groups
    ALTER COLUMN created_by DROP NOT NULL,
    DROP CONSTRAINT groups_created_by_fkey,
    ADD CONSTRAINT groups_created_by_fkey
        FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL NOT VALID;
