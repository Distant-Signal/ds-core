SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Account continuity (Ranma-Config docs/specs/ds-public-auth-idp.md): give
-- every foreign key to `users(id)` ON UPDATE CASCADE, so the login
-- callback can re-key an existing account to the new OIDC subject that the
-- `ds-sso` identity provider mints (`UPDATE users SET id = <new sub>`)
-- and every row that references it follows in the same statement.
-- `users.id` stays equal to the current sub, so the delegated-token,
-- revoke and back-channel logout paths, which use the token's sub
-- directly, need no change.
--
-- All 29 foreign keys to `users(id)`, each single-column and not
-- deferrable. Each keeps its name (including `train_subscriptions`'
-- pre-rename `tracked_trains_user_id_fkey`) and its ON DELETE action
-- (20260927060000: CASCADE everywhere but `groups.created_by`, SET NULL).
-- The one column that copies a user id with no foreign key,
-- `group_train_share_moves.added_by` (bookkeeping for a rollback script,
-- with rows that may name deleted users), is updated by the re-key itself.
--
-- NOT VALID here, VALIDATE in the next migration (20261104090300), as in
-- 20260927060000/060100: this file holds its locks only for catalog
-- updates (no table scans); the next file's VALIDATE CONSTRAINT scans
-- under SHARE UPDATE EXCLUSIVE, which does not block reads or writes. Each
-- old constraint is dropped and re-added in one ALTER TABLE, so there is
-- no moment without the foreign key. Dropping and re-adding a constraint
-- under the same name is not destructive DDL (no column, table or type
-- goes away), so no contract header.
-- -------------------------------------------------------------------------

ALTER TABLE custom_line_group_grants
    DROP CONSTRAINT custom_line_group_grants_granted_by_fkey,
    ADD CONSTRAINT custom_line_group_grants_granted_by_fkey
        FOREIGN KEY (granted_by) REFERENCES users(id)
        ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;

ALTER TABLE custom_lines
    DROP CONSTRAINT custom_lines_user_id_fkey,
    ADD CONSTRAINT custom_lines_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id)
        ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;

ALTER TABLE delegated_deletions
    DROP CONSTRAINT delegated_deletions_user_id_fkey,
    ADD CONSTRAINT delegated_deletions_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id)
        ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;

ALTER TABLE delegated_grant_confirmations
    DROP CONSTRAINT delegated_grant_confirmations_user_id_fkey,
    ADD CONSTRAINT delegated_grant_confirmations_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id)
        ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;

ALTER TABLE delegated_grants
    DROP CONSTRAINT delegated_grants_user_id_fkey,
    ADD CONSTRAINT delegated_grants_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id)
        ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;

ALTER TABLE group_invite_links
    DROP CONSTRAINT group_invite_links_created_by_fkey,
    ADD CONSTRAINT group_invite_links_created_by_fkey
        FOREIGN KEY (created_by) REFERENCES users(id)
        ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;

ALTER TABLE group_journeys
    DROP CONSTRAINT group_journeys_added_by_fkey,
    ADD CONSTRAINT group_journeys_added_by_fkey
        FOREIGN KEY (added_by) REFERENCES users(id)
        ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;

ALTER TABLE group_members
    DROP CONSTRAINT group_members_user_id_fkey,
    ADD CONSTRAINT group_members_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id)
        ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;

ALTER TABLE groups
    DROP CONSTRAINT groups_created_by_fkey,
    ADD CONSTRAINT groups_created_by_fkey
        FOREIGN KEY (created_by) REFERENCES users(id)
        ON UPDATE CASCADE ON DELETE SET NULL NOT VALID;

ALTER TABLE group_trains
    DROP CONSTRAINT group_trains_added_by_fkey,
    ADD CONSTRAINT group_trains_added_by_fkey
        FOREIGN KEY (added_by) REFERENCES users(id)
        ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;

ALTER TABLE journey_breaks
    DROP CONSTRAINT journey_breaks_user_id_fkey,
    ADD CONSTRAINT journey_breaks_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id)
        ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;

ALTER TABLE journey_follows
    DROP CONSTRAINT journey_follows_user_id_fkey,
    ADD CONSTRAINT journey_follows_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id)
        ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;

ALTER TABLE journey_idempotency_keys
    DROP CONSTRAINT journey_idempotency_keys_user_id_fkey,
    ADD CONSTRAINT journey_idempotency_keys_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id)
        ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;

ALTER TABLE journey_leg_notification_state
    DROP CONSTRAINT journey_leg_notification_state_user_id_fkey,
    ADD CONSTRAINT journey_leg_notification_state_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id)
        ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;

ALTER TABLE journeys
    DROP CONSTRAINT journeys_user_id_fkey,
    ADD CONSTRAINT journeys_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id)
        ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;

ALTER TABLE journey_templates
    DROP CONSTRAINT journey_templates_user_id_fkey,
    ADD CONSTRAINT journey_templates_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id)
        ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;

ALTER TABLE line_notification_state
    DROP CONSTRAINT line_notification_state_user_id_fkey,
    ADD CONSTRAINT line_notification_state_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id)
        ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;

ALTER TABLE notification_holds
    DROP CONSTRAINT notification_holds_user_id_fkey,
    ADD CONSTRAINT notification_holds_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id)
        ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;

ALTER TABLE notification_preferences
    DROP CONSTRAINT notification_preferences_user_id_fkey,
    ADD CONSTRAINT notification_preferences_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id)
        ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;

ALTER TABLE personal_api_tokens
    DROP CONSTRAINT personal_api_tokens_user_id_fkey,
    ADD CONSTRAINT personal_api_tokens_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id)
        ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;

ALTER TABLE pinned_lines
    DROP CONSTRAINT pinned_lines_user_id_fkey,
    ADD CONSTRAINT pinned_lines_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id)
        ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;

ALTER TABLE pinned_operators
    DROP CONSTRAINT pinned_operators_user_id_fkey,
    ADD CONSTRAINT pinned_operators_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id)
        ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;

ALTER TABLE pinned_stations
    DROP CONSTRAINT pinned_stations_user_id_fkey,
    ADD CONSTRAINT pinned_stations_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id)
        ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;

ALTER TABLE push_subscriptions
    DROP CONSTRAINT push_subscriptions_user_id_fkey,
    ADD CONSTRAINT push_subscriptions_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id)
        ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;

ALTER TABLE sessions
    DROP CONSTRAINT sessions_user_id_fkey,
    ADD CONSTRAINT sessions_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id)
        ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;

ALTER TABLE tracked_train_tickets
    DROP CONSTRAINT tracked_train_tickets_user_id_fkey,
    ADD CONSTRAINT tracked_train_tickets_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id)
        ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;

ALTER TABLE train_notification_state
    DROP CONSTRAINT train_notification_state_user_id_fkey,
    ADD CONSTRAINT train_notification_state_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id)
        ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;

ALTER TABLE train_subscriptions
    DROP CONSTRAINT tracked_trains_user_id_fkey,
    ADD CONSTRAINT tracked_trains_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id)
        ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;

ALTER TABLE unlisted_links
    DROP CONSTRAINT unlisted_links_created_by_fkey,
    ADD CONSTRAINT unlisted_links_created_by_fkey
        FOREIGN KEY (created_by) REFERENCES users(id)
        ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;
