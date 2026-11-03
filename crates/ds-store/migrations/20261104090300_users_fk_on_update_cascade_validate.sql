SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Validates the 29 `users(id)` foreign keys re-added NOT VALID with ON
-- UPDATE CASCADE by 20261104090200_users_fk_on_update_cascade.sql.
-- VALIDATE CONSTRAINT takes SHARE UPDATE EXCLUSIVE on the referencing
-- table (reads and writes carry on) and ROW SHARE on `users`. Every
-- existing row already satisfied the identical foreign key that each
-- constraint replaced (only its ON UPDATE action changed), so none of
-- these can fail.
-- -------------------------------------------------------------------------

ALTER TABLE custom_line_group_grants VALIDATE CONSTRAINT custom_line_group_grants_granted_by_fkey;
ALTER TABLE custom_lines VALIDATE CONSTRAINT custom_lines_user_id_fkey;
ALTER TABLE delegated_deletions VALIDATE CONSTRAINT delegated_deletions_user_id_fkey;
ALTER TABLE delegated_grant_confirmations VALIDATE CONSTRAINT delegated_grant_confirmations_user_id_fkey;
ALTER TABLE delegated_grants VALIDATE CONSTRAINT delegated_grants_user_id_fkey;
ALTER TABLE group_invite_links VALIDATE CONSTRAINT group_invite_links_created_by_fkey;
ALTER TABLE group_journeys VALIDATE CONSTRAINT group_journeys_added_by_fkey;
ALTER TABLE group_members VALIDATE CONSTRAINT group_members_user_id_fkey;
ALTER TABLE groups VALIDATE CONSTRAINT groups_created_by_fkey;
ALTER TABLE group_trains VALIDATE CONSTRAINT group_trains_added_by_fkey;
ALTER TABLE journey_breaks VALIDATE CONSTRAINT journey_breaks_user_id_fkey;
ALTER TABLE journey_follows VALIDATE CONSTRAINT journey_follows_user_id_fkey;
ALTER TABLE journey_idempotency_keys VALIDATE CONSTRAINT journey_idempotency_keys_user_id_fkey;
ALTER TABLE journey_leg_notification_state VALIDATE CONSTRAINT journey_leg_notification_state_user_id_fkey;
ALTER TABLE journeys VALIDATE CONSTRAINT journeys_user_id_fkey;
ALTER TABLE journey_templates VALIDATE CONSTRAINT journey_templates_user_id_fkey;
ALTER TABLE line_notification_state VALIDATE CONSTRAINT line_notification_state_user_id_fkey;
ALTER TABLE notification_holds VALIDATE CONSTRAINT notification_holds_user_id_fkey;
ALTER TABLE notification_preferences VALIDATE CONSTRAINT notification_preferences_user_id_fkey;
ALTER TABLE personal_api_tokens VALIDATE CONSTRAINT personal_api_tokens_user_id_fkey;
ALTER TABLE pinned_lines VALIDATE CONSTRAINT pinned_lines_user_id_fkey;
ALTER TABLE pinned_operators VALIDATE CONSTRAINT pinned_operators_user_id_fkey;
ALTER TABLE pinned_stations VALIDATE CONSTRAINT pinned_stations_user_id_fkey;
ALTER TABLE push_subscriptions VALIDATE CONSTRAINT push_subscriptions_user_id_fkey;
ALTER TABLE sessions VALIDATE CONSTRAINT sessions_user_id_fkey;
ALTER TABLE tracked_train_tickets VALIDATE CONSTRAINT tracked_train_tickets_user_id_fkey;
ALTER TABLE train_notification_state VALIDATE CONSTRAINT train_notification_state_user_id_fkey;
ALTER TABLE train_subscriptions VALIDATE CONSTRAINT tracked_trains_user_id_fkey;
ALTER TABLE unlisted_links VALIDATE CONSTRAINT unlisted_links_created_by_fkey;
