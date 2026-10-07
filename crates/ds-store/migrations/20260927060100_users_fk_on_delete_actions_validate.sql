SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- Validates the ten `users(id)` foreign keys re-added NOT VALID by
-- 20260927060000_users_fk_on_delete_actions.sql. VALIDATE CONSTRAINT takes
-- SHARE UPDATE EXCLUSIVE on the referencing table (reads and writes carry
-- on) and ROW SHARE on `users`. Every existing row already satisfied the
-- identical FK that constraint replaced (only its ON DELETE action
-- changed), so none of these can fail.
-- -------------------------------------------------------------------------

ALTER TABLE train_subscriptions VALIDATE CONSTRAINT tracked_trains_user_id_fkey;
ALTER TABLE tracked_train_tickets VALIDATE CONSTRAINT tracked_train_tickets_user_id_fkey;
ALTER TABLE journeys VALIDATE CONSTRAINT journeys_user_id_fkey;
ALTER TABLE journey_templates VALIDATE CONSTRAINT journey_templates_user_id_fkey;
ALTER TABLE unlisted_links VALIDATE CONSTRAINT unlisted_links_created_by_fkey;
ALTER TABLE group_invite_links VALIDATE CONSTRAINT group_invite_links_created_by_fkey;
ALTER TABLE group_trains VALIDATE CONSTRAINT group_trains_added_by_fkey;
ALTER TABLE group_journeys VALIDATE CONSTRAINT group_journeys_added_by_fkey;
ALTER TABLE custom_line_group_grants VALIDATE CONSTRAINT custom_line_group_grants_granted_by_fkey;
ALTER TABLE groups VALIDATE CONSTRAINT groups_created_by_fkey;
