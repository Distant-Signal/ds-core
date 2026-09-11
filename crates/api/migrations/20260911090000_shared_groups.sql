-- -------------------------------------------------------------------------
-- Shared groups: named groups with join-link-based membership. A tracked
-- train added to a group becomes visible to other group members (custom
-- name + live status, never tickets). See
-- docs/superpowers/specs/2026-09-11-shared-groups-design.md.
--
-- Four tables, not three:
--   groups              -- one row per group. `id` is a short random id,
--                          the same shape as auth::generate_session_token()
--                          (base64url of 32 random bytes), doubling as a
--                          non-guessable URL identifier.
--   group_members        -- membership + role. Three tiers, not two: the
--                          creator is a PERMANENT `owner`, inserted in the
--                          same transaction as the group -- never
--                          removable/demotable by a co-`admin` (§2.1).
--   group_trains          -- a JOIN TABLE (not a `group_id` FK on
--                          train_subscriptions) so one train can be shared
--                          into more than one group at once (§2.2). This
--                          repo already paid the cost of under-modeling an
--                          analogous relationship once for
--                          tracked_train_tickets
--                          (20260901140000_standalone_tickets.sql) -- a
--                          join table avoids repeating that here.
--   group_invite_links     -- reusable, rotatable join links. `token` reuses
--                          auth::generate_session_token()'s own opaque,
--                          high-entropy shape directly as the primary key
--                          (unlike sessions.id, this is NOT hashed --
--                          the token doubles as a bearer capability meant
--                          to be shared verbatim via a URL, and a group's
--                          own membership list is the actual access-control
--                          boundary once someone has joined, not secrecy of
--                          this table's contents).
-- -------------------------------------------------------------------------

CREATE TABLE groups (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    created_by  TEXT NOT NULL REFERENCES users(id),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE group_members (
    group_id    TEXT NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    user_id     TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role        TEXT NOT NULL DEFAULT 'member' CHECK (role IN ('owner', 'admin', 'member')),
    joined_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (group_id, user_id)
);

-- "Which groups is user X in" (GET /groups) -- the PK's leading column
-- (group_id) doesn't cover this.
CREATE INDEX group_members_user_id ON group_members (user_id);

CREATE TABLE group_trains (
    group_id               TEXT   NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    train_subscription_id  BIGINT NOT NULL REFERENCES train_subscriptions(id) ON DELETE CASCADE,
    added_by               TEXT   NOT NULL REFERENCES users(id),
    added_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (group_id, train_subscription_id)
);

-- Untracking a train removes it from every group it was shared into
-- immediately, automatically, with no application code needed -- the same
-- cascade pattern already used for train_movement_events/train_current_state.
-- (This comment documents the FK above; ON DELETE CASCADE is already part
-- of the column definition.)

CREATE INDEX group_trains_train_subscription_id ON group_trains (train_subscription_id);

CREATE TABLE group_invite_links (
    token       TEXT PRIMARY KEY,
    group_id    TEXT NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    created_by  TEXT NOT NULL REFERENCES users(id),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMPTZ NOT NULL,
    revoked_at  TIMESTAMPTZ
);

-- "The group's currently active link" (GET /groups/{id}, POST rotate,
-- DELETE revoke) is always looked up by group_id first.
CREATE INDEX group_invite_links_group_id ON group_invite_links (group_id);
