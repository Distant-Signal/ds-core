-- -------------------------------------------------------------------------
-- Sharing a private custom line into a group: the line's OWNER grants one
-- or more groups they belong to read access, so fellow members can see the
-- line's definition, live status and disruption history without it ever
-- becoming public. See
-- docs/superpowers/specs/2026-09-12-custom-line-group-sharing-design.md and
-- docs/superpowers/plans/2026-09-15-custom-line-group-sharing.md.
--
-- A join table, not a `group_id` column on `custom_lines`: a household's
-- "my commute" line can be relevant to a "family" group and a "commute
-- buddies" group at the same time, exactly as `group_trains` already
-- models for tracked trains (§2.2).
--
-- Deliberately a SEPARATE table from any future `group_lines`
-- (catalogue/TfL sharing, designed but not built): every row here requires
-- a live membership check on read, and making that a structural fact of
-- the schema is safer than a discipline someone has to remember to apply
-- to a shared table with a `kind` discriminator (§2.1).
--
-- `line_id` carries a REAL foreign key, which `pinned_lines.line_id`
-- deliberately does not. `pinned_lines` has to tolerate free-form,
-- client-supplied ids that were never real lines (a stale pin of a typo),
-- so `delete_custom_line` hand-rolls its cleanup; a grant, by contrast,
-- can only ever be created for an id that really was the caller's own
-- custom line at grant time, so there is no such tolerance requirement and
-- the FK is strictly better. This cascade IS the whole implementation of
-- "the owner deleted the line entirely" -- every grant into every group
-- disappears in the same statement, with zero application code (§2.2).
--
-- `granted_by` is an attribution field, never a permission-bearing one,
-- and so has no ON DELETE CASCADE -- the same shape
-- `group_trains.added_by`, `groups.created_by` and
-- `group_invite_links.created_by` already use, consistent with this app
-- having no user-deletion feature at all (the same latent hazard those
-- three columns already document; not newly introduced here).
--
-- No `revoked_at`: removing a grant is a real DELETE, matching
-- `group_trains`'s hard-delete removal semantics. (`group_invite_links`
-- soft-deletes because a revoked link's history still matters for that
-- feature's own reasoning; nothing here needs a removed grant's history.)
--
-- NOT cleaned up when the granter leaves the group, deliberately diverging
-- from `group_trains`'s departed-member cleanup in
-- `groups::remove_member` -- see design §2.7 and the comment beside that
-- cleanup. Leaving a group is not an event that touches
-- `custom_lines.user_id` at all, and the owner retains three independent
-- ways to revoke at any time.
-- -------------------------------------------------------------------------

CREATE TABLE custom_line_group_grants (
    group_id    TEXT NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    line_id     TEXT NOT NULL REFERENCES custom_lines(id) ON DELETE CASCADE,
    granted_by  TEXT NOT NULL REFERENCES users(id),
    granted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (group_id, line_id)
);

-- "Which groups is this custom line shared into" -- read by the owner's own
-- /lines/{id} edit page ("Shared with: Family, Commute Club"). The PK's
-- leading column (group_id) doesn't cover this, the same reason
-- group_trains_train_subscription_id and group_members_user_id exist.
CREATE INDEX custom_line_group_grants_line_id ON custom_line_group_grants (line_id);
