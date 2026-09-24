-- -------------------------------------------------------------------------
-- Unlisted links: a generic, reusable share-link primitive. One opaque,
-- high-entropy token resolves to one (resource_type, resource_id) pair --
-- polymorphic by a bare string discriminator rather than a typed foreign
-- key, since genericity across future resource types is the whole point
-- (a future feature adds its own resource_type string and calls the
-- functions in crates/api/src/data/unlisted_links.rs; no schema change).
-- See docs/superpowers/specs/2026-09-23-unlisted-links-design.md.
--
-- Deliberately NOT a retrofit of group_invite_links
-- (20260911090000_shared_groups.sql) -- that table stays exactly as it
-- is; this is a new, independent table. See the design doc's own
-- reasoning (§3) for why: group_invite_links carries a group-specific
-- side effect (consume_invite_link's membership insert) with no generic
-- equivalent, and its expires_at is NOT NULL with a hard 7-day TTL by
-- design, which this table's per-resource-type-optional expires_at would
-- either lose or need a conditional CHECK for.
--
-- expires_at is NULLABLE, unlike group_invite_links.expires_at -- whether
-- a resource type forces a TTL is that resource type's own call, made by
-- passing Some(ttl)/None to create_link/rotate_link. Journeys pass None
-- (design doc §5): a journey's link grants read-only access to one
-- already-bounded resource, not an ever-growing membership boundary, so
-- explicit revoke/regenerate are its only two owner-facing levers.
--
-- No ON DELETE CASCADE on resource_id -- it isn't a real foreign key (it
-- can't be: it points at a different table per resource_type). A
-- dangling row after its resource is deleted is inert, not unsafe: the
-- resource-specific resolution step (e.g. looking up a journey by the id
-- this row names) simply fails to find anything and 404s, the same
-- outcome an invalid/expired/revoked token already produces. Matches
-- custom_line_group_grants.granted_by's own accepted un-cascaded
-- attribution column, generalized.
-- -------------------------------------------------------------------------

CREATE TABLE unlisted_links (
    token         TEXT PRIMARY KEY,
    resource_type TEXT NOT NULL,
    resource_id   TEXT NOT NULL,
    created_by    TEXT NOT NULL REFERENCES users(id),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at    TIMESTAMPTZ,
    revoked_at    TIMESTAMPTZ
);

-- "The active link for this resource" (rotate/revoke/get_active) is
-- always looked up by (resource_type, resource_id) first -- the PK's
-- leading column (token) doesn't cover this, the same reason
-- group_invite_links_group_id exists.
CREATE INDEX unlisted_links_resource ON unlisted_links (resource_type, resource_id);
