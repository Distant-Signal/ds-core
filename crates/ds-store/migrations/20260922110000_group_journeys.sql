-- -------------------------------------------------------------------------
-- Sharing a journey into a group: a near-verbatim copy of group_trains's
-- own shape (20260911090000_shared_groups.sql), for the same reason --
-- one journey can be shared into more than one group at once, so this is
-- a JOIN TABLE, not a `group_id` column on `journeys` itself. See
-- docs/superpowers/specs/2026-09-22-journey-tracking-design.md §6.
--
-- Departed-member cleanup: mirrors group_trains (cascade -- see
-- groups::remove_member's own extended cleanup), NOT
-- custom_line_group_grants (which deliberately persists after the
-- granter leaves). Spec §6's own reasoning: "a journey is an active,
-- live-tracked personal thing, not a static definition" -- same category
-- as a tracked train, not a custom line.
--
-- View access to a shared journey's full live detail (every leg's bound
-- train_subscriptions row) is NOT gated by this table alone -- see
-- journeys::journey_readable_by (crates/api/src/data/journeys.rs), the
-- new authorization path this table's existence makes possible but which
-- lives in its own function, not a view or a second table.
-- -------------------------------------------------------------------------

CREATE TABLE group_journeys (
    group_id   TEXT   NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    journey_id BIGINT NOT NULL REFERENCES journeys(id) ON DELETE CASCADE,
    added_by   TEXT   NOT NULL REFERENCES users(id),
    added_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (group_id, journey_id)
);

-- "Which groups is this journey shared into" -- needed by
-- journey_readable_by's membership check and by a future owner-facing
-- "shared with: Family, Commute Club" indicator (not built in this
-- phase -- see this plan's Non-goals). The PK's leading column (group_id)
-- doesn't cover this, the same reason group_trains_train_subscription_id
-- and custom_line_group_grants_line_id exist.
CREATE INDEX group_journeys_journey_id ON group_journeys (journey_id);
