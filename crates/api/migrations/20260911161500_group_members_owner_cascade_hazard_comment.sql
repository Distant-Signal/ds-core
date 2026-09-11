-- Documents, in the schema itself, a latent hazard already noted in Rust
-- on `groups::get_group_detail` (crates/api/src/data/groups.rs): this
-- column's `ON DELETE CASCADE` removes an owner's own membership row
-- directly if their `users` row is ever deleted, bypassing
-- `remove_member`'s ownership-transfer/group-deletion logic entirely --
-- `get_group_detail`'s inner join on `role = 'owner'` then finds no owner
-- row and 404s for every remaining member forever (an
-- unreadable-but-not-deleted group). No user-deletion feature exists
-- today, so this is latent, not an active bug; a future one should either
-- run `remove_member`-equivalent logic before deleting the user, or
-- otherwise repair/reassign ownership as part of that deletion.
--
-- Additive only (a native Postgres `COMMENT ON`, not a rewrite of
-- 20260911090000_shared_groups.sql, which is already applied in
-- production -- see that migration's own immutability, per this
-- session's finding after an earlier attempt to edit it directly).
COMMENT ON COLUMN group_members.user_id IS
    'ON DELETE CASCADE removes the owner''s own membership row directly '
    'if their users row is ever deleted, bypassing remove_member''s '
    'ownership-transfer/group-deletion logic. See groups::get_group_detail''s '
    'doc comment for the full hazard this creates. Latent only -- no '
    'user-deletion feature exists today.';
