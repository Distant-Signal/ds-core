-- Final, purely cosmetic step of
-- docs/superpowers/specs/2026-09-06-shared-train-identity-design.md §1/§2 --
-- every functional change in this plan has already landed and been
-- verified by this point (Tasks 1-23). Index renames are optional/cosmetic
-- (Postgres does not require them -- a foreign key `REFERENCES
-- tracked_trains(id)` from another table follows a table rename
-- automatically, by OID, with no SQL text change needed anywhere else),
-- included here anyway for one consistent naming convention across the
-- whole schema.
ALTER TABLE tracked_trains RENAME TO train_subscriptions;
ALTER INDEX tracked_trains_user_id RENAME TO train_subscriptions_user_id;
ALTER INDEX tracked_trains_trains_id RENAME TO train_subscriptions_trains_id;
ALTER INDEX tracked_trains_resolution_status RENAME TO train_subscriptions_resolution_status;
