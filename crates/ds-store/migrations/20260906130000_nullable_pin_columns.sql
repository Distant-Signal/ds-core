-- An NR-primary subscription (Task 20) may point at a bare train_uid with
-- no schedule match yet (the design spec's own §1 accepted gap) -- there
-- is no pin_origin_crs/pin_scheduled_departure to store in that case.
-- These two columns were NOT NULL only because every subscription used to
-- be created via the legacy CRS+time pin flow, which always supplies them
-- (validate_pin enforces that upstream, unchanged). Relaxing them here is
-- additive/safe: no existing row has a NULL value in either column today.
ALTER TABLE tracked_trains ALTER COLUMN pin_origin_crs DROP NOT NULL;
ALTER TABLE tracked_trains ALTER COLUMN pin_scheduled_departure DROP NOT NULL;
