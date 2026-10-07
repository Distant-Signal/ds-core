-- Reinstates the index reverted in commit 86c5058 (an earlier, less-
-- justified attempt at this same fix, made before real performance
-- analysis existed) -- this time for a verified, quantified reason found
-- by this plan's own final whole-branch review: neither existing index on
-- schedule_destination_departures leads with train_uid, so
-- reconciliation::true_origin_departure's (service_date, train_uid) lookup
-- forces a filtered scan of up to one whole rail day's ~377k rows per
-- candidate per sweep tick. Also benefits trains::is_known_scheduled_train's
-- identical (train_uid, service_date) existence probe.
-- IF NOT EXISTS: an earlier, reverted attempt at this same index
-- (86c5058) left the physical index in place on some already-migrated
-- databases (its own commit only reverted the migration FILE, deliberately
-- not the index object, to avoid an unnecessary DDL against a database
-- other concurrent work might share -- see 86c5058's own history). This
-- migration is the first to apply cleanly on a fresh database and a no-op
-- on one of those already-carrying-the-leftover-index.
CREATE INDEX IF NOT EXISTS schedule_destination_departures_train_uid_idx
    ON schedule_destination_departures (service_date, train_uid);
