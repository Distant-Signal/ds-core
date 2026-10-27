SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- train_movement_events.platform and trust_event_backlog.platform: the
-- platform a TRUST movement report says the train was at (DS-180).
--
-- Set only for an AUTOMATIC (SMART, train describer berth) ARRIVAL or
-- DEPARTURE with a non-blank platform; see
-- `trust_schema::schema::movement_reported_platform`. A MANUAL report, a
-- pass, and every row written before this column existed are NULL:
-- raw_body has always been stored as `{}`, so there is nothing to
-- backfill from. The api serves it as the stop's confirmed platform
-- (`platformBasis: "confirmed"`).
--
-- Nullable, no default, catalog-only.
-- -------------------------------------------------------------------------
ALTER TABLE train_movement_events ADD COLUMN platform TEXT;
ALTER TABLE trust_event_backlog ADD COLUMN platform TEXT;
