SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- The internal-read views (ingest architecture spec R2 and §11.2, plan
-- 4.1): what trust-consumer and poller-ldbws read directly once their
-- `*_SOURCE` is `db`, instead of the api's /private/tracked-trains and
-- /private/sample-stations. Each exposes only what its reader needs and no
-- user id or owner column, so a reader role granted SELECT on the view
-- alone (files/db-grants.yaml) never sees whose subscription, pin or custom
-- line a row is. Ordinary views (no security_invoker): they run with the
-- owner's privileges, so SELECT on the view is the whole grant.
--
-- They are part of the migration-defined interface: change them only by
-- expand/contract, like a table (spec §11.2).
--
-- Expand only: nothing reads them until a reader's source is switched to
-- `db` (default `http`).
-- -------------------------------------------------------------------------

-- trust-consumer's reference set: ds_store::tracking::list_active_tracked_trains's
-- SELECT, unchanged (its doc explains each condition). `id` is the
-- subscription's own id, which trust-consumer keys its index on, not a user.
CREATE VIEW ingest_active_tracked_trains AS
SELECT tt.id, tt.service_date, tt.pin_origin_crs, tt.pin_scheduled_departure,
       tt.resolution_status, tr.train_uid, tr.train_id, tt.trains_id,
       tr.destination_crs
FROM train_subscriptions tt
LEFT JOIN trains tr ON tr.id = tt.trains_id
LEFT JOIN train_current_state cs ON cs.trains_id = tt.trains_id
WHERE tt.resolution_status != 'unresolved'
  AND tt.service_date >= CURRENT_DATE - INTERVAL '2 days'
  AND tt.service_date <= CURRENT_DATE + INTERVAL '1 day'
  AND (cs.status IS NULL OR cs.status NOT IN ('completed', 'cancelled'))
  AND NOT EXISTS (
      SELECT 1 FROM schedule_services ss
      WHERE ss.service_date = tr.service_date AND ss.uid = tr.train_uid
        AND ss.mode <> 'train');

-- poller-ldbws's sample-station priority (LEG-18): how many users pinned
-- each line, counts only (the api's preferences::count_pins_per_line).
CREATE VIEW ingest_sample_station_pins AS
SELECT line_id, COUNT(*) AS pins
FROM pinned_lines
GROUP BY line_id;

-- poller-ldbws's custom lines: each custom line's id and station list (its
-- sample stations), without its owner, name or anything else.
CREATE VIEW ingest_custom_line_stations AS
SELECT id, stations
FROM custom_lines;
