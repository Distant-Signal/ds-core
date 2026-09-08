-- Generalizes the destination-first search
-- (2026-09-07-train-listing-destination-search-sizing-design.md) into a
-- calling-point-first one
-- (2026-09-08-calling-point-train-search-design.md). See that document's
-- §0 for why this is an additive column + index, not a new table: the
-- table is already one-row-per-departure-bearing-calling-point; only the
-- QUERY's leading equality column changes, from `destination_crs` to
-- `origin_crs` (already exactly "the calling point of this row" -- see
-- `schedule_query::DestinationDeparture`'s own doc comment), plus one new
-- column to carry the schedule's TRUE origin independently of which
-- calling point a row represents.

ALTER TABLE schedule_destination_departures ADD COLUMN true_origin_crs TEXT;

-- New leading index for the new primary query shape: equality on
-- (service_date, origin_crs) -- "calls at this station" -- then a range
-- scan on scheduled, with train_uid as the keyset cursor's tiebreaker.
-- Does NOT replace the existing primary key, which remains required for
-- upsert idempotency (ON CONFLICT DO NOTHING targets it) and still serves
-- destination_crs as a real, if no-longer-leading, filter column.
CREATE INDEX schedule_destination_departures_calling_point_idx
    ON schedule_destination_departures (service_date, origin_crs, scheduled, train_uid);
