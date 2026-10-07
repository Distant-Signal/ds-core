-- ---------------------------------------------------------------------
-- One row per (crs, service_date): a station's next-10, `now`-forward-
-- filtered, CIF-SCHEDULE-derived departures for one rail day, published
-- by schedule-reference (POST, its own existing writer credential -- the
-- same `internal_oauth_group_schedule_reference` /stanox-crs and
-- /schedule-line-population already use) and read by `api` itself
-- directly, via a public passthrough route -- unlike
-- schedule_line_population, there is no second private GET pair: the only
-- reader is this same crate's own SQL. `departures` is opaque JSONB here
-- -- a Vec<schedule_query::ScheduleDeparture> -- `api` never deserializes
-- it into that Rust type, only stores/relays it, same "opaque blob"
-- posture schedule_line_population.population already established. See
-- docs/superpowers/specs/2026-09-04-whole-network-trip-search-design.md
-- Decision 1 and
-- docs/superpowers/plans/2026-09-04-whole-network-trip-search-plan.md
-- Task 2.
-- ---------------------------------------------------------------------

CREATE TABLE schedule_network_departures (
    crs          TEXT        NOT NULL,
    service_date DATE        NOT NULL,
    departures   JSONB       NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (crs, service_date)
);
