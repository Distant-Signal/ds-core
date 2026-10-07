-- ---------------------------------------------------------------------
-- One row per (line_id, service_date): a shadow-computed line's full CIF
-- SCHEDULE population for one rail day, published by schedule-reference
-- (POST, its own existing writer credential) and read by
-- full-coverage-consumer (GET, a new credential) to build its in-memory
-- correlation index. `population` is opaque JSONB here -- a
-- Vec<schedule_query::LinePopulationEntry> -- `api` never deserializes
-- it, only stores/relays it, the same "opaque blob" posture
-- station_full_coverage_samples.stats already established for a
-- different table. See
-- docs/superpowers/specs/2026-09-04-option-b-live-consumer-design.md
-- Decision 2a/2b and
-- docs/superpowers/plans/2026-09-04-option-b-live-consumer-plan.md Task 5.
-- ---------------------------------------------------------------------

CREATE TABLE schedule_line_population (
    line_id      TEXT        NOT NULL,
    service_date DATE        NOT NULL,
    population   JSONB       NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (line_id, service_date)
);
