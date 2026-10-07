SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- `line_train_summaries`: one slim row per train per catalogue line per
-- service date, derived by `api` from `schedule_line_population` in the
-- same transaction that stores the population
-- (`data::line_train_summaries::upsert_population_with_summaries`;
-- docs/superpowers/specs/2026-10-06-line-page-trains-design.md section 5).
--
-- Why: the line page's summary and the full-day timetable
-- (`/public/lines/{id}/timetable`) need per train only its membership,
-- its time on the line, its public calls at the line's stations and its
-- two bookable ends. Working that out from the population JSONB costs
-- ~0.3-0.6 s of SQL for a main line per request; this table holds the
-- answer, and the timetable pages it by a keyset cursor.
--
-- Rows for a (line_id, service_date) are replaced as a set. A reader uses
-- them only when `derivation` matches its own catalogue fingerprint, and
-- otherwise (no rows yet, an older derivation, a catalogue change) falls
-- back to the population, so the table is a cache: deleting rows is
-- always safe. Pruned by the aggregator with `schedule_line_population`.
--
-- No CHECK constraints on the population's values (scope, direction):
-- a new value from `schedule-reference` must not fail the population's
-- own publish, which shares the transaction.
-- -------------------------------------------------------------------------
CREATE TABLE line_train_summaries (
    line_id         TEXT    NOT NULL,
    service_date    DATE    NOT NULL,
    uid             TEXT    NOT NULL,
    -- Train membership of the line: line | shared | touch; NULL on a
    -- population published before membership existed.
    scope           TEXT,
    -- up | down | loop; NULL as scope.
    direction       TEXT,
    -- lineDue (first public call on the line) as minutes after the
    -- service date's midnight (> 1439 the next morning). NULL when the
    -- train makes no public call on the line.
    due_minute      INTEGER,
    -- Last on-line public arrival, same scale; NULL without on-line calls.
    end_minute      INTEGER,
    operator_atoc   TEXT,
    -- CIF Train Status (one character), the service-mode fallback when
    -- `schedule_services` has no row -- the mode itself is resolved at
    -- read time, as for every other schedule surface.
    train_status    TEXT,
    -- First/last calling point resolving to a bookable CRS.
    origin_crs      TEXT,
    destination_crs TEXT,
    -- [{"crs": "WAT", "minute": 600, "arrival": 600}, ...]: public calls
    -- at the line's catalogue stations, in order (minute = departure, else
    -- arrival; arrival = arrival, else departure).
    on_line_stops   JSONB   NOT NULL DEFAULT '[]',
    -- Does the population carry membership (the same for every row of a
    -- (line_id, service_date)).
    has_scope       BOOLEAN NOT NULL,
    -- `data::line_train_summaries::derivation_fingerprint`: derivation
    -- version and the line's catalogue stations, when the row was written.
    derivation      TEXT    NOT NULL,
    PRIMARY KEY (line_id, service_date, uid)
);

-- The timetable's keyset order (and the summary's window) within one
-- line and date. A new table, so a plain CREATE INDEX holds no lock
-- anyone else is waiting on.
CREATE INDEX line_train_summaries_window_idx
    ON line_train_summaries (line_id, service_date, scope, due_minute, uid);

COMMENT ON TABLE line_train_summaries IS
    'Per (line_id, service_date, uid) slim train summary derived from schedule_line_population at publish; a cache readers fall back from.';
